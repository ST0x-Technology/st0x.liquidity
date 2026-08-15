# OCI images for the GCP staging VM (t0.devops terraform/staging-liquidity).
# Built by the flake (dockerTools, no Dockerfile, no base image); `created` is
# pinned so the same commit rebuilds to the same digest — the convention
# st0x.bebop established for its Cloud Run image. Pushed + attested to the
# t0-liquidity Artifact Registry repo in t0-artifacts by
# .github/workflows/build-oci.yml (master only).
#
# Image contract (consumed by the stack's docker-compose in t0.devops —
# keep the two in sync):
#   bot:       Entrypoint = the `server` binary; env configs baked at
#              /app/config/{staging,staging-gcp,prod,prod-gcp}/st0x-hedge.toml; the compose
#              command appends --config <baked path> --secrets <mounted
#              Secret Manager file>. database_url in the configs stays
#              sqlite:///mnt/data/st0x-hedge.db (the VM mounts its data
#              disk there — byte-identical to the droplet).
#   datasette: the nixpkgs datasette (same package the droplet runs), NOT
#              the upstream Docker image -- that one's bundled SQLite is too
#              old to parse the schema's STRICT table (dashboard_trade_delivery)
#              and dies with "malformed database schema".
#   dashboard: nginx serving the SvelteKit build, proxying the API paths to
#              the compose service name `bot` on :8001 (same location list
#              the droplet's os.nix nginx serves, minus TLS — the VM has no
#              tailnet; humans reach it over an IAP tunnel).
{
  pkgs,
  st0x-liquidity,
  st0x-dashboard,
}:

let
  bakedConfigs = pkgs.runCommand "st0x-hedge-configs" { } ''
    mkdir -p $out/app/config/staging $out/app/config/staging-gcp $out/app/config/prod $out/app/config/prod-gcp
    cp ${../config/staging/st0x-hedge.toml} $out/app/config/staging/st0x-hedge.toml
    # The GCP VM runs this variant: kms_api_key (keyless Turnkey auth, so
    # the secrets file has no [wallet]) and no [telemetry]. #1182 added the
    # file but not this line, so the image shipped without the directory
    # and the bot crash-looped on "failed to read config file" at cutover.
    cp ${../config/staging-gcp/st0x-hedge.toml} $out/app/config/staging-gcp/st0x-hedge.toml
    cp ${../config/prod/st0x-hedge.toml} $out/app/config/prod/st0x-hedge.toml
    # The GCP production VM's variant, same shape as staging-gcp: kms_api_key
    # (keyless Turnkey, no [wallet] secret) and no [telemetry]. The plain
    # prod config remains the droplet's. Baked HERE, not just committed --
    # the staging cutover crash-looped on exactly that omission (#1182/#1190).
    cp ${../config/prod-gcp/st0x-hedge.toml} $out/app/config/prod-gcp/st0x-hedge.toml
  '';

  dashboardRoot = pkgs.runCommand "st0x-dashboard-root" { } ''
    mkdir -p $out/usr/share
    cp -r ${st0x-dashboard} $out/usr/share/t0-liquidity-dashboard
  '';

  # Same proxy surface as os.nix's virtualHost locations, addressed to the
  # compose service `bot` instead of 127.0.0.1 (the two containers share the
  # compose default network). Logs to stdout/stderr for the gcplogs driver.
  nginxConf = pkgs.writeText "nginx.conf" ''
    user nobody;
    worker_processes 1;
    error_log /dev/stderr warn;
    pid /tmp/nginx.pid;

    events { worker_connections 1024; }

    http {
      include ${pkgs.nginx}/conf/mime.types;
      default_type application/octet-stream;
      access_log /dev/stdout;

      client_body_temp_path /tmp/client_body;
      proxy_temp_path /tmp/proxy;
      fastcgi_temp_path /tmp/fastcgi;
      uwsgi_temp_path /tmp/uwsgi;
      scgi_temp_path /tmp/scgi;

      # Docker's embedded DNS. Without an explicit resolver + variable
      # upstream, nginx resolves `bot` at CONFIG LOAD and exits
      # "host not found in upstream" whenever the bot container is not
      # already up -- which is every restart where the bot is slower, and
      # every moment the workload is gated off.
      resolver 127.0.0.11 valid=10s ipv6=off;

      server {
        listen 80;
        root /usr/share/t0-liquidity-dashboard;
        set $bot_upstream "bot:8001";

        location / {
          try_files $uri $uri/ /index.html;
        }

        # NO URI after $bot_upstream, in any of these. nginx normally
        # replaces the matched location prefix with the proxy_pass URI, but
        # when proxy_pass contains a VARIABLE that rewriting is skipped and
        # the literal URI is sent instead of the request path. So
        # `proxy_pass http://$bot_upstream/orders/` sent every /orders/*
        # request to the bot as plain `/orders/` -- 404 for /orders/pending
        # and /orders/raindex (the whole Orders tab), and 404 for all four
        # /performance/* endpoints. Worse than the 404s: /transfers/interrupted
        # and /trades/{venue}/{id}/events silently returned the plain
        # /transfers and /trades lists, a wrong answer with a 200.
        # With no URI part, nginx forwards the original request URI unchanged.
        # Every location is GET-only (limit_except GET also admits HEAD).
        # The dashboard is a read surface; the bot's /transfers/resume and
        # /transfers/recheck/{kind}/{id} are POST mutation endpoints that
        # src/api.rs marks TODO(auth) -- unauthenticated by design until the
        # role-gated API work, so they must stay unreachable through any
        # exposed surface. Forwarding paths correctly (the fix above) would
        # otherwise have exposed them here. Operators invoke them via an IAP
        # tunnel to the bot port directly; this proxy refuses the method.
        location /api/ws {
          limit_except GET { deny all; }
          proxy_pass http://$bot_upstream;
          proxy_http_version 1.1;
          proxy_set_header Upgrade $http_upgrade;
          proxy_set_header Connection "upgrade";
          proxy_connect_timeout 60;
          proxy_send_timeout 60;
          proxy_read_timeout 86400;
        }

        location /health      { limit_except GET { deny all; } proxy_pass http://$bot_upstream; }
        location /logs        { limit_except GET { deny all; } proxy_pass http://$bot_upstream; }
        location /orders/     { limit_except GET { deny all; } proxy_pass http://$bot_upstream; }
        location /pnl         { limit_except GET { deny all; } proxy_pass http://$bot_upstream; }
        location /trades      { limit_except GET { deny all; } proxy_pass http://$bot_upstream; }
        location /transfers   { limit_except GET { deny all; } proxy_pass http://$bot_upstream; }
        location /performance { limit_except GET { deny all; } proxy_pass http://$bot_upstream; }
      }
    }
  '';
in
{
  bot-oci = pkgs.dockerTools.streamLayeredImage {
    name = "t0-liquidity";
    tag = "latest";
    created = "1970-01-01T00:00:01Z";
    contents = [
      st0x-liquidity
      pkgs.cacert
      bakedConfigs
    ];
    config = {
      Entrypoint = [ "${st0x-liquidity}/bin/server" ];
      Env = [
        "SSL_CERT_FILE=/etc/ssl/certs/ca-bundle.crt"
      ];
      ExposedPorts = {
        "8001/tcp" = { };
        "8002/tcp" = { };
      };
    };
  };

  # Waits for the database before exec'ing: it only appears at cutover (copied
  # from the droplet), and datasette exits immediately on a missing file.
  datasette-oci = pkgs.dockerTools.streamLayeredImage {
    name = "t0-liquidity-datasette";
    tag = "latest";
    created = "1970-01-01T00:00:01Z";
    contents = [
      pkgs.datasette
      pkgs.busybox
      pkgs.cacert
    ];
    config = {
      Entrypoint = [
        "/bin/sh"
        "-c"
      ];
      Cmd = [
        (
          "until [ -f /mnt/data/st0x-hedge.db ]; do "
          + "echo 'waiting for /mnt/data/st0x-hedge.db'; sleep 10; done; "
          + "exec ${pkgs.datasette}/bin/datasette serve /mnt/data/st0x-hedge.db "
          + "--host 0.0.0.0 --port 8081 --setting sql_time_limit_ms 5000"
        )
      ];
      ExposedPorts."8081/tcp" = { };
    };
  };

  dashboard-oci = pkgs.dockerTools.streamLayeredImage {
    name = "t0-liquidity-dashboard";
    tag = "latest";
    created = "1970-01-01T00:00:01Z";
    contents = [
      pkgs.nginx
      pkgs.dockerTools.fakeNss
      dashboardRoot
    ];
    extraCommands = ''
      mkdir -p tmp var/log/nginx var/cache/nginx
    '';
    config = {
      Cmd = [
        "${pkgs.nginx}/bin/nginx"
        "-c"
        "${nginxConf}"
        "-g"
        "daemon off;"
      ];
      ExposedPorts."80/tcp" = { };
    };
  };
}
