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
#              /app/config/{staging,prod}/st0x-hedge.toml; the compose
#              command appends --config <baked path> --secrets <mounted
#              Secret Manager file>. database_url in the configs stays
#              sqlite:///mnt/data/st0x-hedge.db (the VM mounts its data
#              disk there — byte-identical to the droplet).
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
    mkdir -p $out/app/config/staging $out/app/config/prod
    cp ${../config/staging/st0x-hedge.toml} $out/app/config/staging/st0x-hedge.toml
    cp ${../config/prod/st0x-hedge.toml} $out/app/config/prod/st0x-hedge.toml
  '';

  dashboardRoot = pkgs.runCommand "st0x-dashboard-root" { } ''
    mkdir -p $out/usr/share
    cp -r ${st0x-dashboard} $out/usr/share/st0x-dashboard
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

      server {
        listen 80;
        root /usr/share/st0x-dashboard;

        location / {
          try_files $uri $uri/ /index.html;
        }

        location /api/ws {
          proxy_pass http://bot:8001/api/ws;
          proxy_http_version 1.1;
          proxy_set_header Upgrade $http_upgrade;
          proxy_set_header Connection "upgrade";
          proxy_connect_timeout 60;
          proxy_send_timeout 60;
          proxy_read_timeout 86400;
        }

        location /health      { proxy_pass http://bot:8001/health; }
        location /logs        { proxy_pass http://bot:8001/logs; }
        location /orders/     { proxy_pass http://bot:8001/orders/; }
        location /pnl         { proxy_pass http://bot:8001/pnl; }
        location /trades      { proxy_pass http://bot:8001/trades; }
        location /transfers   { proxy_pass http://bot:8001/transfers; }
        location /performance { proxy_pass http://bot:8001/performance; }
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
