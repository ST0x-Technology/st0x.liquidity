# OCI images for the GCP staging VM (t0.devops terraform/staging-liquidity).
# Built by the flake (dockerTools, no Dockerfile, no base image); `created` is
# pinned so the same commit rebuilds to the same digest — the convention
# st0x.bebop established for its Cloud Run image. Pushed + attested to the
# t0-liquidity Artifact Registry repo in t0-artifacts by
# .github/workflows/build-oci.yml (master only).
#
# Image contract (consumed by the stack's docker-compose in t0.devops —
# keep the two in sync):
#   bot:       Entrypoint = the `server` binary; the compose command mounts
#              the config from Secret Manager and appends --config
#              /run/t0-config/st0x-hedge.toml --secrets <mounted Secret
#              Manager file>. database_url in the config stays
#              sqlite:///mnt/data/st0x-hedge.db (the VM mounts its data
#              disk there, byte-identical to the droplet). Also ships
#              /bin/st0x-cli for operators (docker exec into the running
#              bot container, same config/secrets/DB and the container's
#              ambient identity for keyless signing; see docs/cli-ops.md).
#   datasette: the nixpkgs datasette (same package the droplet runs), NOT
#              the upstream Docker image -- that one's bundled SQLite is too
#              old to parse the schema's STRICT table (dashboard_trade_delivery)
#              and dies with "malformed database schema".
#   dashboard: nginx in front of the IAP load balancer. It proxies the ops
#              API prefixes (/liquidity-read, /liquidity-debug,
#              /liquidity-capital) to the compose service name `bot` on
#              :8001, and answers / with a page that sends people to the
#              Grafana liquidity board, which replaced the SvelteKit
#              dashboard. The load balancer's health check reads /, so it
#              must stay a 200.
{
  pkgs,
  st0x-liquidity,
  st0x-cli,
}:

let
  # The ops API prefixes, addressed to the compose service `bot` (the two
  # containers share the compose default network). Logs to stdout/stderr
  # for the gcplogs driver.
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
        set $bot_upstream "bot:8001";

        # The SvelteKit dashboard this image served is gone; the Grafana
        # board replaced it. A bookmark lands here, and so does the load
        # balancer's health check, which needs a 200. no-store, so an IAP
        # session that expired re-runs its sign-in on the next visit.
        location = / {
          default_type text/html;
          add_header Cache-Control "no-store" always;
          return 200 '<!doctype html><meta charset="utf-8"><meta http-equiv="refresh" content="0; url=https://grafana.t0trade.com/d/t0-liquidity/liquidity-bot"><title>Liquidity bot</title><p>The liquidity dashboard moved to Grafana: <a href="https://grafana.t0trade.com/d/t0-liquidity/liquidity-bot">Liquidity bot</a>.</p>';
        }

        # NO URI after $bot_upstream in the locations below. nginx normally
        # replaces the matched location prefix with the proxy_pass URI, but
        # when proxy_pass contains a VARIABLE that rewriting is skipped and
        # the literal URI is sent instead of the request path, so a URI part
        # would send every request to the same path. With no URI part, nginx
        # forwards the original request URI unchanged.
        #
        # Role-gated ops API. The load balancer sends each of these prefixes
        # to its own IAP backend, and the bot verifies the assertion and pins
        # the audience per prefix before running the handler. So the method
        # restriction here is not the thing keeping mutations safe -- it is a
        # second, cheaper fence that keeps a prefix from carrying verbs its
        # role never intended. Every other path is a 404.
        #
        # /liquidity-debug and /liquidity-capital are the only locations in
        # this file that admit POST, one per mutation tier: debug repairs
        # state, capital starts new fund movements, and each has its own IAP
        # backend and pinned audience. Each stays a single exact-ish prefix
        # for that reason: widening one, or adding another writable location,
        # hands the method past the point where the audience check can
        # distinguish roles. The bot does not mount /liquidity-capital until
        # its capital_audience is configured, so before then this location
        # forwards to a 404.
        location /liquidity-read/    { limit_except GET  { deny all; } proxy_pass http://$bot_upstream; }
        location /liquidity-debug/   { limit_except POST { deny all; } proxy_pass http://$bot_upstream; }
        location /liquidity-capital/ { limit_except POST { deny all; } proxy_pass http://$bot_upstream; }

        location / { return 404; }
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
      # Operator CLI (bin/st0x-cli): run via `docker exec` against the same
      # mounted config/secrets and the live /mnt/data database, signing
      # keylessly through the container's ambient service-account identity
      # (same loader as the server). Not the entrypoint; the server stays
      # the only long-running process.
      st0x-cli
      pkgs.cacert
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

  # Runs the dashboard image's nginx config and probes it, so a slip in the
  # inline page or a location fails CI, not the staging roll: the load
  # balancer's health check needs `/` to answer 200, and the ops API needs
  # its method fences. The bot upstream is not running here, so no request
  # reaches it.
  dashboard-nginx-check =
    pkgs.runCommand "dashboard-nginx-check"
      {
        nativeBuildInputs = [
          pkgs.nginx
          pkgs.curl
        ];
      }
      ''
        sed -e "s#/tmp/#$TMPDIR/#g" -e 's#listen 80;#listen 127.0.0.1:18080;#' \
          -e 's#^user nobody;##' ${nginxConf} > nginx.conf
        nginx -t -c "$PWD/nginx.conf" -p "$TMPDIR" -e /dev/stderr
        nginx -c "$PWD/nginx.conf" -p "$TMPDIR" -e /dev/stderr
        trap 'nginx -c "$PWD/nginx.conf" -p "$TMPDIR" -e /dev/stderr -s stop' EXIT
        for _ in $(seq 50); do
          curl -s -o /dev/null http://127.0.0.1:18080/ && break
          sleep 0.1
        done
        check() {
          got=$(curl -s -o /dev/null -w '%{http_code}' -X "$1" "http://127.0.0.1:18080$2")
          [ "$got" = "$3" ] || { echo "$1 $2: expected $3, got $got" >&2; exit 1; }
        }
        check GET / 200
        curl -s -D - -o /dev/null http://127.0.0.1:18080/ | grep -qi '^cache-control: no-store'
        curl -s http://127.0.0.1:18080/ | grep -q 'grafana.t0trade.com/d/t0-liquidity'
        check GET /pnl 404
        check GET /api/ws 404
        check POST /liquidity-read/x 403
        check GET /liquidity-debug/x 403
        check GET /liquidity-capital/x 403
        touch "$out"
      '';

  dashboard-oci = pkgs.dockerTools.streamLayeredImage {
    name = "t0-liquidity-dashboard";
    tag = "latest";
    created = "1970-01-01T00:00:01Z";
    contents = [
      pkgs.nginx
      pkgs.dockerTools.fakeNss
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
