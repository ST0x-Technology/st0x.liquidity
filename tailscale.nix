{
  pkgs,
  environment,
  tailscaleMagicDnsName,
  ...
}:

let
  certDir = "/var/lib/tailscale-cert";
in
{
  # Per-environment reusable, tagged auth key. Used only on first
  # enrollment — after that Tailscale re-authenticates via the stored
  # node key in /var/lib/tailscale. To rotate the node identity
  # (e.g. re-tag), run `tailscale up --force-reauth --auth-key ...`
  # manually on the droplet.
  services.tailscale = {
    enable = true;
    authKeyFile = "/run/agenix/tailscale-authkey-${environment}";
    permitCertUid = "nginx";
  };

  networking.firewall = {
    allowedUDPPorts = [
      41641 # Tailscale WireGuard
    ];
    trustedInterfaces = [ "tailscale0" ];
  };

  age.secrets."tailscale-authkey-${environment}" = {
    file = ./secret/tailscale-authkey-${environment}.age;
    mode = "0400";
  };

  systemd = {
    tmpfiles.rules = [
      "d ${certDir} 0750 nginx nginx -"
    ];

    services = {
      # Clean up stale TUN device before tailscaled starts. During NixOS
      # activation the old tailscaled may still hold /dev/net/tun when the
      # new unit starts, causing a crash-loop.
      tailscaled.serviceConfig.ExecStartPre = [ "-${pkgs.iproute2}/bin/ip link delete tailscale0" ];

      # Provision a Tailscale-issued TLS certificate so the dashboard is
      # served over HTTPS. Runs before nginx to guarantee cert files exist.
      tailscale-cert = {
        description = "Provision Tailscale HTTPS certificate for ${tailscaleMagicDnsName}";
        after = [ "tailscaled.service" ];
        wants = [ "tailscaled.service" ];
        before = [ "nginx.service" ];
        wantedBy = [ "multi-user.target" ];
        path = [
          pkgs.tailscale
          pkgs.jq
        ];
        serviceConfig = {
          Type = "oneshot";
          RemainAfterExit = true;
          User = "nginx";
          Group = "nginx";
          # Reload nginx so it picks up renewed certs on subsequent runs
          # triggered by the timer. || true keeps the unit green on first
          # boot when nginx hasn't started yet.
          ExecStartPost = "+${pkgs.bash}/bin/bash -c 'systemctl is-active --quiet nginx.service && systemctl reload nginx.service || true'";
        };
        script = ''
          set -euo pipefail
          retries=0
          until [ "$(tailscale status --json 2>/dev/null | jq -r '.BackendState' 2>/dev/null)" = "Running" ]; do
            retries=$((retries + 1))
            if [ "$retries" -ge 30 ]; then
              echo "Tailscale BackendState != Running after 60s" >&2
              exit 1
            fi
            sleep 2
          done
          tailscale cert \
            --cert-file ${certDir}/${tailscaleMagicDnsName}.crt \
            --key-file ${certDir}/${tailscaleMagicDnsName}.key \
            ${tailscaleMagicDnsName}
        '';
      };

      nginx.after = [ "tailscale-cert.service" ];
      nginx.requires = [ "tailscale-cert.service" ];
    };

    timers.tailscale-cert = {
      description = "Renew Tailscale HTTPS certificate daily";
      wantedBy = [ "timers.target" ];
      timerConfig = {
        OnCalendar = "daily";
        Persistent = true;
      };
    };
  };
}
