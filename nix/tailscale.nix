# Tailscale stack for the deployed hosts: enrolls the node onto our
# tailnet via a per-environment agenix-encrypted auth key, opens the
# WireGuard port + marks `tailscale0` as a trusted firewall interface,
# and provisions a Tailscale-issued HTTPS certificate that nginx
# serves the dashboard from.
#
# The `tailscale-cert` oneshot provisions the initial certificate and
# remains active so later NixOS switches do not call the certificate API.
# A separate daily `tailscale-cert-renew` oneshot renews the certificate
# and reloads nginx. `tailscaled.ExecStartPre` deletes the stale
# `tailscale0` TUN device so the unit doesn't crash-loop when a previous
# tailscaled hasn't released it yet.
{
  pkgs,
  environment,
  tailscaleMagicDnsName,
  ...
}:

let
  certDir = "/var/lib/tailscale-cert";
  certPath = "${certDir}/${tailscaleMagicDnsName}.crt";
  keyPath = "${certDir}/${tailscaleMagicDnsName}.key";
  certificateScript = ''
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
      --cert-file ${certPath} \
      --key-file ${keyPath} \
      ${tailscaleMagicDnsName}
  '';
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
    file = ../secret/tailscale-authkey-${environment}.age;
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
          TimeoutStartSec = "5min";
          User = "nginx";
          Group = "nginx";
          # An inactive nginx is expected while the first certificate is
          # provisioned, so there is nothing to reload on first boot.
          ExecStartPost = "+${pkgs.bash}/bin/bash -c 'if systemctl is-active --quiet nginx.service; then systemctl reload nginx.service; fi'";
        };
        script = certificateScript;
      };

      # Renew independently of nginx and NixOS activation. A slow certificate
      # API can fail this timer run without blocking or rolling back a deploy.
      tailscale-cert-renew = {
        description = "Renew Tailscale HTTPS certificate for ${tailscaleMagicDnsName}";
        after = [ "tailscale-cert.service" ];
        wants = [ "tailscale-cert.service" ];
        path = [
          pkgs.tailscale
          pkgs.jq
        ];
        serviceConfig = {
          Type = "oneshot";
          RemainAfterExit = false;
          TimeoutStartSec = "5min";
          User = "nginx";
          Group = "nginx";
          ExecStartPost = "+${pkgs.bash}/bin/bash -c 'if systemctl is-active --quiet nginx.service; then systemctl reload nginx.service; fi'";
        };
        script = certificateScript;
      };

      nginx.after = [ "tailscale-cert.service" ];
      nginx.requires = [ "tailscale-cert.service" ];
    };

    timers.tailscale-cert-renew = {
      description = "Renew Tailscale HTTPS certificate daily";
      wantedBy = [ "timers.target" ];
      timerConfig = {
        OnCalendar = "daily";
        Persistent = true;
      };
    };
  };
}
