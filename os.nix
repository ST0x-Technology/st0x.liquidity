{ pkgs, lib, modulesPath, st0x-cli, environment, volumeName, ... }:

let
  inherit (import ./keys.nix) roles;
  envRoles = roles.${environment};

  tailscaleFqdn = {
    prod = "st0x-liquidity-nixos.taile5cf8a.ts.net";
    staging = "st0x-liquidity-staging.taile5cf8a.ts.net";
  }.${environment};
  certDir = "/var/lib/tailscale-cert";

  services = import ./services.nix;
  enabledServices = lib.filterAttrs (_: v: v.enabled) services;

  cli = pkgs.writeShellApplication {
    name = "stox";
    runtimeInputs = [ st0x-cli ];
    text = ''
      exec cli \
        --config "''${STOX_CONFIG:-/run/st0x/st0x-hedge.config}" \
        --secrets "''${STOX_SECRETS:-/run/agenix/st0x-hedge.toml}" \
        "$@"
    '';
  };

  mkService = name: cfg: {
    description = "st0x ${cfg.bin} (${name})";

    # Service is started by deploy.nix profile, not by systemd on boot.
    # This avoids coordination issues during deployments.
    wantedBy = [ ];

    restartIfChanged = false;
    stopIfChanged = false;

    unitConfig = {
      "X-OnlyManualStart" = true;
      StartLimitBurst = 10;
      StartLimitIntervalSec = 300;

      # Marker file created ONLY by service profile activation.
      # Guarantees service is SKIPPED (not failed) during system activation.
      ConditionPathExists = cfg.markerFile;
    };

    serviceConfig = {
      User = "st0x";
      Group = "st0x";
      ExecStart = builtins.concatStringsSep " " [
        "${cfg.profilePath}/bin/${cfg.bin}"
        "--config"
        cfg.configPath
        "--secrets"
        cfg.decryptedSecretPath
      ];
      Restart = "always";
      RestartSec = 30;
    };
  };

in {
  imports = [
    (modulesPath + "/virtualisation/digital-ocean-config.nix")
    (modulesPath + "/profiles/qemu-guest.nix")
    ./disko.nix
  ];

  boot.loader.grub = {
    efiSupport = true;
    efiInstallAsRemovable = true;
  };

  networking.useDHCP = lib.mkForce false;

  services = {
    cloud-init = {
      enable = true;
      network.enable = true;
      settings = {
        datasource_list = [ "ConfigDrive" "Digitalocean" ];
        datasource.ConfigDrive = { };
        datasource.Digitalocean = { };
        cloud_init_modules = [
          "seed_random"
          "bootcmd"
          "write_files"
          "growpart"
          "resizefs"
          "set_hostname"
          "update_hostname"
          "set_password"
        ];
        cloud_config_modules =
          [ "ssh-import-id" "keyboard" "runcmd" "disable_ec2_metadata" ];
        cloud_final_modules = [
          "write_files_deferred"
          "puppet"
          "chef"
          "ansible"
          "mcollective"
          "salt_minion"
          "reset_rmc"
          "scripts_per_once"
          "scripts_per_boot"
          "scripts_user"
          "ssh_authkey_fingerprints"
          "keys_to_console"
          "install_hotplug"
          "phone_home"
          "final_message"
        ];
      };
    };

    openssh = {
      enable = true;
      settings = {
        PasswordAuthentication = false;
        PermitRootLogin = "prohibit-password";
        MaxStartups = "50:30:100";
      };
    };

    # Per-environment reusable, tagged auth key. Used only on first
    # enrollment — after that Tailscale re-authenticates via the stored
    # node key in /var/lib/tailscale. To rotate the node identity
    # (e.g. re-tag), run `tailscale up --force-reauth --auth-key ...`
    # manually on the droplet.
    tailscale = {
      enable = true;
      authKeyFile = "/run/agenix/tailscale-authkey-${environment}";
      permitCertUid = "nginx";
    };

    fail2ban = {
      enable = true;
      bantime = "1h";
      maxretry = 3;
    };

    nginx = {
      enable = true;
      virtualHosts.${tailscaleFqdn} = {
        default = true;
        forceSSL = true;
        sslCertificate = "${certDir}/${tailscaleFqdn}.crt";
        sslCertificateKey = "${certDir}/${tailscaleFqdn}.key";
        root = "/nix/var/nix/profiles/per-service/dashboard";

        locations = let
          port = "8001";
          backend = "http://127.0.0.1:${port}";
          apiProxy = path: { proxyPass = "${backend}${path}"; };
          wsProxy = {
            proxyPass = "${backend}/api/ws";
            proxyWebsockets = true;
            extraConfig = ''
              proxy_connect_timeout 60;
              proxy_send_timeout 60;
              proxy_read_timeout 86400;
            '';
          };
        in {
          "/".tryFiles = "$uri $uri/ /index.html";
          "/api/ws" = wsProxy;
          "/health" = apiProxy "/health";
          "/logs" = apiProxy "/logs";
          "/orders/" = apiProxy "/orders/";
          "/trades" = apiProxy "/trades";
          "/transfers" = apiProxy "/transfers";
        };
      };
    };

    grafana = {
      enable = false;
      settings.server = {
        http_addr = "0.0.0.0";
        http_port = 3000;
      };
      settings.database = {
        type = "sqlite3";
        path = "/mnt/data/grafana/grafana.db";
      };
    };
  };

  users = {
    users.root.openssh.authorizedKeys.keys = envRoles.ssh;
    users.st0x = {
      isSystemUser = true;
      group = "st0x";
    };
    groups.st0x = { };
  };

  networking.firewall = {
    enable = true;
    # All inbound access is gated by the DO Cloud Firewall (infra/modules/stack)
    # which only permits Tailscale WireGuard. SSH and the dashboard are reached
    # exclusively over tailscale0, which is trusted below and bypasses the
    # NixOS firewall entirely.
    allowedTCPPorts = [ ];
    allowedUDPPorts = [
      41641 # Tailscale WireGuard
    ];
    trustedInterfaces = [ "tailscale0" ];
  };

  fileSystems."/mnt/data" = {
    device = "/dev/disk/by-id/scsi-0DO_Volume_${volumeName}";
    fsType = "ext4";
  };

  nix = {
    settings = {
      experimental-features = [ "nix-command" "flakes" ];
      auto-optimise-store = true;
      download-buffer-size = 268435456;
    };

    gc = {
      automatic = true;
      dates = "weekly";
      options = "--delete-older-than 30d";
    };
  };

  programs.bash.interactiveShellInit = "set -o vi";

  age.secrets = {
    "tailscale-authkey-${environment}" = {
      file = ./secret/tailscale-authkey-${environment}.age;
      mode = "0400";
    };
  };
  systemd = {
    tmpfiles.rules = [
      "d /mnt/data 0755 st0x st0x -"
      "d /mnt/data/logs 0755 st0x st0x -"
      "d /mnt/data/grafana 0750 grafana grafana -"
      "d ${certDir} 0750 nginx nginx -"
    ];

    services = lib.recursiveUpdate (lib.mapAttrs mkService enabledServices) {
      # Clean up stale TUN device before tailscaled starts. During NixOS
      # activation the old tailscaled may still hold /dev/net/tun when the
      # new unit starts, causing a crash-loop.
      tailscaled.serviceConfig.ExecStartPre =
        [ "-${pkgs.iproute2}/bin/ip link delete tailscale0" ];

      # Provision a Tailscale-issued TLS certificate so the dashboard is
      # served over HTTPS. Runs before nginx to guarantee cert files exist.
      tailscale-cert = {
        description =
          "Provision Tailscale HTTPS certificate for ${tailscaleFqdn}";
        after = [ "tailscaled.service" ];
        wants = [ "tailscaled.service" ];
        before = [ "nginx.service" ];
        wantedBy = [ "multi-user.target" ];
        path = [ pkgs.tailscale pkgs.jq ];
        serviceConfig = {
          Type = "oneshot";
          RemainAfterExit = true;
          User = "nginx";
          Group = "nginx";
          # Reload nginx so it picks up renewed certs on subsequent runs
          # triggered by the timer. || true keeps the unit green on first
          # boot when nginx hasn't started yet.
          ExecStartPost =
            "+${pkgs.bash}/bin/bash -c 'systemctl is-active --quiet nginx.service && systemctl reload nginx.service || true'";
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
            --cert-file ${certDir}/${tailscaleFqdn}.crt \
            --key-file ${certDir}/${tailscaleFqdn}.key \
            ${tailscaleFqdn}
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

  environment.systemPackages = with pkgs; [
    bat
    curl
    htop
    magic-wormhole
    sqlite
    rage
    vim
    zellij
    cli
  ];

  system.activationScripts.per-service-profiles.text = ''
    mkdir -p /nix/var/nix/profiles/per-service

    # Managed services use restartIfChanged = false + ConditionPathExists so
    # that deploy.nix's per-service profile owns stop/install/restart. But if
    # a previous deploy left one crash-looping (Restart = always), its failed
    # state persists into the next activation and switch-to-configuration's
    # final "units failed" check exits 4, which makes deploy-rs roll back
    # before it ever reaches the per-service profile that would install the
    # fix. Stop + reset-failed any managed service that is currently broken
    # so activation can complete; the service profile restarts it afterwards.
    for svc in ${
      builtins.concatStringsSep " " (builtins.attrNames enabledServices)
    }; do
      state=$(${pkgs.systemd}/bin/systemctl show -p ActiveState --value "$svc.service" 2>/dev/null || echo "")
      if [ "$state" = "failed" ] || [ "$state" = "activating" ]; then
        ${pkgs.systemd}/bin/systemctl stop "$svc.service" 2>/dev/null || true
        ${pkgs.systemd}/bin/systemctl reset-failed "$svc.service" 2>/dev/null || true
      fi
    done
  '';

  system.stateVersion = "24.11";

}
