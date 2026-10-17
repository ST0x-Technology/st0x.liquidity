{
  pkgs,
  lib,
  modulesPath,
  st0x-cli,
  environment,
  volumeName,
  tailscaleMagicDnsName,
  ...
}:

let
  inherit (import ./keys.nix) roles;
  envRoles = roles.${environment};

  certDir = "/var/lib/tailscale-cert";

  # `stox` runs the CLI as the market-making bot: it defaults to the deployed
  # liquidity config/secrets so operators don't pass --config/--secrets.
  cli = pkgs.writeShellApplication {
    name = "stox";
    runtimeInputs = [ st0x-cli ];
    text = ''
      exec st0x-cli \
        --config "''${STOX_CONFIG:-/run/st0x/st0x-hedge.config}" \
        --secrets "''${STOX_SECRETS:-/run/agenix/st0x-hedge.toml}" \
        "$@"
    '';
  };

  # `s01` runs the same CLI as the issuer: it defaults to the issuer
  # config/secrets (separate turnkey wallet + Alpaca account + DB) so the
  # dividend bump (buy -> tokenize -> donate) is funded and signed by the
  # issuer rather than the market-making wallet. Override with
  # S01_CONFIG/S01_SECRETS to point at an ad-hoc issuer config.
  s01 = pkgs.writeShellApplication {
    name = "s01";
    runtimeInputs = [ st0x-cli ];
    text = ''
      exec st0x-cli \
        --config "''${S01_CONFIG:-/run/st0x/s01-issuer.config}" \
        --secrets "''${S01_SECRETS:-/run/agenix/s01-issuer.toml}" \
        "$@"
    '';
  };

in
{
  imports = [
    (modulesPath + "/virtualisation/digital-ocean-config.nix")
    (modulesPath + "/profiles/qemu-guest.nix")
    ./disko.nix
    ./nix/tailscale.nix
    ./nix/upgradeable-services.nix
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
        datasource_list = [
          "ConfigDrive"
          "Digitalocean"
        ];
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
        cloud_config_modules = [
          "ssh-import-id"
          "keyboard"
          "runcmd"
          "disable_ec2_metadata"
        ];
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

    fail2ban = {
      enable = true;
      bantime = "1h";
      maxretry = 3;
    };

    nginx = {
      enable = true;
      virtualHosts.${tailscaleMagicDnsName} = {
        default = true;
        forceSSL = true;
        sslCertificate = "${certDir}/${tailscaleMagicDnsName}.crt";
        sslCertificateKey = "${certDir}/${tailscaleMagicDnsName}.key";
        # The SvelteKit dashboard this host served is gone; the Grafana
        # board replaced it, so the vhost only points there.
        locations."= /".extraConfig = ''
          default_type text/html;
          add_header Cache-Control "no-store" always;
          return 200 '<!doctype html><meta charset="utf-8"><meta http-equiv="refresh" content="0; url=https://grafana.t0trade.com/d/t0-liquidity/liquidity-bot"><title>Liquidity bot</title><p>The liquidity dashboard moved to Grafana: <a href="https://grafana.t0trade.com/d/t0-liquidity/liquidity-bot">Liquidity bot</a>.</p>';
        '';
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
    # which only permits Tailscale WireGuard. SSH and the nginx vhost are reached
    # exclusively over tailscale0, which is configured as a trusted interface
    # in tailscale.nix and bypasses the NixOS firewall entirely.
    allowedTCPPorts = [ ];
  };

  fileSystems."/mnt/data" = {
    device = "/dev/disk/by-id/scsi-0DO_Volume_${volumeName}";
    fsType = "ext4";
  };

  nix = {
    settings = {
      experimental-features = [
        "nix-command"
        "flakes"
      ];
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

  systemd.tmpfiles.rules = [
    "d /mnt/data 0755 st0x st0x -"
    "d /mnt/data/logs 0755 st0x st0x -"
    "d /mnt/data/grafana 0750 grafana grafana -"
  ];

  # The system bus implementation cannot be live-switched safely. Deploy this
  # change with deploy-rs --boot and a reboot, not a normal switch.
  services.dbus.implementation = "broker";

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
    s01
  ];

  system.stateVersion = "24.11";

}
