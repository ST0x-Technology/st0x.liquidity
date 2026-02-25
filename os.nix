{ pkgs, lib, modulesPath, dashboard, st0x-liquidity, ... }:

let
  inherit (import ./keys.nix) roles;

  services = import ./services.nix;
  enabledServices = lib.filterAttrs (_: v: v.enabled) services;

  hedgeCfg = services.st0x-hedge;

  cli = pkgs.writeShellApplication {
    name = "stox";
    runtimeInputs = [ st0x-liquidity ];
    text = ''
      exec cli --config ${./config/st0x-hedge.toml} \
        --secrets ${hedgeCfg.decryptedSecretPath} "$@"
    '';
  };

  mkService = name: cfg:
    let
      path = "/nix/var/nix/profiles/per-service/${name}/bin/${cfg.bin}";
      configFile = ./config/${name}.toml;
    in {
      description = "st0x ${cfg.bin} (${name})";

      # Service is started by deploy.nix profile, not by systemd on boot.
      # This avoids coordination issues during deployments.
      wantedBy = [ ];

      restartIfChanged = false;
      stopIfChanged = false;

      unitConfig = {
        "X-OnlyManualStart" = true;

        # Marker file created ONLY by service profile activation.
        # Guarantees service is SKIPPED (not failed) during system activation.
        ConditionPathExists = cfg.markerFile;
      };

      serviceConfig = {
        DynamicUser = true;
        SupplementaryGroups = [ "st0x" ];
        ExecStart = builtins.concatStringsSep " " [
          path
          "--config"
          "${configFile}"
          "--secrets"
          cfg.decryptedSecretPath
        ];
        Restart = "always";
        RestartSec = 5;
        ReadWritePaths = [ "/mnt/data" ];
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
      };
    };

    nginx = {
      enable = true;
      virtualHosts.default = {
        default = true;
        root = "${dashboard}";

        locations = let
          wsProxy = port: {
            proxyPass = "http://127.0.0.1:${toString port}/api/ws";
            proxyWebsockets = true;
            extraConfig = ''
              proxy_connect_timeout 60;
              proxy_send_timeout 60;
              proxy_read_timeout 86400;
            '';
          };
        in {
          "/".tryFiles = "$uri $uri/ /index.html";
          "/api/alpaca/ws" = wsProxy 8081;
        };
      };
    };

    grafana = {
      enable = true;
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

  users.users.root.openssh.authorizedKeys.keys = roles.ssh;

  networking.firewall = {
    enable = true;
    allowedTCPPorts = [
      22 # SSH
      80 # Dashboard
      3000 # Grafana
    ];
  };

  fileSystems."/mnt/data" = {
    device = "/dev/disk/by-id/scsi-0DO_Volume_st0x-liquidity-data";
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

  users.groups.st0x = { };
  programs.bash.interactiveShellInit = "set -o vi";

  age.secrets = lib.mapAttrs' (_: cfg:
    lib.nameValuePair cfg.decryptedSecret {
      file = ./secret/${cfg.encryptedSecret};
      group = "st0x";
      mode = "0640";
    }) enabledServices;
  systemd.tmpfiles.rules = [ "d /mnt/data/grafana 0750 grafana grafana -" ];
  systemd.services = lib.mapAttrs mkService enabledServices;

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

  system.activationScripts.per-service-profiles.text =
    "mkdir -p /nix/var/nix/profiles/per-service";

  system.stateVersion = "24.11";
}
