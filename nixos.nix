{ pkgs, lib, modulesPath, ... }:

{
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

  users.users.root.openssh.authorizedKeys.keys = [
    "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIBnMyJGDpOBOsOsJsxPsKBwwXLOCPwAZ2bBGqjXFAMgR st0x-op"
  ];

  networking.firewall = {
    enable = true;
    allowedTCPPorts = [
      22 # SSH
      80 # HTTP
      443 # HTTPS
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

  system.activationScripts.per-service-profiles.text =
    "mkdir -p /nix/var/nix/profiles/per-service";

  age.secrets = {
    "server-schwab.toml".file = ./config/server-schwab.toml.age;
    "server-alpaca.toml".file = ./config/server-alpaca.toml.age;
    "reporter-schwab.toml".file = ./config/reporter-schwab.toml.age;
    "reporter-alpaca.toml".file = ./config/reporter-alpaca.toml.age;
  };

  systemd.services = let
    mkServer = name: {
      description = "st0x hedging bot (${name})";
      wantedBy = [ "multi-user.target" ];
      unitConfig.ConditionPathExists =
        "/nix/var/nix/profiles/per-service/server/bin/server";
      serviceConfig = {
        DynamicUser = true;
        ExecStart = builtins.concatStringsSep " " [
          "/nix/var/nix/profiles/per-service/server/bin/server"
          "--config-file"
          "/run/agenix/${name}.toml"
        ];
        Restart = "always";
        RestartSec = 5;
        ReadWritePaths = [ "/mnt/data" ];
      };
    };

    mkReporter = name: {
      description = "st0x position reporter (${name})";
      wantedBy = [ "multi-user.target" ];
      unitConfig.ConditionPathExists =
        "/nix/var/nix/profiles/per-service/reporter/bin/reporter";
      serviceConfig = {
        DynamicUser = true;
        ExecStart = builtins.concatStringsSep " " [
          "/nix/var/nix/profiles/per-service/reporter/bin/reporter"
          "--config-file"
          "/run/agenix/${name}.toml"
        ];
        Restart = "always";
        RestartSec = 5;
        ReadWritePaths = [ "/mnt/data" ];
      };
    };

  in {
    server-schwab = mkServer "server-schwab";
    server-alpaca = mkServer "server-alpaca";
    reporter-schwab = mkReporter "reporter-schwab";
    reporter-alpaca = mkReporter "reporter-alpaca";
  };

  environment.systemPackages = with pkgs; [
    bat
    curl
    htop
    magic-wormhole
    zellij
  ];

  programs.bash.interactiveShellInit = "set -o vi";

  system.stateVersion = "24.11";
}
