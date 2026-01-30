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

  age.secrets.example = {
    file = ./config/example.toml.age;
    path = "/run/agenix/example.toml";
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
