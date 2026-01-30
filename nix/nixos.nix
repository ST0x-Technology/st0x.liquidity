{ pkgs, lib, modulesPath, ... }:

let
  mkService = name: bin: {
    description = "st0x ${name}";
    after = [ "network-online.target" "mnt-data.mount" ];
    wants = [ "network-online.target" ];
    requires = [ "mnt-data.mount" ];
    wantedBy = [ "multi-user.target" ];

    serviceConfig = {
      ExecStart =
        "/nix/var/nix/profiles/per-service/${name}/bin/${bin} --config-file /run/agenix/${name}.toml";
      DynamicUser = true;
      ReadWritePaths = [ "/mnt/data/${name}" ];
      Restart = "on-failure";
      RestartSec = 10;
    };
  };
in {
  imports = [
    (modulesPath + "/virtualisation/digital-ocean-config.nix")
    (modulesPath + "/profiles/qemu-guest.nix")
    ./disk-config.nix
  ];

  boot.loader.grub = {
    efiSupport = true;
    efiInstallAsRemovable = true;
  };

  networking.useDHCP = lib.mkForce false;

  services.cloud-init = {
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

  services.openssh = {
    enable = true;
    settings = {
      PasswordAuthentication = false;
      PermitRootLogin = "prohibit-password";
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

  systemd.services = {
    server-schwab = mkService "server-schwab" "server";
    server-alpaca = mkService "server-alpaca" "server";
    reporter-schwab = mkService "reporter-schwab" "reporter";
    reporter-alpaca = mkService "reporter-alpaca" "reporter";
  };

  environment.systemPackages = with pkgs; [ bat curl htop magic-wormhole zellij ];

  programs.bash.interactiveShellInit = "set -o vi";

  system.stateVersion = "24.11";
}
