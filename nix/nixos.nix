{ pkgs, lib, modulesPath, ... }:

let
  mkService = name: bin: {
    description = "st0x ${name}";
    after = [ "network-online.target" "mnt-data.mount" ];
    wants = [ "network-online.target" ];
    requires = [ "mnt-data.mount" ];
    wantedBy = [ "multi-user.target" ];

    serviceConfig = {
      ExecStart = "/nix/var/nix/profiles/per-service/${name}/bin/${bin} --config-file /run/agenix/${name}.toml";
      DynamicUser = true;
      ReadWritePaths = [ "/mnt/data/${name}" ];
      Restart = "on-failure";
      RestartSec = 10;
    };
  };
in {
  imports = [ (modulesPath + "/virtualisation/digital-ocean-config.nix") ];
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

  environment.systemPackages = with pkgs; [ curl htop ];

  system.stateVersion = "24.11";
}
