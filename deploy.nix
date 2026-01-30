{ deploy-rs, self }:

let
  system = "x86_64-linux";
  inherit (deploy-rs.lib.${system}) activate;

  profileBase = "/nix/var/nix/profiles/per-service";

  mkServiceProfile = name: package:
    activate.custom package "systemctl restart ${name}";

in {
  nodes.st0x-liquidity = {
    hostname = builtins.getEnv "DEPLOY_HOST";
    sshUser = "root";
    user = "root";

    profilesOrder = [ "system" "dummy" ];

    profiles = {
      system.path = activate.nixos self.nixosConfigurations.st0x-liquidity;

      dummy = {
        path = mkServiceProfile "dummy" self.packages.${system}.dummy;
        profilePath = "${profileBase}/dummy";
      };
    };
  };
}
