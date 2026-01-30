{ deploy-rs, self }:

let
  system = "x86_64-linux";
  inherit (deploy-rs.lib.${system}) activate;

in {
  nodes.st0x-liquidity = {
    hostname = "209.38.48.141";
    sshUser = "root";
    user = "root";

    profilesOrder = [ "system" ];

    profiles = {
      system.path = activate.nixos self.nixosConfigurations.st0x-liquidity;
    };
  };
}
