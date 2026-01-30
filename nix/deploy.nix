{ deploy-rs, self }:

let
  system = "x86_64-linux";
  activate = deploy-rs.lib.${system}.activate;

in {
  nodes.st0x-liquidity = {
    hostname = "FIXME";
    sshUser = "root";
    user = "root";

    profilesOrder = [ "system" ];

    profiles = {
      system.path = activate.nixos self.nixosConfigurations.st0x-liquidity;
    };
  };
}
