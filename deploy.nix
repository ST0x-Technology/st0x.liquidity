{ deploy-rs, self }:

let
  system = "x86_64-linux";
  inherit (deploy-rs.lib.${system}) activate;

  profileBase = "/nix/var/nix/profiles/per-service";

  mkServiceProfile = { package, services }:
    activate.custom package (builtins.concatStringsSep " && "
      (map (s: "systemctl restart ${s}") services));

in {
  nodes.st0x-liquidity = {
    hostname = builtins.getEnv "DEPLOY_HOST";
    sshUser = "root";
    user = "root";

    profilesOrder = [ "system" "server" "reporter" ];

    profiles = {
      system.path = activate.nixos self.nixosConfigurations.st0x-liquidity;

      server = {
        path = mkServiceProfile {
          package = self.packages.${system}.st0x-liquidity;
          services = [ "server-schwab" "server-alpaca" ];
        };
        profilePath = "${profileBase}/server";
      };

      reporter = {
        path = mkServiceProfile {
          package = self.packages.${system}.st0x-liquidity;
          services = [ "reporter-schwab" "reporter-alpaca" ];
        };
        profilePath = "${profileBase}/reporter";
      };
    };
  };
}
