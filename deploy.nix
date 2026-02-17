{ deploy-rs, self }:

let
  system = "x86_64-linux";
  inherit (deploy-rs.lib.${system}) activate;
  profileBase = "/nix/var/nix/profiles/per-service";

  st0xPackage = self.packages.${system}.st0x-liquidity;

  rage = "/run/current-system/sw/bin/rage";
  hostKey = "/etc/ssh/ssh_host_ed25519_key";

  services = import ./services.nix;
  enabledServices = builtins.attrNames (builtins.removeAttrs services
    (builtins.filter (n: !services.${n}.enabled)
      (builtins.attrNames services)));

  # Decrypts secrets and restarts service atomically
  mkServiceProfile = name:
    let
      markerFile = "/run/st0x/${name}.ready";
      secretsFile = ./secret/${name}.toml.age;
      decryptedSecrets = "/run/agenix/${name}.toml";
    in activate.custom st0xPackage (builtins.concatStringsSep " && " [
      "systemctl stop ${name} || true"
      "rm -f ${markerFile}"
      "mkdir -p /run/agenix /run/st0x"
      "${rage} -d -i ${hostKey} ${secretsFile} > ${decryptedSecrets}"
      "chown root:st0x ${decryptedSecrets}"
      "chmod 0640 ${decryptedSecrets}"
      "touch ${markerFile}"
      "systemctl restart ${name}"
    ]);

  mkProfile = name: {
    path = mkServiceProfile name;
    profilePath = "${profileBase}/${name}";
  };

in {
  config = {
    nodes.st0x-liquidity = {
      hostname = builtins.getEnv "DEPLOY_HOST";
      sshUser = "root";
      user = "root";

      profilesOrder = [ "system" ] ++ enabledServices;

      profiles = {
        system.path = activate.nixos self.nixosConfigurations.st0x-liquidity;
      } // builtins.listToAttrs (map (name: {
        inherit name;
        value = mkProfile name;
      }) enabledServices);
    };
  };

  wrappers = { pkgs, infraPkgs, localSystem }:
    let
      deployInputs = infraPkgs.buildInputs
        ++ [ deploy-rs.packages.${localSystem}.deploy-rs ];

      deployPreamble = ''
        ${infraPkgs.resolveIp}
        export DEPLOY_HOST="$host_ip"

        ssh_flag=""
        if [ "$identity" != "$HOME/.ssh/id_ed25519" ]; then
          export NIX_SSHOPTS="-i $identity"
          ssh_flag="--ssh-opts=-i $identity"
        fi
      '';

      deployFlags = if localSystem == "x86_64-linux" then
        ""
      else
        "--skip-checks --remote-build";

    in {
      deployNixos = pkgs.writeShellApplication {
        name = "deploy-nixos";
        runtimeInputs = deployInputs;
        text = ''
          ${deployPreamble}
          deploy ${deployFlags} ''${ssh_flag:+"$ssh_flag"} .#st0x-liquidity.system \
            -- --impure "$@"
        '';
      };

      deployService = pkgs.writeShellApplication {
        name = "deploy-service";
        runtimeInputs = deployInputs;
        text = ''
          ${deployPreamble}
          profile="''${1:?usage: deploy-service <profile>}"
          shift
          deploy ${deployFlags} ''${ssh_flag:+"$ssh_flag"} ".#st0x-liquidity.$profile" \
            -- --impure "$@"
        '';
      };

      deployAll = pkgs.writeShellApplication {
        name = "deploy-all";
        runtimeInputs = deployInputs;
        text = ''
          ${deployPreamble}
          deploy ${deployFlags} ''${ssh_flag:+"$ssh_flag"} .#st0x-liquidity \
            -- --impure "$@"
        '';
      };
    };
}
