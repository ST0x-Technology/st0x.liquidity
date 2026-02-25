{ deploy-rs, self }:

let
  system = "x86_64-linux";
  inherit (deploy-rs.lib.${system}) activate;
  profileBase = "/nix/var/nix/profiles/per-service";

  st0xPackage = self.packages.${system}.st0x-liquidity;

  rage = "/run/current-system/sw/bin/rage";
  hostKey = "/etc/ssh/ssh_host_ed25519_key";
  expectedHostPubKey = (import ./keys.nix).keys.host;

  services = import ./services.nix;
  enabledServices = builtins.attrNames (builtins.removeAttrs services
    (builtins.filter (n: !services.${n}.enabled)
      (builtins.attrNames services)));

  # Decrypts secrets and restarts service atomically
  mkServiceProfile = name:
    let
      cfg = services.${name};
      secretsFile = ./secret/${cfg.encryptedSecret};
    in activate.custom st0xPackage (builtins.concatStringsSep " && " [
      "systemctl stop ${name} || true"
      "rm -f ${cfg.markerFile}"
      "mkdir -p /run/st0x"
      "${rage} -d -i ${hostKey} ${secretsFile} > ${cfg.decryptedSecretPath}"
      "chown root:st0x ${cfg.decryptedSecretPath}"
      "chmod 0640 ${cfg.decryptedSecretPath}"
      "touch ${cfg.markerFile}"
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
        ++ [ deploy-rs.packages.${localSystem}.deploy-rs pkgs.openssh ];

      deployPreamble = ''
        ${infraPkgs.resolveIp}
        export DEPLOY_HOST="$host_ip"

        # Verify host key against keys.nix trust anchor
        scanned=$(ssh-keyscan -t ed25519 "$host_ip" 2>/dev/null | grep -v '^#')
        expected="$host_ip ${expectedHostPubKey}"

        if [ -z "$scanned" ]; then
          echo "ERROR: ssh-keyscan returned no key for $host_ip" >&2
          exit 1
        fi

        if [ "$(echo "$scanned" | wc -l)" -ne 1 ]; then
          echo "ERROR: expected 1 key from $host_ip, got multiple:" >&2
          echo "$scanned" >&2
          exit 1
        fi

        if [ "$scanned" != "$expected" ]; then
          echo "ERROR: host key mismatch for $host_ip" >&2
          echo "  expected: $expected" >&2
          echo "  got:      $scanned" >&2
          exit 1
        fi

        ssh-keygen -R "$host_ip" >/dev/null 2>&1 || true
        echo "$expected" >> "$HOME/.ssh/known_hosts"

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
            -- --impure --accept-flake-config \
            --extra-experimental-features 'nix-command flakes' "$@"
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
            -- --impure --accept-flake-config \
            --extra-experimental-features 'nix-command flakes' "$@"
        '';
      };

      deployAll = pkgs.writeShellApplication {
        name = "deploy-all";
        runtimeInputs = deployInputs;
        text = ''
          ${deployPreamble}
          deploy ${deployFlags} ''${ssh_flag:+"$ssh_flag"} .#st0x-liquidity \
            -- --impure --accept-flake-config \
            --extra-experimental-features 'nix-command flakes' "$@"
        '';
      };
    };
}
