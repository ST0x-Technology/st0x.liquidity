{ deploy-rs, self, environments }:

let
  system = "x86_64-linux";
  inherit (deploy-rs.lib.${system}) activate;
  profileBase = "/nix/var/nix/profiles/per-service";

  st0xPackage = self.packages.${system}.st0x-liquidity;
  dashboardPackage = self.packages.${system}.st0x-dashboard;

  rage = "/run/current-system/sw/bin/rage";
  hostKey = "/etc/ssh/ssh_host_ed25519_key";

  services = import ./services.nix;
  enabledServices = builtins.attrNames (builtins.removeAttrs services
    (builtins.filter (n: !services.${n}.enabled)
      (builtins.attrNames services)));

  # Links latest config, decrypts secrets, and restarts service atomically
  mkServiceProfile = name:
    let
      cfg = services.${name};
      configFile = ./config/${name}.toml;
      secretsFile = ./secret/${cfg.encryptedSecret};
    in activate.custom st0xPackage (builtins.concatStringsSep " && " [
      "systemctl stop ${name} || true"
      "rm -f ${cfg.markerFile}"
      "mkdir -p /run/st0x"
      "ln -sf ${configFile} ${cfg.configPath}"
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

  mkNode = { nixosConfig }: {
    hostname = builtins.getEnv "DEPLOY_HOST";
    sshUser = "root";
    user = "root";

    profilesOrder = [ "system" "dashboard" ] ++ enabledServices;

    profiles = {
      system.path = activate.nixos nixosConfig;

      dashboard = {
        path = activate.custom dashboardPackage "systemctl reload nginx";
        profilePath = "${profileBase}/dashboard";
      };
    } // builtins.listToAttrs (map (name: {
      inherit name;
      value = mkProfile name;
    }) enabledServices);
  };

in {
  config = {
    nodes = builtins.listToAttrs (map (env:
      let cfg = environments.${env};
      in {
        name = cfg.nodeName;
        value =
          mkNode { nixosConfig = self.nixosConfigurations.${cfg.nodeName}; };
      }) (builtins.attrNames environments));
  };

  mkDeployScripts = { pkgs, infraPkgs, localSystem }:
    let
      deployInputs = infraPkgs.buildInputs
        ++ [ deploy-rs.packages.${localSystem}.deploy-rs pkgs.openssh ];

      deployFlags = if localSystem == "x86_64-linux" then
        "--debug-logs --skip-checks"
      else
        "--debug-logs --skip-checks --remote-build";

      nixFlags =
        "--impure --accept-flake-config --extra-experimental-features 'nix-command flakes'";

      mkEnvDeployScripts = env:
        let
          cfg = environments.${env};
          inherit (cfg) hostKey nodeName;
          envInfraPkgs = infraPkgs.perEnv.${env};

          deployPreamble = ''
            if [ -n "''${DEPLOY_HOST:-}" ]; then
              host_ip="$DEPLOY_HOST"
              echo "Using pre-set DEPLOY_HOST=$host_ip"
            else
              ${envInfraPkgs.resolveIp}
              export DEPLOY_HOST="$host_ip"
            fi

            # Verify host key against keys.nix trust anchor
            scanned=$(ssh-keyscan -t ed25519 "$host_ip" 2>/dev/null | grep -v '^#')
            expected="$host_ip ${hostKey}"

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

          mkDeployScript = name:
            { prelude ? "", target }:
            pkgs.writeShellApplication {
              inherit name;
              runtimeInputs = deployInputs;
              text = ''
                ${deployPreamble}
                ${prelude}
                deploy ${deployFlags} ''${ssh_flag:+"$ssh_flag"} ${target} \
                  -- ${nixFlags} "$@"
              '';
            };

        in {
          "${env}DeployNixos" =
            mkDeployScript "deploy-nixos" { target = ".#${nodeName}.system"; };

          "${env}DeployService" = mkDeployScript "deploy-service" {
            prelude = ''
              profile="''${1:?usage: deploy-service <profile>}"
              shift
            '';
            target = ''.#${nodeName}."$profile"'';
          };

          "${env}DeployAll" =
            mkDeployScript "deploy-all" { target = ".#${nodeName}"; };
        };

    in builtins.foldl' (acc: env: acc // mkEnvDeployScripts env) { }
    (builtins.attrNames environments);
}
