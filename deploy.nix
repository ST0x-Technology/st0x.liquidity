{ deploy-rs, self, environments }:

let
  system = "x86_64-linux";
  inherit (deploy-rs.lib.${system}) activate;
  profileBase = "/nix/var/nix/profiles/per-service";

  st0xPackage = self.packages.${system}.st0x-liquidity;
  dashboardPackage = self.packages.${system}.st0x-dashboard;

  gitRev = self.rev or self.dirtyRev or "unknown";

  rage = "/run/current-system/sw/bin/rage";
  hostKey = "/etc/ssh/ssh_host_ed25519_key";

  services = import ./services.nix;
  enabledServices = builtins.attrNames (builtins.removeAttrs services
    (builtins.filter (n: !services.${n}.enabled)
      (builtins.attrNames services)));

  # Links latest config, decrypts secrets, and restarts service atomically
  mkServiceProfile = env: name:
    let
      cfg = services.${name};
      configFile = ./config/${env}/${name}.toml;
      secretsFile = ./secret/${cfg.encryptedSecret};
    in activate.custom st0xPackage (builtins.concatStringsSep " && " [
      "systemctl stop ${name} || true"
      "rm -f ${cfg.markerFile}"
      "mkdir -p /run/st0x"
      "install -D -m 0640 -o root -g st0x ${configFile} ${cfg.configPath}"
      "${rage} -d -i ${hostKey} ${secretsFile} | install -D -m 0640 -o root -g st0x /dev/stdin ${cfg.decryptedSecretPath}"
      # Validate config + secrets before restarting. If validation fails,
      # the activation script exits non-zero and deploy-rs rolls back.
      "${cfg.profilePath}/bin/validate-config --config ${cfg.configPath} --secrets ${cfg.decryptedSecretPath}"
      "(chown st0x:st0x /mnt/data/*.db /mnt/data/*.db-wal /mnt/data/*.db-shm /mnt/data/*.db-journal 2>/dev/null || true)"
      "echo '${gitRev}' > /run/st0x/${name}.git-rev"
      "touch ${cfg.markerFile}"
      "systemctl restart ${name}"
    ]);

  mkProfile = env: name: {
    path = mkServiceProfile env name;
    profilePath = "${profileBase}/${name}";
  };

  mkNode = { env, nixosConfig }: {
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
      value = mkProfile env name;
    }) enabledServices);
  };

in {
  config = {
    nodes = builtins.listToAttrs (map (env:
      let cfg = environments.${env};
      in {
        name = cfg.nodeName;
        value = mkNode {
          inherit env;
          nixosConfig = self.nixosConfigurations.${cfg.nodeName};
        };
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

            # Pin the host key from keys.nix so SSH verifies it during
            # the handshake. This is safer than ssh-keyscan (which is
            # unauthenticated and fails silently on some CI runners).
            mkdir -p "$HOME/.ssh"
            ssh-keygen -R "$host_ip" >/dev/null 2>&1 || true
            echo "$host_ip ${hostKey}" >> "$HOME/.ssh/known_hosts"

            identity="''${SSH_IDENTITY:-$HOME/.ssh/id_ed25519}"
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
          "${env}DeployNixos" = mkDeployScript "${env}-deploy-nixos" {
            target = ".#${nodeName}.system";
          };

          "${env}DeployService" = mkDeployScript "${env}-deploy-service" {
            prelude = ''
              profile="''${1:?usage: ${env}-deploy-service <profile>}"
              shift
            '';
            target = ''.#${nodeName}."$profile"'';
          };

          "${env}DeployAll" =
            mkDeployScript "${env}-deploy-all" { target = ".#${nodeName}"; };
        };

    in builtins.foldl' (acc: env: acc // mkEnvDeployScripts env) { }
    (builtins.attrNames environments);
}
