{
  lib,
  deploy-rs,
  self,
  environments,
}:

let
  system = "x86_64-linux";
  inherit (deploy-rs.lib.${system}) activate;
  profileBase = "/nix/var/nix/profiles/per-service";

  gitRev = self.rev or self.dirtyRev or "unknown";

  rage = "/run/current-system/sw/bin/rage";
  hostKey = "/etc/ssh/ssh_host_ed25519_key";

  inherit (import ./services.nix { inherit lib; }) enabled;

  enabledNames = builtins.attrNames enabled;

  # Explicit ordering: sort enabled services by each service's declared `order`
  # field so adding a new entry forces choosing its slot rather than inheriting
  # the alphabetical attribute order. Duplicate orders would produce a
  # non-deterministic activation sequence, so assert uniqueness here.
  enabledOrders = map (name: enabled.${name}.order) enabledNames;

  uniqueOrders = builtins.foldl' (
    acc: o: if builtins.elem o acc then acc else acc ++ [ o ]
  ) [ ] enabledOrders;

  orderedServices =
    if (builtins.length enabledOrders) == (builtins.length uniqueOrders) then
      builtins.sort (a: b: enabled.${a}.order < enabled.${b}.order) enabledNames
    else
      throw "services.nix: duplicate `order` values among enabled services: ${builtins.toJSON enabledOrders}";

  # Builds the per-service activation command. Branches by service kind so we
  # don't special-case each new service in this file -- new services land in
  # services.nix and inherit the correct pipeline.
  mkServiceProfile =
    env: name:
    let
      cfg = enabled.${name};
      pkg = self.packages.${system}.${cfg.package};
    in
    if cfg.kind == "st0x" then
      let
        configFile = ./config/${env}/${name}.toml;
        secretsFile = ./secret/${cfg.encryptedSecret};
      in
      activate.custom pkg (
        builtins.concatStringsSep " && " [
          "systemctl stop ${name} || true"
          "rm -f ${cfg.markerFile}"
          "mkdir -p /run/st0x"
          "install -D -m 0640 -o root -g st0x ${configFile} ${cfg.configPath}"
          "${rage} -d -i ${hostKey} ${secretsFile} | install -D -m 0640 -o root -g st0x /dev/stdin ${cfg.decryptedSecretPath}"
          # Validate config + secrets before restarting. If validation fails,
          # the activation script exits non-zero and deploy-rs rolls back.
          "${cfg.profilePath}/bin/validate-config --config ${cfg.configPath} --secrets ${cfg.decryptedSecretPath}"
          "(chown st0x:st0x /mnt/data/*.db /mnt/data/*.db-wal /mnt/data/*.db-shm /mnt/data/*.db-journal 2>/dev/null || true)"
          "(chown -R st0x:st0x /mnt/data/logs 2>/dev/null || true)"
          "echo '${gitRev}' > /run/st0x/${name}.git-rev"
          "touch ${cfg.markerFile}"
          "systemctl restart ${name}"
        ]
      )
    else if cfg.kind == "plain" then
      # Marker must exist BEFORE systemctl restart, because the unit's
      # ConditionPathExists is evaluated when systemd processes the start
      # request -- if the marker is absent, systemd silently skips the unit
      # (returning exit 0 from `systemctl restart`) and the service never
      # actually starts. Touch it first, then remove it on restart failure so
      # a broken unit doesn't satisfy the condition on the next system
      # activation.
      activate.custom pkg (
        builtins.concatStringsSep " && " [
          "systemctl stop ${name} || true"
          "mkdir -p /run/st0x"
          "touch ${cfg.markerFile}"
          "systemctl restart ${name} || { rm -f ${cfg.markerFile}; exit 1; }"
        ]
      )
    else if cfg.kind == "static" then
      activate.custom pkg cfg.activation
    else
      throw "services.${name}: unknown kind ${cfg.kind}";

  mkProfile = env: name: {
    path = mkServiceProfile env name;
    profilePath = "${profileBase}/${name}";
  };

  mkNode =
    { env, nixosConfig }:
    {
      hostname = builtins.getEnv "DEPLOY_HOST";
      sshUser = "root";
      user = "root";

      profilesOrder = [ "system" ] ++ orderedServices;

      profiles = {
        system.path = activate.nixos nixosConfig;
      }
      // builtins.listToAttrs (
        map (name: {
          inherit name;
          value = mkProfile env name;
        }) orderedServices
      );
    };

in
{
  config = {
    nodes = builtins.listToAttrs (
      map (
        env:
        let
          cfg = environments.${env};
        in
        {
          name = cfg.nodeName;
          value = mkNode {
            inherit env;
            nixosConfig = self.nixosConfigurations.${cfg.nodeName};
          };
        }
      ) (builtins.attrNames environments)
    );
  };

  mkDeployScripts =
    {
      pkgs,
      infraPkgs,
      localSystem,
    }:
    let
      deployInputs = infraPkgs.buildInputs ++ [
        deploy-rs.packages.${localSystem}.deploy-rs
        pkgs.openssh
      ];

      deployFlags =
        if localSystem == "x86_64-linux" then
          "--debug-logs --skip-checks"
        else
          "--debug-logs --skip-checks --remote-build";

      nixFlags = "--impure --accept-flake-config --extra-experimental-features 'nix-command flakes'";

      mkEnvDeployScripts =
        env:
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

          mkDeployScript =
            name:
            {
              prelude ? "",
              target,
            }:
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

        in
        {
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

          "${env}DeployAll" = mkDeployScript "${env}-deploy-all" { target = ".#${nodeName}"; };
        };

    in
    builtins.foldl' (acc: env: acc // mkEnvDeployScripts env) { } (builtins.attrNames environments);
}
