{ pkgs, ragenix, rainix, system, environments }:

let
  buildInputs =
    [ pkgs.terraform pkgs.rage pkgs.jq ragenix.packages.${system}.default ];

  sshBuildInputs = [ pkgs.rage ];

  tfPlanFile = "infra/tfplan";

  mkEncrypted = { file, role }: {
    path = file;
    agePath = "${file}.age";
    decrypt = ''
      if [ -f ${file}.age ]; then
        rage -d -i "$identity" ${file}.age > ${file}
      fi
    '';
    encrypt = ''
      if [ -f ${file} ]; then
        nix eval --raw --file ${
          ../keys.nix
        } roles.${role} --apply 'builtins.concatStringsSep "\n"' \
          | rage -e -R /dev/stdin -o ${file}.age ${file}
      fi
    '';
  };

  state = mkEncrypted {
    file = "infra/terraform.tfstate";
    role = "infra";
  };
  vars = mkEncrypted {
    file = "infra/terraform.tfvars";
    role = "infra";
  };

  # Per-environment remote IP caches for SSH access
  mkRemote = env: sshRole:
    mkEncrypted {
      file = "infra/.remote-${env}";
      role = sshRole;
    };

  remoteFiles = builtins.listToAttrs (map (env: {
    name = env;
    value = mkRemote env "${env}.ssh";
  }) environments);

  parseIdentity = ''
    set -eo pipefail

    if [ "''${1:-}" = "-i" ]; then
      if [ -z "''${2:-}" ]; then
        echo "ERROR: identity is empty -- pass -i <path> or set a default" >&2
        exit 1
      fi
      identity="$2"
      shift 2
    else
      identity="$HOME/.ssh/id_ed25519"
      if [ ! -f "$identity" ]; then
        echo "ERROR: no -i flag and default key $identity not found" >&2
        exit 1
      fi
    fi
  '';

  cleanup = "rm -f ${state.path} ${state.path}.backup ${vars.path}";
  cleanupWithPlan = "${cleanup} ${tfPlanFile}";

  syncRemotes = ''
    if [ -f ${state.path} ]; then
      ${
        builtins.concatStringsSep "\n" (map (env:
          let rf = remoteFiles.${env};
          in ''
            jq -r '.outputs.${env}_droplet_ipv4.value // empty' ${state.path} > ${rf.path} || true
            if [ -s ${rf.path} ]; then
              ${rf.encrypt}
            else
              rm -f ${rf.agePath}
            fi
            rm -f ${rf.path}
          '') environments)
      }
    fi
  '';

  preamble = ''
    ${parseIdentity}
    on_exit() { ${cleanup}; }
    trap on_exit EXIT
    ${vars.decrypt}
  '';

  preambleWithEncrypt = ''
    ${parseIdentity}
    on_exit() {
      ${syncRemotes}
      ${state.encrypt}
      ${cleanupWithPlan}
    }
    trap on_exit EXIT
    ${vars.decrypt}
  '';

  inherit (import ../keys.nix) tailscaleHost;

  tfRekey = ''
    ${parseIdentity}
    on_exit() { ${cleanup}; }
    trap on_exit EXIT
    ${state.decrypt}
    ${state.encrypt}
    ${syncRemotes}
    ${vars.decrypt}
    ${vars.encrypt}
  '';

  mkEnv = env:
    let
      remoteFile = remoteFiles.${env};
      outputKey = "${env}_droplet_ipv4";
      sshInputs = sshBuildInputs ++ [ pkgs.openssh ];

      resolveIp = ''
        ${parseIdentity}
        trap 'rm -f ${state.path}' EXIT
        ${state.decrypt}
        host_ip=$(jq -r '.outputs.${outputKey}.value' ${state.path})
        rm -f ${state.path}
        if [ -z "$host_ip" ] || [ "$host_ip" = "null" ]; then
          echo "ERROR: could not resolve IP from terraform output '${outputKey}' in ${state.path}" >&2
          exit 1
        fi
      '';

      resolveHost = ''
        ${parseIdentity}
        ${if env == "prod" then ''
          if tailscale status >/dev/null 2>&1; then
            host_ip="${tailscaleHost}"
          else
            ${remoteFile.decrypt}
            host_ip=$(cat ${remoteFile.path})
            rm -f ${remoteFile.path}
          fi
        '' else ''
          ${remoteFile.decrypt}
          host_ip=$(cat ${remoteFile.path})
          rm -f ${remoteFile.path}
        ''}
      '';

    in {
      inherit resolveIp resolveHost;

      "${env}Remote" = pkgs.writeShellApplication {
        name = "${env}-remote";
        runtimeInputs = sshInputs;
        text = ''
          ${resolveHost}
          exec ssh -i "$identity" "root@$host_ip" "$@"
        '';
      };

      "${env}Status" = pkgs.writeShellApplication {
        name = "${env}-status";
        runtimeInputs = sshBuildInputs ++ [ pkgs.openssh pkgs.curl pkgs.jq ];
        text = ''
          ${resolveHost}
          export identity host_ip ENV="${env}"
          exec bash scripts/status.sh "$@"
        '';
      };

      "${env}BotStart" = pkgs.writeShellApplication {
        name = "${env}-bot-start";
        runtimeInputs = sshInputs;
        text = ''
          ${resolveHost}
          echo "Starting st0x-hedge on ${env}..."
          ssh -i "$identity" "root@$host_ip" "mkdir -p /run/st0x && touch /run/st0x/st0x-hedge.ready && systemctl start st0x-hedge"
          ssh -i "$identity" "root@$host_ip" systemctl is-active st0x-hedge
        '';
      };

      "${env}BotStop" = pkgs.writeShellApplication {
        name = "${env}-bot-stop";
        runtimeInputs = sshInputs;
        text = ''
          ${resolveHost}
          echo "Stopping st0x-hedge on ${env}..."
          ssh -i "$identity" "root@$host_ip" "systemctl stop st0x-hedge && rm -f /run/st0x/st0x-hedge.ready"
          echo "Stopped."
        '';
      };

      "${env}BotRestart" = pkgs.writeShellApplication {
        name = "${env}-bot-restart";
        runtimeInputs = sshInputs;
        text = ''
          ${resolveHost}
          echo "Restarting st0x-hedge on ${env}..."
          ssh -i "$identity" "root@$host_ip" "mkdir -p /run/st0x && touch /run/st0x/st0x-hedge.ready && systemctl restart st0x-hedge"
          ssh -i "$identity" "root@$host_ip" systemctl is-active st0x-hedge
        '';
      };

      "${env}Dashboard" = pkgs.writeShellApplication {
        name = "${env}-dashboard";
        runtimeInputs = sshBuildInputs ++ [ pkgs.openssh pkgs.bun ];
        text = ''
          ${resolveHost}
          export identity host_ip ENV="${env}"
          exec bash scripts/dashboard.sh "$@"
        '';
      };
    };

  envResults = builtins.listToAttrs (map (env: {
    name = env;
    value = mkEnv env;
  }) environments);

  perEnv =
    builtins.mapAttrs (_: result: { inherit (result) resolveIp resolveHost; })
    envResults;

  envPkgs = builtins.foldl' (acc: env:
    acc // builtins.removeAttrs envResults.${env} [ "resolveIp" "resolveHost" ])
    { } environments;

in {
  inherit buildInputs sshBuildInputs parseIdentity tfRekey;
  inherit perEnv;

  packages = {
    tfInit = rainix.mkTask.${system} {
      name = "tf-init";
      additionalBuildInputs = buildInputs;
      body = ''
        ${preamble}
        terraform -chdir=infra init "$@"
      '';
    };

    tfPlan = rainix.mkTask.${system} {
      name = "tf-plan";
      additionalBuildInputs = buildInputs;
      body = ''
        ${preamble}
        ${state.decrypt}
        terraform -chdir=infra plan -out=tfplan "$@"
      '';
    };

    tfApply = rainix.mkTask.${system} {
      name = "tf-apply";
      additionalBuildInputs = buildInputs;
      body = ''
        ${preambleWithEncrypt}
        ${state.decrypt}
        terraform -chdir=infra apply "$@" tfplan
      '';
    };

    tfDestroy = rainix.mkTask.${system} {
      name = "tf-destroy";
      additionalBuildInputs = buildInputs;
      body = ''
        ${preambleWithEncrypt}
        ${state.decrypt}
        terraform -chdir=infra destroy "$@"
      '';
    };

    tfEditVars = rainix.mkTask.${system} {
      name = "tf-edit-vars";
      additionalBuildInputs = buildInputs;
      body = ''
        ${parseIdentity}
        on_exit() { rm -f ${vars.path}; }
        trap on_exit EXIT

        ${vars.decrypt}
        ''${EDITOR:-vi} ${vars.path}
        ${vars.encrypt}
      '';
    };
  } // envPkgs;
}
