{
  pkgs,
  ragenix,
  system,
  environments,
}:

let
  buildInputs = [
    pkgs.terraform
    pkgs.rage
    pkgs.jq
    ragenix.packages.${system}.default
  ];

  sshBuildInputs = [ pkgs.rage ];

  tfPlanFile = "infra/tfplan";

  mkEncrypted =
    { file, role }:
    {
      path = file;
      agePath = "${file}.age";
      decrypt = ''
        if [ -f ${file}.age ]; then
          if [ -z "''${identity:-}" ]; then
            echo "ERROR: decrypting ${file}.age requires an identity. Options:" >&2
            echo "  -i <path>                    explicit key" >&2
            echo "  SSH_IDENTITY=<path>          env var" >&2
            echo "  --op op://vault/item         1Password CLI" >&2
            echo "  ~/.ssh/id_ed25519            default key" >&2
            exit 1
          fi
          rage -d -i "$identity" ${file}.age > ${file}
        fi
      '';
      encrypt = ''
        if [ -f ${file} ]; then
          nix eval --raw --file ${../keys.nix} roles.${role} --apply 'builtins.concatStringsSep "\n"' \
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
  mkRemote =
    env: sshRole:
    mkEncrypted {
      file = "infra/.remote-${env}";
      role = sshRole;
    };

  remoteFiles = builtins.listToAttrs (
    map (env: {
      name = env;
      value = mkRemote env "${env}.ssh";
    }) environments
  );

  # Callers must invoke _cleanup_identity in their own cleanup/on_exit,
  # or call it explicitly before exec, to remove the temporary key file.
  parseIdentity = ''
    set -eo pipefail

    _identity_tmpfile=""
    _cleanup_identity() { [ -n "$_identity_tmpfile" ] && rm -f "$_identity_tmpfile"; }

    if [ "''${1:-}" = "--op" ]; then
      if [ -z "''${2:-}" ]; then
        echo "ERROR: --op requires an op:// URI" >&2
        exit 1
      fi
      _op_uri="$2"
      shift 2
      _op_args=()
      if [ "''${1:-}" = "--op-account" ]; then
        if [ -z "''${2:-}" ]; then
          echo "ERROR: --op-account requires an account" >&2
          exit 1
        fi
        _op_args+=(--account "$2")
        shift 2
      fi
      _identity_tmpfile="$(mktemp)"
      chmod 600 "$_identity_tmpfile"
      _op="$(command -v op 2>/dev/null || echo /opt/homebrew/bin/op)"
      if [ ! -x "$_op" ]; then
        echo "ERROR: 1Password CLI (op) not found" >&2
        exit 1
      fi
      "$_op" read "$_op_uri" "''${_op_args[@]}" > "$_identity_tmpfile"
      identity="$_identity_tmpfile"
    elif [ "''${1:-}" = "-i" ]; then
      if [ -z "''${2:-}" ]; then
        echo "ERROR: identity is empty -- pass -i <path> or set a default" >&2
        exit 1
      fi
      identity="$2"
      shift 2
    elif [ -n "''${SSH_IDENTITY:-}" ]; then
      identity="$SSH_IDENTITY"
    elif [ -f "$HOME/.ssh/id_ed25519" ]; then
      identity="$HOME/.ssh/id_ed25519"
    else
      identity=""
    fi
  '';

  cleanup = "rm -f ${state.path} ${state.path}.backup ${vars.path}";
  cleanupWithPlan = "${cleanup} ${tfPlanFile}";

  syncRemotes = ''
    if [ -f ${state.path} ]; then
      ${builtins.concatStringsSep "\n" (
        map (
          env:
          let
            rf = remoteFiles.${env};
          in
          ''
            jq -r '.outputs.${env}_droplet_ipv4.value // empty' ${state.path} > ${rf.path} || true
            if [ -s ${rf.path} ]; then
              ${rf.encrypt}
            else
              rm -f ${rf.agePath}
            fi
            rm -f ${rf.path}
          ''
        ) environments
      )}
    fi
  '';

  preamble = ''
    ${parseIdentity}
    on_exit() { ${cleanup}; _cleanup_identity; }
    trap on_exit EXIT
    ${vars.decrypt}
  '';

  preambleWithEncrypt = ''
    ${parseIdentity}
    on_exit() {
      ${syncRemotes}
      ${state.encrypt}
      ${cleanupWithPlan}
      _cleanup_identity
    }
    trap on_exit EXIT
    ${vars.decrypt}
  '';

  inherit (import ../keys.nix) tailscaleHost;

  mkEnv =
    env:
    let
      remoteFile = remoteFiles.${env};
      outputKey = "${env}_droplet_ipv4";
      sshInputs = sshBuildInputs ++ [ pkgs.openssh ];

      resolveIp = ''
        ${parseIdentity}
        trap 'rm -f ${state.path}; _cleanup_identity' EXIT
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
        if tailscale status >/dev/null 2>&1; then
          host_ip="${tailscaleHost.${env}}"
        else
          echo "Tailscale not connected, resolving ${env} host via encrypted IP cache..." >&2
          ${remoteFile.decrypt}
          host_ip=$(cat ${remoteFile.path})
          rm -f ${remoteFile.path}
        fi
      '';

    in
    {
      inherit resolveIp resolveHost;

      "${env}Remote" = pkgs.writeShellApplication {
        name = "${env}-remote";
        runtimeInputs = sshInputs;
        text = ''
          ${resolveHost}
          trap _cleanup_identity EXIT
          # shellcheck disable=SC2029
          exec ssh ''${identity:+-i "$identity"} "root@$host_ip" "$@"
        '';
      };

      "${env}Status" = pkgs.writeShellApplication {
        name = "${env}-status";
        runtimeInputs = sshBuildInputs ++ [
          pkgs.openssh
          pkgs.nushell
        ];
        text = ''
          ${resolveHost}
          trap _cleanup_identity EXIT
          export identity host_ip
          nu scripts/status.nu "${env}" "$@"
        '';
      };

      # Downloads a *consistent* copy of the live database: `scp`-ing
      # /mnt/data/st0x-hedge.db directly (as ${env}-status does for its
      # bundled DB) can race the bot's live writes and produce a torn,
      # corrupt file -- SQLite's file format has no atomicity guarantee
      # against a plain file copy of a database still being written to.
      # `VACUUM INTO` runs a normal read transaction against the live DB and
      # writes a guaranteed-consistent copy server-side first; only that
      # copy gets shipped over the wire, then deleted from the remote host.
      "${env}DbSnapshot" = pkgs.writeShellApplication {
        name = "${env}-db-snapshot";
        runtimeInputs = sshInputs ++ [ pkgs.coreutils ];
        text = ''
          ${resolveHost}
          trap _cleanup_identity EXIT

          ssh_remote() {
            # shellcheck disable=SC2029
            ssh ''${identity:+-i "$identity"} "root@$host_ip" "$@"
          }

          db_path="/mnt/data/st0x-hedge.db"
          remote_snapshot="/tmp/st0x-hedge-snapshot-$(date -u +%Y%m%d%H%M%S).db"
          out_dir="''${1:-./.tmp/$(date -u +%Y-%m-%d_%H-%M-%S)-${env}-db-snapshot}"
          mkdir -p "$out_dir"
          local_snapshot="$out_dir/st0x-hedge.db"

          echo "Taking a consistent snapshot of ${env}'s live database..." >&2
          ssh_remote "sqlite3 $db_path \"VACUUM INTO '$remote_snapshot'\""

          cleanup_remote_snapshot() {
            ssh_remote "rm -f $remote_snapshot" || true
          }
          trap 'cleanup_remote_snapshot; _cleanup_identity' EXIT

          echo "Downloading snapshot to $local_snapshot..." >&2
          scp ''${identity:+-i "$identity"} "root@$host_ip:$remote_snapshot" "$local_snapshot"

          echo "$local_snapshot"
        '';
      };

      "${env}BotStart" = pkgs.writeShellApplication {
        name = "${env}-bot-start";
        runtimeInputs = sshInputs;
        text = ''
          ${resolveHost}
          trap _cleanup_identity EXIT
          echo "Starting st0x-hedge on ${env}..."
          ssh ''${identity:+-i "$identity"} "root@$host_ip" "mkdir -p /run/st0x && touch /run/st0x/st0x-hedge.ready && systemctl start st0x-hedge"
          ssh ''${identity:+-i "$identity"} "root@$host_ip" systemctl is-active st0x-hedge
        '';
      };

      "${env}BotStop" = pkgs.writeShellApplication {
        name = "${env}-bot-stop";
        runtimeInputs = sshInputs;
        text = ''
          ${resolveHost}
          trap _cleanup_identity EXIT
          echo "Stopping st0x-hedge on ${env}..."
          ssh ''${identity:+-i "$identity"} "root@$host_ip" "systemctl stop st0x-hedge && rm -f /run/st0x/st0x-hedge.ready"
          echo "Stopped."
        '';
      };

      "${env}BotRestart" = pkgs.writeShellApplication {
        name = "${env}-bot-restart";
        runtimeInputs = sshInputs;
        text = ''
          ${resolveHost}
          trap _cleanup_identity EXIT
          echo "Restarting st0x-hedge on ${env}..."
          ssh ''${identity:+-i "$identity"} "root@$host_ip" "mkdir -p /run/st0x && touch /run/st0x/st0x-hedge.ready && systemctl restart st0x-hedge"
          ssh ''${identity:+-i "$identity"} "root@$host_ip" systemctl is-active st0x-hedge
        '';
      };

      "${env}DbReset" = pkgs.writeShellApplication {
        name = "${env}-db-reset";
        runtimeInputs = sshInputs ++ [
          pkgs.curl
          pkgs.python3
        ];
        text = ''
          ${resolveHost}
          trap _cleanup_identity EXIT

          stay_stopped=false
          for arg in "$@"; do
            case "$arg" in
              --yes) ;;
              --stopped) stay_stopped=true ;;
              *)
                echo "Unknown flag: $arg" >&2
                echo "Usage: ${env}-db-reset --yes [--stopped]" >&2
                exit 1
                ;;
            esac
          done

          if ! printf '%s\n' "$@" | grep -qx -- '--yes'; then
            echo "Refusing destructive reset without --yes" >&2
            echo "Usage: ${env}-db-reset --yes [--stopped]" >&2
            exit 1
          fi

          ssh_remote() {
            # shellcheck disable=SC2029
            ssh ''${identity:+-i "$identity"} "root@$host_ip" "$@"
          }

          _restart_service() {
            if [ "$stay_stopped" = false ]; then
              echo "Ensuring st0x-hedge is restarted on ${env}..." >&2
              ssh_remote "mkdir -p /run/st0x && touch /run/st0x/st0x-hedge.ready && systemctl start st0x-hedge" || true
            fi
            # datasette holds the pre-reset DB inode open; recreating the DB
            # leaves it serving the deleted file forever. Reopen the live inode
            # on every exit path, even under --stopped. The marker must be
            # touched first: datasette's ConditionPathExists gates the start, so
            # a bare restart with an absent marker silently stops it (exit 0).
            echo "Restarting datasette on ${env}..." >&2
            ssh_remote "mkdir -p /run/st0x && touch /run/st0x/datasette.ready && systemctl restart datasette" \
              || echo "WARNING: datasette restart failed on ${env}; it may still serve the deleted inode" >&2
            _cleanup_identity
          }
          trap _restart_service EXIT

          config="/run/st0x/st0x-hedge.config"
          db_path="/mnt/data/st0x-hedge.db"
          backup_dir="/mnt/data/backups/$(date -u +%Y-%m-%d_%H%M%S)"

          echo "Fetching latest Base block..."
          latest_block=$(curl -sf https://mainnet.base.org \
            -X POST -H "Content-Type: application/json" \
            -d '{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}' \
            | python3 -c "import sys,json; print(int(json.load(sys.stdin)['result'],16))")
          target_block=$((latest_block - 2))
          echo "Latest block: $latest_block, will backfill from: $target_block"

          echo "Stopping st0x-hedge on ${env}..."
          ssh_remote "systemctl stop st0x-hedge && rm -f /run/st0x/st0x-hedge.ready"

          echo "Backing up database to $backup_dir..."
          ssh_remote "mkdir -p $backup_dir && cp $db_path $db_path-wal $db_path-shm $db_path-journal $backup_dir/ 2>/dev/null; echo 'Backup done'"

          echo "Deleting live database..."
          ssh_remote "rm -f $db_path $db_path-wal $db_path-shm $db_path-journal"

          # Recreate the DB as an empty st0x-owned file immediately. Leaving the
          # path empty opens a window where any root process that touches it
          # (a manual sqlite3 inspection, a status query, monitoring) creates a
          # root:root database the st0x service can never write to, sending the
          # unit into a SQLITE_READONLY crash loop. SQLite treats a zero-length
          # file as a valid empty database, so the bot still runs migrations on
          # next start -- it just inherits an already-correctly-owned file.
          echo "Recreating empty database owned by st0x:st0x..."
          ssh_remote "install -o st0x -g st0x -m 644 /dev/null $db_path"

          echo "Updating deployment_block to $target_block..."
          ssh_remote "sed -i 's/^deployment_block = .*/deployment_block = $target_block/' $config"
          ssh_remote "grep -q '^deployment_block = $target_block$' $config"
          echo "Verified deployment_block=$target_block"

          if [ "$stay_stopped" = false ]; then
            echo "Starting st0x-hedge on ${env}..."
            ssh_remote "mkdir -p /run/st0x && touch /run/st0x/st0x-hedge.ready && systemctl start st0x-hedge"
            ssh_remote systemctl is-active st0x-hedge
          else
            echo "Bot left stopped (--stopped flag). Start manually with ${env}-bot-start."
          fi

          echo "Database reset complete. Backup at: $backup_dir"
        '';
      };

      "${env}Dashboard" = pkgs.writeShellApplication {
        name = "${env}-dashboard";
        runtimeInputs = sshBuildInputs ++ [
          pkgs.openssh
          pkgs.nushell
          pkgs.bun
        ];
        text = ''
          ${resolveHost}
          trap _cleanup_identity EXIT
          export identity host_ip
          nu scripts/dashboard.nu "${env}" "$@"
        '';
      };
    };

  envResults = builtins.listToAttrs (
    map (env: {
      name = env;
      value = mkEnv env;
    }) environments
  );

  perEnv = builtins.mapAttrs (_: result: { inherit (result) resolveIp resolveHost; }) envResults;

  envPkgs = builtins.foldl' (
    acc: env:
    acc
    // builtins.removeAttrs envResults.${env} [
      "resolveIp"
      "resolveHost"
    ]
  ) { } environments;

in
{
  inherit
    buildInputs
    sshBuildInputs
    parseIdentity
    ;
  inherit perEnv;

  packages = {
    tfInit = pkgs.writeShellApplication {
      name = "tf-init";
      runtimeInputs = buildInputs;
      text = ''
        ${preamble}
        terraform -chdir=infra init "$@"
      '';
    };

    tfPlan = pkgs.writeShellApplication {
      name = "tf-plan";
      runtimeInputs = buildInputs;
      text = ''
        ${preamble}
        ${state.decrypt}
        terraform -chdir=infra plan -out=tfplan "$@"
      '';
    };

    tfApply = pkgs.writeShellApplication {
      name = "tf-apply";
      runtimeInputs = buildInputs;
      text = ''
        ${preambleWithEncrypt}
        ${state.decrypt}
        terraform -chdir=infra apply "$@" tfplan
      '';
    };

    tfDestroy = pkgs.writeShellApplication {
      name = "tf-destroy";
      runtimeInputs = buildInputs;
      text = ''
        ${preambleWithEncrypt}
        ${state.decrypt}
        terraform -chdir=infra destroy "$@"
      '';
    };

    tfEditVars = pkgs.writeShellApplication {
      name = "tf-edit-vars";
      runtimeInputs = buildInputs;
      text = ''
        ${parseIdentity}
        on_exit() { rm -f ${vars.path}; _cleanup_identity; }
        trap on_exit EXIT

        ${vars.decrypt}
        ''${EDITOR:-vi} ${vars.path}
        ${vars.encrypt}
      '';
    };
  }
  // envPkgs;
}
