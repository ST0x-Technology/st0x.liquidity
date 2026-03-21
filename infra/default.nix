{ pkgs, ragenix, rainix, system }:

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
  remote = mkEncrypted {
    file = "infra/.remote";
    role = "ssh";
  };

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

  syncRemote = ''
    if [ -f ${state.path} ]; then
      jq -r '.outputs.droplet_ipv4.value' ${state.path} > ${remote.path}
      ${remote.encrypt}
      rm -f ${remote.path}
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
      ${syncRemote}
      ${state.encrypt}
      ${cleanupWithPlan}
    }
    trap on_exit EXIT
    ${vars.decrypt}
  '';

  resolveIp = ''
    ${parseIdentity}
    trap 'rm -f ${state.path}' EXIT
    ${state.decrypt}
    host_ip=$(jq -r '.outputs.droplet_ipv4.value' ${state.path})
    rm -f ${state.path}
  '';

  resolveHost = ''
    ${parseIdentity}
    ${remote.decrypt}
    host_ip=$(cat ${remote.path})
    rm -f ${remote.path}
  '';

  tfRekey = ''
    ${parseIdentity}
    on_exit() { ${cleanup}; }
    trap on_exit EXIT
    ${state.decrypt}
    ${state.encrypt}
    ${syncRemote}
    ${vars.decrypt}
    ${vars.encrypt}
  '';

in {
  inherit buildInputs sshBuildInputs parseIdentity resolveIp resolveHost
    tfRekey;

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
}
