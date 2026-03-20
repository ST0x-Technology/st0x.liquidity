{ pkgs, ragenix, rainix, system }:

let
  buildInputs =
    [ pkgs.terraform pkgs.rage pkgs.jq ragenix.packages.${system}.default ];

  tfState = "infra/terraform.tfstate";
  tfVars = "infra/terraform.tfvars";
  tfPlanFile = "infra/tfplan";

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

  decryptState = ''
    if [ -f ${tfState}.age ]; then
      rage -d -i "$identity" ${tfState}.age > ${tfState}
    fi
  '';

  encryptState = ''
    if [ -f ${tfState} ]; then
      nix eval --raw --file ${
        ../keys.nix
      } roles.infra --apply 'builtins.concatStringsSep "\n"' \
        | rage -e -R /dev/stdin -o ${tfState}.age ${tfState}
    fi
  '';

  cleanup = "rm -f ${tfState} ${tfState}.backup ${tfVars}";
  cleanupWithPlan = "${cleanup} ${tfPlanFile}";

  preamble = ''
    ${parseIdentity}
    on_exit() { ${cleanup}; }
    trap on_exit EXIT
    ${decryptVars}
  '';

  preambleWithEncrypt = ''
    ${parseIdentity}
    on_exit() {
      ${encryptState}
      ${cleanupWithPlan}
    }
    trap on_exit EXIT
    ${decryptVars}
  '';

  resolveIp = ''
    ${parseIdentity}
    trap 'rm -f ${tfState}' EXIT
    ${decryptState}
    host_ip=$(jq -r '.outputs.droplet_ipv4.value' ${tfState})
    rm -f ${tfState}
  '';

  decryptVars = ''
    rage -d -i "$identity" ${tfVars}.age > ${tfVars}
  '';

  encryptVars = ''
    nix eval --raw --file ${
      ../keys.nix
    } roles.infra --apply 'builtins.concatStringsSep "\n"' \
      | rage -e -R /dev/stdin -o ${tfVars}.age ${tfVars}
  '';

  tfRekey = ''
    ${parseIdentity}
    on_exit() { ${cleanup}; }
    trap on_exit EXIT
    ${decryptState}
    ${encryptState}
    ${decryptVars}
    ${encryptVars}
  '';

in {
  inherit buildInputs parseIdentity resolveIp tfRekey;

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
      ${decryptState}
      terraform -chdir=infra plan -out=tfplan "$@"
    '';
  };

  tfApply = rainix.mkTask.${system} {
    name = "tf-apply";
    additionalBuildInputs = buildInputs;
    body = ''
      ${preambleWithEncrypt}
      ${decryptState}
      terraform -chdir=infra apply "$@" tfplan
    '';
  };

  tfDestroy = rainix.mkTask.${system} {
    name = "tf-destroy";
    additionalBuildInputs = buildInputs;
    body = ''
      ${preambleWithEncrypt}
      ${decryptState}
      terraform -chdir=infra destroy "$@"
    '';
  };

  tfEditVars = rainix.mkTask.${system} {
    name = "tf-edit-vars";
    additionalBuildInputs = buildInputs;
    body = ''
      ${parseIdentity}
      on_exit() { rm -f ${tfVars}; }
      trap on_exit EXIT

      ${decryptVars}
      ''${EDITOR:-vi} ${tfVars}
      ${encryptVars}
    '';
  };
}
