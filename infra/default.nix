{ pkgs, ragenix, rainix, system }:

let
  buildInputs = [ pkgs.terraform ragenix.packages.${system}.default ];

  tfState = "infra/terraform.tfstate";
  tfVars = "infra/terraform.tfvars";

  preamble = ''
    set -eo pipefail

    identity=~/.ssh/id_ed25519
    if [ "$1" = "-i" ]; then
      identity="$2"
      shift 2
    fi

    rage -d -i "$identity" ${tfVars}.age > ${tfVars}
  '';

  decryptState = ''
    if [ -f ${tfState}.age ]; then
      rage -d -i "$identity" ${tfState}.age > ${tfState}
    fi
  '';

  encryptState = ''
    nix eval --raw --file ${
      ./keys.nix
    } roles.infra --apply 'builtins.concatStringsSep "\n"' \
      | rage -e -R /dev/stdin -o ${tfState}.age ${tfState}
  '';

  cleanup = "rm -f ${tfState} ${tfState}.backup ${tfVars}";

in {
  tfInit = rainix.mkTask.${system} {
    name = "tf-init";
    additionalBuildInputs = buildInputs;
    body = ''
      ${preamble}
      terraform -chdir=infra init "$@"
      ${cleanup}
    '';
  };

  tfPlan = rainix.mkTask.${system} {
    name = "tf-plan";
    additionalBuildInputs = buildInputs;
    body = ''
      ${preamble}
      ${decryptState}
      terraform -chdir=infra plan -out=tfplan "$@"
      ${cleanup}
    '';
  };

  tfApply = rainix.mkTask.${system} {
    name = "tf-apply";
    additionalBuildInputs = buildInputs;
    body = ''
      ${preamble}
      ${decryptState}
      terraform -chdir=infra apply "$@" tfplan
      ${encryptState}
      ${cleanup}
    '';
  };

  tfDestroy = rainix.mkTask.${system} {
    name = "tf-destroy";
    additionalBuildInputs = buildInputs;
    body = ''
      ${preamble}
      ${decryptState}
      terraform -chdir=infra destroy "$@"
      ${encryptState}
      ${cleanup}
    '';
  };
}
