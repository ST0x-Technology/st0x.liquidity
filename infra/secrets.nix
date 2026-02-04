{
  "terraform.tfstate.age".publicKeys = (import ../keys.nix).roles.infra;
  "terraform.tfvars.age".publicKeys = (import ../keys.nix).roles.infra;
}
