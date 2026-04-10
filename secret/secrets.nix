let
  inherit (import ../keys.nix) roles;
  services = import ../services.nix;
  lib = import <nixpkgs/lib>;
  # Deduplicate keys across environments (st0x-op appears in both roles)
  allServiceKeys = lib.unique (roles.prod.service ++ roles.staging.service);
  # Service secrets are encrypted to both environments' service roles.
  # Each environment's deploy.nix decrypts with its own host key.
  # When staging needs different secrets, add staging-specific entries below.
  serviceSecrets = builtins.listToAttrs (map (name: {
    name = services.${name}.encryptedSecret;
    value.publicKeys = allServiceKeys;
  }) (builtins.attrNames services));

in serviceSecrets // {
  "cli.toml.age".publicKeys = allServiceKeys;
  "tailscale-authkey.age".publicKeys = allServiceKeys;
}
