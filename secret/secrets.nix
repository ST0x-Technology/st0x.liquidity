let
  inherit (import ../keys.nix) roles;
  services = import ../services.nix;
  # Service secrets are encrypted to prod's service role since the same
  # service binary runs on both environments. Each environment's deploy.nix
  # decrypts with its own host key (which is in the respective service role).
  # When staging needs different secrets, add staging-specific entries below.
  serviceSecrets = builtins.listToAttrs (map (name: {
    name = services.${name}.encryptedSecret;
    value.publicKeys = roles.prod.service ++ roles.staging.service;
  }) (builtins.attrNames services));

in serviceSecrets // {
  "cli.toml.age".publicKeys = roles.prod.service ++ roles.staging.service;
  "tailscale-authkey.age".publicKeys = roles.prod.service
    ++ roles.staging.service;
}
