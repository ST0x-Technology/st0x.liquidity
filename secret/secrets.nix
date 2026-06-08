let
  inherit (import ../keys.nix) roles;
  # services.nix is `{ lib }: { byName, enabled }`. These rules only read each
  # service's `kind`/`encryptedSecret` from the `byName` map; `enabled` (the
  # sole consumer of `lib`) is never forced here, so a stub `lib` keeps this
  # rules file evaluable under ragenix's pure, NIX_PATH-free eval.
  services =
    (import ../services.nix {
      lib = {
        filterAttrs = _: attrs: attrs;
      };
    }).byName;
  # Deduplicate keys across environments (st0x-op appears in both roles).
  # Uses builtins only so this rules file stays pure — ragenix evaluates it
  # without NIX_PATH, so `import <nixpkgs/lib>` would be fragile.
  dedup = builtins.foldl' (acc: key: if builtins.elem key acc then acc else acc ++ [ key ]) [ ];
  allServiceKeys = dedup (roles.prod.service ++ roles.staging.service);
  # Service secrets are encrypted to both environments' service roles.
  # Each environment's deploy.nix decrypts with its own host key.
  # When staging needs different secrets, add staging-specific entries below.
  # Only services with kind = "st0x" carry an encryptedSecret; "plain" and
  # "static" kinds (e.g. datasette, dashboard) have no secrets to manage.
  st0xServiceNames = builtins.filter (name: services.${name}.kind == "st0x") (
    builtins.attrNames services
  );
  serviceSecrets = builtins.listToAttrs (
    map (name: {
      name = services.${name}.encryptedSecret;
      value.publicKeys = allServiceKeys;
    }) st0xServiceNames
  );

in
serviceSecrets
// {
  "tailscale-authkey-prod.age".publicKeys = roles.prod.service;
  "tailscale-authkey-staging.age".publicKeys = roles.staging.service;
}
