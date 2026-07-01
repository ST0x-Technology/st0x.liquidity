let
  inherit (import ../keys.nix) roles;
  # services.nix is `{ lib }: { byName, enabled, carriesConfig, hasUnit }`. These
  # rules read only the `byName` map and the pure `carriesConfig` predicate;
  # `enabled` (the sole consumer of `lib`) is never forced here, so a stub `lib`
  # keeps this rules file evaluable under ragenix's pure, NIX_PATH-free eval.
  servicesModule = import ../services.nix {
    lib = {
      filterAttrs = _: attrs: attrs;
    };
  };
  inherit (servicesModule) byName carriesConfig;
  # Deduplicate keys across environments (st0x-op appears in both roles).
  # Uses builtins only so this rules file stays pure — ragenix evaluates it
  # without NIX_PATH, so `import <nixpkgs/lib>` would be fragile.
  dedup = builtins.foldl' (acc: key: if builtins.elem key acc then acc else acc ++ [ key ]) [ ];
  allServiceKeys = dedup (roles.prod.service ++ roles.staging.service);
  # Service secrets are encrypted to both environments' service roles.
  # Each environment's deploy.nix decrypts with its own host key.
  # When staging needs different secrets, add staging-specific entries below.
  # `carriesConfig` (services.nix) selects the kinds with an encryptedSecret
  # ("st0x"/"cli"); "plain" and "static" (datasette, dashboard) have none.
  secretServiceNames = builtins.filter (name: carriesConfig byName.${name}.kind) (
    builtins.attrNames byName
  );
  serviceSecrets = builtins.listToAttrs (
    map (name: {
      name = byName.${name}.encryptedSecret;
      value.publicKeys = allServiceKeys;
    }) secretServiceNames
  );

in
serviceSecrets
// {
  "tailscale-authkey-prod.age".publicKeys = roles.prod.service;
  "tailscale-authkey-staging.age".publicKeys = roles.staging.service;
}
