let
  inherit (import ../keys.nix) roles;
  # Deduplicate keys across the old environment rosters (st0x-op appears in
  # both). builtins only: ragenix evaluates this file pure and NIX_PATH-free.
  dedup = builtins.foldl' (acc: key: if builtins.elem key acc then acc else acc ++ [ key ]) [ ];
  allServiceKeys = dedup (roles.prod.service ++ roles.staging.service);
in
{
  # The one survivor of the droplet world: the dividend-ops (s01) CLI
  # secret, decrypted locally by a keyholder to run `dividend-bump` (see
  # docs/cli-ops.md). Recipients kept identical to the pre-retirement
  # roster so the existing ciphertext stays valid. Follow-up: re-home this
  # to Secret Manager next to the GCE mounts and delete the agenix path.
  "s01-issuer.toml.age".publicKeys = allServiceKeys;
}
