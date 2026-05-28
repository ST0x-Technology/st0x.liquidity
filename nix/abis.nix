{
  pkgs,
  mkAbi,
  sources,
}:

# Aggregator for the per-feature ABI derivations and the merged env-var map
# they expose to cargo. Each entry under `nix/<feature>.nix` defines:
#   - `abi`: a derivation producing the artifact directory
#   - `abiEnv`: an attrset of ST0X_*_ABI -> JSON path
# We collect them here and assert their key sets are disjoint, so two features
# accidentally claiming the same env var fails flake evaluation loudly instead
# of silently letting whichever is merged last win.

let
  features = {
    forgeStd = import ./forge-std.nix {
      inherit mkAbi;
      src = sources.forge-std;
    };
    rainMathFloat = import ./rain-math-float.nix {
      src = sources.rain-math-float;
    };
    rainOrderbook = import ./rain-orderbook.nix {
      inherit mkAbi;
      src = sources.rain-orderbook;
    };
    pyth = import ./pyth.nix { inherit pkgs; };
  };

  envs = builtins.mapAttrs (_: feature: feature.abiEnv) features;

  collisions =
    pkgs.lib.foldl
      (
        acc: name:
        let
          keys = builtins.attrNames envs.${name};
          dups = builtins.filter (key: builtins.hasAttr key acc.seen) keys;
          fresh = builtins.filter (key: !(builtins.hasAttr key acc.seen)) keys;
          newSeen = pkgs.lib.genAttrs fresh (_: name);
        in
        {
          seen = acc.seen // newSeen;
          dups = acc.dups ++ map (key: "${key} (in ${acc.seen.${key}} and ${name})") dups;
        }
      )
      {
        seen = { };
        dups = [ ];
      }
      (builtins.attrNames envs);

  mergedEnv =
    if collisions.dups != [ ] then
      throw "abiEnv key collisions: ${builtins.concatStringsSep ", " collisions.dups}"
    else
      builtins.foldl' (acc: name: acc // envs.${name}) { } (builtins.attrNames envs);

in
{
  abis = builtins.mapAttrs (_: feature: feature.abi) features;
  abiEnv = mergedEnv;
}
