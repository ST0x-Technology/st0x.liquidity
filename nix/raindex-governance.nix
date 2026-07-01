{
  pkgs,
  mkAbi,
  src,
}:

# raindex.governance ships RaindexInventory, the shared-inventory proxy that
# owns Raindex orders/vaults on behalf of multiple venue adapters (Bebop hook,
# univ4 hook) and the liquidity bot. deposit4/withdraw4 have drop-in identical
# signatures to IRaindexV6, plus OperatorDeposit/OperatorWithdraw events keyed
# by the operator address so the bot can watch every venue-driven vault delta
# from a single contract.
#
# Same Soldeer vendoring pattern as rain-orderbook.nix — deterministic fetch of
# the pinned closure so `forge build` runs offline against the sandbox.

let
  soldeerLock = builtins.fromTOML (builtins.readFile (src + "/soldeer.lock"));

  unpackDep =
    dep:
    let
      archive = pkgs.fetchurl {
        inherit (dep) url;
        sha256 = dep.checksum;
        name = pkgs.lib.strings.sanitizeDerivationName "${dep.name}-${dep.version}.zip";
      };
    in
    ''
      mkdir -p "$out/${dep.name}-${dep.version}"
      unzip -q ${archive} -d "$out/${dep.name}-${dep.version}"
    '';

  soldeerDeps = pkgs.runCommand "raindex-governance-soldeer-deps" {
    nativeBuildInputs = [ pkgs.unzip ];
  } ("mkdir -p $out\n" + pkgs.lib.concatMapStrings unpackDep soldeerLock.dependencies);

  abi = mkAbi {
    pname = "raindex-governance-abi";
    inherit src;

    buildPhase = ''
      runHook preBuild
      export HOME="$TMPDIR"
      cp -r ${soldeerDeps} dependencies
      chmod -R +w dependencies
      forge build
      runHook postBuild
    '';

    installPhase = ''
      runHook preInstall
      mkdir -p $out
      cp -r out/. $out/
      runHook postInstall
    '';
  };

in
rec {
  inherit abi soldeerDeps;

  abiEnv = {
    ST0X_RAINDEX_INVENTORY_ABI = "${abi}/RaindexInventory.sol/RaindexInventory.json";
  };
}
