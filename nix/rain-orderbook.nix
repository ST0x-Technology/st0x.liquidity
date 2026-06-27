{
  pkgs,
  mkAbi,
  src,
}:

# rain.orderbook (now "raindex") dropped git submodules in favour of Soldeer.
# Soldeer fetches the Solidity dependency closure over the network, so we isolate
# that in a fixed-output derivation and let the offline `forge build` consume the
# vendored result for the orderbook's own contracts (RaindexV6, the subparser,
# ArbTest's test token, TOFUTokenDecimals).
#
# The rainlang interpreter stack (interpreter/store/parser/expression deployer)
# is NOT rebuilt here: those contracts embed codegen'd function-pointer tables
# whose jump offsets only match bytecode compiled with rainlang's own optimizer
# profile. Recompiling them under rain.orderbook's profile yields runtime
# `InvalidJump`s. We therefore consume rainlang's shipped canonical artifacts
# (under its Soldeer package's crates/abi), whose bytecode matches the
# deterministic "zoltu" deploy addresses the expression deployer hardcodes.

let
  # Vendor the Soldeer closure deterministically: every dependency is pinned in
  # soldeer.lock with its registry URL and the raw sha256 of its zip (the
  # `checksum` field), so we fetch each one individually rather than trusting a
  # non-reproducible `forge soldeer install`. Each archive unpacks at its root
  # into dependencies/<name>-<version>/, matching the repo's remappings.txt.
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

  soldeerDeps = pkgs.runCommand "raindex-soldeer-deps" { nativeBuildInputs = [ pkgs.unzip ]; } (
    "mkdir -p $out\n" + pkgs.lib.concatMapStrings unpackDep soldeerLock.dependencies
  );

  rainlangAbi = "${soldeerDeps}/rainlang-0.1.2/crates/abi";

  abi = mkAbi {
    pname = "rain-orderbook-abi";
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
    ST0X_IORDERBOOK_V6_ABI = "${abi}/IRaindexV6.sol/IRaindexV6.json";
    ST0X_ORDERBOOK_ABI = "${abi}/RaindexV6.sol/RaindexV6.json";
    ST0X_TEST_ERC20_ABI = "${abi}/ArbTest.sol/Token.json";
    ST0X_TOFU_TOKEN_DECIMALS_ABI = "${abi}/TOFUTokenDecimals.sol/TOFUTokenDecimals.json";
    ST0X_INTERPRETER_ABI = "${rainlangAbi}/RainlangInterpreter.json";
    ST0X_STORE_ABI = "${rainlangAbi}/RainlangStore.json";
    ST0X_PARSER_ABI = "${rainlangAbi}/RainlangParser.json";
    ST0X_DEPLOYER_ABI = "${rainlangAbi}/RainlangExpressionDeployer.json";
    ST0X_DEPLOYABLE_ERC20_ABI = "${rainlangAbi}/TestERC20.json";
  };
}
