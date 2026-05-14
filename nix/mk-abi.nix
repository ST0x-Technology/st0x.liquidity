{
  pkgs,
  foundry,
  solc,
}:

# Builder for Solidity ABI artifacts via foundry. Feature modules under
# nix/ describe WHAT contracts they expose; this captures HOW to compile
# them so the per-feature files don't repeat foundry plumbing.

let
  defaultBuild = ''
    runHook preBuild
    export HOME="$TMPDIR"
    forge build
    runHook postBuild
  '';

  defaultInstall = ''
    runHook preInstall
    mkdir -p $out
    cp -r out/. $out/
    runHook postInstall
  '';

in
{
  inherit defaultBuild defaultInstall;

  mkAbi =
    {
      pname,
      src,
      buildPhase ? defaultBuild,
      installPhase ? defaultInstall,
    }:
    pkgs.stdenvNoCC.mkDerivation {
      inherit
        pname
        src
        buildPhase
        installPhase
        ;
      version = "0.0.0";
      nativeBuildInputs = [
        foundry
        solc
      ];
      FOUNDRY_SOLC = "${solc}/bin/solc-0.8.25";
      FOUNDRY_OFFLINE = "true";
    };
}
