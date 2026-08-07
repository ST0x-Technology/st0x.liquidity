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

  # Two-phase builder for projects that use soldeer for dependency management.
  # Phase 1 (soldeerDeps): FOD with network access — runs `forge soldeer install`
  #   and caches the resulting dependencies/ directory. Hash only changes when
  #   soldeer.lock changes, not when contracts change.
  # Phase 2 (main build): sandboxed — injects the vendored deps then runs
  #   `forge build` offline.
  #
  # To get soldeerDepsHash: set it to "sha256-AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
  # run `nix develop`, and replace with the hash printed in the error message.
  mkSoldeerAbi =
    {
      pname,
      src,
      soldeerDepsHash,
      installPhase ? defaultInstall,
    }:
    let
      soldeerDeps = pkgs.stdenvNoCC.mkDerivation {
        name = "${pname}-soldeer-deps";
        inherit src;
        outputHashAlgo = "sha256";
        outputHash = soldeerDepsHash;
        outputHashMode = "recursive";
        nativeBuildInputs = [
          foundry
          pkgs.git
          pkgs.cacert
        ];
        buildPhase = ''
          export HOME="$TMPDIR"
          export GIT_SSL_CAINFO="${pkgs.cacert}/etc/ssl/certs/ca-bundle.crt"
          export SSL_CERT_FILE="${pkgs.cacert}/etc/ssl/certs/ca-bundle.crt"
          forge soldeer install
        '';
        installPhase = ''
          cp -r dependencies $out
        '';
      };
    in
    pkgs.stdenvNoCC.mkDerivation {
      inherit
        pname
        src
        installPhase
        ;
      version = "0.0.0";
      nativeBuildInputs = [
        foundry
        solc
      ];
      FOUNDRY_SOLC = "${solc}/bin/solc-0.8.25";
      FOUNDRY_OFFLINE = "true";
      buildPhase = ''
        runHook preBuild
        export HOME="$TMPDIR"
        cp -r ${soldeerDeps}/. dependencies
        chmod -R u+w dependencies
        forge build
        runHook postBuild
      '';
    };
}
