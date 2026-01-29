{ pkgs, bun2nix, codegen }:

let bunDeps = bun2nix.fetchBunDeps { bunNix = ../dashboard/bun.nix; };
in pkgs.stdenv.mkDerivation {
  pname = "st0x-dashboard";
  version = "0.1.0";

  src = ../.;

  nativeBuildInputs = [ bun2nix.hook pkgs.bun ];

  inherit bunDeps;

  bunRoot = "dashboard";

  dontUseBunBuild = true;
  dontUseBunCheck = true;
  dontUseBunInstall = true;
  dontRunLifecycleScripts = true;

  buildPhase = ''
    set -eo pipefail

    # Generate TypeScript types from Rust definitions
    ${codegen}/bin/codegen

    cd dashboard
    bun run build
  '';

  installPhase = ''
    cp -r build $out
  '';

  meta = {
    description = "st0x liquidity dashboard";
    homepage = "https://github.com/ST0x-Technology/st0x.liquidity";
  };
}
