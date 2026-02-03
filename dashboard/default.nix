{ pkgs, bun2nix }:

let bunDeps = bun2nix.fetchBunDeps { bunNix = ./bun.nix; };
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
