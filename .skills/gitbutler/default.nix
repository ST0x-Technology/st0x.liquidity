{ pkgs, lib }:

let
  version = "0.19.3";
  build = "2869";
  baseUrl =
    "https://releases.gitbutler.com/releases/release/${version}-${build}";

  sources = {
    aarch64-darwin = {
      url = "${baseUrl}/macos/aarch64/GitButler.app.tar.gz";
      hash = "sha256-OCoXXvxzztSCiobTdJYMZV0A9aXe05vKVRLzvWTyIu4=";
    };
    x86_64-darwin = {
      url = "${baseUrl}/macos/x86_64/GitButler.app.tar.gz";
      hash = "sha256-kCCPyL3ZgVYMDtLVx4VDP3Gk39DntYyEdpnK13dW6ok=";
    };
    x86_64-linux = {
      url = "${baseUrl}/linux/x86_64/GitButler_${version}_amd64.deb";
      hash = "sha256-K7d5HO5dm/N0a2r5TaJh+lE0wrwi+GzDPer+n/YOtL0=";
    };
    aarch64-linux = {
      url = "${baseUrl}/linux/aarch64/GitButler_${version}_arm64.deb";
      hash = "sha256-mwFALCtltwz9SqN8MrPKMTEjHiyTFKiI8kF47sXhyMA=";
    };
  };

  source = sources.${pkgs.stdenv.hostPlatform.system};

  meta = {
    description = "GitButler CLI";
    homepage = "https://gitbutler.com/cli";
    license = lib.licenses.unfree;
    platforms = builtins.attrNames sources;
    mainProgram = "but";
  };
in if pkgs.stdenv.hostPlatform.isDarwin then
  pkgs.stdenvNoCC.mkDerivation {
    pname = "gitbutler-cli";
    inherit version meta;
    src = pkgs.fetchurl { inherit (source) url hash; };
    sourceRoot = ".";
    nativeBuildInputs = [ pkgs.darwin.sigtool pkgs.darwin.cctools ];
    installPhase = ''
      mkdir -p $out/bin
      cp GitButler.app/Contents/MacOS/gitbutler-tauri $out/bin/but
      cp GitButler.app/Contents/MacOS/gitbutler-git-askpass $out/bin/
      cp GitButler.app/Contents/MacOS/gitbutler-git-setsid $out/bin/
      codesign -f -s - $out/bin/but
      codesign -f -s - $out/bin/gitbutler-git-askpass
      codesign -f -s - $out/bin/gitbutler-git-setsid
    '';
  }
else
  pkgs.stdenvNoCC.mkDerivation {
    pname = "gitbutler-cli";
    inherit version meta;
    src = pkgs.fetchurl { inherit (source) url hash; };
    sourceRoot = ".";
    nativeBuildInputs = [ pkgs.dpkg pkgs.autoPatchelfHook ];
    buildInputs = [ pkgs.stdenv.cc.cc.lib pkgs.openssl ];
    unpackPhase = "dpkg-deb -x $src .";
    installPhase = ''
      mkdir -p $out/bin
      cp usr/bin/gitbutler-tauri $out/bin/but
      cp usr/bin/gitbutler-git-askpass $out/bin/
      cp usr/bin/gitbutler-git-setsid $out/bin/
    '';
  }
