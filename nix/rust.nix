{ pkgs, rustPlatform, sqlx-cli }:

let
  # Requires --impure. Submodules must be checked out and prepSolArtifacts
  # must have been run before building.
  libDir = builtins.path {
    path = builtins.getEnv "PWD" + "/lib";
    name = "lib";
  };
in rustPlatform.buildRustPackage {
  pname = "st0x-liquidity";
  version = "0.1.0";

  src = ../.;

  cargoLock = {
    lockFile = ../Cargo.lock;
    outputHashes = {
      "sqlite-es-0.1.0" = "sha256-Pf9nBYz2glSuEvBXnH0+5yqs+ZAOhd7xVTByWt6FMm0=";
      "rain-error-decoding-0.1.0" =
        "sha256-dDsvRkrGXhfoFunvk6fwP+12fSsjiWYoxz/CzVVGpHA=";
      "wasm-bindgen-utils-0.0.10" =
        "sha256-MkuPc9mWAmry5Yzjph4/IbaIvjevFUerji1lipLUK4g=";
    };
  };

  nativeBuildInputs = [ sqlx-cli pkgs.pkg-config ];

  buildInputs = [ pkgs.openssl pkgs.sqlite ]
    ++ pkgs.lib.optionals pkgs.stdenv.hostPlatform.isDarwin
    [ pkgs.apple-sdk_15 ];

  preBuild = ''
    set -eo pipefail
    rm -rf lib
    ln -s ${libDir} lib

    export DATABASE_URL="sqlite:$TMPDIR/build.db"
    sqlx db create
    sqlx migrate run --source migrations

    # sqlite-es uses sqlx::migrate!("../../migrations") relative to its
    # vendored source. Place our migrations where it expects them.
    ln -sf "$(pwd)/migrations" "$NIX_BUILD_TOP/migrations"
  '';

  cargoBuildFlags = [ "--bin" "server" ];

  doCheck = true;
  cargoTestFlags = [ "--bin" "server" ];

  meta = {
    description = "st0x liquidity market making system";
    homepage = "https://github.com/ST0x-Technology/st0x.liquidity";
  };
}
