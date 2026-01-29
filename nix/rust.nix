{ pkgs, rustPlatform, sol-build-inputs, sqlx-cli, repoUrl, self }:

let
  srcWithSubmodules = let
    fetched = builtins.fetchGit {
      url = repoUrl;
      inherit (self) rev;
      submodules = true;
      shallow = true;
    };
  in pkgs.runCommand "src-with-submodules" { } ''
    set -euxo pipefail
    cp -r ${fetched} $out
    chmod -R u+w $out

    # Overlay local source on top, keeping lib/ (submodules) from the fetch
    for item in ${self}/*; do
      name=$(basename "$item")
      if [ "$name" != "lib" ]; then
        rm -rf "$out/$name"
        cp -r "$item" "$out/$name"
      fi
    done
  '';
in rustPlatform.buildRustPackage {
  pname = "st0x-liquidity";
  version = "0.1.0";

  src = srcWithSubmodules;

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

  nativeBuildInputs = sol-build-inputs ++ [ sqlx-cli pkgs.pkg-config ];

  buildInputs = [ pkgs.openssl pkgs.sqlite ]
    ++ pkgs.lib.optionals pkgs.stdenv.hostPlatform.isDarwin
    [ pkgs.apple-sdk_15 ];

  preBuild = ''
    set -euxo pipefail
    (cd lib/rain.orderbook/ && forge build)
    (cd lib/rain.orderbook/lib/rain.orderbook.interface/lib/rain.interpreter.interface/lib/rain.math.float/ && forge build)
    (cd lib/forge-std/ && forge build)
    (cd lib/pyth-crosschain/target_chains/ethereum/sdk/solidity/ && forge build)

    export DATABASE_URL="sqlite:$TMPDIR/build.db"
    sqlx db create
    sqlx migrate run --source migrations
  '';

  cargoBuildFlags = [ "--bin" "server" ];

  doCheck = true;
  cargoTestFlags = [ "--bin" "server" ];

  meta = {
    description = "st0x liquidity market making system";
    homepage = "https://github.com/ST0x-Technology/st0x.liquidity";
  };
}
