{ pkgs, craneLib, sqlx-cli, sol-build-inputs }:

let
  # Requires --impure. Submodules must be checked out and prepSolArtifacts
  # must have been run before building.
  libDir = builtins.path {
    path = builtins.getEnv "PWD" + "/lib";
    name = "lib";
  };

  # Vendor cargo deps with git dependency hashes
  baseVendorDir = craneLib.vendorCargoDeps {
    src = ./.;
    cargoLock = ./Cargo.lock;
    outputHashes = {
      "sqlite-es-0.1.0" = "sha256-Pf9nBYz2glSuEvBXnH0+5yqs+ZAOhd7xVTByWt6FMm0=";
      "rain-error-decoding-0.1.0" =
        "sha256-dDsvRkrGXhfoFunvk6fwP+12fSsjiWYoxz/CzVVGpHA=";
      "wasm-bindgen-utils-0.0.10" =
        "sha256-MkuPc9mWAmry5Yzjph4/IbaIvjevFUerji1lipLUK4g=";
    };
  };

  # sqlite-es uses sqlx::migrate!("../../migrations") which resolves inside
  # the vendor dir. Fetch migrations from st0x.issuance at the same commit
  # as Cargo.lock specifies for sqlite-es.
  cargoLock = builtins.fromTOML (builtins.readFile ./Cargo.lock);
  sqliteEsPackage = builtins.head
    (builtins.filter (p: p.name or "" == "sqlite-es") cargoLock.package);
  sqliteEsRev =
    builtins.head (builtins.match ".*#([a-f0-9]+)" sqliteEsPackage.source);

  sqliteEsMigrations = builtins.fetchGit {
    url = "https://github.com/ST0x-Technology/st0x.issuance";
    rev = sqliteEsRev;
  } + "/migrations";

  cargoVendorDir = pkgs.runCommand "vendor-with-migrations" { } ''
    cp -rL --no-preserve=mode ${baseVendorDir} $out

    # sqlite-es's ../../migrations resolves from crate root (sqlite-es-0.1.0/),
    # going up two levels to vendor root
    cp -r ${sqliteEsMigrations} "$out/migrations"

    # config.toml tells cargo where to find vendored crates. It contains
    # absolute nix store paths like:
    #   [source.nix-sources-c798c58f...]
    #   directory = "/nix/store/xxx-vendor-cargo-deps/c798c58f..."
    # We must update these to point to our wrapped vendor dir, otherwise
    # cargo will look in the original (immutable, no migrations) location.
    ${pkgs.gnused}/bin/sed -i "s|${baseVendorDir}|$out|g" $out/config.toml
  '';

  # Common arguments shared between deps and final build
  commonArgs = {
    pname = "st0x-liquidity";
    version = "0.1.0";

    src = ./.;

    inherit cargoVendorDir;

    nativeBuildInputs = [ sqlx-cli pkgs.pkg-config ];

    buildInputs = [ pkgs.openssl pkgs.sqlite ]
      ++ pkgs.lib.optionals pkgs.stdenv.hostPlatform.isDarwin
      [ pkgs.apple-sdk_15 ];

    # Submodules with path dependencies need lib/ symlinked early
    postUnpack = ''
      rm -rf $sourceRoot/lib
      ln -s ${libDir} $sourceRoot/lib
    '';
  };

  # Build only dependencies (cached separately from source changes)
  cargoArtifacts = craneLib.buildDepsOnly commonArgs;

  # sqlx needs DATABASE_URL at compile time for query checking
  # Only needed for final build and clippy, not deps
  sqlxSetup = ''
    set -eo pipefail

    export DATABASE_URL="sqlite:$TMPDIR/build.db"
    sqlx db create
    sqlx migrate run --source migrations
  '';

in {
  # Main package with all binaries
  package = craneLib.buildPackage (commonArgs // {
    inherit cargoArtifacts;

    preBuild = sqlxSetup;

    nativeCheckInputs = sol-build-inputs;

    doCheck = true;

    meta = {
      description = "st0x liquidity market making system";
      homepage = "https://github.com/ST0x-Technology/st0x.liquidity";
    };
  });

  # Clippy check (reuses cached deps)
  clippy = craneLib.cargoClippy (commonArgs // {
    inherit cargoArtifacts;

    preBuild = sqlxSetup;

    cargoClippyExtraArgs = "--all-targets --all-features -- -D clippy::all";
  });
}
