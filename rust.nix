{ pkgs, craneLib }:

let
  # Requires --impure. Submodules must be checked out and prepSolArtifacts
  # must have been run before building.
  libDir = builtins.path {
    path = builtins.getEnv "PWD" + "/lib";
    name = "lib";
  };

  # Cargo manifests only -- deps derivation hash changes only when dependencies change
  depsSrc = pkgs.lib.cleanSourceWith {
    src = ./.;
    filter = path: type:
      let base = builtins.baseNameOf path;
      in type == "directory" || base == "Cargo.toml" || base == "Cargo.lock";
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

    nativeBuildInputs = [ pkgs.pkg-config ];

    buildInputs = [ pkgs.openssl pkgs.sqlite ]
      ++ pkgs.lib.optionals pkgs.stdenv.hostPlatform.isDarwin
      [ pkgs.apple-sdk_15 ];

    # Use offline sqlx query verification so builds don't need a live database.
    # Each crate (ours + apalis-sqlite) ships its own .sqlx/ with prepared data.
    # Run `cargo sqlx prepare` after changing sqlx macros to regenerate.
    SQLX_OFFLINE = "true";

    # Submodules with path dependencies need lib/ symlinked early
    postUnpack = ''
      rm -rf $sourceRoot/lib
      ln -s ${libDir} $sourceRoot/lib
    '';
  };

  # Build only dependencies (cached separately from source changes)
  cargoArtifacts = craneLib.buildDepsOnly (commonArgs // { src = depsSrc; });

in {
  # DTO crate for TypeScript codegen - no sqlx deps needed
  dto = craneLib.buildPackage (commonArgs // {
    pname = "st0x-dto";
    cargoExtraArgs = "-p st0x-dto";
    doCheck = false;

    meta = {
      description = "st0x DTO types for TypeScript codegen";
      homepage = "https://github.com/ST0x-Technology/st0x.liquidity";
    };
  });

  # Server binary for deployment
  package = craneLib.buildPackage (commonArgs // {
    inherit cargoArtifacts;

    cargoExtraArgs = "--bin server --features wallet-private-key";
    doCheck = false;

    meta = {
      description = "st0x liquidity market making server";
      homepage = "https://github.com/ST0x-Technology/st0x.liquidity";
    };
  });

  # CLI binary for remote operations
  cli = craneLib.buildPackage (commonArgs // {
    pname = "st0x-cli";
    inherit cargoArtifacts;

    cargoExtraArgs = "--bin cli --features wallet-private-key";
    doCheck = false;

    meta = {
      description = "st0x liquidity CLI";
      homepage = "https://github.com/ST0x-Technology/st0x.liquidity";
    };
  });
}
