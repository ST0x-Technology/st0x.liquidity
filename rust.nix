{ pkgs, craneLib, abiEnv, rainMathFloatSrc }:

let
  # Cargo `rain-math-float` is a path-dep at `.tmp/rain-math-float/crates/float`.
  # Both the dev shell and crane builds materialise that path inside the
  # source tree, keeping nix and cargo on a single pinned source. Crane's
  # `mkDummySrc` walks the tree to find Cargo.toml files but strips
  # symlinks during cleaning, so we copy the source as a real directory
  # rather than symlinking it. Living under `.tmp/` keeps generated paths
  # off the repo root (see `Generated paths` in AGENTS.md).
  rainMathFloatPath = ".tmp/rain-math-float";

  withRainMathFloat = name: src:
    pkgs.runCommand name { } ''
      cp -rL --no-preserve=mode ${src} $out
      chmod -R u+w $out
      # Drop any .tmp/ from the source (e.g. a dev-shell symlink to
      # rain-math-float) so the second copy below doesn't nest into
      # $out/.tmp/rain-math-float/rain-math-float-patched/ and break the
      # cargo path-dep at .tmp/rain-math-float/crates/float.
      rm -rf $out/.tmp
      mkdir -p $(dirname $out/${rainMathFloatPath})
      cp -rL --no-preserve=mode ${rainMathFloatSrc} $out/${rainMathFloatPath}
    '';

  # Filter `.tmp/` out of the workspace source so dev-shell-materialised
  # paths (e.g. `.tmp/rain-math-float` symlink) don't get baked into the
  # nix build inputs.
  cleanedSrc = pkgs.lib.cleanSourceWith {
    src = pkgs.lib.cleanSource ./.;
    filter = path: _type: !(pkgs.lib.hasInfix "/.tmp" path);
  };

  fullSrc = withRainMathFloat "st0x-src" cleanedSrc;

  # Vendor cargo deps with git dependency hashes
  baseVendorDir = craneLib.vendorCargoDeps {
    src = fullSrc;
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

  # Build args without ABI env vars. Used for the deps-only derivation so an
  # ABI change doesn't bust the cached dependency artifacts -- third-party
  # deps don't reference any ST0X_*_ABI variable.
  depsArgs = {
    pname = "st0x-liquidity";
    version = "0.1.0";
    src = fullSrc;

    inherit cargoVendorDir;

    nativeBuildInputs = [ pkgs.pkg-config ];

    buildInputs = [ pkgs.openssl pkgs.sqlite ]
      ++ pkgs.lib.optionals pkgs.stdenv.hostPlatform.isDarwin
      [ pkgs.apple-sdk_15 ];

    # Use offline sqlx query verification so builds don't need a live database.
    # Each crate (ours + apalis-sqlite) ships its own .sqlx/ with prepared data.
    # Run `cargo sqlx prepare` after changing sqlx macros to regenerate.
    SQLX_OFFLINE = "true";
  };

  # Full build args for our crates: deps args plus ABI env vars consumed by
  # our build.rs / sol! macros.
  commonArgs = depsArgs // abiEnv;

  # Build only dependencies (cached separately from source changes).
  # Crane's mkDummySrc internally strips to manifests + dummy crate roots,
  # so we feed it fullSrc rather than pre-stripping ourselves -- our prior
  # manifest-only filter dropped src/lib.rs files for crates with implicit
  # `[lib]` detection, which broke the deps build.
  cargoArtifacts = craneLib.buildDepsOnly depsArgs;

in {
  # DTO crate for TypeScript codegen
  dto = craneLib.buildPackage (commonArgs // {
    pname = "st0x-dto";
    inherit cargoArtifacts;
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

    cargoExtraArgs =
      "--bin server --bin validate-config --features wallet-turnkey";
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

    cargoExtraArgs = "--bin cli --features wallet-turnkey";
    doCheck = false;

    postInstall = ''
      mv $out/bin/cli $out/bin/st0x-cli
    '';

    meta = {
      description = "st0x liquidity CLI";
      homepage = "https://github.com/ST0x-Technology/st0x.liquidity";
    };
  });

  # Float decoder used by status scripts to render Raindex vault balances
  decodeFloats = craneLib.buildPackage (commonArgs // {
    pname = "decode-floats";
    inherit cargoArtifacts;

    cargoExtraArgs = "--bin decode-floats";
    doCheck = false;

    meta = {
      description = "Decode Rain Float hex values to human-readable decimals";
      homepage = "https://github.com/ST0x-Technology/st0x.liquidity";
    };
  });
}
