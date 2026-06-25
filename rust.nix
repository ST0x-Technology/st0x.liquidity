{
  pkgs,
  craneLib,
  abiEnv,
  rainMathFloatAbiEnv,
  rainMathFloat,
}:

let
  # Cargo `rain-math-float` is a path-dep at `.tmp/rain-math-float/crates/float`.
  # Both the dev shell and crane builds materialise that path inside the
  # source tree, keeping nix and cargo on a single pinned source. Crane's
  # `mkDummySrc` walks the tree to find Cargo.toml files but strips
  # symlinks during cleaning, so we copy the source as a real directory
  # rather than symlinking it. Living under `.tmp/` keeps generated paths
  # off the repo root (see `Generated paths` in AGENTS.md).
  rainMathFloatPath = ".tmp/rain-math-float";

  withRainMathFloat =
    name: src:
    pkgs.runCommand name { } ''
      cp -rL --no-preserve=mode ${src} $out
      chmod -R u+w $out
      # Drop any .tmp/ from the source (e.g. a dev-shell symlink to
      # rain-math-float) so the second copy below doesn't nest into
      # $out/.tmp/rain-math-float/<source-name>/ and break the cargo
      # path-dep at .tmp/rain-math-float/crates/float.
      rm -rf $out/.tmp
      mkdir -p $(dirname $out/${rainMathFloatPath})
      cp -rL --no-preserve=mode ${rainMathFloat} $out/${rainMathFloatPath}
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
  cargoVendorDir = craneLib.vendorCargoDeps {
    src = fullSrc;
    cargoLock = ./Cargo.lock;
    outputHashes = {
      "git+https://github.com/rainlanguage/rain.error#3d2ed70fb2f7c6156706846e10f163d1e493a8d3" =
        "sha256-dDsvRkrGXhfoFunvk6fwP+12fSsjiWYoxz/CzVVGpHA=";
      "git+https://github.com/ST0x-Technology/event-sorcery.git?tag=0.2.0-rc2#70267675e3e7ad5dda57e52addbc23822a929d84" =
        "sha256-CBPWEhc5HGtTiwHBon2s8dbpfaCTwMbInb3k/pPzX0w=";
      "git+https://github.com/rainlanguage/rain.wasm?rev=06990d85a0b7c55378a1c8cca4dd9e2bc34a596a#06990d85a0b7c55378a1c8cca4dd9e2bc34a596a" =
        "sha256-MkuPc9mWAmry5Yzjph4/IbaIvjevFUerji1lipLUK4g=";
    };

    # st0x.issuance is a Solidity repo with a deep git submodule tree
    # (ethgild -> rain.* -> openzeppelin-contracts), but the st0x-issuance-dto
    # and st0x-issuance-client crates we depend on are pure Rust. Crane's git
    # fetcher (downloadCargoPackageFromGit) pulls submodules unconditionally, so
    # the default vendor recursively clones that whole tree -- slow and a
    # recurring CI flake. Override the issuance checkout to skip submodules.
    # The rev is read from Cargo.lock (`issuanceRev` below) so it cannot drift
    # from the st0x-issuance-* pins. Only `hash` is hand-maintained: regenerate
    # it with `nix-prefetch-git --fetch-lfs` (without submodules) when re-pinning.
    overrideVendorGitCheckout =
      packages: drv:
      if
        pkgs.lib.any (
          package:
          pkgs.lib.hasPrefix "git+https://github.com/ST0x-Technology/st0x.issuance.git" package.source
        ) packages
      then
        drv.overrideAttrs (_: {
          src = pkgs.fetchgit {
            url = "https://github.com/ST0x-Technology/st0x.issuance.git";
            rev = issuanceRev;
            hash = "sha256-r3U9nZ0y8u1kLM4BJO11aHR/O1t6OsZefc9VcfcUpuA=";
            fetchSubmodules = false;
            fetchLFS = true;
          };
        })
      else
        drv;
  };

  # event-sorcery 0.2.0-rc2 makes sqlite-es self-contained: its migrations live
  # in-crate (`migrate!("./migrations")`) and survive `cargo package`, so the
  # vendored crate already carries them. No migration-copy workaround is needed
  # (the pre-0.2.0 `migrate!("../../migrations")` hack is gone).
  cargoLock = builtins.fromTOML (builtins.readFile ./Cargo.lock);

  # Issuance vendor override (above) reuses the rev Cargo.lock locks for the
  # st0x-issuance-* crates, so the rev can never drift from the Cargo.toml pins.
  # Both crates resolve to the same checkout; read it from either one.
  issuancePackage = builtins.head (
    builtins.filter (p: p.name or "" == "st0x-issuance-dto") cargoLock.package
  );
  issuanceRev = builtins.head (builtins.match ".*#([a-f0-9]+)" issuancePackage.source);

  # Build args without ABI env vars. Used for the deps-only derivation so an
  # ABI change doesn't bust the cached dependency artifacts -- third-party
  # deps don't reference any ST0X_*_ABI variable.
  depsArgs = {
    pname = "st0x-liquidity";
    version = "0.1.0";
    src = fullSrc;

    inherit cargoVendorDir;

    nativeBuildInputs = [ pkgs.pkg-config ];

    buildInputs = [
      pkgs.openssl
      pkgs.sqlite
    ]
    ++ pkgs.lib.optionals pkgs.stdenv.hostPlatform.isDarwin [ pkgs.apple-sdk_15 ];

    # Use offline sqlx query verification so builds don't need a live database.
    # Each crate (ours + apalis-sqlite) ships its own .sqlx/ with prepared data.
    # Run `cargo sqlx prepare` after changing sqlx macros to regenerate.
    SQLX_OFFLINE = "true";
  };

  # Full build args for our crates: deps args plus ABI env vars consumed by
  # our build.rs / sol! macros.
  #
  # `doCheck = false`: tests run in the cargo nextest matrix with full
  # `--workspace --all-features` coverage. Letting crane re-run `cargo test`
  # per derivation recompiles our local code under test cfg without
  # exercising any tests the matrix doesn't already cover.
  commonArgs = depsArgs // abiEnv // { doCheck = false; };

  # DTO only needs rain-math-float's ABI env through its Float dependency; keep
  # dashboard builds from realizing backend contract ABIs.
  dtoArgs = depsArgs // rainMathFloatAbiEnv // { doCheck = false; };

  # Build only dependencies (cached separately from source changes).
  # Crane's mkDummySrc internally strips to manifests + dummy crate roots,
  # so we feed it fullSrc rather than pre-stripping ourselves -- our prior
  # manifest-only filter dropped src/lib.rs files for crates with implicit
  # `[lib]` detection, which broke the deps build.
  cargoArtifacts = craneLib.buildDepsOnly depsArgs;

in
{
  # DTO crate for TypeScript codegen. Build config lives next to the crate
  # source at crates/dto/default.nix; rust.nix only supplies shared crane
  # infra.
  st0x-dto = import ./crates/dto {
    inherit craneLib cargoArtifacts;
    commonArgs = dtoArgs;
  };

  # Server binary for deployment
  st0x-liquidity = craneLib.buildPackage (
    commonArgs
    // {
      inherit cargoArtifacts;

      cargoExtraArgs = "--bin server --bin validate-config --features wallet-turnkey";

      meta = {
        description = "st0x liquidity market making server";
        homepage = "https://github.com/ST0x-Technology/st0x.liquidity";
      };
    }
  );

  # CLI binary for remote operations
  st0x-cli = craneLib.buildPackage (
    commonArgs
    // {
      pname = "st0x-cli";
      inherit cargoArtifacts;

      cargoExtraArgs = "--bin cli --features wallet-turnkey";

      postInstall = ''
        mv $out/bin/cli $out/bin/st0x-cli
      '';

      meta = {
        description = "st0x liquidity CLI";
        homepage = "https://github.com/ST0x-Technology/st0x.liquidity";
      };
    }
  );

  # Float decoder used by status scripts to render Raindex vault balances
  decodeFloats = craneLib.buildPackage (
    commonArgs
    // rec {
      pname = "decode-floats";
      inherit cargoArtifacts;

      cargoExtraArgs = "--bin ${pname}";

      meta = {
        description = "Decode Rain Float hex values to human-readable decimals";
        homepage = "https://github.com/ST0x-Technology/st0x.liquidity";
      };
    }
  );
}
