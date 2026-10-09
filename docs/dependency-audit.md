# Rust dependency auditing

Run
`nix develop .#ci-audit -c bash -c './scripts/test-rust-audit.sh && ./scripts/audit-rust.sh'`
to check the RustSec policy. The same command runs in the CI hooks job and in
`nix run .#ci`; backend compilation and tests run independently of the advisory
database fetch.

Vulnerabilities, unmaintained crates, and unsound crates fail the audit except
for the documented advisory IDs in `.cargo/audit.toml`. Since cargo-audit
ignores an advisory across the entire lockfile, `scripts/audit-rust.sh` checks
the exact affected package versions and immediate dependents using all features,
all targets, and normal, build, and development edges. A changed path requires
reassessment of the exception. The lockfile must already be current; auditing
does not regenerate it.

The script permits only the existing yanked `spin 0.9.8` dependency, with its
exact parent set checked separately. Any additional yanked package fails.
`scripts/test-rust-audit.sh` checks that weakened policy, a selected RSA path,
dependency-inspection failures, audit failures, and new yanked packages are
rejected without fetching dependencies or advisories.

When updating a vulnerable dependency, check every workspace binary's declared
Rust minimum. The patched `ruint 1.20.0` requires Rust 1.90, including the CLI.
