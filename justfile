# Run all local checks in fail-fast order (same as CLAUDE.md verification workflow)
pr: check test clippy fmt

# Fast compilation check
check:
    cargo check --workspace

# Run all workspace tests
test:
    cargo nextest run --workspace

# Run clippy lints
clippy:
    cargo clippy --workspace --all-targets --all-features

# Format code
fmt:
    cargo fmt

# Check formatting without modifying files
fmt-check:
    cargo fmt -- --check

# Reset the local sqlx database
db-reset:
    sqlx db reset -y
