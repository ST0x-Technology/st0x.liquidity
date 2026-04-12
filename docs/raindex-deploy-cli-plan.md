# Raindex Strategy Deployment — Plan

## Goal

Deploy Rain strategies from the command line, signing via the existing
Turnkey-backed `base_wallet()`. The deployment is split across two repos because
of irreconcilable Cargo dependency conflicts (see "Why two binaries" below).

Compose via pipe:

```
raindex strategy-builder \
  --registry <url> --strategy <name> --deployment <key> \
  --select-token input=0x... --select-token output=0x... \
  --set-field max-spread=0.002 --set-deposit output=0.5 \
  --owner 0xA9C16673... \
| cli submit --to 0xe522cB4a5fCb2eb31a52Ff41a4653d85A4fd7C9D
```

No on-disk state between runs. The chain is the record of truth.

## Architecture: two binaries, one pipe

### 1. `raindex strategy-builder` — lives in `rain.orderbook`

New subcommand added to the existing `rain_orderbook_cli` binary
(`crates/cli/`), which already has `order`, `vault`, `trade`, `subgraph`,
`chart`, `quote`, `words`, and `local-db` subcommands. Uses `DotrainOrderGui` to
parse .rain files and produce raw hex calldata.

Pure flags — no config file (a bash wrapper can map a TOML to flags if needed):

```
raindex strategy-builder \
  --registry <url> \
  --strategy <name> \
  --deployment <key> \
  --select-token <slot>=<addr> \   # repeatable
  --set-field <binding>=<value> \  # repeatable
  --set-deposit <token>=<amount> \ # repeatable
  --owner <addr>
```

Outputs newline-delimited hex to stdout — one line per transaction (approvals
first, then `addOrder2`). Each line is `<to>:<calldata>` so the submitter knows
where to send each tx. Example:

```
0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913:0x095ea7b3...
0xe522cB4a5fCb2eb31a52Ff41a4653d85A4fd7C9D:0x...
```

Exit code 0 on success, non-zero with a human-readable error on stderr if the
registry fetch, GUI construction, or validation fails.

This subcommand is **not** part of `st0x.liquidity`. It lives in
`rain.orderbook` where all rain crate dependencies resolve cleanly.

### 2. `cli submit` — lives in `st0x.liquidity`

New subcommand added to the existing CLI. Generic calldata submitter, follows
`cast send` conventions:

```
cli submit --to <address> --data 0x<hex>        # single tx from flags
cli submit                                       # multi-tx from stdin pipe
cli submit --to <address> --data 0x<hex> --yes   # skip confirmation prompt
```

**Stdin mode** (for pipe composition): reads `<to>:<calldata>` lines from stdin,
submits each in sequence. Shows a review block before signing unless `--yes` is
passed.

**Flag mode** (standalone): `--to` and `--data` are required, submits one tx.

Signs via the existing `base_wallet()` -> `TurnkeyWallet`. Prompts before each
transaction unless `--yes`.

### Review block

Before submitting, `cli submit` prints a summary:

```
Chain:    base (8453)
Signer:   0xA9C16673F65AE808688cB18952AFE3d9658C808f  (turnkey)

Tx 1/3
  to:     0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913
  data:   0x095ea7b3... (68 bytes)
  gas:    ~46,300

Tx 2/3
  to:     0xe522cB4a5fCb2eb31a52Ff41a4653d85A4fd7C9D
  data:   0x... (1,240 bytes)
  gas:    ~380,000

Submit all 3 transactions? [y/N]
```

The review is intentionally minimal — `cli submit` is a generic submitter and
does not decode Rain-specific calldata. Decoding (approve, addOrder2, etc.)
belongs in `raindex-calldata --verbose` if we want it later.

## Why two binaries

Tried for hours to compose `rain.orderbook`'s Rust crates into `st0x.liquidity`
via path deps. Failed due to:

- **workspace.dependencies routing**: Cargo routes `workspace = true`
  inheritance through the OUTERMOST workspace, not the target package's own
  workspace. Would need to mirror ~40 `[workspace.dependencies]` entries.
- **alloy version conflict**: `revm-inspectors 0.23.0` (transitive via
  `foundry-evm` -> `rain-interpreter-eval`) is incompatible with
  `alloy-rpc-types-trace > 1.0.12`. `alloy-signer-turnkey` minimum is 1.0.39.
  Mutually exclusive in one Cargo graph.
- **Rust edition mismatch**: rain crates use edition 2021, st0x uses 2024.
  `workspace.package.edition` also routes through the outer workspace, breaking
  rain's match ergonomics.
- **Feature gating**: Feature-gating `foundry-evm` behind `eval` in
  `rain_orderbook_common` helped for lib-only builds but dev-dep resolution
  pulled it back in.
- `st0x.rest.api` avoids all this by having NO `[workspace]` declaration and
  pinning `alloy = "=1.0.12"`. st0x.liquidity's `[workspace]` makes this
  impossible.

The pipe architecture cleanly separates the two dependency graphs.

## Context

- `st0x.liquidity` already ships a working Turnkey signer
  (`crates/evm/src/turnkey.rs`), ragenix-decrypted secrets, and a CLI with
  `vault deposit` / `vault withdraw` commands that call
  `rebalancing_ctx.base_wallet().send(to, calldata, note)`. The new `submit`
  command follows the same pattern.
- `DotrainOrderGui::get_deployment_transaction_args(owner)` is the exact
  function the webapp uses to produce
  `{ approvals, deployment_calldata,
  orderbook_address }`.
  `raindex strategy-builder` calls this and serializes the output to stdout.
- Turnkey transaction policy already whitelists the deploy-relevant calls on the
  hedge-bot wallet (`0xA9C16673...`).

## Non-goals

- Porting `DotrainOrderGui` logic into st0x.liquidity — that's why we pipe.
- Adding a `deployment.toml` config schema to st0x.liquidity — config parsing
  belongs on the `raindex strategy-builder` side (or in a bash wrapper).
- Interactive mode in `cli submit` — it's a dumb submitter. Interactive strategy
  selection belongs in `raindex strategy-builder` if we want it.
- Changes to Turnkey wiring, ragenix secrets, or the existing `wallet-turnkey` /
  `wallet-private-key` feature flags.

## New files in st0x.liquidity

```
src/cli/submit.rs     # clap subcommand: parse --to/--data or stdin,
                      #   review block, per-tx prompt, base_wallet().send loop
```

`src/cli/mod.rs` grows a `Submit(SubmitCommand)` arm dispatching to
`submit::run`. No changes to existing CLI subcommands.

### Dependency additions

None. `cli submit` uses only `alloy` types (`Address`, `Bytes`) and the existing
`Wallet` trait already in the workspace.

## Secrets

Unchanged. The `submit` path reads `api_private_key` from the same
ragenix-decrypted secrets TOML the hedge bot uses. The review phase touches zero
secrets and can run on any host (with `--dry-run` if we add it later).

## Turnkey policy

Already in place on `0xA9C16673...`. The CLI should fail cleanly if a policy
rejection comes back — surface the Turnkey error message verbatim, no
translation.

## Testing

- **Unit**: `submit.rs` stdin parser tests — feed `<to>:<calldata>` lines,
  assert correct `(Address, Bytes)` pairs are extracted. Test malformed input
  rejection.
- **Unit**: review block rendering — fixture input, snapshot the output.
- **Integration (Anvil)**: pipe fixture calldata lines into `cli submit`, signer
  = local raw key (not Turnkey). Assert transactions land onchain with expected
  `to` and `input` fields.
- **Integration (gated on `TURNKEY_*` env)**: same pipeline with
  `TurnkeyWallet`, still against local Anvil. Mirrors the existing
  `turnkey_integration` test.
- **No e2e against real chains**: manual smoke test on Base is the acceptance
  gate, not CI.

`raindex-calldata` testing lives in `rain.orderbook` — not covered here.

## Delivery

### In `st0x.liquidity`: single PR

- `src/cli/submit.rs` — stdin parser, flag parser, review block, prompt loop,
  `base_wallet().send(...)` per tx
- `src/cli/mod.rs` — `Submit` variant in `Commands` enum
- Unit tests for parser and review rendering
- Anvil integration test with local signer
- `docs/raindex-deploy-cli.md` — usage docs

Expected size: ~300–500 lines. This is a simple command — the complexity lives
in `raindex-calldata` on the rain.orderbook side.

### In `rain.orderbook`: separate PR (tracked separately)

- `strategy-builder` subcommand added to the existing `rain_orderbook_cli`
- Uses `DotrainRegistry` + `DotrainOrderGui` to parse .rain strategies
- Clap CLI with the flags listed above
- Outputs `<to>:<calldata>` lines to stdout
- Tests against fixture `.rain` files

### Compose and smoke test

After both PRs land:

```
raindex strategy-builder \
  --registry <url> --strategy fixed-spread \
  --deployment base-dia \
  --select-token input=0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913 \
  --select-token output=0x4200000000000000000000000000000000000006 \
  --set-field max-spread=0.002 --set-field oracle-key=ETH/USD \
  --set-deposit output=0.5 \
  --owner 0xA9C16673F65AE808688cB18952AFE3d9658C808f \
| cli submit
```

Manual verification on Base with a known-good strategy.

## Per-PR implementation order (st0x.liquidity)

1. `submit.rs` — stdin line parser (`<to>:<calldata>` ->
   `Vec<(Address, Bytes)>`)
2. `submit.rs` — flag mode parser (`--to` + `--data`)
3. `submit.rs` — review block rendering
4. `submit.rs` — per-tx prompt + `base_wallet().send(...)` loop
5. `mod.rs` — clap wiring for `Submit`
6. Unit tests for parser + review
7. Anvil integration test
8. `docs/raindex-deploy-cli.md`
9. `nix run .#ci` -> `gt submit`

## Verification: run `nix run .#ci` before `gt submit`

CI in this repo is driven by the `ci` task in `flake.nix`, which runs:

```
cargo check --workspace
cargo check --workspace --all-features
cargo nextest run --workspace --all-features
cargo clippy --workspace --all-targets --all-features
cargo fmt -- --check
```

Always run `nix run .#ci` locally before `gt submit`. See CLAUDE.md for
rationale (pinned toolchain, feature coverage, clippy/fmt drift).

## Risks / open concerns

- **Wire format**: `<to>:<calldata>` is simple but ad-hoc. If
  `raindex strategy-builder` ever needs to convey more per-tx metadata (value,
  gas limit), we'd need to evolve the format. JSON-lines is the natural upgrade
  path, but start simple.
- **Gas estimation**: `cli submit` does RPC gas estimation at submit time. If
  the RPC underprovisions (dRPC load balancing, cold provider), the review
  block's gas estimate may differ from what lands. Flag as approximate.
- **Multi-tx atomicity**: transactions are submitted sequentially. If tx 2/3
  fails, tx 1 (an approval) has already landed. This matches the webapp's
  behavior but is worth noting. A future improvement could use multicall.
