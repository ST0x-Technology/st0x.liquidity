# ADR 0009: Composite `dividend-bump` saga and the `s01` issuer CLI

- **Status:** Proposed
- **Date:** 2026-06-19
- **Linear:** RAI-1040 (alternate issuer CLI config), RAI-395 (`issuer mint`),
  RAI-896 (`issuer donate`), RAI-1046 (V1 port); parent RAI-1033 (Dividends MVP,
  milestone M1 — wtStock NAV bump)
- **Related:** ADR 0008 (counter-trading during a dividend freeze), RAI-1039
  (wtStock NAV bump), [docs/wrapper-nav-bump.md](../docs/wrapper-nav-bump.md),
  [docs/cli-ops.md](../docs/cli-ops.md)

## Context

A dividend (or other corporate action) bumps a tokenized equity's wtStock
wrapper NAV by moving newly-acquired underlying shares **into** the ERC-4626
wrapper as a bare ERC-20 transfer: it raises `convertToAssets` for every
existing holder and mints no new shares (see
[docs/wrapper-nav-bump.md](../docs/wrapper-nav-bump.md)). Producing those shares
is a three-step pipeline the **issuer** runs:

1. **Buy** the equity offchain with the dividend cash (Alpaca).
2. **Tokenize/mint** it onchain to the issuer wallet (Alpaca tokenization ->
   ERC-20 mint).
3. **Donate** (bare transfer) the tokenized shares into the wrapper.

Today this is three separate fire-and-forget CLI subcommands (`buy`,
`alpaca-tokenize`, `donate-equity`) run as a manual runbook
([docs/cli-ops.md](../docs/cli-ops.md)) against issuer config/secrets passed by
hand. Two problems, both grounded in the current code:

- **No settlement coupling.** Each step settles asynchronously (broker fill:
  seconds to hours; onchain mint landing; tx confirmation), and the operator
  must watch each one and manually start the next. The buy command is
  **fire-and-forget** — there is no fill-wait anywhere in the CLI today
  (`execute_order_with_writers` returns immediately after `place_market_order`).
- **No crash-safety.** An interruption between steps strands funds (bought
  shares un-tokenized, or minted shares un-donated) with no resume path, and
  re-running risks double-buy / double-mint because Alpaca does not deduplicate
  requests.

The address-by-hand misconfiguration juan flagged on the CLI is a symptom of the
same "operator stitches raw steps together" shape. This is issuer-side work, but
the `st0x`/`stox` CLI today only injects the market-making bot's config/secrets;
there is no first-class issuer identity.

## Decision (proposed — pending review)

Build the dividend bump as **one resumable composite command** backed by a
**`DividendBump` CQRS aggregate**, run via a new **`s01` issuer CLI** (a
config-injecting wrapper around the existing `st0x-cli`). The operator supplies
the share quantity explicitly (`-q`); no delta formula in M1.

### 1. `DividendBump` aggregate (event-sourced saga)

Model buy -> tokenize -> donate as an event-sourced state machine following the
`UsdcRebalance` template (`src/usdc_rebalance.rs`,
`src/rebalancing/usdc/manager.rs`, and the CLI resume/redrive pattern in
`src/cli/rebalancing.rs`). State sketch:

- `BuySubmitting` (durable intent + `ClientOrderId` persisted **before** the
  Alpaca call) -> `Buying` (broker order id recorded) -> `BuyFilled` (fill
  confirmed, price recorded)
- `MintSubmitting` -> `Minting` (`issuer_request_id` + `tokenization_request_id`
  recorded) -> `TokensReceived` (mint landed onchain, verified)
- `DonateSubmitting` -> `Donating` (donate tx hash recorded) ->
  `DonateConfirmed` (terminal success)
- Failure terminals classified by an in-progress guard like
  `holds_rebalance_guard()`: pre-buy failures clear the guard; **post-mint**
  failures (shares minted but not donated) **hold** it for operator
  reconciliation.

Each phase persists a durable intent + a scan bound **before** the irreversible
external call, and on resume **scans-or-adopts** the already-submitted operation
(Alpaca by `client_order_id` / `issuer_request_id`; onchain by
token/amount/recipient) so a re-drive never double-submits.

### 2. CLI commands + redrive loop

- `s01 dividend-bump -s <ticker> -q <shares>` — generate a `DividendBumpId`,
  flush the recovery id to stdout **before** the first external call, drive to a
  terminal state.
- `s01 resume-dividend-bump --id <uuid> -s <ticker> -q <shares>` —
  preflight-reject an unknown id (never start a fresh bump from a typoed id),
  reload state, re-drive from the persisted phase.
- A `redrive_dividend_bump_until_settled` loop (mirroring
  `redrive_transfer_until_settled`) treats settlement lags (buy fill pending,
  mint not yet landed, donate under-confirmed) as retryable — sleep + re-invoke
  on the same id — and surfaces terminal errors.
- Reconciliation commands (`fail-dividend-bump` / `reconcile-dividend-bump`) for
  stuck guarded terminals, mirroring `fail-usdc-transfer` /
  `reconcile-usdc-transfer`.

### 3. Reuse vs. new

- **Reuse:** broker placement (`execute_broker_order` + `ClientOrderId::cli`
  idempotency + `Executor::get_order_status`), the mint + poll + onchain-verify
  primitives from the `TokenizedEquityMint` flow (`request_mint` /
  `poll_mint_until_complete` / onchain verification), `Wrapper::donate` +
  `lookup_underlying` / `lookup_derivative`, the CQRS event store, and the
  `UsdcRebalance` resume/guard/redrive scaffolding.
- **New:** the saga must add the **fill-wait** the CLI lacks today (buy is
  fire-and-forget), the `DividendBump` aggregate/commands/events, and the donate
  phase's scan-or-adopt.

### 4. `s01` issuer CLI derivation + issuer config/secret

- `s01` is a second `writeShellApplication` wrapping the **same** `st0x-cli`
  binary (analogous to `stox` in `os.nix`), injecting issuer `--config` /
  `--secrets` via dedicated env vars (e.g. `S01_CONFIG` / `S01_SECRETS`) with
  issuer default paths — never colliding with `STOX_CONFIG` / `STOX_SECRETS`.
- A new plaintext issuer config (issuer turnkey wallet, issuer Alpaca account,
  the equity addresses) and a **new** agenix secret (e.g.
  `secret/s01-issuer.toml.age`) declared in `secret/secrets.nix` with a role in
  `keys.nix`, wired into the `deploy.nix` validate-config gate.
- A **separate `database_url`** for the issuer so its `DividendBump` events do
  not intermix with the bot's aggregates/projections.
- The encrypted issuer secret must be populated with **real** issuer credentials
  by a human before deploy (validate-config fails closed otherwise). The
  implementing agent must **not** read or create secret material — only the
  declaration plumbing.

## Options considered

### Option A — Composite CQRS saga + `s01` (recommended)

One resumable command, crash-safe, idempotent, fits the repo's CQRS architecture
and the `UsdcRebalance` precedent.

- **Pro:** an interruption is resumable by id; no double-buy/double-mint; the
  operator runs one command, not a babysat runbook.
- **Pro:** later automation (the dividend scheduler) reuses the same aggregate
  via a bot job — no second mechanism.
- **Con:** a new aggregate + issuer infra is a multi-PR effort.

### Option B — Keep the 3-command runbook

- **Pro:** zero new code.
- **Con (decisive):** the operator babysits async settlement; an interruption
  strands funds with no resume; re-runs risk double-buy/double-mint; the
  address-by-hand footgun persists.

### Option C — A throwaway orchestrator script

A shell script chaining the three commands.

- **Con (decisive):** not crash-safe, no durable state, can't resume mid-flight,
  and lives outside the event-sourced architecture the rest of the system relies
  on.

## Consequences

- Multi-PR effort. Suggested decomposition (each independently valid, CI-green
  on its own):
  1. `s01` derivation + issuer config schema + agenix secret **declaration** (no
     real creds) + validate-config wiring + separate DB path.
  2. `DividendBump` aggregate (types/events/states) + the **buy** phase
     (placement + the new fill-wait).
  3. The **tokenize** phase (mint + poll + onchain verify) on the aggregate.
  4. The **donate** phase + terminal/guard classification.
  5. The CLI `dividend-bump` / `resume-dividend-bump` + redrive loop +
     reconciliation commands.
  6. cli-ops runbook update (the composite replaces the 3-step manual sequence).
- **Build once (RAI-1040):** this composite is **the** single M1 mechanism; the
  issuer-CLI track (RAI-395 mint, RAI-896 donate) and RAI-1040's alt-config
  approach converge here, and the other becomes the RAI-1046 V1 port. Do not
  build both.
- Later automation (the dividend scheduler, post-M1) reuses the **same**
  `DividendBump` aggregate via a bot apalis job — not a new mechanism — so the
  resume/idempotency guarantees carry over.

## Open questions for review

- `s01` env-var names + issuer config layout (reuse the existing config schema
  vs. an issuer-specific subset).
- Whether the issuer and bot share a host (shared host key) or run separately (a
  separate `.age` per host key).
- The exact guard-holding terminal set (which post-failure states require
  operator reconciliation vs. clear automatically).
