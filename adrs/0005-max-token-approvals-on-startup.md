# ADR 0005: Grant one-time MAX token approvals to trusted spenders on startup

## Status

Accepted

## Context

The market maker repeatedly performs two on-chain actions that move ERC20 tokens
via a spender's `transferFrom`:

- **Wrapping**: depositing the underlying tokenized equity (tToken) into its
  ERC-4626 wrapper vault (wtToken) to receive wrapped shares.
- **Vault deposits**: depositing wrapped equity (wtToken) and USDC into the
  Raindex orderbook via `deposit4`.

Each of these previously approved exactly the operation amount immediately
before the action: `WrapperService::submit_wrap` approved `(tToken -> wtToken)`
and `RaindexService::approve_for_orderbook` approved `(token -> orderbook)`.

That per-operation pattern is gas-coupled and race-prone:

- The approve is a separate transaction that must land before the wrap/deposit.
  On low gas it can fail to mine, so the subsequent `transferFrom` reverts with
  `ERC20InsufficientAllowance`.
- The skip-if-sufficient check reads `allowance(owner, spender)` first. Behind a
  load-balanced RPC (the dRPC hazard AGENTS.md warns about), a stale node can
  return an allowance that omits a just-issued approve, so the code skips the
  approve it actually needed -- and again the transfer reverts.

These reverts surfaced in production and, critically, wedged the COIN
unwrapped-equity recovery path: recovery wraps then deposits, so an
allowance-coupled revert there leaves operational equity stranded.

## Decision

Grant a single idempotent `U256::MAX` approval per `(token, spender)` pair at
startup, to the **trusted spenders only**:

1. `approve(underlying tToken -> wtToken vault, MAX)` -- enables wrapping.
2. `approve(wtToken -> orderbook, MAX)` -- enables depositing wrapped equity.
3. `approve(USDC -> orderbook, MAX)` -- enables USDC vault deposits.

Targets 1 and 2 are built for every configured equity symbol that has trading or
rebalancing enabled; the symbol -> address mapping is resolved through the
`Wrapper` trait (`lookup_underlying` / `lookup_derivative`). The orderbook comes
from the EVM config, USDC from the shared `USDC_BASE` constant, and the owner is
the base bot wallet.

The grant runs inline during conductor startup, after the RPC probe and vault
registry seeding but **before any worker or rebalancer spawns**, so allowances
are durably on chain before the first wrap/deposit. It submits through the same
`Wallet::submit` path every other on-chain write uses, so confirmation depth and
nonce handling are consistent. A failure to grant fails startup fast with a
typed error (`StartupApprovalError`): the bot must not come up looking healthy
while wrap/deposit would revert.

## Idempotency and re-arm

For each pair the routine reads `allowance(owner, spender)` and applies a pure
decision: if the allowance is already at or above a high watermark
(`U256::MAX / 2`) it skips, otherwise it submits `approve(spender, MAX)`. The
watermark cleanly separates an already-granted MAX approval (possibly partially
consumed by transfers) from a bounded per-operation allowance that still needs
upgrading. A restart therefore submits no redundant approves once MAX is in
place, and a newly configured symbol or a wallet whose allowance was reset is
re-armed automatically on the next startup.

## Security trade-off

An infinite approval is only as safe as its spender. Here the spenders are our
own ERC-4626 wrapper vaults and the audited Raindex orderbook -- contracts the
bot already entrusts with its operating balances every cycle. The wallet holds
only operational balances (not a treasury), so the marginal risk of an unlimited
versus per-operation allowance to these specific, bot-controlled protocol
contracts is acceptable, and it removes a whole class of liveness failures.

## Consequences

- Wrap and vault-deposit no longer depend on a per-operation approve landing, so
  the `ERC20InsufficientAllowance` revert class -- and the recovery wedge it
  caused -- is eliminated for the trusted spenders.
- The per-operation approvals in `WrapperService::submit_wrap` and
  `RaindexService::approve_for_orderbook` remain in place as a defensive
  fallback. Once the startup MAX grant lands they short-circuit to a no-op (the
  allowance already exceeds any operation amount), so they cost nothing but
  still cover any spender not pre-approved at startup.
- A standalone bot with no `[wallet]` configured skips the grant entirely: it
  never wraps or deposits, so it has no allowances to grant.

## Alternatives considered

- **Keep per-operation approvals, just retry harder.** Rejected: it does not
  remove the gas coupling or the stale-allowance race; it only narrows the
  window. The failure recurred in production despite the skip-if-sufficient
  guard.
- **Approve exactly the operation amount but as a separate confirmed step with
  its own recovery.** Rejected: more moving parts and persisted state for a
  problem a one-time MAX grant removes outright.
