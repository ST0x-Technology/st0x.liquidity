# How to Add a New Asset (Non-Technical Guide)

Adding a new asset to the st0x system requires changes across **four systems**
in a specific order. This guide walks through each step.

---

## Overview

When you add a new asset (e.g. SGOV), you need to:

1. **Get the token contract addresses** (from the registry)
2. **Allow the vault in the issuer's Turnkey policy** and grant `DEPOSIT` and
   `WITHDRAW` to the issuance bot's signing wallet, not the liquidity bot's
   wallet (so mints and burns can be signed). Do this before step 3.
3. **Register it in the Issuance Bot** (so Alpaca knows about it)
4. **Add it to the Liquidity Bot's token file** (so it starts trading/hedging)

---

## Step 1: Get the Token Addresses

Every tokenized asset has two contract addresses on each chain that lists it
(Base today; the same two exist per chain when an asset is listed elsewhere):

| Name                            | What it is                                                                                   | Example (SGOV)                               |
| ------------------------------- | -------------------------------------------------------------------------------------------- | -------------------------------------------- |
| **tokenized_equity**            | The base ERC-20 token (e.g. tSGOV). This is also the "vault" address the issuance bot needs. | `0xc941C1506B7555Ba8C506Fb6c9b9CC259902d612` |
| **tokenized_equity_derivative** | The wrapped/dividend-accruing version (e.g. wtSGOV).                                         | `0x78c31580c97101694c70022c83d570150c11e935` |

**Where to find them:** The `ST0x-Technology/st0x.registry` GitHub repo, in
`token-lists/<chain>.json`. Each entry is the wrapped token (`wtSYM`): its
`address` is `tokenized_equity_derivative` and its `extensions.unwrappedAddress`
is `tokenized_equity`. For a token that is not listed yet, the addresses come
from the deploy run that created it (see the prerequisites in the issuance
onboarding runbook below).

---

## Step 2: Allow the Vault in the Issuer's Turnkey Policy

The issuance bot signs its onchain transactions (minting and redemption) with
Turnkey. Fireblocks is decommissioned (see the st0x.issuance runbook
`docs/runbooks/fireblocks-decommission.md`), so there is no Fireblocks whitelist
step any more.

Instead, the issuer's Turnkey policies carry an explicit per-chain list of
receipt vaults. A vault missing from that list is refused at signing on every
mint and burn. Get the policy change applied and verified, together with the
vault's `DEPOSIT` and `WITHDRAW` grants to the issuance bot's signing wallet
(not the liquidity bot's wallet), **before** you register the asset in step 3.
Both are prerequisites in the issuance onboarding runbook. A listing that goes
live before signing works fails its first mint at signing (Turnkey 403), and
that mint then needs manual recovery.

If a mint or a burn fails on a missing policy or another transient error, fix
the cause first. Then recover the mint or the redemption with the admin
endpoints in the st0x.issuance recovery guide (`docs/ops-recovery-guide.md`),
called from the issuer host as in step 3.

---

## Step 3: Register the Asset in the Issuance Bot

The listing is runtime state in the issuance bot, written by one
`POST /tokenized-assets` call against the running service. Nothing ships and
nothing restarts. The authoritative procedure, with the exact pre-check,
register and verify commands and what each status code means, is the
`ST0x-Technology/st0x.issuance` runbook
[`docs/runbooks/tokenized-asset-onboarding.md`](https://github.com/ST0x-Technology/st0x.issuance/blob/main/docs/runbooks/tokenized-asset-onboarding.md).
Follow it; this section only says where to run it.

Do this only after step 2: the vault must already be in the issuer's Turnkey
policy, with its `DEPOSIT` and `WITHDRAW` grants.

### 3a. Get onto the issuer host

Get onto the issuer host as the `S01-Issuer/s01.devops` access runbook says. The
admin API is not reachable from outside that host.

### 3b. Call the admin API from the issuer host

`POST /tokenized-assets` uses "internal auth": it needs the `X-API-KEY` header
**and** a client IP inside the service's internal ranges, so call it on the
issuer host. Follow the st0x.issuance onboarding runbook for the base URL and
the API key. Do not assume a fixed port: confirm the port that the service
listens on, as the st0x.issuance recovery guide does, and use `ISSUER_BASE_URL`
as the onboarding runbook does. On the wrong port the pre-check `GET` can return
a `404` that does not come from the issuance bot.

Never print the API key or the secrets env that holds it.

**Important notes:**

- The `vault` field = the `tokenized_equity` address (the base token, NOT the
  derivative).
- The `network` field is the chain the asset is listed on, spelled as the bot's
  chain name (`base`, `ethereum`, `hyperevm`, `robinhood`). Register once per
  chain.
- Pre-check with `GET /tokenized-assets/<underlying>?network=<network>` (also
  internal auth). Register only when it returns `404`: re-adding an existing
  asset with a different vault silently repoints the listing.
- Verify with the same `GET`, which must return `200` with the expected token,
  network and vault. The list endpoint `GET /tokenized-assets` is for Alpaca and
  is not the way to verify.
- `422` means an unconfigured network, an invalid symbol, or a vault that
  already serves another underlying on that network.

### 3c. Wait for Alpaca to pick it up

Alpaca periodically calls `GET /tokenized-assets` on the issuance bot to
discover available assets. After you add the asset, Alpaca's internal
"tokencache" needs to refresh before minting works.

- There is no way to force this from our side.
- If minting returns `"Token symbol for X not found in tokencache"`, Alpaca
  hasn't refreshed yet. Wait and retry, or contact Alpaca to force a refresh.

---

## Step 4: Add the Asset to the Token File

The liquidity bot (this repo) reads its per-symbol tables from the token file
that the private `T0Trade/t0.tokens` repository publishes to
`gs://t0-artifacts-tokens/<env>/tokens.toml` (`t0/<env>.toml` in that
repository; it moved there from `st0x.registry`, which now keeps only the
`token-lists/<chain>.json` address lists). The bot's configs,
`config/staging/st0x-hedge.toml` and `config/prod/st0x-hedge.toml`, name that
file under `[registry]` and must not carry a per-symbol table themselves: a
config that does is refused at startup.

### 4a. Edit the token file

Edit `t0/staging.toml` (or `t0/production.toml`) in `t0.tokens`. The bot takes
two tables per asset. The first says where the asset is listed on-chain, so it
goes under the chain that lists it. The second says how the bot hedges it, which
is independent of any chain -- there is one broker account and one position per
symbol. Other services own other keys on the same tables (the file's header
lists them). The bot takes its own keys and ignores the others; which keys may
appear, and their spelling, is checked by `t0.tokens`' CI (`t0/check.jq`) before
the file is published, not by the bot. Publishing `rebalancing = "paused"` needs
this order so an older binary never reads a token file it cannot load:

1. Deploy the bot that reads `paused`.
2. Merge the `t0/check.jq` validator change that accepts `paused`.
3. Publish `paused` in the token file.

On rollback, restore the token value before downgrading the binary. If the
validator lands first and someone publishes `paused`, a restart of an older
binary (staging loads the latest token file on every restart) refuses the
registry and stops hedging.

```toml
[chains.base.assets.equities.SGOV]
trading = "disabled"                          # "enabled" or "disabled"
rebalancing = "disabled"                      # "enabled", "paused" or "disabled"
wrapped_equity_recovery = "disabled"          # "enabled" or "disabled"
vault_ids = ["0xfab"]                         # Raindex vault IDs (required when rebalancing = "enabled" or "paused")
tokenized_equity = "0xc941C1506B7555Ba8C506Fb6c9b9CC259902d612"
tokenized_equity_derivative = "0x78c31580c97101694c70022c83d570150c11e935"

[assets.equities.SGOV]
extended_hours_counter_trading = "disabled"   # "enabled" or "disabled"
```

Both are required: a symbol listed on a chain with no hedging policy, or a
hedging policy for a symbol listed on no chain, fails startup.

A chain the token file lists the asset on must be declared with a
`[chains.<name>.trading]` table in the bot's config, or the bot refuses the
file.

The chain table the asset goes under decides what the bot uses for it: that
chain's signing wallet, orderbook, `redemption_wallet` and
`[orchestrator.addresses]` entry. On each hedged chain where the asset is listed
(Base included), check before enabling the asset:

- On a redemption-capable chain -- the primary, and a secondary where at least
  one equity sets `rebalancing = "enabled"` or `"paused"` --
  `[chains.<name>.trading]` carries a `redemption_wallet` (the issuer's wallet
  on that chain). Startup builds that chain's tokenization services and refuses,
  naming the chain, without it. A hedge-only secondary, where every equity has
  `rebalancing = "disabled"`, needs no redemption wallet, issuer client or mint
  authorizer, and gets no wrap or deposit approvals.
- Every chain that lists the asset, hedge-only secondaries included, needs its
  own `tokenized_equity_derivative`: that address is the token its vaults hold
  and the one its fills are checked against, and the daily portfolio capture
  reads that chain's vault ratio to value those balances in underlying shares.
- The vault at `tokenized_equity_derivative` reports `tokenized_equity` as its
  `asset()`. Startup attests this on each redemption-capable chain -- every
  equity with trading enabled or rebalancing enabled or paused on the primary,
  those with rebalancing enabled or paused on a secondary -- and fails naming
  the chain and symbol otherwise.
- The Turnkey policies allow the startup approvals on that chain's id: the
  approvals (underlying to vault, vault to that chain's deposit spender, that
  chain's USDC to the same deposit spender) are granted per hedged chain, and
  the deploy gate checks coverage per chain. The deposit spender is the chain's
  orderbook when its `inventory_mode` is `legacy`, and its configured
  `inventory` when it is `managed`, so a chain that has migrated needs its
  policies on the inventory address.
- If the asset is in orchestrator mode, `[orchestrator.addresses]` has an entry
  for that chain (keys are chain names: `base`, `ethereum`, `hyperevm`,
  `robinhood`). The order is fixed per chain: deploy the `ST0xOrchestrator`
  there, add its address to both bots' `[orchestrator.addresses]` and deploy
  both, extend the Turnkey signing policy to `MintAuth` typed data with that
  chain's id and orchestrator as the verifying contract, and only then cut the
  asset over at issuance (issuance keys the mode by symbol, so the cutover
  applies on every chain the asset is listed on). In rebalancing mode, startup
  refuses, naming the chain and symbol, when issuance reports the asset as
  orchestrator-mode while the chain has no entry; see "Orchestrator rollout per
  chain" in [cli-ops.md](cli-ops.md).

**Fields:**

- `trading`: Set to `"disabled"` initially, enable once everything else is
  ready. A fill on this chain accounted while it is disabled is never counter
  traded, not even after trading is enabled: it is recorded in `skipped_fills`
  with reason `trading_disabled` and has to be covered by hand. The flag is read
  when the bot accounts the fill, not when the fill lands on chain, so after
  enabling trading the bot hedges every fill it has not accounted yet, including
  fills that landed while trading was disabled but were still queued, not yet
  backfilled, or landed during the restart. Every excluded fill logs
  `Fill on
  DISABLED asset <SYMBOL> (chain <chain>, ...)`, which alerts in
  production once per chain and symbol. If trading was left disabled by mistake,
  enabling it does not hedge the fills already excluded: sum that symbol and
  chain's `trading_disabled` rows in `skipped_fills` and hedge them by hand. If
  it is disabled on purpose and hedged by hand, silence the alert for that chain
  and symbol.
- `rebalancing`: Whether the bot auto-rebalances this asset between venues.
  Usually `"disabled"` at first. `"paused"` starts no new mint or redemption but
  keeps the chain's equity transfer services, so work already under way
  finishes. Before you disable a listing that may have a transfer in flight,
  pause it and wait until its in-flight transfers finish. Pausing does not move
  the chain's equity: the planner skips a paused chain and `transfer-equity`
  refuses a new transfer on it. To move that equity first, use the manual vault,
  unwrap, and redeem commands (`vault-withdraw`, `unwrap-equity`, then
  `alpaca-redeem` with the unwrapped quantity, see [cli-ops.md](cli-ops.md)). A
  paused chain still counts in the planner's total, so it must stay polled and
  readable: a stale paused chain stops equity rebalancing for the symbol on
  every chain until it is fresh again or set to `"disabled"`. Wallet polling and
  both wrapped and unwrapped wallet recovery keep running on that chain.
  Recovery retains its symbol hold until the work completes; pausing does not
  release it. A zero allocation target starts redemptions and must not be used
  as a pause.
- `wrapped_equity_recovery`: Explicit opt-in for recovery of wrapped-equity
  positions. Set to `"enabled"` to allow the bot to recover wrapped equity;
  `"disabled"` skips recovery for this asset. Must be specified for every equity
  entry. On every chain, `rebalancing = "enabled"` or `"paused"` requires
  recovery enabled, covering mints as well as redemptions. A secondary listing
  may keep recovery enabled with rebalancing disabled while its open work
  finishes: the chain keeps its transfer services for that work, but takes no
  new transfers. Once the work is done, disable recovery too. If no listing on
  that chain rebalances, the chain gets no transfer services, so tokens stranded
  there later are not recovered; the bot logs a startup warning for such an idle
  listing. Config errors name both chain and symbol. Enable a secondary listing
  only in release R, which ships the complete recovery stack, or later, and only
  once its chain capability is available. For rollback, pause first, wait for
  recovery and provider operations to drain and the wallet to empty, then
  disable rebalancing and recovery together. Roll back the binary to R; older
  binaries do not safely execute secondary recovery or signed pending issuer
  sends. R itself enables Robinhood DNUT, so for that listing the pause is the
  rollback. Never delete a compacted inventory snapshot to force replay.
- `extended_hours_counter_trading`: Explicit opt-in for counter-trading during
  extended hours (pre-market and after-hours). Set to `"enabled"` to allow the
  bot to place offsetting broker trades outside regular market hours;
  `"disabled"` restricts counter-trading to regular session only. Must be
  specified for every equity entry.
- `vault_ids`: The Raindex vault IDs. Required when `rebalancing` is `"enabled"`
  or `"paused"`: the bot refuses a token file with a rebalancing row that has
  none. With rebalancing disabled they can be omitted, and the bot discovers the
  vaults from its trade events.
- `tokenized_equity`: The base token contract address.
- `tokenized_equity_derivative`: The wrapped token contract address.

### 4b. Publish and verify adoption

Merge the `t0.tokens` change. Its `publish-t0-tokens` workflow uploads the
staging file on merge. The production file is uploaded only when someone runs
that workflow by hand on `main`, and the upload waits for an approved PAM grant.
After the unpinned reload release, the bot polls every ten seconds and validates
each observed generation before a graceful restart. No liquidity pin-bump PR or
config-only release is needed. Start with a disabled listing, verify adoption,
then enable trading in a second publish. Newly configured contracts are probed
including disabled rows; newly selected tokenization routes and changed startup
approval targets are checked too. Missing Turnkey coverage or transient RPC
failures defer adoption until the dependency is fixed, without a republish.

Check `registry_applied_generation`, the structured generation/hash logs and
`registry_reloads_total{result}`. A refused copy sets `registry_invalid` and
leaves the current configuration running. Promotion to last-good requires ten
minutes of uptime; two failed boots fall back to the previous good set plus new
listings disabled. Reloads are debounced for two minutes.

During rollout production still pins `generation`, and the watcher only reports
changes. Release the state-seeding code first, verify last-good exists on the
data disk, then remove the pin in a second release. The t0.devops compose gates
must pass `--registry-state /mnt/data/registry` and manage the deployment hold
before that second release. Keep the production fixture aligned with the pin
until it is removed; `tokens-production-migration.toml` stays frozen.

Offline checks continue to accept `--registry-file`. Deployment checks accept
`--registry-state` and judge pending plus fallback, or running. These readers
never change boot attempts or promote state.

The production release gate runs `validate-config` without `--registry-file`, so
it does not check the token file: a pin that the new binary refuses passes the
gate and fails only when the bot starts. Run this check yourself.

**Recovery stack release (RAI-2596):** that release refuses an `enabled` or
`paused` equity listing without recovery. The production pin from before it has
Base RKLB with rebalancing enabled and recovery disabled. The generation that
disables Base RKLB rebalancing (`1791378253362331`) also enables Robinhood DNUT,
which the binary from before RAI-2596 refuses. So the new binary and that pin
ship in one release, with the Robinhood switch-on
([RAI-2780](https://linear.app/makeitrain/issue/RAI-2780)). Do not release
master without that pin. A tag rollback from that release is safe only before it
has run any equity operation on any chain: every redemption it runs, on Base
too, persists `SendPrepared`, which the previous release cannot load. After
that, pause the Robinhood listing with a new token-file generation plus a pin
bump, or roll forward; going below R follows SPEC.md.

### Retiring an asset

Removing a listing from the token file carries its previous row forward with
trading, rebalancing and wrapped-equity recovery disabled. The same applies to a
listing removed on one chain while the symbol stays on another. Existing
transfers and recoveries finish; new work stops. Already queued hedges still
cover fills accepted before the disable.

To remove the retained runtime row, list the symbol in `retired_symbols` of the
`[assets.equities]` table in the bot's config and release it. The migration gate
refuses retirement while unfinished work still needs configuration. Durable
registry and snapshot residue may then remain covered by the retirement
exception. Token-address changes under an existing listing are refused: retire
first and add the replacement identity through the reviewed asset process.

**Tip:** Start with `trading = "disabled"` first. Publish, verify the bot sees
the asset, then enable trading in a follow-up change.

---

## Quick Reference: Complete Checklist

For adding asset **XYZ**:

- [ ] Get `tokenized_equity` and `tokenized_equity_derivative` addresses from
      `st0x.registry` (`token-lists/<chain>.json`)
- [ ] Get the vault into the issuer's Turnkey policy and its `DEPOSIT` and
      `WITHDRAW` grants to the issuance bot's signing wallet
- [ ] Get onto the issuer host as the `S01-Issuer/s01.devops` access runbook
      says
- [ ] Pre-check, then run `POST /tokenized-assets` with the vault address, as in
      the st0x.issuance onboarding runbook
- [ ] Verify with `GET /tokenized-assets/<underlying>?network=<network>`
- [ ] Wait for Alpaca to refresh their tokencache (or ask them to force it)
- [ ] Test a mint via the liquidity bot CLI:
      `stox alpaca-tokenize -t <token_addr> -s XYZ -q 1 -r <receiving_wallet>`
- [ ] Add the asset to `t0/staging.toml` in `t0.tokens` (disabled first)
- [ ] On each hedged chain where the asset is listed: that chain's own
      `tokenized_equity_derivative`, and Turnkey approval policies for that
      chain's id
- [ ] On each redemption-capable chain that lists it -- the primary, and a
      secondary where at least one equity sets `rebalancing = "enabled"` or
      `"paused"` -- `redemption_wallet` and, before the asset is cut over to
      orchestrator mode, the orchestrator entry for that chain (see step 4a) and
      the Turnkey `MintAuth` policy for that chain's id and orchestrator; the
      first orchestrator-mode mint fails at signing without it
- [ ] Verify staging applied the disabled asset generation
- [ ] Enable trading in the token file and verify adoption
- [ ] Repeat for production when staging looks good: run `publish-t0-tokens` on
      `main` and get its PAM grant approved (during pinned rollout, also apply a
      reviewed generation bump and release)

---

## Troubleshooting

| Problem                                        | Cause                                            | Fix                                                                                                            |
| ---------------------------------------------- | ------------------------------------------------ | -------------------------------------------------------------------------------------------------------------- |
| `"Token symbol for X not found in tokencache"` | Alpaca hasn't refreshed their cache              | Wait, or contact Alpaca to force refresh                                                                       |
| Mint refused at signing (Turnkey 403)          | Vault missing from the issuer's Turnkey policy   | Get the policy change applied (step 2), then recover the mint (recovery guide, "Recovering mints")             |
| Burn refused at signing (Turnkey 403)          | Vault missing from the issuer's Turnkey policy   | Get the policy change applied (step 2), then recover the redemption (recovery guide, "Recovering redemptions") |
| Mint stuck in `MintingFailed`                  | Transient error (signing, network, etc.)         | Fix root cause, then recover it with the st0x.issuance recovery guide                                          |
| `curl` to `GET /tokenized-assets` returns 403  | That endpoint is locked to Alpaca IPs            | Use `GET /tokenized-assets/<underlying>?network=<network>` on the issuer host (step 3b)                        |
| Bot shows empty curl response                  | Bot is still backfilling (happens after restart) | Wait for backfill to complete, then retry                                                                      |
| `"user balance exceeded"` on RPC               | dRPC credits depleted                            | Top up dRPC credits (ask Josh)                                                                                 |
