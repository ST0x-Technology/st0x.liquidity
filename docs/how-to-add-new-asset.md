# How to Add a New Asset (Non-Technical Guide)

Adding a new asset to the st0x system requires changes across **four systems**
in a specific order. This guide walks through each step.

---

## Overview

When you add a new asset (e.g. SGOV), you need to:

1. **Get the token contract addresses** (from the registry)
2. **Register it in the Issuance Bot** (so Alpaca knows about it)
3. **Whitelist the contract in Fireblocks** (so minting transactions can be
   signed)
4. **Add it to the Liquidity Bot config** (so it starts trading/hedging)

---

## Step 1: Get the Token Addresses

Every tokenized asset has two contract addresses on each chain that lists it
(Base today; the same two exist per chain when an asset is listed elsewhere):

| Name                            | What it is                                                                                   | Example (SGOV)                               |
| ------------------------------- | -------------------------------------------------------------------------------------------- | -------------------------------------------- |
| **tokenized_equity**            | The base ERC-20 token (e.g. tSGOV). This is also the "vault" address the issuance bot needs. | `0xc941C1506B7555Ba8C506Fb6c9b9CC259902d612` |
| **tokenized_equity_derivative** | The wrapped/dividend-accruing version (e.g. wtSGOV).                                         | `0x78c31580c97101694c70022c83d570150c11e935` |

**Where to find them:** The `ST0x-Technology/st0x.registry` GitHub repo. If
they've been removed from the current code, check the git history.

---

## Step 2: Register the Asset in the Issuance Bot

The issuance bot runs on a DigitalOcean droplet. You need to SSH into it and
call the bot's internal API to register the new asset.

### 2a. SSH into the issuance bot server

```bash
ssh root@<ISSUANCE_BOT_DROPLET_IP>
```

### 2b. Verify the bot is running

```bash
docker ps
```

You should see `issuance-bot` with status `Up ...` (not `Restarting`).

### 2c. Check what assets are already registered

```bash
sqlite3 /mnt/volume_nyc3_02/issuance.db "SELECT payload FROM tokenized_asset_view"
```

This shows all currently registered assets. Verify your new asset is NOT already
there.

### 2d. Add the new asset

Run this command, replacing the values for your asset:

```bash
read -rsp "Issuance bot internal API key: " INTERNAL_API_KEY; echo
curl -s -X POST http://localhost:8000/tokenized-assets \
  -H 'Content-Type: application/json' \
  -H "X-API-KEY: ${INTERNAL_API_KEY}" \
  -d '{"underlying":"SGOV","token":"tSGOV","network":"base","vault":"0xc941C1506B7555Ba8C506Fb6c9b9CC259902d612"}'
unset INTERNAL_API_KEY
```

**Important notes:**

- The `vault` field = the `tokenized_equity` address (the base token, NOT the
  derivative).
- The `network` field is the chain the asset is listed on, spelled as the bot's
  chain name (`base`, `ethereum`, `hyperevm`, `robinhood`). Register once per
  chain.
- The `X-API-KEY` is the internal API key stored on the server. Check the `.env`
  file on the droplet if you don't know it. The `read -rsp` command above
  prompts for the key without echoing it or saving it to shell history.
- `POST /tokenized-assets` uses "internal auth" which allows requests from the
  Docker network. Run it from the droplet host, NOT from outside.
- The `GET /tokenized-assets` endpoint is locked to Alpaca's IPs, so you can't
  use it to verify. Use the sqlite3 command from step 2c instead.
- This is idempotent -- calling it twice with the same data is safe.

### 2e. Verify it was added

```bash
sqlite3 /mnt/volume_nyc3_02/issuance.db "SELECT payload FROM tokenized_asset_view"
```

You should now see your new asset in the list.

### 2f. Wait for Alpaca to pick it up

Alpaca periodically calls `GET /tokenized-assets` on the issuance bot to
discover available assets. After you add the asset, Alpaca's internal
"tokencache" needs to refresh before minting works.

- There is no way to force this from our side.
- If minting returns `"Token symbol for X not found in tokencache"`, Alpaca
  hasn't refreshed yet. Wait and retry, or contact Alpaca to force a refresh.

---

## Step 3: Whitelist the Contract in Fireblocks

The issuance bot uses Fireblocks to sign onchain transactions (minting). If the
new asset's vault contract isn't whitelisted in Fireblocks, minting will fail
with: `"contract 0x... is not whitelisted in Fireblocks"`

**Action:** Add the vault contract address (`tokenized_equity`) to the
Fireblocks workspace's whitelist via the Fireblocks console/UI.

This is done by whoever has admin access to the Fireblocks workspace (likely
Josh or another team member with Fireblocks access).

If a mint fails before whitelisting, the mint enters `MintingFailed` state.
After whitelisting, restart the bot (`docker restart issuance-bot`) -- the
auto-recovery will retry the failed mint on startup.

---

## Step 4: Add the Asset to the Token File

The liquidity bot (this repo) reads its per-symbol tables from the token file
that `st0x.registry` publishes to `gs://t0-artifacts-tokens/<env>/tokens.toml`
(`t0/<env>.toml` in that repository). The bot's configs,
`config/staging/st0x-hedge.toml` and `config/prod/st0x-hedge.toml`, name that
file under `[registry]` and must not carry a per-symbol table themselves: a
config that does is refused at startup.

### 4a. Edit the token file

Edit `t0/staging.toml` (or `t0/production.toml`) in `st0x.registry`. The bot
takes two tables per asset. The first says where the asset is listed on-chain,
so it goes under the chain that lists it. The second says how the bot hedges it,
which is independent of any chain -- there is one broker account and one
position per symbol. Other services own other keys on the same tables (the
file's header lists them). The bot takes its own keys and ignores the others;
which keys may appear, and their spelling, is checked by `st0x.registry`'s CI
(`t0/check.jq`) before the file is published, not by the bot. Publishing
`rebalancing = "paused"` needs this order so an older binary never reads a token
file it cannot load:

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
  wallet recovery run where the chain already has them (the primary chain
  today); pausing does not add them to a secondary.
- `wrapped_equity_recovery`: Explicit opt-in for recovery of wrapped-equity
  positions. Set to `"enabled"` to allow the bot to recover wrapped equity;
  `"disabled"` skips recovery for this asset. Must be specified for every equity
  entry.
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

Merge the `st0x.registry` change; its CI publishes the token file. Staging and
production follow the latest copy. The bot polls every ten seconds and validates
each observed generation before a graceful restart. No liquidity PR or release
is needed. Start with a disabled listing, verify adoption, then enable trading
in a second publish. Newly configured contracts are probed including disabled
rows; newly selected tokenization routes and changed startup approval targets
are checked too. Missing Turnkey coverage or transient RPC failures defer
adoption until the dependency is fixed, without a republish.

Check `registry_applied_generation`, the structured generation/hash logs and
`registry_reloads_total{result}`. A refused copy sets `registry_invalid` and
leaves the current configuration running. Promotion to last-good requires ten
minutes of uptime; two failed boots fall back to the previous good set plus new
listings disabled. Reloads are debounced for two minutes.

The bot config has no `generation` pin, and the bot refuses one. To hold a
change back, do not merge it in `st0x.registry`. To undo a change, publish the
previous file from `st0x.registry`: the bot applies it like any other change. A
listing that the revert removes is carried forward disabled (see below).

`tests/fixtures/tokens-production.toml` is a recent copy of the production file
for tests. Refresh it by hand when a test needs a newer listing;
`tokens-production-migration.toml` stays frozen.

Offline checks continue to accept `--registry-file`. Deployment checks accept
`--registry-state` and judge pending plus fallback, or running. These readers
never change boot attempts or promote state.

### Retiring an asset

Removing a listing from the token file carries its previous row forward with
trading, rebalancing and wrapped-equity recovery disabled. The same applies to a
listing removed on one chain while the symbol stays on another. The bot does
this as soon as it adopts the new copy; there is no pin to bump. Existing
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
      `st0x.registry`
- [ ] SSH into issuance bot droplet
- [ ] Run `POST /tokenized-assets` curl command with the vault address
- [ ] Verify asset appears in the database
- [ ] Whitelist the vault contract in Fireblocks (ask team member with access)
- [ ] Wait for Alpaca to refresh their tokencache (or ask them to force it)
- [ ] Test a mint via the liquidity bot CLI:
      `stox alpaca-tokenize -t <token_addr> -s XYZ -q 1 -r <receiving_wallet>`
- [ ] Add the asset to `t0/staging.toml` in `st0x.registry` (disabled first)
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
- [ ] Repeat for production in `t0/production.toml` when staging looks good, and
      verify production applied the new generation

---

## Troubleshooting

| Problem                                             | Cause                                            | Fix                                                                           |
| --------------------------------------------------- | ------------------------------------------------ | ----------------------------------------------------------------------------- |
| `"Token symbol for X not found in tokencache"`      | Alpaca hasn't refreshed their cache              | Wait, or contact Alpaca to force refresh                                      |
| `"contract 0x... is not whitelisted in Fireblocks"` | Vault contract not in Fireblocks whitelist       | Add it in Fireblocks console, then restart bot                                |
| Mint stuck in `MintingFailed`                       | Transient error (Fireblocks, network, etc.)      | Fix root cause, then `docker restart issuance-bot` (recovery runs on startup) |
| `curl` to `GET /tokenized-assets` returns 403       | That endpoint is locked to Alpaca IPs            | Use `sqlite3` to query the DB directly instead                                |
| Bot shows empty curl response                       | Bot is still backfilling (happens after restart) | Wait for backfill to complete, then retry                                     |
| `"user balance exceeded"` on RPC                    | dRPC credits depleted                            | Top up dRPC credits (ask Josh)                                                |
