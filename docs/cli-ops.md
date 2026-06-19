# CLI Operations Guide

On the deployed server, the CLI is available as `stox`. It automatically loads
the server config and secrets, so you don't need to pass `--config` or
`--secrets`.

```
stox <command> [options]
```

Use `stox --help` to list all commands and `stox <command> --help` for details
on any specific command.

## Token Address Reference

The **unwrapped** tokenized-equity contract address per symbol. The tokenization
and redemption commands resolve this from `-s` via `[assets.equities]`, so it no
longer has to be passed by hand; the table is kept for reference and
cross-checking. Current addresses:

| Symbol | Unwrapped Token Address                      |
| ------ | -------------------------------------------- |
| RKLB   | `0xf6744fd94e27c2f58f6110aa9fdc77a87e41766b` |
| SPYM   | `0x8fdf41116f755771bfe0747d5f8c3711d5debfbb` |
| TSLA   | `0x4e169cd2ab4f82640a8c65c68fed55863866fdb0` |
| AMZN   | `0x466cb2e46fa1afc0ab5e22274b34d0391db18efd` |
| NVDA   | `0x7271a3c91bb6070ed09333b84a815949d4f16d14` |
| MSTR   | `0x013b782f402d61aa1004cca95b9f5bb402c9d5fe` |
| QSEP   | `0x4a9a9fc94a507559481270d0bff3315ab92fcefa` |
| IAU    | `0x9a507314ea2a6c5686c0d07bfecb764dcf324dff` |
| COIN   | `0x626757e6f50675d17fcad312e82f989ae7a23d38` |
| SIVR   | `0x58ce5024b89b4f73c27814c0f0abbea331c99be8` |
| CRCL   | `0x38eb797892ed71da69bdc27a456a7c83ff813b52` |
| PPLT   | `0x1f17523b147ccc2a2328c0f014f6d49c479ea063` |
| BMNR   | `0xfbde45df60249203b12148452fc77c3b5f811eb2` |

## Common Workflows

### Buying and Minting (Acquiring Tokenized Shares)

To get tokenized shares into the **market-making** wallet, buy offchain shares
via the broker and then tokenize them onchain. These run as the market-making
bot (`stox`). For a dividend bump, use `s01 dividend-bump` instead (it runs as
the issuer -- see below); do not follow the steps here with the liquidity
wallet.

**Step 1: Buy shares offchain**

```
stox buy -s COIN -q 10
```

The command submits the order but does **not** wait for the fill. Check the
Alpaca dashboard to confirm the order filled before proceeding.

**Step 2: Tokenize (mint) onchain**

```
stox alpaca-tokenize -s COIN -q 10 \
  -r 0xbd41F40D91eE4E816Ada1Aa842e94aEb6B6385a6
```

- The tokenized-equity address is resolved from `-s` via `[assets.equities]`, so
  it never has to be entered by hand
- `-r` is the wallet that receives the minted tokens -- **always specify this**.
  Use the Fireblocks liquidity address
  (`0xbd41F40D91eE4E816Ada1Aa842e94aEb6B6385a6`)

### Applying a Dividend NAV Bump (Donating into the Wrapper)

When a dividend or corporate action revalues an equity, bump the wtStock
wrapper's NAV with a single `dividend-bump` command: it buys the equivalent
shares with the dividend cash, tokenizes them onchain, and **donates** the
tokenized shares into the wrapper, waiting for each step to settle before the
next. A bare ERC-20 transfer into the ERC-4626 vault raises its
`convertToAssets` ratio without minting any wrapped shares -- see
[wrapper-nav-bump.md](wrapper-nav-bump.md) for why.

Run it as the **issuer** with `s01` -- the issuer-config counterpart of `stox`
(same binary, but defaulting to the issuer's `[wallet]` turnkey signer, Alpaca
account, and database) -- so the buy, tokenize, and donate are funded and signed
by the issuer rather than the market-making wallet:

```
s01 dividend-bump -s COIN -q 10
```

`s01` defaults to `/run/st0x/s01-issuer.config` and
`/run/agenix/s01-issuer.toml`; override with `S01_CONFIG`/`S01_SECRETS`.
**Always run a dividend bump as the issuer:** a plain `stox dividend-bump` would
buy, tokenize, and donate from the **market-making** wallet, not the issuer. To
use `stox` you must pass the issuer `--config`/`--secrets` explicitly.

The command buys 10 COIN offchain and waits for the fill, tokenizes the shares
to the configured wallet and waits for them to arrive onchain, then transfers
them into the wrapper and waits for confirmation. No wrapped shares are minted
-- every existing wtCOIN holder's shares are simply worth more. The standalone
`buy`, `alpaca-tokenize`, and `donate-equity` subcommands remain for running a
single step in isolation; use `wrap-equity` only when you want to _receive_
wrapped shares (a deposit), never for a dividend bump.

### Selling and Redeeming (Liquidating Tokenized Shares)

Reverse of buying and minting: redeem tokens offchain, then sell the shares.

**Step 1: Redeem tokens**

```
stox alpaca-redeem -s COIN -q 10
```

**Step 2: Sell shares offchain**

```
stox sell -s COIN -q 10
```

### Checking Order Status

```
stox order-status --order-id <order-id>
```

### Moving Stranded Raindex Equity Vault Funds

Use this when an equity vault ID was removed from config but inventory polling
warns that the retired vault still has a positive balance. After the bot has
restarted with the new config, new deposits and rebalancing paths use the first
entry in the configured vault list (config file order), but the old vault
remains registered for balance visibility.

Use the token address stored in
`assets.equities.symbols.<SYMBOL>.tokenized_equity_derivative`, not the
unwrapped token address table above. For the symbol you are moving, confirm the
configured destination vault ID in the current config before moving funds.

**Step 1: Withdraw from the retired vault**

```bash
stox vault-withdraw \
  --amount <shares> \
  --token <tokenized-equity-derivative-address> \
  --vault-id <retired-vault-id>
```

**Step 2: Deposit the same tokens into the configured vault**

```bash
stox vault-deposit \
  --amount <shares> \
  --token <tokenized-equity-derivative-address> \
  --vault-id <configured-vault-id>
```

The commands print the amount, token, wallet, orderbook, vault ID, decimals, and
smallest-unit amount before submitting the transaction. Verify those values
match the retired source vault and configured destination vault before relying
on the printed transaction hash.

## Alpaca Crypto Wallet Management

### USDC Deposits and Withdrawals

```
stox alpaca-deposit -a 500           # Deposit USDC from Ethereum to Alpaca
stox alpaca-withdraw -a 500          # Withdraw USDC from Alpaca
stox alpaca-withdraw -a 500 -t 0x... # Withdraw to a specific address
```

### USD/USDC Conversion

```
stox alpaca-convert -d to-usd -a 1000    # USDC -> USD (for buying shares)
stox alpaca-convert -d to-usdc -a 1000   # USD -> USDC (for withdrawals)
```

### Address Whitelisting

Addresses must be whitelisted before Alpaca will send withdrawals to them:

```
stox alpaca-whitelist -a 0x...       # Whitelist an address
stox alpaca-whitelist-list           # List whitelisted addresses
stox alpaca-unwhitelist -a 0x...     # Remove an address
```

### Transfer History

```
stox alpaca-transfers                # All transfers
stox alpaca-transfers --pending      # Only pending transfers
```

### Tokenization Request History

```
stox alpaca-tokenization-requests
```

### Rechecking Failed Equity Transfers

Use `recheck-transfer` when the bot marked an equity mint or redemption as
failed, but Alpaca later shows the same provider request as completed.

**The bot must be running.** `recheck-transfer` delegates to the bot's REST API
(`POST /transfers/recheck/<kind>/<id>` on the configured `server_port`) rather
than mutating the database directly. Recovery has to run inside the bot process
so the recovery event dispatches through the in-process inventory reactor (which
corrects the live inventory view) and shares the bot's resume lock (so it cannot
race `/transfers/resume` into a double on-chain wrap).

> **Operational guardrail:** like `/transfers/resume`, the `/transfers/recheck`
> endpoint is currently **unauthenticated** — any caller that can reach
> `server_port` can recover live transfers and trigger inventory-affecting
> workflow steps. Until an auth guard is added, the bot's `server_port` **must**
> be bound to an operator-only/firewalled interface and never exposed publicly.

Recoverable cases:

- Mint failed at acceptance (accepted by Alpaca, but tokens never received),
  then Alpaca later reports the mint completed.
  `stox recheck-transfer --type mint --id <issuer-request-id>` records provider
  completion and resumes wrapping/depositing to Raindex. A mint that already
  received tokens and then failed while wrapping or depositing is **not**
  recoverable this way (recovery would re-wrap tokens that already moved); the
  command reports it as not recoverable.
- Redemption failed after tokens were sent, with a redemption tx in the
  aggregate.
  `stox recheck-transfer --type redemption --id <redemption-aggregate-id>`
  completes it if Alpaca now reports completed.
- Non-failed active mints/redemptions can also be passed to `recheck-transfer`;
  the command resumes the normal workflow instead of forcing recovery.

The command prints the recovery outcome: `recovered`, `resumed`,
`already_completed`, `left_unchanged`, `not_detected_yet`, or `not_recoverable`.

Not covered by `recheck-transfer` yet:

- Mint requests rejected before Alpaca acceptance. There is no provider
  completion to discover.
- Mints that failed after receiving tokens (wrapping/deposit failures). The
  tokens already left the issuer, so provider-completion recovery does not
  apply.
- Redemptions that failed before tokens were sent. The bot has no provider tx or
  request id to look up, so this needs a retry/resume-send style CLI.
- Provider rejections. These remain failed unless an operator performs a
  separate manual reconciliation.
- USDC rebalancing failures. Those use the USDC/CCTP state machine and have
  their own recovery commands. A manual `transfer-usdc` prints its transfer id
  and, if interrupted after the burn, is resumed with
  `stox resume-usdc-transfer --id <id> --direction <to-raindex|to-alpaca>
  --amount <amount>`.
  The `--direction` must match the original (a mismatch is rejected to avoid
  mis-driving) and an unknown id is rejected rather than starting a fresh burn;
  the `--amount` is required for symmetry with `transfer-usdc` but a resume uses
  the aggregate's persisted amount. Run it only when the bot is not concurrently
  driving that same id, since it drives the aggregate directly rather than
  through the bot's resume lock.

### Clearing a pre-burn guard latch

Use `fail-usdc-transfer` when a USDC rebalance is stranded at
`WithdrawalComplete` or `BridgingSubmitting` and the guard must be released.
This transitions the aggregate to `BridgingFailed` (pre-burn,
`burn_tx_hash: None`), which is non-guard-holding. The rebalancing guard clears
on the next bot restart.

**Stop the bot before running this command** to eliminate the race where the bot
advances the transfer to `Bridging` between the preflight and the send.

`WithdrawalComplete` is unconditionally pre-burn: no CCTP burn has been
broadcast yet. However, the Alpaca->on-chain USDC withdrawal has already
completed, so the USDC is sitting in the market-maker wallet. After clearing the
guard with this command, reconcile or handle those funds separately as needed.
The command is safe to run once the bot is stopped.

`BridgingSubmitting` is NOT unconditionally safe. A crash at this state may have
already broadcast a CCTP burn whose `BridgingInitiated` event never persisted.
Before running this command on a `BridgingSubmitting` transfer, verify on-chain
that no recent CCTP burn was submitted from the market-maker wallet (e.g. via
`cast` against the Circle CCTP contract or by inspecting recent wallet txs).

- **If no burn is found**: run `fail-usdc-transfer` to clear the guard.
- **If a burn IS found** while the aggregate is still `BridgingSubmitting`: do
  NOT run `fail-usdc-transfer` (strands the burned funds) and do NOT run
  `reconcile-usdc-transfer` (its preflight rejects `BridgingSubmitting` -- it
  only accepts persisted post-burn terminals such as `DepositFailed`). Instead,
  run `resume-usdc-transfer`: its `find_recent_burn` scan adopts the orphan
  burn, persists `BridgingInitiated`, and the transfer continues normally.

`reconcile-usdc-transfer` is the path for persisted post-burn terminal failures
(e.g. `DepositFailed`, `BridgingFailed` with a burn tx recorded).

    stox fail-usdc-transfer --id <uuid> --reason "pre-burn crash, burn not attempted"

For local dashboard testing, run:

```
nix run .#simulate-failures
```

The backend creates failed mint and redemption transfers whose mock Alpaca
provider later completes, then prints the exact `recheck-transfer` commands for
that run's generated config, secrets, database, and mock API port.
