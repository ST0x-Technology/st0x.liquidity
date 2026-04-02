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

The `-t` flag on tokenization/redemption commands expects the **unwrapped**
token contract address for that equity. Current addresses:

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

To get tokenized shares into a wallet, buy offchain shares via the broker and
then tokenize them onchain.

**Step 1: Buy shares offchain**

```
stox buy -s COIN -q 10
```

The command submits the order but does **not** wait for the fill. Check the
Alpaca dashboard to confirm the order filled before proceeding.

`buy` and `sell` accept fractional quantities for brokers that support them:

```
stox buy -s COIN -q 6.15
stox sell -s COIN -q 2.5
```

Schwab still rejects fractional `buy` and `sell` orders. Use whole shares there.

**Step 2: Tokenize (mint) onchain**

```
stox alpaca-tokenize -s COIN -q 10 \
  -t 0x626757e6f50675d17fcad312e82f989ae7a23d38 \
  -r 0xbd41F40D91eE4E816Ada1Aa842e94aEb6B6385a6
```

- `-t` is the unwrapped token contract address (see table above)
- `-r` is the wallet that receives the minted tokens -- **always specify this**.
  Use the Fireblocks liquidity address
  (`0xbd41F40D91eE4E816Ada1Aa842e94aEb6B6385a6`)

### Selling and Redeeming (Liquidating Tokenized Shares)

Reverse of buying and minting: redeem tokens offchain, then sell the shares.

**Step 1: Redeem tokens**

```
stox alpaca-redeem -s COIN -q 10 \
  -t 0x626757e6f50675d17fcad312e82f989ae7a23d38
```

**Step 2: Sell shares offchain**

```
stox sell -s COIN -q 10
```

### Manual Alpaca Broker API Limit Orders

For manual operator intervention, `buy` and `sell` can place limit orders when
the configured broker is Alpaca Broker API:

```
stox buy -s COIN -q 10 --limit-price 195.25
stox sell -s COIN -q 5.5 --limit-price 201.00
```

Extended-hours limit orders are supported with `--extended-hours`:

```
stox buy -s COIN -q 10 --limit-price 195.25 --extended-hours
```

- `--extended-hours` requires `--limit-price`
- `--limit-price` is only supported with Alpaca Broker API
- `--time-in-force` cannot be combined with `--limit-price`
- limit-order commands submit the order and return the Alpaca order ID; they do
  not wait for the fill

## Raindex Vault Operations

### Generic ERC20 Vault Withdrawals

`vault-withdraw` can withdraw arbitrary ERC20s from a Raindex vault to the
configured Base liquidity wallet:

```
stox vault-withdraw \
  -a 10.5 \
  -t 0x626757e6f50675d17fcad312e82f989ae7a23d38 \
  -v 0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
```

- `-a` is a human-readable token amount
- `-t` is the ERC20 token address
- `-v` is the Raindex vault ID
- token decimals are resolved automatically from onchain metadata

### USDC Cash Vault Shortcut

The old USDC-specific withdrawal flow still exists as a convenience wrapper:

```
stox vault-withdraw-usdc -a 250
```

This resolves `assets.cash.vault_id` from config and forwards into the generic
vault withdrawal logic.

### Wrap Tokenized Equity Into Wrapped Vault Shares

`wrap-equity` deposits tokenized equity into the configured ERC-4626 vault and
returns wrapped vault shares:

```
stox wrap-equity -s AAPL -q 10.5
```

This command:

- requires rebalancing mode
- uses the configured Base liquidity wallet
- resolves the wrapped and underlying token addresses from config
- prints the transaction hash and wrapped amount received

### Unwrap Wrapped Vault Shares Into Tokenized Equity

`unwrap-equity` redeems wrapped ERC-4626 vault shares back into the underlying
tokenized equity:

```
stox unwrap-equity -s AAPL -q 10.5
```

This command:

- requires rebalancing mode
- uses the configured Base liquidity wallet
- resolves the wrapped and underlying token addresses from config
- prints the transaction hash and underlying amount received

### Checking Order Status

```
stox order-status --order-id <order-id>
```

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
