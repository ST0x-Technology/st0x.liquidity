# st0x-evm

EVM wallet abstraction for the st0x liquidity system.

## Overview

All onchain write operations (token approvals, vault deposits/withdrawals,
bridge burns/mints, ERC-4626 wrap/unwrap, ERC-20 transfers) are submitted
through this crate's `Wallet` trait rather than calling `.send().await` directly
on contract instances.

This separates "what to call" (calldata construction, owned by each consumer)
from "how to sign and submit" (key management, owned by this crate).

## Implementations

### `TurnkeyWallet`

Wraps `alloy-signer-turnkey` to sign transactions via Turnkey's AWS Nitro secure
enclaves. Low-latency signing (50-100ms). Enabled behind the `turnkey` feature
flag.

### `RawPrivateKeyWallet`

Signs locally with a raw private key. Wraps an alloy provider with an embedded
`EthereumWallet`. Enabled behind the `local-signer` feature flag.

## Configuration

The wallet backend is selected at compile time via cargo features on the main
crate (`wallet-turnkey` or `wallet-private-key`). Wallet credentials live in the
main crate's `RebalancingConfig` / `RebalancingSecrets` TOML sections under
`[rebalancing.wallet]`.

### Turnkey (`wallet-turnkey`)

| Field             | Location | Description                       |
| ----------------- | -------- | --------------------------------- |
| `address`         | config   | Wallet address on both chains     |
| `organization_id` | config   | Turnkey organization identifier   |
| `api_private_key` | secrets  | Hex-encoded P-256 API private key |

### Raw private key (`wallet-private-key`)

| Field         | Location | Description                       |
| ------------- | -------- | --------------------------------- |
| `address`     | config   | Wallet address on both chains     |
| `private_key` | secrets  | Raw EVM private key (hex-encoded) |
