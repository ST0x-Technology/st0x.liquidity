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

### `FireblocksWallet`

Resolves the vault's onchain address from the Fireblocks API and submits
`CONTRACT_CALL` transactions through Fireblocks. Enabled behind the `fireblocks`
feature flag.

### `TurnkeyWallet`

Wraps `alloy-signer-turnkey` to sign transactions via Turnkey's AWS Nitro secure
enclaves. Low-latency signing (50-100ms). Enabled behind the `turnkey` feature
flag.

### `RawPrivateKeyWallet`

Signs locally with a raw private key. Wraps an alloy provider with an embedded
`EthereumWallet`. Enabled behind the `local-signer` feature flag.

## Configuration

Cargo features on the main crate (`wallet-fireblocks` / `wallet-turnkey` /
`wallet-private-key`) gate which backends are compiled into the binary. The
active backend is then chosen from `[rebalancing.wallet]` in `RebalancingConfig`
/ `RebalancingSecrets`; only switching to a backend that was not compiled in
requires a rebuild.

### Fireblocks (`wallet-fireblocks`)

| Field                  | Location | Description                                    |
| ---------------------- | -------- | ---------------------------------------------- |
| `vault_account_id`     | config   | Fireblocks vault account containing the wallet |
| `environment`          | config   | `production` or `sandbox`                      |
| `chain_asset_ids`      | config   | Mapping from chain ID to Fireblocks base asset |
| `api_key`              | secrets  | Fireblocks API key identifier                  |
| `api_private_key_path` | secrets  | Path to the RSA PEM used for Fireblocks auth   |

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
