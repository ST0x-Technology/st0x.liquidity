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

### `FireblocksWallet` (production)

Wraps the Fireblocks SDK client. Builds a `CONTRACT_CALL` transaction request,
submits it to the Fireblocks API, polls for completion, and fetches the receipt
from a read-only RPC provider. Uses MPC-based key management -- the private key
never exists in a single location.

### `RawPrivateKeyWallet` (tests only)

Wraps an alloy provider with an embedded `EthereumWallet` for anvil-based
testing. Not compiled in production builds.

## Configuration

Fireblocks credentials live in the encrypted secrets file. The main crate's
`RebalancingConfig` / `RebalancingSecrets` are responsible for parsing TOML and
assembling a `FireblocksCtx`, which is this crate's public construction
interface (per `docs/domain.md`).

| Field                         | Location | Description                                    |
| ----------------------------- | -------- | ---------------------------------------------- |
| `fireblocks_api_user_id`      | config   | Fireblocks API user identifier                 |
| `fireblocks_secret_path`      | secrets  | Path to RSA private key for API authentication |
| `fireblocks_vault_account_id` | config   | Vault account containing the signing key       |
| `fireblocks_chain_asset_ids`  | config   | Mapping of chain IDs to Fireblocks asset IDs   |
| `fireblocks_environment`      | config   | `production` or `sandbox`                      |

The market maker wallet address is derived from the Fireblocks vault account at
startup via the Fireblocks API.
