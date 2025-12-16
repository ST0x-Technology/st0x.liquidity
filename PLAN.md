# Plan: Manual Rebalancing CLI Commands

This plan adds CLI commands to manually test rebalancing operations before
enabling the full automated rebalancing flow. The commands allow testing equity
tokenization and USDC bridging independently.

## Task 1. Add CLI Command Definitions

Add four new CLI subcommands to the `Commands` enum in `src/cli.rs`:

- [ ] `MintEquity` - Mint tokenized equity (move equity from Alpaca → Base)
  - Parameters: `--ticker` (e.g., AAPL), `--quantity` (decimal shares)
  - Flow: Calls Alpaca tokenization API to mint tokens on Base

- [ ] `RedeemEquity` - Redeem tokenized equity (move equity from Base → Alpaca)
  - Parameters: `--ticker` (e.g., AAPL), `--quantity` (decimal shares),
    `--token-address` (0x...)
  - Flow: Sends tokens to redemption wallet, polls for completion

- [ ] `UsdcToBase` - Move USDC from Alpaca wallet to Base vault
  - Parameters: `--amount` (decimal USDC)
  - Flow: Alpaca withdrawal → CCTP Ethereum→Base → vault deposit

- [ ] `UsdcToAlpaca` - Move USDC from Base vault to Alpaca wallet
  - Parameters: `--amount` (decimal USDC)
  - Flow: Vault withdrawal → CCTP Base→Ethereum → Alpaca deposit

## Task 2. Add Rebalancing Configuration Requirement

The manual rebalancing commands require the same configuration as the automated
rebalancing system. We need to:

- [ ] Add a helper function to check if rebalancing config is available
- [ ] Add clear error messages when rebalancing env vars are missing
- [ ] Document required environment variables in CLI help text

Required env vars for rebalancing commands:

- `REDEMPTION_WALLET` - Issuer's wallet for equity redemptions
- `MARKET_MAKER_WALLET` - Our wallet for USDC operations
- `ETHEREUM_RPC_URL` - For CCTP operations
- `ETHEREUM_PRIVATE_KEY` - For signing Ethereum transactions
- `BASE_ORDERBOOK` - For vault operations
- `USDC_VAULT_ID` - Vault ID for USDC deposits

## Task 3. Implement MintEquity Command

- [ ] Add handler in `run_command_with_writers` for `MintEquity`
- [ ] Validate ticker format and quantity > 0
- [ ] Create `AlpacaTokenizationService` from config
- [ ] Generate unique `IssuerRequestId` for the operation
- [ ] Call `service.request_mint(symbol, quantity, wallet)`
- [ ] Poll with `service.poll_mint_until_complete(request_id)` until terminal
      status
- [ ] Display progress and final status to user
- [ ] Add tests for the command

## Task 4. Implement RedeemEquity Command

- [ ] Add handler in `run_command_with_writers` for `RedeemEquity`
- [ ] Validate ticker format, quantity > 0, and token address format
- [ ] Create `AlpacaTokenizationService` from config
- [ ] Convert quantity to U256 with 18 decimals
- [ ] Call `service.send_for_redemption(token, amount)` to send tokens
- [ ] Poll with `service.poll_for_redemption(tx_hash)` until detected
- [ ] Poll with `service.poll_redemption_until_complete(request_id)` until
      terminal
- [ ] Display progress and final status to user
- [ ] Add tests for the command

## Task 5. Implement UsdcToBase Command

- [ ] Add handler in `run_command_with_writers` for `UsdcToBase`
- [ ] Validate amount > 0
- [ ] Create required services (AlpacaWalletService, CctpBridge, VaultService)
- [ ] Generate unique `UsdcRebalanceId` for tracking
- [ ] Execute the flow with progress output:
  1. Initiate Alpaca withdrawal to market maker wallet
  2. Poll until withdrawal complete
  3. Execute CCTP burn on Ethereum
  4. Poll Circle API for attestation
  5. Execute CCTP mint on Base
  6. Deposit to Rain vault
- [ ] Display final status to user
- [ ] Add tests for the command

## Task 6. Implement UsdcToAlpaca Command

- [ ] Add handler in `run_command_with_writers` for `UsdcToAlpaca`
- [ ] Validate amount > 0
- [ ] Create required services (AlpacaWalletService, CctpBridge, VaultService)
- [ ] Generate unique `UsdcRebalanceId` for tracking
- [ ] Execute the flow with progress output:
  1. Withdraw from Rain vault on Base
  2. Execute CCTP burn on Base
  3. Poll Circle API for attestation
  4. Execute CCTP mint on Ethereum (to Alpaca deposit address)
  5. Poll Alpaca until deposit credited
- [ ] Display final status to user
- [ ] Add tests for the command

## Design Decisions

### Why Not Reuse Full Managers?

The existing `MintManager`, `RedemptionManager`, and `UsdcRebalanceManager`
include CQRS event sourcing for production durability. For CLI testing, we want:

- Direct execution without event sourcing overhead
- Simple progress output to stdout
- Clear error messages without aggregate state concerns

We'll create simpler execution paths that use the underlying services directly.

### Service Construction

Rather than duplicating the service construction logic from `spawn.rs`, we'll:

1. Extract service construction into a shared helper if needed
2. Create services on-demand for CLI commands
3. Require rebalancing config environment variables

### Error Handling

All commands should:

- Validate inputs before starting operations
- Display clear progress messages
- Show detailed error information on failure
- Return non-zero exit code on failure

### Broker Requirement

The equity commands (MintEquity, RedeemEquity) require Alpaca broker
configuration since they use the Alpaca tokenization API. We should validate
this upfront and show a clear error if Schwab broker is configured instead.
