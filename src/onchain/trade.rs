//! Onchain trade conversion and persistence. Converts raw blockchain events
//! ([`RaindexTradeEvent`]) into structured [`OnchainTrade`]s with symbol
//! resolution and price calculation. Also provides
//! vault extraction utilities for the vault registry.

use alloy::primitives::ruint::FromUintError;
use alloy::primitives::{Address, B256, TxHash, U256};
use alloy::providers::Provider;
use alloy::rpc::types::Log;
use alloy::sol_types::SolEvent;
use backon::{ExponentialBuilder, Retryable};
use chrono::{DateTime, Utc};
use rain_math_float::{Float, FloatError};
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

use st0x_config::{ChainAssets, InventoryAdapterVenue, InventoryAdapters, TradingChain};
use st0x_evm::{Chain, Evm, EvmError, IERC20, OpenChainErrorRegistry, USDC_BASE};
use st0x_execution::{Direction, FractionalShares, HasZero, Symbol};
use st0x_float_serde::format_float_with_fallback;
use st0x_registry::SymbolCache;

use crate::bindings::IRaindexInventory::{OperatorDeposit, OperatorWithdraw};
use crate::bindings::IRaindexV6::{ClearV3, OrderV4, TakeOrderV3};
use crate::onchain::OnChainError;
use crate::onchain::backfill::{bucket_inventory_logs, pair_inventory_settlements};
use crate::onchain::io::{
    InputToken, OutputToken, TokenizedSymbol, TradeDetails, Usdc, WrappedTokenizedShares,
};
use crate::onchain_trade::{InventoryVenue, OnChainTradeSource};

/// Onchain trade event feeding the hedge pipeline.
///
/// * `ClearV3` / `TakeOrderV3` -- direct Raindex clears against the orderbook.
///   Emitted when a solver clears a Raindex order the bot owns; unchanged path.
/// * `InventoryTrade` -- settlement through the shared `RaindexInventory` on
///   behalf of a venue adapter (Bebop hook, univ4 hook, or any future venue
///   holding `OPERATOR_ROLE`). Emitted as a pair of `OperatorWithdraw` +
///   `OperatorDeposit` events on the same tx: the vault deltas ARE the trade,
///   agnostic to which adapter routed it. The `operator` topic identifies the
///   venue for attribution but is not used as a hedge signal.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RaindexTradeEvent {
    ClearV3(Box<ClearV3>),
    TakeOrderV3(Box<TakeOrderV3>),
    InventoryTrade(Box<InventoryTrade>),
}

/// One venue-driven settlement against the shared inventory: the vault
/// balances went down by `withdraw.amount` for `withdraw.token` and up by
/// `deposit.amount` for `deposit.token`, both within the same tx. Direction
/// is determined by which side is the equity token (non-USDC): a withdraw of
/// the equity vault means the pool sent equity out (bot is now short) and a
/// deposit means the pool received equity (bot is now long).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InventoryTrade {
    pub deposit: OperatorDeposit,
    pub withdraw: OperatorWithdraw,
}

impl RaindexTradeEvent {
    pub(crate) fn kind(&self) -> &'static str {
        match self {
            Self::ClearV3(_) => "ClearV3",
            Self::TakeOrderV3(_) => "TakeOrderV3",
            Self::InventoryTrade(_) => "InventoryTrade",
        }
    }
}

/// The bot's own signing wallet address.
///
/// The bot's rebalancing also calls
/// `deposit4`/`withdraw4` on the shared inventory (treasury moves, not venue
/// fills), so `InventoryTrade` pairing must filter out any `OperatorDeposit`/
/// `OperatorWithdraw` whose `operator` is this address. Distinct from the
/// Raindex order/vault owner (`order_owner`/`vault_owner`) even though both
/// are plain `Address` -- without this wrapper a positional swap at a call
/// site (e.g. `OnchainTrade::try_from_tx_hash`) would compile silently and
/// misattribute which legs are "the bot's own", the same risk `InputToken`/
/// `OutputToken` guard against in `io.rs`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BotOperator(pub Address);

/// Addresses that distinguish the bot's two roles while recovering a trade
/// from a transaction receipt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecoveryActors {
    pub order_owner: Address,
    pub bot_operator: BotOperator,
}

/// Information about a vault extracted from an order's IO specification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct VaultInfo {
    pub(crate) token: Address,
    pub(crate) vault_id: B256,
}

/// Extracts vault information from an OrderV4 for both input and output at the
/// specified indices.
///
/// Returns `(input_vault, output_vault)` if both indices
/// are valid, or `None` if either is out of bounds.
pub(crate) fn extract_vault_info(
    order: &OrderV4,
    input_index: usize,
    output_index: usize,
) -> Option<(VaultInfo, VaultInfo)> {
    let input = order.validInputs.get(input_index)?;
    let output = order.validOutputs.get(output_index)?;

    Some((
        VaultInfo {
            token: input.token,
            vault_id: input.vaultId,
        },
        VaultInfo {
            token: output.token,
            vault_id: output.vaultId,
        },
    ))
}

/// Vault info paired with the order owner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct OwnedVaultInfo {
    pub(crate) owner: Address,
    pub(crate) vault: VaultInfo,
}

/// Extracts all vault information from a ClearV3 event.
///
/// Returns vaults from both alice and bob orders. Each
/// order contributes two vaults (input and output) as
/// determined by the clear config indices.
pub(crate) fn extract_vaults_from_clear(event: &ClearV3) -> Vec<OwnedVaultInfo> {
    let participants = [
        (
            &event.alice,
            event.clearConfig.aliceInputIOIndex,
            event.clearConfig.aliceOutputIOIndex,
        ),
        (
            &event.bob,
            event.clearConfig.bobInputIOIndex,
            event.clearConfig.bobOutputIOIndex,
        ),
    ];

    participants
        .into_iter()
        .flat_map(|(order, input_idx, output_idx)| {
            extract_owned_vaults(order, input_idx, output_idx)
        })
        .collect()
}

pub(crate) fn extract_owned_vaults(
    order: &OrderV4,
    input_idx: U256,
    output_idx: U256,
) -> Vec<OwnedVaultInfo> {
    let (Ok(in_idx), Ok(out_idx)) = (input_idx.try_into(), output_idx.try_into()) else {
        warn!(
            target: "hedge",
            owner = %order.owner,
            %input_idx,
            %output_idx,
            "extract_owned_vaults: failed to convert input_idx or output_idx from U256 to usize"
        );
        return vec![];
    };

    let Some((input, output)) = extract_vault_info(order, in_idx, out_idx) else {
        warn!(
            target: "hedge",
            owner = %order.owner,
            input_index = %in_idx,
            output_index = %out_idx,
            "extract_vault_info: IO indices out of bounds"
        );
        return vec![];
    };

    vec![
        OwnedVaultInfo {
            owner: order.owner,
            vault: input,
        },
        OwnedVaultInfo {
            owner: order.owner,
            vault: output,
        },
    ]
}

#[derive(Debug, Clone)]
pub struct OnchainTrade {
    pub source: OnChainTradeSource,
    pub chain: Chain,
    pub tx_hash: TxHash,
    pub log_index: u64,
    pub(crate) symbol: TokenizedSymbol<WrappedTokenizedShares>,
    pub(crate) equity_token: Address,
    pub amount: FractionalShares,
    pub direction: Direction,
    pub(crate) price: Usdc,
    /// Block the fill was confirmed in. Needed to witness the fill into the
    /// `OnChainTrade` log (the `Witness` command requires it). Absent only when
    /// neither the log nor the receipt carried a block number.
    pub block_number: Option<u64>,
    pub block_timestamp: Option<DateTime<Utc>>,
}

impl OnchainTrade {
    pub fn symbol(&self) -> &Symbol {
        self.symbol.base()
    }

    pub fn price(&self) -> Float {
        self.price.value()
    }
}

/// Number of retries for `fetch_inventory_token_decimals`'s `decimals()`
/// call. Matches `SymbolCache::resolve_symbol`'s retry budget (which
/// `resolve_inventory_token_symbol` inherits) so a single transient RPC blip
/// on either introspection call gets the same bounded number of attempts
/// before the fill is either hedged or genuinely skipped.
pub(crate) const INVENTORY_TOKEN_DECIMALS_MAX_RETRIES: usize = 3;

/// Classifies an `EvmError` from `InventoryTrade` token introspection
/// (`decimals()`/`symbol()` on a venue-supplied token) into either a caught,
/// skippable [`TradeValidationError::TokenIntrospectionFailed`] or a
/// retryable [`OnChainError::Evm`].
///
/// `trade.deposit.token`/`trade.withdraw.token` come from `OperatorDeposit`/
/// `OperatorWithdraw` events emitted by *any* `OPERATOR_ROLE` holder on the
/// shared inventory (Bebop hook, univ4 hook, or any future venue) -- not the
/// bot's own trusted order config. A non-standard token (reverts, has no
/// code, or returns malformed data) is a genuine defect in that token and
/// must not surface as an opaque `OnChainError::Evm`: `perform()` doesn't
/// catch that variant, so apalis retries would exhaust and trip the
/// conductor-wide fail-stop over a single misbehaving third-party token.
///
/// But `EvmError::Contract` is *also* produced for pure transport failures
/// (connection reset, timeout) when no revert data could be decoded --
/// `is_revert()` disambiguates by checking for actual on-chain revert data.
/// A transient transport failure on an otherwise-fine, already-configured
/// token must not permanently drop a legitimate fill: it is reclassified
/// back to `OnChainError::Evm` so `perform()` leaves it uncaught and apalis
/// retries with backoff, same as the trusted ClearV3/TakeOrderV3 path.
/// `EvmError::AbiDecode` is treated as a genuine token defect (not
/// transient): it means the token returned data that could not be decoded as
/// the expected return type, e.g. a non-standard `decimals()`/`symbol()`
/// implementation.
fn classify_inventory_token_introspection_error(source: EvmError, token: Address) -> OnChainError {
    if source.is_revert() || matches!(source, EvmError::AbiDecode(_)) {
        OnChainError::Validation(TradeValidationError::TokenIntrospectionFailed {
            token,
            source: Box::new(source),
        })
    } else {
        OnChainError::Evm(source)
    }
}

/// Introspects an `InventoryTrade`-supplied token's `decimals()`, retrying a
/// bounded number of times (matching `SymbolCache::resolve_symbol`'s retry
/// budget) before classifying a persistent failure via
/// [`classify_inventory_token_introspection_error`].
async fn fetch_inventory_token_decimals<E: Evm>(
    evm: &E,
    token: Address,
) -> Result<u8, OnChainError> {
    (|| async {
        evm.call::<OpenChainErrorRegistry, _>(token, IERC20::decimalsCall {})
            .await
    })
    .retry(ExponentialBuilder::new().with_max_times(INVENTORY_TOKEN_DECIMALS_MAX_RETRIES))
    .await
    .map_err(|source| classify_inventory_token_introspection_error(source, token))
}

/// Resolves an `InventoryTrade`-supplied token's symbol (`SymbolCache::resolve_symbol`
/// already retries internally), classifying a persistent failure via
/// [`classify_inventory_token_introspection_error`] for the same reason as
/// [`fetch_inventory_token_decimals`].
async fn resolve_inventory_token_symbol<E: Evm>(
    cache: &SymbolCache,
    evm: &E,
    chain: Chain,
    token: Address,
) -> Result<String, OnChainError> {
    cache
        .resolve_symbol(evm, chain, token)
        .await
        .map_err(|source| classify_inventory_token_introspection_error(source, token))
}

/// Block-number metadata derived from a transaction receipt. The constructors
/// use it only when the triggering log has no block number. A caller that
/// already fetched the receipt (the `process-tx` recovery path) can pass it
/// through both orderbook and inventory conversion instead of triggering a
/// second `eth_getTransactionReceipt` for the same tx.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ReceiptMetadata {
    block_number: Option<u64>,
}

/// Fetches the triggering transaction's block number, tolerating a missing
/// receipt rather than failing the trade. Shared by both `OnchainTrade`
/// constructors when the log itself has no block number.
async fn fetch_receipt_metadata<P: Provider>(
    provider: &P,
    tx_hash: TxHash,
) -> Result<ReceiptMetadata, OnChainError> {
    Ok(match provider.get_transaction_receipt(tx_hash).await? {
        Some(receipt) => ReceiptMetadata {
            block_number: receipt.block_number,
        },
        None => ReceiptMetadata { block_number: None },
    })
}

impl OnchainTrade {
    /// Core parsing logic for converting blockchain events to trades
    pub(crate) async fn try_from_order_and_fill_details<EvmImpl: Evm>(
        chain: Chain,
        cache: &SymbolCache,
        evm: &EvmImpl,
        order: OrderV4,
        fill: OrderFill,
        log: Log,
    ) -> Result<Option<Self>, OnChainError> {
        let tx_hash = log.transaction_hash.ok_or(TradeValidationError::NoTxHash)?;
        let log_index = log.log_index.ok_or(TradeValidationError::NoLogIndex)?;

        let receipt_block_number = if log.block_number.is_none() {
            fetch_receipt_metadata(evm.provider(), tx_hash)
                .await?
                .block_number
        } else {
            None
        };

        let input = order
            .validInputs
            .get(fill.input_index)
            .ok_or(TradeValidationError::NoInputAtIndex(fill.input_index))?;

        let output = order
            .validOutputs
            .get(fill.output_index)
            .ok_or(TradeValidationError::NoOutputAtIndex(fill.output_index))?;

        // ClearV3/TakeOrderV3 fill amounts are already rain-Float-encoded by the
        // orderbook, so they decode straight off the raw bytes -- contrast with
        // `try_from_inventory_trade` below, whose `amount` fields are raw
        // fixed-decimal token integers.
        let onchain_input_amount = Float::from_raw(fill.input_amount);

        let onchain_output_amount = Float::from_raw(fill.output_amount);

        let (onchain_input_symbol, onchain_output_symbol) = tokio::try_join!(
            cache.resolve_symbol(evm, chain, input.token),
            cache.resolve_symbol(evm, chain, output.token),
        )?;

        // Use centralized TradeDetails::try_from_io to extract all trade data consistently
        let trade_details = TradeDetails::try_from_io(
            &onchain_input_symbol,
            InputToken(input.token),
            onchain_input_amount,
            &onchain_output_symbol,
            OutputToken(output.token),
            onchain_output_amount,
        )?;

        finalize_onchain_trade(
            evm.provider(),
            trade_details,
            TradeMetadata {
                source: OnChainTradeSource::Raindex,
                chain,
                tx_hash,
                log_index,
                block_number: log.block_number.or(receipt_block_number),
                log_timestamp: log.block_timestamp,
            },
        )
        .await
    }

    /// Converts a paired [`InventoryTrade`] (an `OperatorDeposit` +
    /// `OperatorWithdraw` for the same tx on the shared inventory) into an
    /// [`OnchainTrade`] the hedge pipeline can consume.
    ///
    /// The pool received `deposit.amount` of `deposit.token` and sent
    /// `withdraw.amount` of `withdraw.token` out. One side must be USDC and
    /// the other a tokenized-equity ERC20; anything else fails validation the
    /// same way a mis-shaped Raindex clear would. Both leg addresses are also
    /// validated against `assets`' configured canonical addresses -- see
    /// [`validate_inventory_token_addresses`].
    ///
    /// `receipt_metadata`: `None` fetches the tx receipt internally only when
    /// the log has no block number; `Some` reuses a receipt the caller already
    /// fetched (the `process-tx` recovery path via `try_from_tx_hash`), avoiding
    /// a second `eth_getTransactionReceipt` round-trip for the same tx.
    pub(crate) async fn try_from_inventory_trade<EvmImpl: Evm>(
        chain: Chain,
        cache: &SymbolCache,
        evm: &EvmImpl,
        assets: &ChainAssets,
        inventory_adapters: &InventoryAdapters,
        trade: &InventoryTrade,
        log: Log,
        receipt_metadata: Option<ReceiptMetadata>,
    ) -> Result<Option<Self>, OnChainError> {
        let tx_hash = log.transaction_hash.ok_or(TradeValidationError::NoTxHash)?;
        let log_index = log.log_index.ok_or(TradeValidationError::NoLogIndex)?;

        let receipt_block_number = match receipt_metadata {
            Some(metadata) => metadata.block_number,
            None if log.block_number.is_none() => {
                fetch_receipt_metadata(evm.provider(), tx_hash)
                    .await?
                    .block_number
            }
            None => None,
        };

        // Deposit is IN (pool received), Withdraw is OUT (pool sent) --
        // mirroring the input/output convention `TradeDetails::try_from_io`
        // expects for the ClearV3/TakeOrderV3 path. Both sides are
        // externally-supplied token addresses (any OPERATOR_ROLE holder), so
        // introspection failures are mapped to `TokenIntrospectionFailed`
        // rather than left as an opaque `OnChainError::Evm`. Symbol
        // resolution and decimals introspection are independent RPC calls
        // (decimals does not depend on the resolved symbol), so all four run
        // in a single concurrent batch rather than two sequential waves.
        let (input_symbol, output_symbol, deposit_decimals, withdraw_decimals) = tokio::try_join!(
            resolve_inventory_token_symbol(cache, evm, chain, trade.deposit.token),
            resolve_inventory_token_symbol(cache, evm, chain, trade.withdraw.token),
            fetch_inventory_token_decimals(evm, trade.deposit.token),
            fetch_inventory_token_decimals(evm, trade.withdraw.token),
        )?;

        // `from_fixed_decimal` reverts on a coefficient/exponent that cannot
        // be losslessly represented as a rain-math-float value (e.g. an
        // extreme raw amount from a misbehaving venue). Reclassified into
        // `InvalidInventoryAmount` rather than the top-level
        // `OnChainError::FloatConversion` `?` would otherwise resolve to, so
        // `perform()` can skip this fill instead of fail-stopping.
        //
        // Unlike the ClearV3/TakeOrderV3 path's `Float::from_raw` above,
        // `OperatorDeposit`/`OperatorWithdraw` amounts are raw fixed-decimal
        // token integers, not rain-Float-encoded values: RaindexInventory.sol
        // emits `OperatorWithdraw.amount` as the raw ERC20 `balanceOf` delta,
        // and `OperatorDeposit.amount` as `_fromFloat(depositAmount, token)`,
        // i.e. `depositAmount.toFixedDecimalLossy(IERC20Metadata(token).decimals())`.
        // So both sides must be scaled back to `Float` via the token's onchain
        // `decimals()` rather than decoded straight off the raw bytes.
        let input_amount = Float::from_fixed_decimal(trade.deposit.amount, deposit_decimals)
            .map_err(TradeValidationError::InvalidInventoryAmount)?;
        let output_amount = Float::from_fixed_decimal(trade.withdraw.amount, withdraw_decimals)
            .map_err(TradeValidationError::InvalidInventoryAmount)?;

        // `try_from_io` is shared with the trusted ClearV3/TakeOrderV3 path
        // (`try_from_order_and_fill_details` above, which calls it directly
        // and leaves its Float-conversion failures uncaught by `perform()` --
        // there they come from the bot's own trusted order config, so such a
        // failure indicates a real bug worth fail-stopping on). Here the
        // amounts are externally supplied, so reclassify only the
        // Float-conversion failures into the caught `InvalidInventoryAmount`
        // variant; everything else (e.g. `InvalidSymbolConfiguration`) passes
        // through unchanged.
        let trade_details = TradeDetails::try_from_io(
            &input_symbol,
            InputToken(trade.deposit.token),
            input_amount,
            &output_symbol,
            OutputToken(trade.withdraw.token),
            output_amount,
        )
        .map_err(reclassify_inventory_float_error)?;

        validate_inventory_token_addresses(assets, &trade_details)?;

        finalize_onchain_trade(
            evm.provider(),
            trade_details,
            TradeMetadata {
                // Pairing quarantines settlements whose deposit and withdrawal
                // operators differ, so either side identifies the adapter.
                chain,
                source: inventory_trade_source(trade.deposit.operator, inventory_adapters),
                tx_hash,
                log_index,
                block_number: log.block_number.or(receipt_block_number),
                log_timestamp: log.block_timestamp,
            },
        )
        .await
    }

    /// Attempts to create an OnchainTrade from a transaction hash by looking up
    /// the transaction receipt and parsing relevant orderbook or inventory
    /// events. Tries `ClearV3`/`TakeOrderV3` orderbook fills first; if none
    /// match, falls back to an `OperatorDeposit`/`OperatorWithdraw`
    /// `InventoryTrade` settlement, paired via the same `bucket_inventory_logs`
    /// rules the backfill path uses -- so the manual `process-tx` recovery
    /// path can recover both fill sources this bot hedges.
    pub async fn try_from_tx_hash<EvmImpl: Evm>(
        tx_hash: TxHash,
        evm: &EvmImpl,
        cache: &SymbolCache,
        ctx: &TradingChain,
        actors: RecoveryActors,
    ) -> Result<Option<Self>, OnChainError> {
        let receipt = evm
            .provider()
            .get_transaction_receipt(tx_hash)
            .await?
            .ok_or_else(|| {
                OnChainError::Validation(TradeValidationError::TransactionNotFound(tx_hash))
            })?;

        let logs = receipt.inner.logs();

        let trades: Vec<_> = logs
            .iter()
            .filter(|log| {
                (log.topic0() == Some(&ClearV3::SIGNATURE_HASH)
                    || log.topic0() == Some(&TakeOrderV3::SIGNATURE_HASH))
                    && log.address() == ctx.orderbook
            })
            .collect();

        if trades.len() > 1 {
            warn!(
                target: "hedge",
                "Found {} potential trades in the tx with hash {tx_hash}, returning first match",
                trades.len()
            );
        }

        let receipt_metadata = ReceiptMetadata {
            block_number: receipt.block_number,
        };

        for log in trades {
            if let Some(trade) = try_convert_log_to_onchain_trade(
                log,
                evm,
                cache,
                ctx,
                receipt_metadata,
                actors.order_owner,
            )
            .await?
            {
                return Ok(Some(trade));
            }
        }

        // The receipt is already in hand here, so pass its block-number
        // metadata through instead of letting `try_from_inventory_trade`
        // re-fetch the same receipt via a second `eth_getTransactionReceipt`.
        let recovery_config = InventoryRecoveryConfig {
            cache,
            trading_chain: ctx,
        };

        Self::try_inventory_trade_from_receipt_logs(
            tx_hash,
            evm,
            &recovery_config,
            actors.bot_operator,
            logs,
            receipt_metadata,
        )
        .await
    }

    /// Fallback half of [`Self::try_from_tx_hash`]: looks for an
    /// `OperatorDeposit`/`OperatorWithdraw` pair emitted by the shared
    /// inventory among the receipt's logs, using the exact same
    /// [`bucket_inventory_logs`]/[`pair_inventory_settlements`]
    /// pairing/quarantine rules as backfill -- an ambiguous multi-deposit or
    /// multi-withdraw settlement is quarantined here identically, never
    /// resolved by picking an arbitrary "first match".
    async fn try_inventory_trade_from_receipt_logs<EvmImpl: Evm>(
        tx_hash: TxHash,
        evm: &EvmImpl,
        config: &InventoryRecoveryConfig<'_>,
        bot_operator: BotOperator,
        logs: &[Log],
        receipt_metadata: ReceiptMetadata,
    ) -> Result<Option<Self>, OnChainError> {
        let inventory_logs: Vec<Log> = logs
            .iter()
            .filter(|log| {
                log.address() == config.trading_chain.inventory_address()
                    && (log.topic0() == Some(&OperatorDeposit::SIGNATURE_HASH)
                        || log.topic0() == Some(&OperatorWithdraw::SIGNATURE_HASH))
            })
            .cloned()
            .collect();

        if inventory_logs.is_empty() {
            return Ok(None);
        }

        let (deposits_by_tx, withdraws_by_tx) = bucket_inventory_logs(inventory_logs, bot_operator);
        // All logs above share `tx_hash` (filtered from a single receipt), so
        // this pairs into at most one `InventoryTrade`; a quarantined or
        // unpaired settlement surfaces as an empty result here, already
        // logged/counted by `pair_inventory_settlements`.
        let paired = pair_inventory_settlements(deposits_by_tx, withdraws_by_tx);

        let Some((RaindexTradeEvent::InventoryTrade(inv), withdraw_log)) =
            paired.into_iter().next()
        else {
            debug!(
                target: "hedge",
                %tx_hash,
                "No pairable OperatorDeposit/OperatorWithdraw settlement in this tx; see \
                 prior inventory warnings/errors for the reason",
            );
            return Ok(None);
        };

        Self::try_from_inventory_trade(
            config.trading_chain.chain,
            config.cache,
            evm,
            &config.trading_chain.assets,
            &config.trading_chain.inventory_adapters,
            &inv,
            withdraw_log,
            Some(receipt_metadata),
        )
        .await
    }
}

/// Read-only config bundled for [`OnchainTrade::try_inventory_trade_from_receipt_logs`],
/// keeping its argument count under the clippy threshold.
struct InventoryRecoveryConfig<'config> {
    cache: &'config SymbolCache,
    trading_chain: &'config TradingChain,
}

fn inventory_trade_source(
    operator: Address,
    inventory_adapters: &InventoryAdapters,
) -> OnChainTradeSource {
    match inventory_adapters.venue_for(operator) {
        Some(InventoryAdapterVenue::Bebop) => OnChainTradeSource::Inventory {
            operator,
            venue: InventoryVenue::Bebop,
        },
        Some(InventoryAdapterVenue::UniswapV4) => OnChainTradeSource::Inventory {
            operator,
            venue: InventoryVenue::UniswapV4,
        },
        None => OnChainTradeSource::UnrecognizedInventory { operator },
    }
}

/// Reclassifies a Float-conversion failure surfaced by the shared
/// `TradeDetails::try_from_io` into [`TradeValidationError::InvalidInventoryAmount`]
/// so `perform()` can skip the fill instead of fail-stopping. Only called
/// from [`OnchainTrade::try_from_inventory_trade`] -- the ClearV3/TakeOrderV3
/// path calls `try_from_io` directly and leaves these variants uncaught,
/// since a Float-conversion failure there comes from the bot's own trusted
/// order config and indicates a real bug rather than an external anomaly.
/// Any other error (e.g. `InvalidSymbolConfiguration`) passes through
/// unchanged.
fn reclassify_inventory_float_error(error: OnChainError) -> OnChainError {
    match error {
        OnChainError::Validation(TradeValidationError::Float(float_error))
        | OnChainError::FloatConversion(float_error) => {
            OnChainError::Validation(TradeValidationError::InvalidInventoryAmount(float_error))
        }
        other => other,
    }
}

/// Validates that an `InventoryTrade`'s USDC and equity leg addresses match
/// this bot's configured canonical addresses, not just their self-reported
/// `symbol()`.
///
/// Unlike ClearV3/TakeOrderV3 (whose token addresses come from the bot's own
/// trusted order config), `InventoryTrade` legs are supplied by any
/// `OPERATOR_ROLE` holder on the shared inventory (Bebop hook, univ4 hook, or
/// any future venue adapter). A compromised or buggy adapter could deploy a
/// spoof token whose `symbol()` returns "USDC" or "wt<SYMBOL>" without being
/// the real thing -- `TradeDetails::try_from_io`'s symbol-only classification
/// would accept it, causing the bot to hedge (and, on a withdraw, pay out of
/// the shared vault against) a fabricated leg. Only called from the
/// `InventoryTrade` path; the trusted ClearV3/TakeOrderV3 path never needs
/// this since its token addresses already come from the bot's own orders.
fn validate_inventory_token_addresses(
    assets: &ChainAssets,
    trade_details: &TradeDetails,
) -> Result<(), TradeValidationError> {
    let usdc_token = trade_details.usdc_token();
    if usdc_token != USDC_BASE {
        return Err(TradeValidationError::UnrecognizedInventoryToken {
            token: usdc_token,
            claimed_symbol: "USDC".to_string(),
        });
    }

    let equity_symbol = trade_details.equity_symbol();
    let equity_token = trade_details.equity_token();
    let configured_derivative = assets
        .equities
        .symbols
        .get(equity_symbol.base())
        .map(|equity| equity.tokenized_equity_derivative);

    if configured_derivative != Some(equity_token) {
        return Err(TradeValidationError::UnrecognizedInventoryToken {
            token: equity_token,
            claimed_symbol: equity_symbol.to_string(),
        });
    }

    Ok(())
}

/// Metadata derived from the triggering log and its transaction receipt,
/// needed by [`finalize_onchain_trade`] to finish constructing an
/// [`OnchainTrade`] once `TradeDetails` has been resolved.
struct TradeMetadata {
    source: OnChainTradeSource,
    chain: Chain,
    tx_hash: TxHash,
    log_index: u64,
    block_number: Option<u64>,
    log_timestamp: Option<u64>,
}

/// Shared tail of `try_from_order_and_fill_details` and
/// `try_from_inventory_trade`: validates the resolved `TradeDetails`, prices
/// the fill and resolves the block timestamp.
async fn finalize_onchain_trade<P: Provider>(
    provider: &P,
    trade_details: TradeDetails,
    metadata: TradeMetadata,
) -> Result<Option<OnchainTrade>, OnChainError> {
    let TradeMetadata {
        source,
        chain,
        tx_hash,
        log_index,
        block_number,
        log_timestamp,
    } = metadata;

    if trade_details.equity_amount().is_zero()? {
        debug!(
            target: "hedge",
            %tx_hash,
            log_index,
            "Skipping trade with zero equity amount; not a tokenized-equity trade"
        );
        return Ok(None);
    }

    // Calculate price per share in USDC (always USDC amount / equity amount)
    let price_per_share_usdc =
        (trade_details.usdc_amount().value() / trade_details.equity_amount().inner())?;

    // Equity is non-zero (checked above), so a non-positive price is a real
    // equity movement we cannot price. Reject it rather than silently dropping
    // it, which would leave the resulting position unhedged.
    if price_per_share_usdc.lt(Float::zero()?)? || price_per_share_usdc.is_zero()? {
        return Err(TradeValidationError::NonPositivePrice(price_per_share_usdc).into());
    }

    let equity_token = trade_details.equity_token();
    let equity_amount = trade_details.equity_amount();
    let direction = trade_details.direction();
    let equity_symbol = trade_details.into_equity_symbol();

    let block_timestamp = resolve_block_timestamp(provider, log_timestamp, block_number).await?;

    let price = Usdc::new(price_per_share_usdc)?;

    Ok(Some(OnchainTrade {
        source,
        chain,
        tx_hash,
        log_index,
        symbol: equity_symbol,
        equity_token,
        amount: equity_amount,
        direction,
        price,
        block_timestamp,
        block_number,
    }))
}

#[derive(Debug)]
pub(crate) struct OrderFill {
    pub input_index: usize,
    pub input_amount: B256,
    pub output_index: usize,
    pub output_amount: B256,
}

async fn try_convert_log_to_onchain_trade<EvmImpl: Evm>(
    log: &Log,
    evm: &EvmImpl,
    cache: &SymbolCache,
    ctx: &TradingChain,
    receipt_metadata: ReceiptMetadata,
    order_owner: Address,
) -> Result<Option<OnchainTrade>, OnChainError> {
    let log_with_metadata = Log {
        inner: log.inner.clone(),
        block_hash: log.block_hash,
        block_number: log.block_number.or(receipt_metadata.block_number),
        block_timestamp: log.block_timestamp,
        transaction_hash: log.transaction_hash,
        transaction_index: log.transaction_index,
        log_index: log.log_index,
        removed: false,
    };

    if let Ok(clear_event) = log.log_decode::<ClearV3>() {
        return OnchainTrade::try_from_clear_v3(
            ctx,
            cache,
            evm,
            clear_event.data().clone(),
            log_with_metadata,
            order_owner,
        )
        .await;
    }

    if let Ok(take_order_event) = log.log_decode::<TakeOrderV3>() {
        return OnchainTrade::try_from_take_order_if_target_owner(
            ctx.chain,
            cache,
            evm,
            take_order_event.data().clone(),
            log_with_metadata,
            order_owner,
        )
        .await;
    }

    Ok(None)
}

async fn resolve_block_timestamp<P: Provider>(
    provider: &P,
    log_timestamp: Option<u64>,
    block_number: Option<u64>,
) -> Result<Option<DateTime<Utc>>, OnChainError> {
    if let Some(timestamp_secs) = log_timestamp {
        return timestamp_from_secs(timestamp_secs);
    }

    let Some(block_number) = block_number else {
        return Ok(None);
    };

    let Some(block) = provider.get_block_by_number(block_number.into()).await? else {
        warn!(
            block_number,
            "Block header unavailable while resolving onchain trade timestamp; \
             provider may be lagging or the block may have reorged"
        );
        return Ok(None);
    };

    timestamp_from_secs(block.header.timestamp)
}

fn timestamp_from_secs(timestamp_secs: u64) -> Result<Option<DateTime<Utc>>, OnChainError> {
    let secs = i64::try_from(timestamp_secs)?;
    Ok(DateTime::from_timestamp(secs, 0))
}

/// Business logic validation errors for trade processing rules.
#[derive(Debug, thiserror::Error)]
pub enum TradeValidationError {
    #[error("No transaction hash found in log")]
    NoTxHash,
    #[error("No log index found in log")]
    NoLogIndex,
    #[error("No block number found in log")]
    NoBlockNumber,
    #[error("Integer conversion error: {0}")]
    IntConversion(#[from] std::num::TryFromIntError),
    #[error("Invalid IO index: {0}")]
    InvalidIndex(#[from] FromUintError<usize>),
    #[error("No input found at index: {0}")]
    NoInputAtIndex(usize),
    #[error("No output found at index: {0}")]
    NoOutputAtIndex(usize),
    #[error(
        "Expected IO to contain USDC and one wrapped \
         tokenized equity (wt prefix) but got {0} and {1}"
    )]
    InvalidSymbolConfiguration(String, String),
    #[error("Transaction not found: {0}")]
    TransactionNotFound(TxHash),
    #[error(
        "Node provider issue: tx receipt missing or has no logs. \
        block={block_number}, tx={tx_hash}, clear_log_index={clear_log_index}"
    )]
    NodeReceiptMissing {
        block_number: u64,
        tx_hash: TxHash,
        clear_log_index: u64,
    },
    #[error(
        "Unexpected: tx receipt has ClearV3 but no AfterClearV2 (should be impossible). \
        block={block_number}, tx={tx_hash}, clear_log_index={clear_log_index}"
    )]
    AfterClearMissingFromReceipt {
        block_number: u64,
        tx_hash: TxHash,
        clear_log_index: u64,
    },
    #[error("Negative shares amount: {}", format_float_with_fallback(.0))]
    NegativeShares(Float),
    #[error("Negative USDC amount: {}", format_float_with_fallback(.0))]
    NegativeUsdc(Float),
    #[error(
        "fill has non-zero equity but a non-positive USDC/share price: {}",
        format_float_with_fallback(.0)
    )]
    NonPositivePrice(Float),
    #[error("Float error: {0}")]
    Float(#[from] FloatError),
    #[error(
        "symbol '{symbol_provided}' is not a tokenized equity \
         (must have 't' or 'wt' prefix, e.g. tAAPL, wtCOIN)"
    )]
    NotTokenizedEquity { symbol_provided: String },
    /// Symbol/decimals introspection failed for a token supplied by an
    /// `InventoryTrade` settlement (`OperatorDeposit`/`OperatorWithdraw`).
    /// Unlike ClearV3/TakeOrderV3, whose token addresses come from the bot's
    /// own trusted order config, these addresses come from any `OPERATOR_ROLE`
    /// holder on the shared inventory -- a non-standard or misconfigured
    /// third-party token must not trip the conductor-wide fail-stop.
    #[error("Failed to introspect InventoryTrade token {token}: {source}")]
    TokenIntrospectionFailed {
        token: Address,
        #[source]
        source: Box<EvmError>,
    },
    /// A fixed-decimal amount/decimals conversion failed for an
    /// `InventoryTrade`-supplied deposit or withdraw amount. Same threat
    /// model as `TokenIntrospectionFailed`: `OperatorDeposit`/
    /// `OperatorWithdraw` amounts come from any `OPERATOR_ROLE` holder, not
    /// the bot's own trusted order config, so a malformed or extreme
    /// amount/decimals pair must not trip the conductor-wide fail-stop.
    #[error("Failed to convert InventoryTrade amount: {0}")]
    InvalidInventoryAmount(#[source] FloatError),
    /// An `InventoryTrade` leg's token address does not match the configured
    /// canonical address for the symbol its `symbol()` claims to be. Unlike
    /// ClearV3/TakeOrderV3, whose token addresses come from the bot's own
    /// trusted order config, these addresses come from any `OPERATOR_ROLE`
    /// holder on the shared inventory -- a spoofed or misconfigured token
    /// must not trip the conductor-wide fail-stop.
    #[error(
        "InventoryTrade token {token} claims symbol '{claimed_symbol}' but does not match \
         the configured canonical address for that symbol"
    )]
    UnrecognizedInventoryToken {
        token: Address,
        claimed_symbol: String,
    },
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use alloy::primitives::{Address, IntoLogData, U256, address, b256, fixed_bytes, uint};
    use alloy::providers::{ProviderBuilder, mock::Asserter};
    use alloy::rpc::types::{Block, Transaction};
    use alloy::sol_types::SolCall;
    use rain_math_float::Float;

    use st0x_config::{
        ChainEquities, ChainEquityAsset, InventoryAdapter, InventoryAdapterVenue,
        InventoryAdapters, InventoryMode, OperationMode, TradingChain,
    };
    use st0x_evm::Chain;
    use st0x_evm::IERC20::decimalsCall;
    use st0x_evm::ReadOnlyEvm;
    use st0x_execution::Symbol;
    use st0x_float_macro::float;
    use st0x_registry::SymbolCache;

    use super::*;

    /// Preloads a symbol under every chain the fixtures run on, so tests
    /// exercising either Base- or Ethereum-flavored parsing hit the cache.
    fn preload_on_all_chains(cache: &SymbolCache, token: Address, symbol: &str) {
        for chain in [Chain::Base, Chain::Ethereum] {
            cache.preload_symbol(chain, token, symbol);
        }
    }
    use crate::bindings::IRaindexV6;
    use crate::test_utils::{
        get_test_order, panic_revert_payload, seed_get_test_order_token_symbols,
    };

    #[tokio::test]
    async fn resolve_block_timestamp_fetches_header_when_log_timestamp_missing() {
        let asserter = Asserter::new();
        let mut block = Block::<Transaction>::default();
        block.header.inner.number = 12_345;
        block.header.inner.timestamp = 1_700_000_123;
        asserter.push_success(&block);
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        let timestamp = resolve_block_timestamp(&provider, None, Some(12_345))
            .await
            .unwrap();

        assert_eq!(
            timestamp,
            DateTime::from_timestamp(1_700_000_123, 0),
            "receipt logs without block_timestamp should use the block header timestamp"
        );
    }

    #[test]
    fn test_float_constants_from_v5_interface() {
        // Verify our implementation matches Float constants from LibDecimalFloat.sol

        // FLOAT_ONE = bytes32(uint256(1)) = coefficient=1, exponent=0 -> 1.0
        let float_one = B256::from([
            0x00, 0x00, 0x00, 0x00, // exponent = 0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x01, // coefficient = 1
        ]);
        let diff = (Float::from_raw(float_one) - float!(1.0))
            .unwrap()
            .abs()
            .unwrap();
        assert!(diff.lt(float!(0.000001)).unwrap());

        // FLOAT_HALF = 0xffffffff...05 = coefficient=5, exponent=-1 -> 0.5
        let float_half = B256::from([
            0xff, 0xff, 0xff, 0xff, // exponent = -1
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x05, // coefficient = 5
        ]);
        let diff = (Float::from_raw(float_half) - float!(0.5))
            .unwrap()
            .abs()
            .unwrap();
        assert!(diff.lt(float!(0.000001)).unwrap());

        // FLOAT_TWO = bytes32(uint256(2)) = coefficient=2, exponent=0 -> 2.0
        let float_two = B256::from([
            0x00, 0x00, 0x00, 0x00, // exponent = 0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x02, // coefficient = 2
        ]);
        let diff = (Float::from_raw(float_two) - float!(2.0))
            .unwrap()
            .abs()
            .unwrap();
        assert!(diff.lt(float!(0.000001)).unwrap());
    }

    /// Test with real production event data from tx
    /// 0xf05d...304d (2 shares of tSPLG at ~$80/share).
    ///
    /// TakeOrderV3 event field semantics (counterintuitive naming):
    /// - event.input = amount the order GAVE = 2 tSPLG shares
    /// - event.output = amount the order RECEIVED = ~160 USDC
    #[test]
    fn test_from_raw_production_event_data() {
        // event.input Float: 2 shares the order gave
        // Raw bytes: ffffffee00000000000000000000000000000000000000001bc16d674ec80000
        let event_input_float =
            fixed_bytes!("ffffffee00000000000000000000000000000000000000001bc16d674ec80000");
        let shares_amount = Float::from_raw(event_input_float);
        let diff = (shares_amount - float!(2.0)).unwrap().abs().unwrap();
        assert!(
            diff.lt(float!(0.000001)).unwrap(),
            "Expected 2.0 shares but got {shares_amount:?}"
        );

        // event.output Float: ~160 USDC the order received
        // Raw bytes: ffffffe500000000000000000000000000000002057d2cd516a29b6174400000
        let event_output_float =
            fixed_bytes!("ffffffe500000000000000000000000000000002057d2cd516a29b6174400000");
        let usdc_amount = Float::from_raw(event_output_float);
        let diff = (usdc_amount - float!(160.15507752)).unwrap().abs().unwrap();
        assert!(
            diff.lt(float!(0.00001)).unwrap(),
            "Expected ~160.15 USDC but got {usdc_amount:?}"
        );

        // After swapping (as done in take_order.rs):
        // - input_amount (order's input token = USDC) = event.output = 160.15
        // - output_amount (order's output token = tSPLG) = event.input = 2.0
        // Then TradeDetails::try_from_io("USDC", 160.15, "wtSPLG", 2.0) correctly extracts:
        // - equity_amount = 2.0 (from output since output is tokenized equity)
        // - usdc_amount = 160.15 (from input since input is USDC)
    }

    #[test]
    fn test_from_raw_edge_cases() {
        let float_zero = Float::from_fixed_decimal(uint!(0_U256), 0)
            .unwrap()
            .get_inner();
        let diff = (Float::from_raw(float_zero) - float!(0))
            .unwrap()
            .abs()
            .unwrap();
        assert!(diff.lt(float!(0.000001)).unwrap());

        let float_one = Float::from_fixed_decimal(uint!(1_U256), 0)
            .unwrap()
            .get_inner();
        let diff = (Float::from_raw(float_one) - float!(1))
            .unwrap()
            .abs()
            .unwrap();
        assert!(diff.lt(float!(0.000001)).unwrap());

        let float_nine = Float::from_fixed_decimal(uint!(9_U256), 0)
            .unwrap()
            .get_inner();
        let result = Float::from_raw(float_nine);
        let diff = (result - float!(9)).unwrap().abs().unwrap();
        assert!(diff.lt(float!(0.000001)).unwrap());

        let float_hundred = Float::from_fixed_decimal(uint!(100_U256), 0)
            .unwrap()
            .get_inner();
        let result = Float::from_raw(float_hundred);
        let diff = (result - float!(100)).unwrap().abs().unwrap();
        assert!(diff.lt(float!(0.000001)).unwrap());

        let float_half = Float::from_fixed_decimal(uint!(5_U256), 1)
            .unwrap()
            .get_inner();
        let result = Float::from_raw(float_half);
        let diff = (result - float!(0.5)).unwrap().abs().unwrap();
        assert!(diff.lt(float!(0.000001)).unwrap());
    }

    #[test]
    fn test_from_raw_large_values() {
        // Test with very large coefficient
        let large_coeff = 1_000_000_000_000_000_i128;
        let float_large = Float::from_fixed_decimal_lossy(U256::from(large_coeff), 0)
            .unwrap()
            .0
            .get_inner();
        let result = Float::from_raw(float_large);
        let diff = (result - float!(1000000000000000)).unwrap().abs().unwrap();
        assert!(diff.lt(float!(1)).unwrap());

        // Test with very small value (high negative exponent)
        // Float preserves small values that Decimal truncated to zero
        let float_small = Float::from_fixed_decimal_lossy(uint!(1_U256), 50)
            .unwrap()
            .0
            .get_inner();
        let result = Float::from_raw(float_small);
        assert!(
            !result.is_zero().unwrap(),
            "Expected tiny value to remain non-zero after conversion"
        );
        let diff = result.abs().unwrap();
        assert!(diff.lt(float!(0.000001)).unwrap());
    }

    #[test]
    fn test_from_raw_formatting_edge_cases() {
        let float_amount = Float::from_fixed_decimal(uint!(123_456_U256), 6)
            .unwrap()
            .get_inner();
        let result = Float::from_raw(float_amount);
        let diff = (result - float!(0.123456)).unwrap().abs().unwrap();
        assert!(diff.lt(float!(0.000001)).unwrap());

        let float_amount = Float::from_fixed_decimal(uint!(5_U256), 10)
            .unwrap()
            .get_inner();
        let result = Float::from_raw(float_amount);
        let diff = (result - float!(&"0.0000000005".to_string()))
            .unwrap()
            .abs()
            .unwrap();
        assert!(diff.lt(float!(&"0.000000000000001".to_string())).unwrap());

        let float_amount = Float::from_fixed_decimal(uint!(12_345_U256), 0)
            .unwrap()
            .get_inner();
        let result = Float::from_raw(float_amount);
        let diff = (result - float!(12345)).unwrap().abs().unwrap();
        assert!(diff.lt(float!(0.000001)).unwrap());

        let float_amount = Float::from_fixed_decimal(uint!(5000_U256), 0)
            .unwrap()
            .get_inner();
        let result = Float::from_raw(float_amount);
        let diff = (result - float!(5000)).unwrap().abs().unwrap();
        assert!(diff.lt(float!(0.000001)).unwrap());
    }

    #[tokio::test]
    async fn test_try_from_tx_hash_transaction_not_found() {
        let asserter = Asserter::new();
        // Mock the eth_getTransactionReceipt call to return null (transaction not found)
        asserter.push_success(&serde_json::Value::Null);
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let cache = SymbolCache::default();
        let ctx = TradingChain::test()
            .orderbook(Address::ZERO)
            .inventory(InventoryMode::Managed {
                inventory: Address::ZERO,
            })
            .vault_owner(Address::ZERO)
            .call();

        let tx_hash =
            fixed_bytes!("0x4444444444444444444444444444444444444444444444444444444444444444");

        // Mock returns empty response by default, simulating transaction not found
        let result = OnchainTrade::try_from_tx_hash(
            tx_hash,
            &ReadOnlyEvm::new(provider),
            &cache,
            &ctx,
            RecoveryActors {
                order_owner: Address::ZERO,
                bot_operator: BotOperator(Address::ZERO),
            },
        )
        .await;

        assert!(matches!(
            result.unwrap_err(),
            OnChainError::Validation(TradeValidationError::TransactionNotFound(_))
        ));
    }

    #[tokio::test]
    async fn try_from_tx_hash_reuses_receipt_block_for_orderbook_log() {
        let orderbook = address!("0x1111111111111111111111111111111111111111");
        let tx_hash =
            fixed_bytes!("0x4545454545454545454545454545454545454545454545454545454545454545");
        let order = get_test_order();
        let order_owner = order.owner;
        let take_order = IRaindexV6::TakeOrderV3 {
            sender: address!("0x2222222222222222222222222222222222222222"),
            config: IRaindexV6::TakeOrderConfigV4 {
                order,
                inputIOIndex: U256::from(0),
                outputIOIndex: U256::from(1),
                signedContext: vec![IRaindexV6::SignedContextV1 {
                    signer: Address::ZERO,
                    signature: Vec::new().into(),
                    context: Vec::new(),
                }],
            },
            input: Float::from_fixed_decimal_lossy(uint!(9_U256), 0)
                .unwrap()
                .0
                .get_inner(),
            output: Float::from_fixed_decimal_lossy(uint!(100_U256), 0)
                .unwrap()
                .0
                .get_inner(),
        };
        let orderbook_log = Log {
            inner: alloy::primitives::Log {
                address: orderbook,
                data: take_order.to_log_data(),
            },
            block_hash: None,
            block_number: None,
            block_timestamp: Some(1_700_000_000),
            transaction_hash: Some(tx_hash),
            transaction_index: None,
            log_index: Some(7),
            removed: false,
        };
        let receipt = serde_json::json!({
            "transactionHash": tx_hash,
            "transactionIndex": "0x1",
            "blockHash": "0x1234567890123456789012345678901234567890123456789012345678901234",
            "blockNumber": "0x2a",
            "from": "0x1234567890123456789012345678901234567890",
            "to": orderbook,
            "gasUsed": "0x5208",
            "effectiveGasPrice": "0x77359400",
            "cumulativeGasUsed": "0x5208",
            "status": "0x1",
            "type": "0x2",
            "logsBloom": format!("0x{}", "0".repeat(512)),
            "logs": [orderbook_log]
        });

        let asserter = Asserter::new();
        asserter.push_success(&receipt);
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let cache = SymbolCache::default();
        seed_get_test_order_token_symbols(&cache);
        let ctx = TradingChain::test()
            .orderbook(orderbook)
            .inventory(InventoryMode::Managed {
                inventory: Address::ZERO,
            })
            .vault_owner(order_owner)
            .call();

        let trade = OnchainTrade::try_from_tx_hash(
            tx_hash,
            &ReadOnlyEvm::new(provider),
            &cache,
            &ctx,
            RecoveryActors {
                order_owner,
                bot_operator: BotOperator(Address::ZERO),
            },
        )
        .await
        .unwrap()
        .unwrap();

        assert_eq!(trade.block_number, Some(42));
    }

    /// `try_from_tx_hash` must recover an `InventoryTrade` settlement (no
    /// `ClearV3`/`TakeOrderV3` in the receipt) by pairing the receipt's
    /// `OperatorDeposit`/`OperatorWithdraw` logs -- the manual `process-tx`
    /// recovery path must support this fill source, not just orderbook
    /// clears. Reuses the real Bebop-routed prod settlement fixture also
    /// pinned in `try_from_inventory_trade_real_bebop_fill_is_sell`.
    #[tokio::test]
    async fn try_from_tx_hash_recovers_real_inventory_trade_settlement() {
        let inventory = address!("0xbeb0009aca35087ce7ccf11637e24dd1aad3bf2a");
        let orderbook = address!("0x1111111111111111111111111111111111111111");
        let bot_operator = address!("0x679df30b30ac2947aa3143490add6717af81dcc3");

        let cache = SymbolCache::default();
        preload_on_all_chains(&cache, REAL_USDC_BASE, "USDC");
        preload_on_all_chains(&cache, REAL_WTCOIN_BASE, "wtCOIN");

        let tx_hash =
            fixed_bytes!("0xe13a11de734768f08a9c1ef66e8de3bcb9072f8cdabce9f1d819e1ae9909d4b9");
        let venue_operator = address!("0x8b8b6e0507c125934c6129563f48e48c66f86475");

        let deposit = OperatorDeposit {
            operator: venue_operator,
            token: REAL_USDC_BASE,
            vaultId: b256!("0x0000000000000000000000000000000000000000000000000000000000000004"),
            amount: uint!(5_000_000_U256), // 5 USDC, 6 decimals
        };
        let withdraw = OperatorWithdraw {
            operator: venue_operator,
            token: REAL_WTCOIN_BASE,
            vaultId: b256!("0x0000000000000000000000000000000000000000000000000000000000000003"),
            amount: uint!(34_172_366_621_067_031_U256), // 0.034172366621067031 wtCOIN, 18 decimals
        };

        let deposit_log = alloy::rpc::types::Log {
            inner: alloy::primitives::Log {
                address: inventory,
                data: deposit.to_log_data(),
            },
            block_hash: None,
            block_number: Some(48_030_415),
            block_timestamp: Some(1_782_850_177),
            transaction_hash: Some(tx_hash),
            transaction_index: None,
            log_index: Some(0),
            removed: false,
        };
        let withdraw_log = alloy::rpc::types::Log {
            inner: alloy::primitives::Log {
                address: inventory,
                data: withdraw.to_log_data(),
            },
            block_hash: None,
            block_number: Some(48_030_415),
            block_timestamp: Some(1_782_850_177),
            transaction_hash: Some(tx_hash),
            transaction_index: None,
            log_index: Some(1),
            removed: false,
        };

        let receipt = serde_json::json!({
            "transactionHash": tx_hash,
            "transactionIndex": "0x3b",
            "blockHash": "0x373307a0e2154c2de6b046e349fc27f9bb02b01fdddbb15eeba57f3ce3b24973",
            "blockNumber": "0x2dce2cf",
            "from": bot_operator,
            "to": inventory,
            "gasUsed": "0x7a62d",
            "effectiveGasPrice": "0x57bcf0",
            "cumulativeGasUsed": "0xa00b4e",
            "status": "0x1",
            "type": "0x2",
            "logsBloom": format!("0x{}", "0".repeat(512)),
            "logs": [deposit_log, withdraw_log]
        });

        let asserter = Asserter::new();
        // `try_from_tx_hash` fetches this receipt once and threads its
        // gas/price/block metadata into `try_from_inventory_trade`, so only
        // one `get_transaction_receipt` response is needed.
        asserter.push_success(&receipt); // get_transaction_receipt
        asserter.push_success(&<decimalsCall as SolCall>::abi_encode_returns(&6u8)); // USDC
        asserter.push_success(&<decimalsCall as SolCall>::abi_encode_returns(&18u8)); // wtCOIN
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        let ctx = TradingChain::test()
            .orderbook(orderbook)
            .inventory(InventoryMode::Managed { inventory })
            .inventory_adapters(inventory_adapters(
                InventoryAdapterVenue::Bebop,
                venue_operator,
            ))
            .vault_owner(inventory)
            .call();
        let ctx = TradingChain {
            assets: assets_config_with_equity("COIN", REAL_WTCOIN_BASE),
            ..ctx
        };

        let trade = OnchainTrade::try_from_tx_hash(
            tx_hash,
            &ReadOnlyEvm::new(provider),
            &cache,
            &ctx,
            RecoveryActors {
                order_owner: inventory,
                bot_operator: BotOperator(bot_operator),
            },
        )
        .await
        .unwrap()
        .unwrap();

        assert_eq!(trade.symbol.to_string(), "wtCOIN");
        assert_eq!(trade.direction, Direction::Sell);
        assert_eq!(trade.equity_token, REAL_WTCOIN_BASE);
        let expected_amount =
            Float::from_fixed_decimal(uint!(34_172_366_621_067_031_U256), 18).unwrap();
        assert!(trade.amount.inner().eq(expected_amount).unwrap());
        assert_eq!(
            trade.source,
            OnChainTradeSource::Inventory {
                operator: venue_operator,
                venue: InventoryVenue::Bebop,
            },
            "the recovered source must come from the trading chain's own adapter registry"
        );
    }

    /// The manual `process-tx` recovery path must quarantine a tx with one
    /// `OperatorDeposit` and two `OperatorWithdraw`s exactly like the batch
    /// backfill path does (`second_withdraw_in_one_tx_is_ambiguous_and_quarantined`
    /// in `backfill.rs`) -- never resolve the ambiguity by picking an
    /// arbitrary "first match" withdraw.
    #[tokio::test]
    async fn try_from_tx_hash_quarantines_ambiguous_multi_withdraw_settlement() {
        let inventory = address!("0xbeb0009aca35087ce7ccf11637e24dd1aad3bf2a");
        let orderbook = address!("0x1111111111111111111111111111111111111111");
        let bot_operator = address!("0x679df30b30ac2947aa3143490add6717af81dcc3");
        let venue_operator = address!("0x8b8b6e0507c125934c6129563f48e48c66f86475");

        let cache = SymbolCache::default();
        let tx_hash =
            fixed_bytes!("0xa1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1");

        let deposit = OperatorDeposit {
            operator: venue_operator,
            token: REAL_USDC_BASE,
            vaultId: b256!("0x0000000000000000000000000000000000000000000000000000000000000004"),
            amount: uint!(5_000_000_U256),
        };
        let first_withdraw = OperatorWithdraw {
            operator: venue_operator,
            token: REAL_WTCOIN_BASE,
            vaultId: b256!("0x0000000000000000000000000000000000000000000000000000000000000003"),
            amount: uint!(34_172_366_621_067_031_U256),
        };
        let second_withdraw = OperatorWithdraw {
            operator: venue_operator,
            token: REAL_WTCOIN_BASE,
            vaultId: b256!("0x0000000000000000000000000000000000000000000000000000000000000003"),
            amount: uint!(1_000_000_000_000_000_U256),
        };

        let deposit_log = alloy::rpc::types::Log {
            inner: alloy::primitives::Log {
                address: inventory,
                data: deposit.to_log_data(),
            },
            block_hash: None,
            block_number: Some(48_030_415),
            block_timestamp: Some(1_782_850_177),
            transaction_hash: Some(tx_hash),
            transaction_index: None,
            log_index: Some(0),
            removed: false,
        };
        let first_withdraw_log = alloy::rpc::types::Log {
            inner: alloy::primitives::Log {
                address: inventory,
                data: first_withdraw.to_log_data(),
            },
            block_hash: None,
            block_number: Some(48_030_415),
            block_timestamp: Some(1_782_850_177),
            transaction_hash: Some(tx_hash),
            transaction_index: None,
            log_index: Some(1),
            removed: false,
        };
        let second_withdraw_log = alloy::rpc::types::Log {
            inner: alloy::primitives::Log {
                address: inventory,
                data: second_withdraw.to_log_data(),
            },
            block_hash: None,
            block_number: Some(48_030_415),
            block_timestamp: Some(1_782_850_177),
            transaction_hash: Some(tx_hash),
            transaction_index: None,
            log_index: Some(2),
            removed: false,
        };

        let receipt = serde_json::json!({
            "transactionHash": tx_hash,
            "transactionIndex": "0x3b",
            "blockHash": "0x373307a0e2154c2de6b046e349fc27f9bb02b01fdddbb15eeba57f3ce3b24973",
            "blockNumber": "0x2dce2cf",
            "from": bot_operator,
            "to": inventory,
            "gasUsed": "0x7a62d",
            "effectiveGasPrice": "0x57bcf0",
            "cumulativeGasUsed": "0xa00b4e",
            "status": "0x1",
            "type": "0x2",
            "logsBloom": format!("0x{}", "0".repeat(512)),
            "logs": [deposit_log, first_withdraw_log, second_withdraw_log]
        });

        let asserter = Asserter::new();
        asserter.push_success(&receipt); // get_transaction_receipt
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        let ctx = TradingChain::test()
            .orderbook(orderbook)
            .inventory(InventoryMode::Managed { inventory })
            .vault_owner(inventory)
            .call();

        let trade = OnchainTrade::try_from_tx_hash(
            tx_hash,
            &ReadOnlyEvm::new(provider),
            &cache,
            &ctx,
            RecoveryActors {
                order_owner: inventory,
                bot_operator: BotOperator(bot_operator),
            },
        )
        .await
        .unwrap();

        assert!(
            trade.is_none(),
            "an ambiguous multi-withdraw settlement must be quarantined, not recovered by \
             picking an arbitrary first withdraw"
        );
    }

    /// The manual `process-tx` recovery path must quarantine a tx whose
    /// single `OperatorDeposit` and single `OperatorWithdraw` were emitted by
    /// different operators, exactly like the batch backfill path does
    /// (`mismatched_operator_deposit_and_withdraw_is_ambiguous_and_quarantined`
    /// in `backfill.rs`) -- pairing them would fabricate a vault delta
    /// between two unrelated venue actions.
    #[tokio::test]
    async fn try_from_tx_hash_quarantines_mismatched_operator_settlement() {
        let inventory = address!("0xbeb0009aca35087ce7ccf11637e24dd1aad3bf2a");
        let orderbook = address!("0x1111111111111111111111111111111111111111");
        let bot_operator = address!("0x679df30b30ac2947aa3143490add6717af81dcc3");
        let deposit_operator = address!("0x8b8b6e0507c125934c6129563f48e48c66f86475");
        let withdraw_operator = address!("0x9c9c6e0507c125934c6129563f48e48c66f8647a");

        let cache = SymbolCache::default();
        let tx_hash =
            fixed_bytes!("0xb2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2");

        let deposit = OperatorDeposit {
            operator: deposit_operator,
            token: REAL_USDC_BASE,
            vaultId: b256!("0x0000000000000000000000000000000000000000000000000000000000000004"),
            amount: uint!(5_000_000_U256),
        };
        let withdraw = OperatorWithdraw {
            operator: withdraw_operator,
            token: REAL_WTCOIN_BASE,
            vaultId: b256!("0x0000000000000000000000000000000000000000000000000000000000000003"),
            amount: uint!(34_172_366_621_067_031_U256),
        };

        let deposit_log = alloy::rpc::types::Log {
            inner: alloy::primitives::Log {
                address: inventory,
                data: deposit.to_log_data(),
            },
            block_hash: None,
            block_number: Some(48_030_415),
            block_timestamp: Some(1_782_850_177),
            transaction_hash: Some(tx_hash),
            transaction_index: None,
            log_index: Some(0),
            removed: false,
        };
        let withdraw_log = alloy::rpc::types::Log {
            inner: alloy::primitives::Log {
                address: inventory,
                data: withdraw.to_log_data(),
            },
            block_hash: None,
            block_number: Some(48_030_415),
            block_timestamp: Some(1_782_850_177),
            transaction_hash: Some(tx_hash),
            transaction_index: None,
            log_index: Some(1),
            removed: false,
        };

        let receipt = serde_json::json!({
            "transactionHash": tx_hash,
            "transactionIndex": "0x3b",
            "blockHash": "0x373307a0e2154c2de6b046e349fc27f9bb02b01fdddbb15eeba57f3ce3b24973",
            "blockNumber": "0x2dce2cf",
            "from": bot_operator,
            "to": inventory,
            "gasUsed": "0x7a62d",
            "effectiveGasPrice": "0x57bcf0",
            "cumulativeGasUsed": "0xa00b4e",
            "status": "0x1",
            "type": "0x2",
            "logsBloom": format!("0x{}", "0".repeat(512)),
            "logs": [deposit_log, withdraw_log]
        });

        let asserter = Asserter::new();
        asserter.push_success(&receipt); // get_transaction_receipt
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        let ctx = TradingChain::test()
            .orderbook(orderbook)
            .inventory(InventoryMode::Managed { inventory })
            .vault_owner(inventory)
            .call();

        let trade = OnchainTrade::try_from_tx_hash(
            tx_hash,
            &ReadOnlyEvm::new(provider),
            &cache,
            &ctx,
            RecoveryActors {
                order_owner: inventory,
                bot_operator: BotOperator(bot_operator),
            },
        )
        .await
        .unwrap();

        assert!(
            trade.is_none(),
            "a deposit and withdraw emitted by different operators must be quarantined, not \
             paired into a fabricated hedge"
        );
    }

    fn make_io(token: Address, vault_id: B256) -> IRaindexV6::IOV2 {
        IRaindexV6::IOV2 {
            token,
            vaultId: vault_id,
        }
    }

    fn make_order(
        owner: Address,
        inputs: Vec<IRaindexV6::IOV2>,
        outputs: Vec<IRaindexV6::IOV2>,
    ) -> IRaindexV6::OrderV4 {
        IRaindexV6::OrderV4 {
            owner,
            evaluable: IRaindexV6::EvaluableV4::default(),
            validInputs: inputs,
            validOutputs: outputs,
            nonce: B256::ZERO,
        }
    }

    #[test]
    fn extract_vault_info_returns_none_for_out_of_bounds_input() {
        let order = make_order(
            address!("0x1111111111111111111111111111111111111111"),
            vec![make_io(
                address!("0x2222222222222222222222222222222222222222"),
                b256!("0x0000000000000000000000000000000000000000000000000000000000000001"),
            )],
            vec![make_io(
                address!("0x3333333333333333333333333333333333333333"),
                b256!("0x0000000000000000000000000000000000000000000000000000000000000002"),
            )],
        );

        assert!(extract_vault_info(&order, 1, 0).is_none());
    }

    #[test]
    fn extract_vault_info_returns_none_for_out_of_bounds_output() {
        let order = make_order(
            address!("0x1111111111111111111111111111111111111111"),
            vec![make_io(
                address!("0x2222222222222222222222222222222222222222"),
                b256!("0x0000000000000000000000000000000000000000000000000000000000000001"),
            )],
            vec![make_io(
                address!("0x3333333333333333333333333333333333333333"),
                b256!("0x0000000000000000000000000000000000000000000000000000000000000002"),
            )],
        );

        assert!(extract_vault_info(&order, 0, 1).is_none());
    }

    #[test]
    fn extract_vault_info_extracts_valid_indices() {
        let input_token = address!("0x2222222222222222222222222222222222222222");
        let output_token = address!("0x3333333333333333333333333333333333333333");
        let input_vault =
            b256!("0x0000000000000000000000000000000000000000000000000000000000000001");
        let output_vault =
            b256!("0x0000000000000000000000000000000000000000000000000000000000000002");

        let order = make_order(
            address!("0x1111111111111111111111111111111111111111"),
            vec![make_io(input_token, input_vault)],
            vec![make_io(output_token, output_vault)],
        );

        let (input, output) = extract_vault_info(&order, 0, 0).unwrap();

        assert_eq!(input.token, input_token);
        assert_eq!(input.vault_id, input_vault);
        assert_eq!(output.token, output_token);
        assert_eq!(output.vault_id, output_vault);
    }

    #[test]
    fn extract_vaults_from_clear_extracts_both_orders() {
        let alice_owner = address!("0xaaaa000000000000000000000000000000000001");
        let bob_owner = address!("0xbbbb000000000000000000000000000000000002");

        let alice = make_order(
            alice_owner,
            vec![make_io(
                address!("0x1111111111111111111111111111111111111111"),
                b256!("0x0000000000000000000000000000000000000000000000000000000000000001"),
            )],
            vec![make_io(
                address!("0x2222222222222222222222222222222222222222"),
                b256!("0x0000000000000000000000000000000000000000000000000000000000000002"),
            )],
        );

        let bob = make_order(
            bob_owner,
            vec![make_io(
                address!("0x3333333333333333333333333333333333333333"),
                b256!("0x0000000000000000000000000000000000000000000000000000000000000003"),
            )],
            vec![make_io(
                address!("0x4444444444444444444444444444444444444444"),
                b256!("0x0000000000000000000000000000000000000000000000000000000000000004"),
            )],
        );

        let event = IRaindexV6::ClearV3 {
            sender: address!("0x0000000000000000000000000000000000000000"),
            alice,
            bob,
            clearConfig: IRaindexV6::ClearConfigV2 {
                aliceInputIOIndex: uint!(0_U256),
                aliceOutputIOIndex: uint!(0_U256),
                bobInputIOIndex: uint!(0_U256),
                bobOutputIOIndex: uint!(0_U256),
                aliceBountyVaultId: B256::ZERO,
                bobBountyVaultId: B256::ZERO,
            },
        };

        let vaults = extract_vaults_from_clear(&event);

        assert_eq!(vaults.len(), 4);
        assert_eq!(vaults[0].owner, alice_owner);
        assert_eq!(vaults[1].owner, alice_owner);
        assert_eq!(vaults[2].owner, bob_owner);
        assert_eq!(vaults[3].owner, bob_owner);
    }

    #[test]
    fn extract_owned_vaults_extracts_order_vaults() {
        let owner = address!("0xaaaa000000000000000000000000000000000001");
        let input_token = address!("0x1111111111111111111111111111111111111111");
        let output_token = address!("0x2222222222222222222222222222222222222222");
        let input_vault =
            b256!("0x0000000000000000000000000000000000000000000000000000000000000001");
        let output_vault =
            b256!("0x0000000000000000000000000000000000000000000000000000000000000002");

        let order = make_order(
            owner,
            vec![make_io(input_token, input_vault)],
            vec![make_io(output_token, output_vault)],
        );

        let vaults = extract_owned_vaults(&order, uint!(0_U256), uint!(0_U256));

        assert_eq!(vaults.len(), 2);
        assert_eq!(vaults[0].owner, owner);
        assert_eq!(vaults[0].vault.token, input_token);
        assert_eq!(vaults[0].vault.vault_id, input_vault);
        assert_eq!(vaults[1].owner, owner);
        assert_eq!(vaults[1].vault.token, output_token);
        assert_eq!(vaults[1].vault.vault_id, output_vault);
    }

    #[test]
    fn extract_owned_vaults_returns_empty_for_invalid_indices() {
        let owner = address!("0xaaaa000000000000000000000000000000000001");

        let order = make_order(
            owner,
            vec![make_io(
                address!("0x1111111111111111111111111111111111111111"),
                b256!("0x0000000000000000000000000000000000000000000000000000000000000001"),
            )],
            vec![],
        );

        let vaults = extract_owned_vaults(&order, uint!(0_U256), uint!(0_U256));
        assert!(vaults.is_empty());
    }

    // `try_from_inventory_trade` validates the USDC leg against the
    // hardcoded canonical `USDC_BASE` address, so the InventoryTrade tests'
    // USDC leg must use it too, not an arbitrary placeholder.
    const INVENTORY_USDC: Address = USDC_BASE;
    const INVENTORY_EQUITY: Address = address!("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");

    /// Builds an `ChainAssets` with a single configured equity symbol,
    /// whose `tokenized_equity_derivative` is the canonical address
    /// `try_from_inventory_trade` validates the equity leg against.
    fn assets_config_with_equity(
        symbol: &str,
        tokenized_equity_derivative: Address,
    ) -> ChainAssets {
        let mut symbols = HashMap::new();
        symbols.insert(
            Symbol::new(symbol).unwrap(),
            ChainEquityAsset {
                tokenized_equity: Address::ZERO,
                tokenized_equity_derivative,
                vault_ids: Vec::new(),
                trading: OperationMode::Enabled,
                rebalancing: OperationMode::Disabled,
                wrapped_equity_recovery: OperationMode::Disabled,
                operational_limit: None,
            },
        );
        ChainAssets {
            equities: ChainEquities {
                operational_limit: None,
                symbols,
            },
            cash: None,
        }
    }

    fn operator_deposit(token: Address, amount: U256) -> OperatorDeposit {
        OperatorDeposit {
            operator: Address::ZERO,
            token,
            vaultId: B256::ZERO,
            amount,
        }
    }

    fn operator_withdraw(token: Address, amount: U256) -> OperatorWithdraw {
        OperatorWithdraw {
            operator: Address::ZERO,
            token,
            vaultId: B256::ZERO,
            amount,
        }
    }

    fn inventory_adapters(venue: InventoryAdapterVenue, operator: Address) -> InventoryAdapters {
        InventoryAdapters::try_new(vec![InventoryAdapter { venue, operator }]).unwrap()
    }

    /// Drives `try_from_inventory_trade` with a pre-seeded symbol cache (so
    /// symbol resolution makes no RPC) and a deterministic mock queue containing
    /// only the deposit-token then withdraw-token `decimals()` calls. The log
    /// already carries its block number, so parsing must not fetch a receipt.
    async fn run_inventory_parser(
        deposit: OperatorDeposit,
        deposit_decimals: u8,
        withdraw: OperatorWithdraw,
        withdraw_decimals: u8,
    ) -> Result<Option<OnchainTrade>, OnChainError> {
        let cache = SymbolCache::default();
        preload_on_all_chains(&cache, INVENTORY_USDC, "USDC");
        preload_on_all_chains(&cache, INVENTORY_EQUITY, "wtAAPL");
        let assets = assets_config_with_equity("AAPL", INVENTORY_EQUITY);

        let asserter = Asserter::new();
        asserter.push_success(&<decimalsCall as SolCall>::abi_encode_returns(
            &deposit_decimals,
        ));
        asserter.push_success(&<decimalsCall as SolCall>::abi_encode_returns(
            &withdraw_decimals,
        ));
        let evm = ReadOnlyEvm::new(ProviderBuilder::new().connect_mocked_client(asserter));

        let log = crate::test_utils::create_log(7);
        let inv = InventoryTrade { deposit, withdraw };

        OnchainTrade::try_from_inventory_trade(
            Chain::Ethereum,
            &cache,
            &evm,
            &assets,
            &InventoryAdapters::default(),
            &inv,
            log,
            None,
        )
        .await
    }

    #[tokio::test]
    async fn block_numbered_inventory_trade_does_not_fetch_receipt() {
        // Pool received 2 wtAAPL (deposit) and sent 160 USDC (withdraw): it
        // bought equity onchain, so the bot is now long and must hedge Buy.
        let trade = run_inventory_parser(
            operator_deposit(INVENTORY_EQUITY, uint!(2000000000000000000_U256)),
            18,
            operator_withdraw(INVENTORY_USDC, uint!(160000000_U256)),
            6,
        )
        .await
        .unwrap()
        .unwrap();

        assert_eq!(trade.symbol.to_string(), "wtAAPL");
        assert_eq!(trade.chain, Chain::Ethereum);
        assert_eq!(trade.direction, Direction::Buy);
        assert_eq!(
            trade.source,
            OnChainTradeSource::UnrecognizedInventory {
                operator: Address::ZERO,
            }
        );
        assert_eq!(trade.equity_token, INVENTORY_EQUITY);
        assert_eq!(trade.amount, FractionalShares::new(float!(2)));
        assert!(trade.price.value().eq(float!(80)).unwrap());
        assert_eq!(trade.block_number, Some(12345));
        // block_timestamp resolves from the log's timestamp (no header fetch).
        assert_eq!(
            trade.block_timestamp,
            DateTime::from_timestamp(1_700_000_000, 0)
        );
    }

    #[tokio::test]
    async fn missing_inventory_log_block_number_uses_receipt_block_number() {
        let cache = SymbolCache::default();
        preload_on_all_chains(&cache, INVENTORY_USDC, "USDC");
        preload_on_all_chains(&cache, INVENTORY_EQUITY, "wtAAPL");
        let assets = assets_config_with_equity("AAPL", INVENTORY_EQUITY);
        let mut log = crate::test_utils::create_log(7);
        log.block_number = None;
        let tx_hash = log.transaction_hash.unwrap();

        let asserter = Asserter::new();
        asserter.push_success(&serde_json::json!({
            "transactionHash": tx_hash,
            "transactionIndex": "0x1",
            "blockHash": "0x1234567890123456789012345678901234567890123456789012345678901234",
            "blockNumber": "0x2a",
            "from": "0x1234567890123456789012345678901234567890",
            "to": "0x5678901234567890123456789012345678901234",
            "gasUsed": "0x5208",
            "effectiveGasPrice": "0x77359400",
            "cumulativeGasUsed": "0x5208",
            "status": "0x1",
            "type": "0x2",
            "logsBloom": format!("0x{}", "0".repeat(512)),
            "logs": []
        }));
        asserter.push_success(&<decimalsCall as SolCall>::abi_encode_returns(&18u8));
        asserter.push_success(&<decimalsCall as SolCall>::abi_encode_returns(&6u8));
        let evm = ReadOnlyEvm::new(ProviderBuilder::new().connect_mocked_client(asserter));
        let inventory_trade = InventoryTrade {
            deposit: operator_deposit(INVENTORY_EQUITY, uint!(2000000000000000000_U256)),
            withdraw: operator_withdraw(INVENTORY_USDC, uint!(160000000_U256)),
        };

        let trade = OnchainTrade::try_from_inventory_trade(
            Chain::Base,
            &cache,
            &evm,
            &assets,
            &InventoryAdapters::default(),
            &inventory_trade,
            log,
            None,
        )
        .await
        .unwrap()
        .unwrap();

        assert_eq!(trade.block_number, Some(42));
    }

    #[tokio::test]
    async fn try_from_inventory_trade_usdc_deposit_flips_direction_to_sell() {
        // USDC on the deposit side: pool received 160 USDC and sent 2 wtAAPL
        // out, i.e. it sold equity onchain -> hedge Sell (mirror of happy path).
        let trade = run_inventory_parser(
            operator_deposit(INVENTORY_USDC, uint!(160000000_U256)),
            6,
            operator_withdraw(INVENTORY_EQUITY, uint!(2000000000000000000_U256)),
            18,
        )
        .await
        .unwrap()
        .unwrap();

        assert_eq!(trade.symbol.to_string(), "wtAAPL");
        assert_eq!(trade.direction, Direction::Sell);
        assert_eq!(trade.equity_token, INVENTORY_EQUITY);
        assert_eq!(trade.amount, FractionalShares::new(float!(2)));
        assert!(trade.price.value().eq(float!(80)).unwrap());
    }

    /// A spoof token whose `symbol()` reports "USDC" but whose address is NOT
    /// the configured canonical `USDC_BASE` must be rejected -- accepting it
    /// on symbol alone would let a compromised/buggy `OPERATOR_ROLE` holder
    /// trick the bot into hedging (and paying out of the shared vault
    /// against) a fabricated leg.
    #[tokio::test]
    async fn try_from_inventory_trade_rejects_spoofed_usdc_symbol() {
        let spoof_usdc = address!("0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef");

        let cache = SymbolCache::default();
        preload_on_all_chains(&cache, spoof_usdc, "USDC");
        preload_on_all_chains(&cache, INVENTORY_EQUITY, "wtAAPL");
        let assets = assets_config_with_equity("AAPL", INVENTORY_EQUITY);

        let asserter = Asserter::new();
        asserter.push_success(&<decimalsCall as SolCall>::abi_encode_returns(&6u8));
        asserter.push_success(&<decimalsCall as SolCall>::abi_encode_returns(&18u8));
        let evm = ReadOnlyEvm::new(ProviderBuilder::new().connect_mocked_client(asserter));

        let inv = InventoryTrade {
            deposit: operator_deposit(spoof_usdc, uint!(160000000_U256)),
            withdraw: operator_withdraw(INVENTORY_EQUITY, uint!(2000000000000000000_U256)),
        };
        let log = crate::test_utils::create_log(7);

        let error = OnchainTrade::try_from_inventory_trade(
            Chain::Base,
            &cache,
            &evm,
            &assets,
            &InventoryAdapters::default(),
            &inv,
            log,
            None,
        )
        .await
        .unwrap_err();

        match error {
            OnChainError::Validation(TradeValidationError::UnrecognizedInventoryToken {
                token,
                claimed_symbol,
            }) => {
                assert_eq!(token, spoof_usdc);
                assert_eq!(claimed_symbol, "USDC");
            }
            other => panic!("expected UnrecognizedInventoryToken, got {other:?}"),
        }
    }

    /// A spoof token whose `symbol()` reports "wtCOIN" but whose address does
    /// not match the configured `tokenized_equity_derivative` for COIN must
    /// be rejected the same way as a spoofed USDC leg.
    #[tokio::test]
    async fn try_from_inventory_trade_rejects_spoofed_equity_symbol() {
        let spoof_equity = address!("0xbadbadbadbadbadbadbadbadbadbadbadbadbad0");

        let cache = SymbolCache::default();
        preload_on_all_chains(&cache, REAL_USDC_BASE, "USDC");
        preload_on_all_chains(&cache, spoof_equity, "wtCOIN");
        let assets = assets_config_with_equity("COIN", REAL_WTCOIN_BASE);

        let asserter = Asserter::new();
        asserter.push_success(&<decimalsCall as SolCall>::abi_encode_returns(&6u8));
        asserter.push_success(&<decimalsCall as SolCall>::abi_encode_returns(&18u8));
        let evm = ReadOnlyEvm::new(ProviderBuilder::new().connect_mocked_client(asserter));

        let inv = InventoryTrade {
            deposit: operator_deposit(REAL_USDC_BASE, uint!(5_000_000_U256)),
            withdraw: operator_withdraw(spoof_equity, uint!(34_172_366_621_067_031_U256)),
        };
        let log = crate::test_utils::create_log(7);

        let error = OnchainTrade::try_from_inventory_trade(
            Chain::Base,
            &cache,
            &evm,
            &assets,
            &InventoryAdapters::default(),
            &inv,
            log,
            None,
        )
        .await
        .unwrap_err();

        match error {
            OnChainError::Validation(TradeValidationError::UnrecognizedInventoryToken {
                token,
                claimed_symbol,
            }) => {
                assert_eq!(token, spoof_equity);
                assert_eq!(claimed_symbol, "wtCOIN");
            }
            other => panic!("expected UnrecognizedInventoryToken, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn try_from_inventory_trade_zero_equity_returns_none() {
        // The equity side moved zero shares: not a tokenized-equity trade, skip.
        let result = run_inventory_parser(
            operator_deposit(INVENTORY_USDC, uint!(160000000_U256)),
            6,
            operator_withdraw(INVENTORY_EQUITY, U256::ZERO),
            18,
        )
        .await
        .unwrap();

        assert!(
            result.is_none(),
            "a zero-equity inventory settlement must be skipped, got {result:?}"
        );
    }

    #[tokio::test]
    async fn try_from_inventory_trade_non_positive_price_errors() {
        // Non-zero equity moved at a zero USDC price -> unpriceable. Must error
        // (mirroring the ClearV3/TakeOrderV3 path) rather than silently drop it,
        // which would leave the position unhedged.
        let error = run_inventory_parser(
            operator_deposit(INVENTORY_USDC, U256::ZERO),
            6,
            operator_withdraw(INVENTORY_EQUITY, uint!(2000000000000000000_U256)),
            18,
        )
        .await
        .unwrap_err();

        assert!(matches!(
            error,
            OnChainError::Validation(TradeValidationError::NonPositivePrice(_))
        ));
    }

    // Real prod fills on Base mainnet, captured via
    // `cast receipt <tx> --rpc-url https://mainnet.base.org` against the
    // shared RaindexInventory at 0x6b7b523fadd1677413ad92c9404c8f0796bacf6f.
    // Pins the parser against the actual contract's event shape/amounts
    // rather than a hand-picked round number.
    const REAL_USDC_BASE: Address = address!("0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913");
    const REAL_WTCOIN_BASE: Address = address!("0x5CdA0E1cA4ce2Af96315F7F8963c85399c172204");

    #[tokio::test]
    async fn try_from_inventory_trade_real_bebop_fill_is_sell() {
        // tx 0xe13a11de734768f08a9c1ef66e8de3bcb9072f8cdabce9f1d819e1ae9909d4b9:
        // Bebop-routed settlement. The pool received 5 USDC (deposit) and
        // sent 0.034172366621067031 wtCOIN (withdraw), i.e. it sold equity
        // onchain, so it must hedge Sell (USDC on the deposit side).
        let cache = SymbolCache::default();
        preload_on_all_chains(&cache, REAL_USDC_BASE, "USDC");
        preload_on_all_chains(&cache, REAL_WTCOIN_BASE, "wtCOIN");

        let tx_hash =
            fixed_bytes!("0xe13a11de734768f08a9c1ef66e8de3bcb9072f8cdabce9f1d819e1ae9909d4b9");
        let operator = address!("0x8b8b6e0507c125934c6129563f48e48c66f86475");

        let deposit = OperatorDeposit {
            operator,
            token: REAL_USDC_BASE,
            vaultId: b256!("0x0000000000000000000000000000000000000000000000000000000000000004"),
            amount: uint!(5_000_000_U256), // 5 USDC, 6 decimals
        };
        let withdraw = OperatorWithdraw {
            operator,
            token: REAL_WTCOIN_BASE,
            vaultId: b256!("0x0000000000000000000000000000000000000000000000000000000000000003"),
            amount: uint!(34_172_366_621_067_031_U256), // 0.034172366621067031 wtCOIN, 18 decimals
        };

        let asserter = Asserter::new();
        asserter.push_success(&<decimalsCall as SolCall>::abi_encode_returns(&6u8)); // USDC
        asserter.push_success(&<decimalsCall as SolCall>::abi_encode_returns(&18u8)); // wtCOIN
        let evm = ReadOnlyEvm::new(ProviderBuilder::new().connect_mocked_client(asserter));

        let mut log = crate::test_utils::create_log(0x97);
        log.transaction_hash = Some(tx_hash);
        log.block_number = Some(48_030_415);
        log.block_timestamp = Some(1_782_850_177);

        let inv = InventoryTrade { deposit, withdraw };
        let assets = assets_config_with_equity("COIN", REAL_WTCOIN_BASE);
        let adapters = inventory_adapters(InventoryAdapterVenue::Bebop, operator);

        let trade = OnchainTrade::try_from_inventory_trade(
            Chain::Base,
            &cache,
            &evm,
            &assets,
            &adapters,
            &inv,
            log,
            None,
        )
        .await
        .unwrap()
        .unwrap();

        assert_eq!(trade.symbol.to_string(), "wtCOIN");
        assert_eq!(trade.direction, Direction::Sell);
        assert_eq!(
            trade.source,
            OnChainTradeSource::Inventory {
                operator,
                venue: InventoryVenue::Bebop,
            }
        );
        assert_eq!(trade.equity_token, REAL_WTCOIN_BASE);
        let expected_amount =
            Float::from_fixed_decimal(uint!(34_172_366_621_067_031_U256), 18).unwrap();
        assert!(trade.amount.inner().eq(expected_amount).unwrap());
        // 5 USDC / 0.034172366621067031 wtCOIN ~= 146.317 USDC/share.
        let price_diff = (trade.price.value() - float!(146.317))
            .unwrap()
            .abs()
            .unwrap();
        assert!(price_diff.lt(float!(0.001)).unwrap());
    }

    #[tokio::test]
    async fn try_from_inventory_trade_real_univ4_fill_is_buy() {
        // tx 0x9ee8e401a6f12227df1a30a236b60ac83c72b2b1eb610d83cf292ae789eb0805:
        // univ4-routed settlement. The pool received 0.01 wtCOIN (deposit)
        // and sent 2 USDC (withdraw), i.e. it bought equity onchain, so it
        // must hedge Buy (equity on the deposit side, mirroring the
        // happy-path unit test above).
        let cache = SymbolCache::default();
        preload_on_all_chains(&cache, REAL_USDC_BASE, "USDC");
        preload_on_all_chains(&cache, REAL_WTCOIN_BASE, "wtCOIN");

        let tx_hash =
            fixed_bytes!("0x9ee8e401a6f12227df1a30a236b60ac83c72b2b1eb610d83cf292ae789eb0805");
        let operator = address!("0x36ebb1e5149c60111dd035f0417a4b00d39caa88");

        let deposit = OperatorDeposit {
            operator,
            token: REAL_WTCOIN_BASE,
            vaultId: b256!("0x0000000000000000000000000000000000000000000000000000000000000003"),
            amount: uint!(10_000_000_000_000_000_U256), // 0.01 wtCOIN, 18 decimals
        };
        let withdraw = OperatorWithdraw {
            operator,
            token: REAL_USDC_BASE,
            vaultId: b256!("0x0000000000000000000000000000000000000000000000000000000000000004"),
            amount: uint!(2_000_000_U256), // 2 USDC, 6 decimals
        };

        let asserter = Asserter::new();
        asserter.push_success(&<decimalsCall as SolCall>::abi_encode_returns(&18u8)); // wtCOIN
        asserter.push_success(&<decimalsCall as SolCall>::abi_encode_returns(&6u8)); // USDC
        let evm = ReadOnlyEvm::new(ProviderBuilder::new().connect_mocked_client(asserter));

        let mut log = crate::test_utils::create_log(0xf4);
        log.transaction_hash = Some(tx_hash);
        log.block_number = Some(48_051_940);
        log.block_timestamp = Some(1_782_893_227);

        let inv = InventoryTrade { deposit, withdraw };
        let assets = assets_config_with_equity("COIN", REAL_WTCOIN_BASE);
        let adapters = inventory_adapters(InventoryAdapterVenue::UniswapV4, operator);

        let trade = OnchainTrade::try_from_inventory_trade(
            Chain::Base,
            &cache,
            &evm,
            &assets,
            &adapters,
            &inv,
            log,
            None,
        )
        .await
        .unwrap()
        .unwrap();

        assert_eq!(trade.symbol.to_string(), "wtCOIN");
        assert_eq!(trade.direction, Direction::Buy);
        assert_eq!(
            trade.source,
            OnChainTradeSource::Inventory {
                operator,
                venue: InventoryVenue::UniswapV4,
            }
        );
        assert_eq!(trade.equity_token, REAL_WTCOIN_BASE);
        assert!(trade.amount.inner().eq(float!(0.01)).unwrap());
        // 2 USDC / 0.01 wtCOIN = 200 USDC/share exactly.
        assert!(trade.price.value().eq(float!(200)).unwrap());
    }

    /// A transient/transport-style RPC failure (connection reset, timeout,
    /// or here: the mock queue draining to empty mid-retry) on `decimals()`
    /// must not be classified as `TokenIntrospectionFailed` -- it must
    /// surface as a retryable `OnChainError::Evm` so apalis retries with
    /// backoff instead of permanently dropping a legitimate fill.
    #[tokio::test]
    async fn fetch_inventory_token_decimals_surfaces_transient_error_as_retryable() {
        let asserter = Asserter::new();
        // INVENTORY_TOKEN_DECIMALS_MAX_RETRIES retries = one more than that
        // many total attempts; push a failure for every attempt so the
        // final (returned) error is guaranteed to be the transient one.
        for _ in 0..=INVENTORY_TOKEN_DECIMALS_MAX_RETRIES {
            asserter.push_failure_msg("connection reset by peer");
        }
        let evm = ReadOnlyEvm::new(ProviderBuilder::new().connect_mocked_client(asserter));

        let error = fetch_inventory_token_decimals(&evm, INVENTORY_EQUITY)
            .await
            .unwrap_err();

        match error {
            OnChainError::Evm(_) => {}
            other => panic!("expected OnChainError::Evm for a transient failure, got {other:?}"),
        }
    }

    /// A genuine revert (malformed/malicious token) on `decimals()` must
    /// still be classified as `TokenIntrospectionFailed` so the fill is
    /// caught and skipped, rather than retried forever and eventually
    /// tripping the conductor-wide fail-stop.
    #[tokio::test]
    async fn fetch_inventory_token_decimals_classifies_genuine_revert_as_token_introspection_failed()
     {
        let asserter = Asserter::new();
        for _ in 0..=INVENTORY_TOKEN_DECIMALS_MAX_RETRIES {
            asserter.push_failure(panic_revert_payload());
        }
        let evm = ReadOnlyEvm::new(ProviderBuilder::new().connect_mocked_client(asserter));

        let error = fetch_inventory_token_decimals(&evm, INVENTORY_EQUITY)
            .await
            .unwrap_err();

        match error {
            OnChainError::Validation(TradeValidationError::TokenIntrospectionFailed {
                token,
                ..
            }) => {
                assert_eq!(token, INVENTORY_EQUITY);
            }
            other => {
                panic!("expected TokenIntrospectionFailed for a genuine revert, got {other:?}")
            }
        }
    }

    /// Mirrors `fetch_inventory_token_decimals_surfaces_transient_error_as_retryable`:
    /// a transient failure resolving an `InventoryTrade` token's symbol must
    /// also surface as a retryable `OnChainError::Evm`, not a permanent skip.
    #[tokio::test]
    async fn resolve_inventory_token_symbol_surfaces_transient_error_as_retryable() {
        let cache = SymbolCache::default();
        let asserter = Asserter::new();
        // SymbolCache::resolve_symbol's internal retry budget matches
        // INVENTORY_TOKEN_DECIMALS_MAX_RETRIES (see that constant's doc).
        for _ in 0..=INVENTORY_TOKEN_DECIMALS_MAX_RETRIES {
            asserter.push_failure_msg("connection reset by peer");
        }
        let evm = ReadOnlyEvm::new(ProviderBuilder::new().connect_mocked_client(asserter));

        let error = resolve_inventory_token_symbol(&cache, &evm, Chain::Base, INVENTORY_EQUITY)
            .await
            .unwrap_err();

        match error {
            OnChainError::Evm(_) => {}
            other => panic!("expected OnChainError::Evm for a transient failure, got {other:?}"),
        }
    }

    /// Mirrors the decimals variant of this test: a genuine revert resolving
    /// an `InventoryTrade` token's symbol must still yield
    /// `TokenIntrospectionFailed` so the fill is skipped.
    #[tokio::test]
    async fn resolve_inventory_token_symbol_classifies_genuine_revert_as_token_introspection_failed()
     {
        let cache = SymbolCache::default();
        let asserter = Asserter::new();
        for _ in 0..=INVENTORY_TOKEN_DECIMALS_MAX_RETRIES {
            asserter.push_failure(panic_revert_payload());
        }
        let evm = ReadOnlyEvm::new(ProviderBuilder::new().connect_mocked_client(asserter));

        let error = resolve_inventory_token_symbol(&cache, &evm, Chain::Base, INVENTORY_EQUITY)
            .await
            .unwrap_err();

        match error {
            OnChainError::Validation(TradeValidationError::TokenIntrospectionFailed {
                token,
                ..
            }) => {
                assert_eq!(token, INVENTORY_EQUITY);
            }
            other => {
                panic!("expected TokenIntrospectionFailed for a genuine revert, got {other:?}")
            }
        }
    }
}
