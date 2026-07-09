//! Seeds simulated cross-venue transfer history (USDC rebalances, equity
//! mints, and equity redemptions) for local dashboard simulation, mirroring
//! `super::seed_simulated_hedge_latency_history`'s approach: real CQRS
//! commands through a temporary store instead of raw inserts.
//!
//! The Transfers panel (`dashboard/transfer_loader.rs`) replays straight from
//! the `events` table via `load_entity`/`load_all_ids`, so the bare `events`
//! rows these commands persist are all it needs -- no reactor required.
//! The mint/redemption stores are still wired with `EquityTimingProjection`
//! (mirroring how the USDC store is wired with `RebalanceTimingProjection`
//! below) so the Performance tab's "Equity rebalance stage breakdown" chart
//! also gets historical data, not just the Transfers panel.
//!
//! Single-run-only, same caveat as the hedge-latency fixture: a second call
//! against the same pool hits each aggregate's own already-initialized guard
//! (`UsdcRebalanceError::AlreadyInitiated` /
//! `TokenizedEquityMintError::AlreadyInProgress` /
//! `EquityRedemptionError::AlreadyStarted`). The only caller (`simulate()` in
//! `tests/e2e/full_system.rs`) always seeds a freshly created database file.

use alloy::consensus::{Receipt, ReceiptEnvelope, ReceiptWithBloom};
use alloy::primitives::{Address, B256, Bloom, Log as PrimitiveLog, TxHash, U256};
use alloy::rpc::types::{Log, TransactionReceipt};
use alloy::sol_types::SolEvent;
use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use rain_math_float::Float;
use sqlx::SqlitePool;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

use st0x_event_sorcery::{Store, StoreBuilder};
use st0x_evm::IERC20;
use st0x_execution::{AlpacaTransferId, ClientOrderId, FractionalShares, Symbol};
use st0x_finance::Usdc;
use st0x_raindex::{Raindex, RaindexError, RaindexVaultId};
use st0x_tokenization::{
    AlpacaTokenizationError, IssuerRequestId, TokenizationRequest, TokenizationRequestId,
    TokenizationRequestStatus, TokenizationRequestType, Tokenizer, TokenizerError,
    tokenization_request_id,
};
use st0x_wrapper::{
    UnderlyingPerWrapped, UnwrapConfirmation, WrapConfirmation, Wrapper, WrapperError,
};

use crate::equity_redemption::{EquityRedemption, EquityRedemptionCommand, RedemptionAggregateId};
use crate::performance::equity_timing::EquityTimingProjection;
use crate::performance::rebalance::RebalanceTimingProjection;
use crate::rebalancing::equity::EquityTransferServices;
use crate::tokenized_equity_mint::{
    TOKENIZED_EQUITY_DECIMALS, TokenizedEquityMint, TokenizedEquityMintCommand,
};
use crate::usdc_rebalance::{
    ConversionAmounts, RebalanceDirection, TransferRef, UsdcRebalance, UsdcRebalanceCommand,
    UsdcRebalanceId,
};
use crate::vault_lookup::{VaultLookup, VaultLookupError};

/// Minimal [`Tokenizer`] shared by [`seed_simulated_mint_history`]'s and
/// [`seed_simulated_equity_redemption_history`]'s temporary stores.
///
/// The mint fixture drives only the mint happy path (`RequestMintAt` ->
/// `PollAt` -> `WrapTokensAt` -> `DepositToVaultAt`), which calls exactly
/// `request_mint`/`poll_mint_until_complete`. The redemption fixture drives
/// only `wait_for_block`/`redemption_wallet`/`send_for_redemption` (its
/// `Detect`/`Complete` steps are pure state transitions with no service
/// call). Every other `Tokenizer` method is unreachable from either path.
///
/// Stateful rather than static (unlike `st0x_tokenization::mock::MockTokenizer`)
/// because `poll_mint_until_complete` must echo back the SAME symbol
/// `request_mint` was called with -- the mint aggregate validates
/// `token_symbol == format!("t{symbol}")`, and this fixture cycles through
/// multiple symbols in one run.
struct FixtureTokenizer {
    pending: Mutex<HashMap<TokenizationRequestId, (Symbol, TxHash)>>,
    redemption_wallet: Address,
    /// Feeds `send_for_redemption`'s synthetic tx hash through
    /// `simulated_transfer_uuid`, keeping it deterministic across runs like
    /// every other id in this module.
    day: u32,
}

impl FixtureTokenizer {
    fn new(redemption_wallet: Address, day: u32) -> Self {
        Self {
            pending: Mutex::new(HashMap::new()),
            redemption_wallet,
            day,
        }
    }
}

#[async_trait]
impl Tokenizer for FixtureTokenizer {
    async fn request_mint(
        &self,
        symbol: Symbol,
        quantity: FractionalShares,
        wallet: Address,
        issuer_request_id: IssuerRequestId,
    ) -> Result<TokenizationRequest, TokenizerError> {
        let id = tokenization_request_id(&format!("sim-mint-{issuer_request_id}"));
        // Derived from the 16-byte `IssuerRequestId` uuid, not the (longer)
        // `TokenizationRequestId` string: `TxHash::left_padding_from` panics
        // above 32 input bytes, and the request-id string exceeds that.
        let tx_hash = TxHash::left_padding_from(issuer_request_id.0.as_bytes());
        self.pending
            .lock()
            .await
            .insert(id.clone(), (symbol.clone(), tx_hash));

        Ok(TokenizationRequest {
            id,
            r#type: Some(TokenizationRequestType::Mint),
            status: TokenizationRequestStatus::Pending,
            underlying_symbol: symbol,
            token_symbol: None,
            quantity,
            wallet: Some(wallet),
            issuer_request_id: Some(issuer_request_id),
            tx_hash: None,
            fees: None,
            created_at: Utc::now(),
        })
    }

    async fn poll_mint_until_complete(
        &self,
        id: &TokenizationRequestId,
    ) -> Result<TokenizationRequest, TokenizerError> {
        let (symbol, tx_hash) = self.pending.lock().await.get(id).cloned().ok_or_else(|| {
            TokenizerError::Alpaca(AlpacaTokenizationError::RequestNotFound { id: id.clone() })
        })?;

        Ok(TokenizationRequest {
            id: id.clone(),
            r#type: Some(TokenizationRequestType::Mint),
            status: TokenizationRequestStatus::Completed,
            token_symbol: Some(format!("t{symbol}")),
            underlying_symbol: symbol,
            quantity: FractionalShares::ZERO,
            wallet: None,
            issuer_request_id: None,
            tx_hash: Some(tx_hash),
            fees: None,
            created_at: Utc::now(),
        })
    }

    async fn get_request(
        &self,
        _id: &TokenizationRequestId,
    ) -> Result<TokenizationRequest, TokenizerError> {
        unimplemented!("FixtureTokenizer: mint fixture never calls get_request")
    }

    fn redemption_wallet(&self) -> Option<Address> {
        Some(self.redemption_wallet)
    }

    async fn wait_for_block(&self, _block: u64) -> Result<(), st0x_evm::EvmError> {
        Ok(())
    }

    async fn send_for_redemption(
        &self,
        _token: Address,
        _amount: U256,
    ) -> Result<TxHash, TokenizerError> {
        Ok(TxHash::left_padding_from(
            simulated_transfer_uuid("redeem-send-tx", self.day).as_bytes(),
        ))
    }

    async fn poll_for_redemption(
        &self,
        _tx_hash: &TxHash,
    ) -> Result<TokenizationRequest, TokenizerError> {
        unimplemented!("FixtureTokenizer: mint fixture never calls poll_for_redemption")
    }

    async fn find_redemption_by_tx(
        &self,
        _tx_hash: &TxHash,
    ) -> Result<Option<TokenizationRequest>, TokenizerError> {
        unimplemented!("FixtureTokenizer: mint fixture never calls find_redemption_by_tx")
    }

    async fn poll_redemption_until_complete(
        &self,
        _id: &TokenizationRequestId,
    ) -> Result<TokenizationRequest, TokenizerError> {
        unimplemented!("FixtureTokenizer: mint fixture never calls poll_redemption_until_complete")
    }

    async fn verify_mint_tx(
        &self,
        _tx_hash: TxHash,
        _token_address: Address,
        _wallet: Address,
        _expected_amount: U256,
    ) -> Result<(), st0x_tokenization::MintVerificationError> {
        unimplemented!("FixtureTokenizer: mint fixture never calls verify_mint_tx")
    }

    async fn list_pending_requests(&self) -> Result<Vec<TokenizationRequest>, TokenizerError> {
        unimplemented!("FixtureTokenizer: mint fixture never calls list_pending_requests")
    }
}

/// Deterministic UUID for this module's fixtures, namespaced separately from
/// `super::simulated_latency_uuid` so the two fixtures' synthetic ids never
/// collide even where they happen to share a `kind`/`day` pair.
fn simulated_transfer_uuid(kind: &str, day: u32) -> uuid::Uuid {
    uuid::Uuid::new_v5(
        &uuid::Uuid::NAMESPACE_OID,
        format!("st0x-simulated-transfer-history:{kind}:{day}").as_bytes(),
    )
}

fn usdc(value: f64) -> anyhow::Result<Usdc> {
    Ok(Usdc::new(Float::parse(format!("{value:.2}"))?))
}

/// Seeds deterministic equity-mint history for local dashboard simulation.
///
/// Drives the `TokenizedEquityMint` aggregate's happy path
/// (`RequestMintAt` -> `PollAt` -> `WrapTokensAt` -> `DepositToVaultAt`)
/// through a temporary store, one mint per day alternating between the same
/// dedicated fixture symbols (`AAPL.SIM`/`TSLA.SIM`) used by
/// [`super::seed_simulated_hedge_latency_history`].
pub async fn seed_simulated_mint_history(
    pool: &SqlitePool,
    now: DateTime<Utc>,
    days: u32,
) -> anyhow::Result<()> {
    sqlx::migrate!().set_ignore_missing(true).run(pool).await?;

    let mut services = EquityTransferServices::panicking();
    // Mint's happy path never calls `redemption_wallet`/`send_for_redemption`;
    // the address and day are meaningful only to
    // `seed_simulated_equity_redemption_history`'s use of this same fixture.
    services.tokenizer = Arc::new(FixtureTokenizer::new(Address::ZERO, 0));

    let mint = StoreBuilder::<TokenizedEquityMint>::new(pool.clone())
        .with(Arc::new(EquityTimingProjection::new(pool.clone())))
        .build(services)
        .await?;

    let range_start = now - Duration::days(i64::from(days)) - Duration::days(1);
    let aapl = Symbol::new("AAPL.SIM")?;
    let tsla = Symbol::new("TSLA.SIM")?;
    let wallet = Address::repeat_byte(0x5A);

    for day in 0..days {
        let symbol = if day % 2 == 0 { &aapl } else { &tsla };
        let requested_at = range_start + Duration::days(i64::from(day)) + Duration::hours(9);
        let received_at = requested_at + Duration::minutes(2) + Duration::seconds(30);
        let wrapped_at = received_at + Duration::seconds(45);
        let deposited_at = wrapped_at + Duration::seconds(30);

        let issuer_request_id = IssuerRequestId(simulated_transfer_uuid("mint", day));
        let quantity = Float::parse("5".to_string())?;

        mint.send(
            &issuer_request_id,
            TokenizedEquityMintCommand::RequestMintAt {
                issuer_request_id: issuer_request_id.clone(),
                symbol: symbol.clone(),
                quantity,
                wallet,
                requested_at,
            },
        )
        .await?;

        mint.send(
            &issuer_request_id,
            TokenizedEquityMintCommand::PollAt { received_at },
        )
        .await?;

        let wrap_tx_hash =
            TxHash::left_padding_from(simulated_transfer_uuid("mint-wrap", day).as_bytes());
        let wrapped_shares = quantity.to_fixed_decimal(TOKENIZED_EQUITY_DECIMALS)?;
        let wrap_block = 2_000_000_u64 + u64::from(day) * 10;

        mint.send(
            &issuer_request_id,
            TokenizedEquityMintCommand::WrapTokensAt {
                wrap_tx_hash,
                wrapped_shares,
                wrap_block,
                wrapped_at,
            },
        )
        .await?;

        let vault_deposit_tx_hash =
            TxHash::left_padding_from(simulated_transfer_uuid("mint-deposit", day).as_bytes());

        mint.send(
            &issuer_request_id,
            TokenizedEquityMintCommand::DepositToVaultAt {
                vault_deposit_tx_hash,
                deposited_at,
            },
        )
        .await?;
    }

    Ok(())
}

/// Seeds deterministic USDC-rebalance (Alpaca<->Base) history for local
/// dashboard simulation.
///
/// Drives the `UsdcRebalance` aggregate (whose `Services = ()`, so no
/// fixture service implementations are needed -- none of its commands call
/// out to an external service) through the full per-direction command
/// sequence, alternating direction by day. Wired with
/// `RebalanceTimingProjection` (the equity-mint/redemption fixtures wire
/// `EquityTimingProjection` for the same reason) so the Performance tab's
/// "Rebalance stage breakdown" chart, not just the dashboard's Transfers
/// panel, gets historical data too.
pub async fn seed_simulated_usdc_rebalance_history(
    pool: &SqlitePool,
    now: DateTime<Utc>,
    days: u32,
) -> anyhow::Result<()> {
    sqlx::migrate!().set_ignore_missing(true).run(pool).await?;

    // `UsdcRebalance::Materialized = Nil`, so `build()` returns `Arc<Store<_>>`
    // alone regardless of `.with()` reactor count (unlike `Position`'s
    // `Materialized = Table`, which returns a `(Store, Projection)` pair) --
    // the reactor is wired into the store's dispatch either way.
    let rebalance = StoreBuilder::<UsdcRebalance>::new(pool.clone())
        .with(Arc::new(RebalanceTimingProjection::new(pool.clone())))
        .build(())
        .await?;

    let range_start = now - Duration::days(i64::from(days)) - Duration::days(1);

    for day in 0..days {
        let id = UsdcRebalanceId(simulated_transfer_uuid("usdc-rebalance", day));
        let base_time = range_start + Duration::days(i64::from(day)) + Duration::hours(15);

        if day % 2 == 0 {
            seed_alpaca_to_base(&rebalance, id, day, base_time).await?;
        } else {
            seed_base_to_alpaca(&rebalance, id, day, base_time).await?;
        }
    }

    Ok(())
}

/// Drives one full AlpacaToBase cycle: pre-withdrawal USD->USDC conversion,
/// Alpaca withdrawal, CCTP bridge, onchain deposit. Terminal at
/// `DepositConfirmed` (`AlpacaToBase` completes there; see
/// `UsdcRebalance::to_dto`).
async fn seed_alpaca_to_base(
    store: &Store<UsdcRebalance>,
    id: UsdcRebalanceId,
    day: u32,
    base_time: DateTime<Utc>,
) -> anyhow::Result<()> {
    let requested_value = f64::from(day).mul_add(50.0, 5_000.0);
    let received_value = requested_value * 0.998;
    let bridged_value = received_value - 1.0;

    let requested = usdc(requested_value)?;
    let received = usdc(received_value)?;
    let bridged = usdc(bridged_value)?;
    let fee = usdc(1.0)?;

    let order_id = ClientOrderId::from_uuid(simulated_transfer_uuid("usdc-a2b-convert", day));
    let t0 = base_time;
    let t1 = t0 + Duration::seconds(20);
    let t2 = t1 + Duration::seconds(5);
    let t3 = t2 + Duration::minutes(3);
    let t4 = t3 + Duration::seconds(30);
    let t5 = t4 + Duration::seconds(10);
    let t6 = t5 + Duration::seconds(15);
    let t7 = t6 + Duration::minutes(2);
    let t8 = t7 + Duration::seconds(45);
    let t9 = t8 + Duration::seconds(20);
    let t10 = t9 + Duration::minutes(1);
    let from_block = 3_000_000_u64 + u64::from(day) * 10;

    store
        .send(
            &id,
            UsdcRebalanceCommand::InitiateConversionAt {
                direction: RebalanceDirection::AlpacaToBase,
                amount: requested,
                order_id,
                initiated_at: t0,
            },
        )
        .await?;

    store
        .send(
            &id,
            UsdcRebalanceCommand::ConfirmConversionAt {
                conversion: ConversionAmounts::new(requested, received),
                converted_at: t1,
            },
        )
        .await?;

    store
        .send(
            &id,
            UsdcRebalanceCommand::BeginWithdrawalAt {
                direction: RebalanceDirection::AlpacaToBase,
                amount: received,
                from_block,
                submitting_at: t2,
            },
        )
        .await?;

    let withdrawal_ref =
        AlpacaTransferId::from(simulated_transfer_uuid("usdc-a2b-withdraw-ref", day));
    store
        .send(
            &id,
            UsdcRebalanceCommand::InitiateAt {
                direction: RebalanceDirection::AlpacaToBase,
                amount: received,
                withdrawal: TransferRef::AlpacaId(withdrawal_ref),
                initiated_at: t3,
            },
        )
        .await?;

    let withdrawal_tx =
        TxHash::left_padding_from(simulated_transfer_uuid("usdc-a2b-withdraw-tx", day).as_bytes());
    store
        .send(
            &id,
            UsdcRebalanceCommand::ConfirmWithdrawalAt {
                withdrawal_tx: Some(withdrawal_tx),
                confirmed_at: t4,
            },
        )
        .await?;

    store
        .send(
            &id,
            UsdcRebalanceCommand::BeginBridgingAt {
                from_block: from_block + 5,
                burn_amount: Some(received),
                submitting_at: t5,
            },
        )
        .await?;

    let burn_tx =
        TxHash::left_padding_from(simulated_transfer_uuid("usdc-a2b-burn", day).as_bytes());
    store
        .send(
            &id,
            UsdcRebalanceCommand::InitiateBridgingAt {
                burn_tx,
                burned_at: t6,
            },
        )
        .await?;

    let attestation = simulated_transfer_uuid("usdc-a2b-attestation", day)
        .into_bytes()
        .to_vec();
    let cctp_nonce =
        B256::left_padding_from(simulated_transfer_uuid("usdc-a2b-nonce", day).as_bytes());
    store
        .send(
            &id,
            UsdcRebalanceCommand::ReceiveAttestationAt {
                attestation: attestation.clone(),
                cctp_nonce,
                message: attestation,
                mint_scan_from_block: from_block + 20,
                attested_at: t7,
            },
        )
        .await?;

    let mint_tx =
        TxHash::left_padding_from(simulated_transfer_uuid("usdc-a2b-mint", day).as_bytes());
    store
        .send(
            &id,
            UsdcRebalanceCommand::ConfirmBridgingAt {
                mint_tx,
                amount_received: bridged,
                fee_collected: fee,
                minted_at: t8,
            },
        )
        .await?;

    let deposit_tx =
        TxHash::left_padding_from(simulated_transfer_uuid("usdc-a2b-deposit", day).as_bytes());
    store
        .send(
            &id,
            UsdcRebalanceCommand::InitiateDepositAt {
                deposit: TransferRef::OnchainTx(deposit_tx),
                deposit_initiated_at: t9,
            },
        )
        .await?;

    store
        .send(
            &id,
            UsdcRebalanceCommand::ConfirmDepositAt {
                deposit_confirmed_at: t10,
            },
        )
        .await?;

    Ok(())
}

/// Drives one full BaseToAlpaca cycle: onchain withdrawal, CCTP bridge,
/// Alpaca deposit, post-deposit USDC->USD conversion. Terminal at
/// `ConversionComplete` (see `UsdcRebalance::to_dto`).
async fn seed_base_to_alpaca(
    store: &Store<UsdcRebalance>,
    id: UsdcRebalanceId,
    day: u32,
    base_time: DateTime<Utc>,
) -> anyhow::Result<()> {
    let withdraw_value = f64::from(day).mul_add(50.0, 5_000.0);
    let bridged_value = withdraw_value - 1.0;
    let final_value = bridged_value * 0.998;

    let withdraw_amount = usdc(withdraw_value)?;
    let bridged_amount = usdc(bridged_value)?;
    let fee = usdc(1.0)?;
    let final_amount = usdc(final_value)?;

    let t0 = base_time;
    let t1 = t0 + Duration::seconds(30);
    let t2 = t1 + Duration::minutes(2);
    let t3 = t2 + Duration::seconds(10);
    let t4 = t3 + Duration::seconds(15);
    let t5 = t4 + Duration::minutes(2);
    let t6 = t5 + Duration::seconds(45);
    let t7 = t6 + Duration::seconds(20);
    let t8 = t7 + Duration::minutes(1);
    let t9 = t8 + Duration::seconds(20);
    let t10 = t9 + Duration::seconds(15);
    let from_block = 3_500_000_u64 + u64::from(day) * 10;

    store
        .send(
            &id,
            UsdcRebalanceCommand::BeginWithdrawalAt {
                direction: RebalanceDirection::BaseToAlpaca,
                amount: withdraw_amount,
                from_block,
                submitting_at: t0,
            },
        )
        .await?;

    let withdrawal_tx =
        TxHash::left_padding_from(simulated_transfer_uuid("usdc-b2a-withdraw-tx", day).as_bytes());
    store
        .send(
            &id,
            UsdcRebalanceCommand::InitiateAt {
                direction: RebalanceDirection::BaseToAlpaca,
                amount: withdraw_amount,
                withdrawal: TransferRef::OnchainTx(withdrawal_tx),
                initiated_at: t1,
            },
        )
        .await?;

    store
        .send(
            &id,
            UsdcRebalanceCommand::ConfirmWithdrawalAt {
                withdrawal_tx: None,
                confirmed_at: t2,
            },
        )
        .await?;

    store
        .send(
            &id,
            UsdcRebalanceCommand::BeginBridgingAt {
                from_block: from_block + 5,
                burn_amount: Some(withdraw_amount),
                submitting_at: t3,
            },
        )
        .await?;

    let burn_tx =
        TxHash::left_padding_from(simulated_transfer_uuid("usdc-b2a-burn", day).as_bytes());
    store
        .send(
            &id,
            UsdcRebalanceCommand::InitiateBridgingAt {
                burn_tx,
                burned_at: t4,
            },
        )
        .await?;

    let attestation = simulated_transfer_uuid("usdc-b2a-attestation", day)
        .into_bytes()
        .to_vec();
    let cctp_nonce =
        B256::left_padding_from(simulated_transfer_uuid("usdc-b2a-nonce", day).as_bytes());
    store
        .send(
            &id,
            UsdcRebalanceCommand::ReceiveAttestationAt {
                attestation: attestation.clone(),
                cctp_nonce,
                message: attestation,
                mint_scan_from_block: from_block + 20,
                attested_at: t5,
            },
        )
        .await?;

    let mint_tx =
        TxHash::left_padding_from(simulated_transfer_uuid("usdc-b2a-mint", day).as_bytes());
    store
        .send(
            &id,
            UsdcRebalanceCommand::ConfirmBridgingAt {
                mint_tx,
                amount_received: bridged_amount,
                fee_collected: fee,
                minted_at: t6,
            },
        )
        .await?;

    let deposit_ref = AlpacaTransferId::from(simulated_transfer_uuid("usdc-b2a-deposit-ref", day));
    store
        .send(
            &id,
            UsdcRebalanceCommand::InitiateDepositAt {
                deposit: TransferRef::AlpacaId(deposit_ref),
                deposit_initiated_at: t7,
            },
        )
        .await?;

    store
        .send(
            &id,
            UsdcRebalanceCommand::ConfirmDepositAt {
                deposit_confirmed_at: t8,
            },
        )
        .await?;

    let post_deposit_order_id =
        ClientOrderId::from_uuid(simulated_transfer_uuid("usdc-b2a-post-convert", day));
    store
        .send(
            &id,
            UsdcRebalanceCommand::InitiatePostDepositConversionAt {
                order_id: post_deposit_order_id,
                amount: bridged_amount,
                initiated_at: t9,
            },
        )
        .await?;

    store
        .send(
            &id,
            UsdcRebalanceCommand::ConfirmConversionAt {
                conversion: ConversionAmounts::new(bridged_amount, final_amount),
                converted_at: t10,
            },
        )
        .await?;

    Ok(())
}

/// Builds a synthetic, always-`status: true` transaction receipt whose logs
/// are exactly `logs`. Mirrors `src/onchain/mock.rs`'s `successful_receipt`
/// (that copy is `#[cfg(test)]`-only, so unusable from a `test-support` e2e
/// build; this is its `test-support`-gated counterpart).
fn successful_receipt(tx_hash: TxHash, block_number: u64, logs: Vec<Log>) -> TransactionReceipt {
    TransactionReceipt {
        inner: ReceiptEnvelope::Eip1559(ReceiptWithBloom {
            receipt: Receipt {
                status: true.into(),
                cumulative_gas_used: 0,
                logs,
            },
            logs_bloom: Bloom::default(),
        }),
        transaction_hash: tx_hash,
        transaction_index: Some(0),
        block_hash: None,
        block_number: Some(block_number),
        gas_used: 21_000,
        effective_gas_price: 1,
        blob_gas_used: None,
        blob_gas_price: None,
        from: Address::ZERO,
        to: Some(Address::ZERO),
        contract_address: None,
    }
}

/// Builds a decodable ERC20 `Transfer` log, mirroring `src/onchain/mock.rs`'s
/// `transfer_log` (see `successful_receipt` above for why this can't just
/// reuse that `#[cfg(test)]`-only copy).
fn transfer_log(token: Address, to: Address, amount: U256) -> Log {
    let event = IERC20::Transfer {
        from: Address::ZERO,
        to,
        value: amount,
    };
    let inner = PrimitiveLog {
        address: token,
        data: event.encode_log_data(),
    };

    Log {
        inner,
        transaction_hash: None,
        transaction_index: None,
        block_hash: None,
        block_number: None,
        block_timestamp: None,
        log_index: None,
        removed: false,
    }
}

/// Minimal [`Raindex`] for one [`seed_simulated_equity_redemption_history`]
/// cycle. Scoped to a single redemption (a fresh instance per day, not
/// shared across the seeding loop), so `submit_withdraw`'s single-slot
/// `pending_withdraw` is always populated by the time `confirm_tx_receipt`
/// reads it -- the redemption fixture always calls them in that order.
/// `withdraw`/`submit_deposit` are unreachable from `EquityRedemption`'s
/// happy path (it only ever calls `submit_withdraw`/`confirm_tx_receipt`).
struct FixtureRaindex {
    /// Recipient the synthetic withdrawal's `Transfer` log credits -- must
    /// match `FixtureWrapper::owner()`, since `ConfirmWithdraw`'s handler
    /// reads `services.wrapper.owner()` as the expected recipient.
    recipient: Address,
    block_number: u64,
    pending_withdraw: Mutex<Option<(Address, U256)>>,
    /// Feeds `submit_withdraw`'s synthetic tx hash through
    /// `simulated_transfer_uuid`, keeping it deterministic across runs like
    /// every other id in this module.
    day: u32,
}

impl FixtureRaindex {
    fn new(recipient: Address, block_number: u64, day: u32) -> Self {
        Self {
            recipient,
            block_number,
            pending_withdraw: Mutex::new(None),
            day,
        }
    }
}

#[async_trait]
impl Raindex for FixtureRaindex {
    async fn withdraw(
        &self,
        _token: Address,
        _vault_id: RaindexVaultId,
        _target_amount: U256,
        _decimals: u8,
    ) -> Result<TxHash, RaindexError> {
        unimplemented!("FixtureRaindex: redemption fixture never calls withdraw")
    }

    async fn submit_deposit(
        &self,
        _token: Address,
        _vault_id: RaindexVaultId,
        _amount: U256,
        _decimals: u8,
    ) -> Result<TxHash, RaindexError> {
        unimplemented!("FixtureRaindex: redemption fixture never calls submit_deposit")
    }

    async fn submit_withdraw(
        &self,
        token: Address,
        _vault_id: RaindexVaultId,
        target_amount: U256,
        _decimals: u8,
    ) -> Result<TxHash, RaindexError> {
        *self.pending_withdraw.lock().await = Some((token, target_amount));
        Ok(TxHash::left_padding_from(
            simulated_transfer_uuid("redeem-withdraw-tx", self.day).as_bytes(),
        ))
    }

    async fn confirm_tx_receipt(
        &self,
        tx_hash: TxHash,
    ) -> Result<TransactionReceipt, RaindexError> {
        let (token, amount) = self
            .pending_withdraw
            .lock()
            .await
            .ok_or(RaindexError::ScanInconclusive { from_block: 0 })?;

        Ok(successful_receipt(
            tx_hash,
            self.block_number,
            vec![transfer_log(token, self.recipient, amount)],
        ))
    }
}

/// Minimal [`VaultLookup`] for one redemption cycle: resolves every token to
/// the same fixed vault id. `vault_token_for_symbol` is unreachable from
/// `EquityRedemption`'s happy path.
struct FixtureVaultLookup {
    vault_id: RaindexVaultId,
}

impl FixtureVaultLookup {
    fn new(vault_id: RaindexVaultId) -> Self {
        Self { vault_id }
    }
}

#[async_trait]
impl VaultLookup for FixtureVaultLookup {
    async fn vault_id_for_token(
        &self,
        _token: Address,
    ) -> Result<RaindexVaultId, VaultLookupError> {
        Ok(self.vault_id)
    }

    async fn vault_token_for_symbol(&self, _symbol: &Symbol) -> Result<Address, VaultLookupError> {
        unimplemented!("FixtureVaultLookup: redemption fixture never calls vault_token_for_symbol")
    }
}

/// Minimal [`Wrapper`] for one [`seed_simulated_equity_redemption_history`]
/// cycle. Scoped to a single redemption, same reasoning as [`FixtureRaindex`]:
/// `submit_unwrap`'s single-slot `pending_unwrap` is always populated by the
/// time `confirm_unwrap` reads it. 1:1 unwrap ratio, matching
/// `st0x_wrapper::mock::MockWrapper`'s convention. Every method beyond
/// `owner`/`lookup_underlying`/`submit_unwrap`/`confirm_unwrap`/
/// `wait_for_block` is unreachable from `EquityRedemption`'s happy path.
struct FixtureWrapper {
    owner: Address,
    underlying_token: Address,
    unwrap_block: u64,
    pending_unwrap: Mutex<Option<U256>>,
    /// Feeds `submit_unwrap`'s synthetic tx hash through
    /// `simulated_transfer_uuid`, keeping it deterministic across runs like
    /// every other id in this module.
    day: u32,
}

impl FixtureWrapper {
    fn new(owner: Address, underlying_token: Address, unwrap_block: u64, day: u32) -> Self {
        Self {
            owner,
            underlying_token,
            unwrap_block,
            pending_unwrap: Mutex::new(None),
            day,
        }
    }
}

#[async_trait]
impl Wrapper for FixtureWrapper {
    async fn get_ratio_for_symbol(
        &self,
        _symbol: &Symbol,
    ) -> Result<UnderlyingPerWrapped, WrapperError> {
        unimplemented!("FixtureWrapper: redemption fixture never calls get_ratio_for_symbol")
    }

    fn lookup_underlying(&self, _symbol: &Symbol) -> Result<Address, WrapperError> {
        Ok(self.underlying_token)
    }

    fn lookup_derivative(&self, _symbol: &Symbol) -> Result<Address, WrapperError> {
        unimplemented!("FixtureWrapper: redemption fixture never calls lookup_derivative")
    }

    async fn to_wrapped(
        &self,
        _wrapped_token: Address,
        _underlying_amount: U256,
        _receiver: Address,
    ) -> Result<(TxHash, U256), WrapperError> {
        unimplemented!("FixtureWrapper: redemption fixture never calls to_wrapped")
    }

    async fn to_underlying(
        &self,
        _wrapped_token: Address,
        _wrapped_amount: U256,
        _receiver: Address,
        _owner: Address,
    ) -> Result<(TxHash, U256), WrapperError> {
        unimplemented!("FixtureWrapper: redemption fixture never calls to_underlying")
    }

    async fn donate(
        &self,
        _wrapped_token: Address,
        _underlying_amount: U256,
    ) -> Result<TxHash, WrapperError> {
        unimplemented!("FixtureWrapper: redemption fixture never calls donate")
    }

    async fn submit_wrap(
        &self,
        _wrapped_token: Address,
        _underlying_amount: U256,
        _receiver: Address,
    ) -> Result<TxHash, WrapperError> {
        unimplemented!("FixtureWrapper: redemption fixture never calls submit_wrap")
    }

    async fn confirm_wrap(
        &self,
        _wrapped_token: Address,
        _tx_hash: TxHash,
    ) -> Result<WrapConfirmation, WrapperError> {
        unimplemented!("FixtureWrapper: redemption fixture never calls confirm_wrap")
    }

    async fn submit_unwrap(
        &self,
        _wrapped_token: Address,
        wrapped_amount: U256,
        _receiver: Address,
        _owner: Address,
    ) -> Result<TxHash, WrapperError> {
        *self.pending_unwrap.lock().await = Some(wrapped_amount);
        Ok(TxHash::left_padding_from(
            simulated_transfer_uuid("redeem-unwrap-tx", self.day).as_bytes(),
        ))
    }

    async fn confirm_unwrap(
        &self,
        _wrapped_token: Address,
        _tx_hash: TxHash,
    ) -> Result<UnwrapConfirmation, WrapperError> {
        let assets = self
            .pending_unwrap
            .lock()
            .await
            .ok_or(WrapperError::MissingWithdrawEvent)?;

        Ok(UnwrapConfirmation {
            assets,
            block: self.unwrap_block,
        })
    }

    async fn wait_for_block(&self, _block: u64) -> Result<(), WrapperError> {
        Ok(())
    }

    fn owner(&self) -> Address {
        self.owner
    }
}

/// Seeds deterministic equity-redemption history for local dashboard
/// simulation.
///
/// Drives the `EquityRedemption` aggregate's happy path (`RedeemAt` ->
/// `SubmitWithdrawAt` -> `ConfirmWithdrawAt` -> `UnwrapTokensAt` ->
/// `SubmitUnwrapAt` -> `ConfirmUnwrapAt` -> `PrepareSendAt` ->
/// `SendTokensAt` -> `DetectAt` -> `CompleteAt`), one redemption per day
/// alternating between the same dedicated fixture symbols
/// (`AAPL.SIM`/`TSLA.SIM`) used by [`super::seed_simulated_hedge_latency_history`].
///
/// Unlike the mint/USDC fixtures, `EquityRedemption`'s service-calling
/// commands (`SubmitWithdraw`/`ConfirmWithdraw`/`SubmitUnwrap`/
/// `ConfirmUnwrap`/`SendTokens`) drive the side effect FROM WITHIN
/// `transition()` itself (mint's analogous commands take the service
/// result as command input instead), so a fresh, single-cycle-scoped
/// [`FixtureRaindex`]/[`FixtureVaultLookup`]/[`FixtureWrapper`] triple is
/// built for every redemption.
pub async fn seed_simulated_equity_redemption_history(
    pool: &SqlitePool,
    now: DateTime<Utc>,
    days: u32,
) -> anyhow::Result<()> {
    sqlx::migrate!().set_ignore_missing(true).run(pool).await?;

    let range_start = now - Duration::days(i64::from(days)) - Duration::days(1);
    let aapl = Symbol::new("AAPL.SIM")?;
    let tsla = Symbol::new("TSLA.SIM")?;
    let owner = Address::repeat_byte(0x5A);
    let redemption_wallet = Address::repeat_byte(0xA1);

    for day in 0..days {
        let symbol = if day % 2 == 0 { &aapl } else { &tsla };
        let token =
            Address::left_padding_from(simulated_transfer_uuid("redeem-token", day).as_bytes());
        let underlying_token = Address::left_padding_from(
            simulated_transfer_uuid("redeem-underlying", day).as_bytes(),
        );
        let vault_id = RaindexVaultId(B256::left_padding_from(
            simulated_transfer_uuid("redeem-vault", day).as_bytes(),
        ));
        let withdraw_block = 4_000_000_u64 + u64::from(day) * 10;
        let unwrap_block = withdraw_block + 5;

        let services = EquityTransferServices {
            raindex: Arc::new(FixtureRaindex::new(owner, withdraw_block, day)),
            vault_lookup: Arc::new(FixtureVaultLookup::new(vault_id)),
            tokenizer: Arc::new(FixtureTokenizer::new(redemption_wallet, day)),
            wrapper: Arc::new(FixtureWrapper::new(
                owner,
                underlying_token,
                unwrap_block,
                day,
            )),
        };

        let redemption = StoreBuilder::<EquityRedemption>::new(pool.clone())
            .with(Arc::new(EquityTimingProjection::new(pool.clone())))
            .build(services)
            .await?;

        let id = RedemptionAggregateId(simulated_transfer_uuid("redemption", day));
        let quantity = Float::parse("5".to_string())?;
        let wrapped_amount = quantity.to_fixed_decimal(TOKENIZED_EQUITY_DECIMALS)?;

        let pending_at = range_start + Duration::days(i64::from(day)) + Duration::hours(11);
        let submitted_at = pending_at + Duration::seconds(20);
        let withdrawn_at = submitted_at + Duration::seconds(30);
        let unwrap_pending_at = withdrawn_at + Duration::seconds(10);
        let unwrap_submitted_at = unwrap_pending_at + Duration::seconds(15);
        let unwrapped_at = unwrap_submitted_at + Duration::seconds(25);
        let send_pending_at = unwrapped_at + Duration::seconds(10);
        let sent_at = send_pending_at + Duration::seconds(20);
        let detected_at = sent_at + Duration::minutes(2);
        let completed_at = detected_at + Duration::seconds(30);

        redemption
            .send(
                &id,
                EquityRedemptionCommand::RedeemAt {
                    symbol: symbol.clone(),
                    quantity,
                    token,
                    amount: wrapped_amount,
                    pending_at,
                },
            )
            .await?;

        redemption
            .send(
                &id,
                EquityRedemptionCommand::SubmitWithdrawAt { submitted_at },
            )
            .await?;

        redemption
            .send(
                &id,
                EquityRedemptionCommand::ConfirmWithdrawAt { withdrawn_at },
            )
            .await?;

        redemption
            .send(
                &id,
                EquityRedemptionCommand::UnwrapTokensAt {
                    pending_at: unwrap_pending_at,
                },
            )
            .await?;

        redemption
            .send(
                &id,
                EquityRedemptionCommand::SubmitUnwrapAt {
                    submitted_at: unwrap_submitted_at,
                },
            )
            .await?;

        redemption
            .send(
                &id,
                EquityRedemptionCommand::ConfirmUnwrapAt { unwrapped_at },
            )
            .await?;

        redemption
            .send(
                &id,
                EquityRedemptionCommand::PrepareSendAt {
                    pending_at: send_pending_at,
                },
            )
            .await?;

        redemption
            .send(&id, EquityRedemptionCommand::SendTokensAt { sent_at })
            .await?;

        redemption
            .send(
                &id,
                EquityRedemptionCommand::DetectAt {
                    tokenization_request_id: tokenization_request_id(&format!("sim-redeem-{id}")),
                    detected_at,
                },
            )
            .await?;

        redemption
            .send(&id, EquityRedemptionCommand::CompleteAt { completed_at })
            .await?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;
    use st0x_dto::EquityOperationKind;
    use st0x_event_sorcery::load_entity;

    use super::*;
    use crate::performance::ReportRange;
    use crate::performance::equity_timing::load_equity_timings;
    use crate::performance::rebalance::load_rebalance_timings;
    use crate::test_utils::setup_test_db;

    #[tokio::test]
    async fn mint_history_reaches_deposited_into_raindex_for_every_seeded_day() {
        let pool = setup_test_db().await;
        let now = Utc.with_ymd_and_hms(2026, 7, 1, 18, 0, 0).unwrap();
        let days = 6;

        seed_simulated_mint_history(&pool, now, days).await.unwrap();

        for day in 0..days {
            let issuer_request_id = IssuerRequestId(simulated_transfer_uuid("mint", day));
            let mint = load_entity::<TokenizedEquityMint>(&pool, &issuer_request_id)
                .await
                .unwrap()
                .unwrap_or_else(|| panic!("mint aggregate for day {day} was never initialized"));

            assert!(
                matches!(mint, TokenizedEquityMint::DepositedIntoRaindex { .. }),
                "day {day} mint did not reach DepositedIntoRaindex: {mint:?}",
            );
        }

        let timings = load_equity_timings(&pool, &ReportRange::all_time())
            .await
            .unwrap();
        assert_eq!(timings.total_operations, days as usize);
        assert_eq!(timings.skipped_operations, 0);
        assert!(
            timings
                .operations
                .iter()
                .all(|operation| operation.kind == EquityOperationKind::Mint),
            "expected every seeded operation to be a mint: {:?}",
            timings.operations,
        );

        // Each seeded day's `RequestMintAt` backdates `requested_at` (which
        // becomes `started_at`) by one additional day. If any `*At` handler
        // silently fell back to `Utc::now()` instead of threading the
        // supplied timestamp, every operation would collapse onto
        // near-identical `started_at` values instead of spanning the full
        // seeded range.
        let mut started_ats: Vec<DateTime<Utc>> = timings
            .operations
            .iter()
            .map(|operation| {
                operation
                    .started_at
                    .unwrap_or_else(|| panic!("mint operation missing started_at: {operation:?}"))
            })
            .collect();
        started_ats.sort_unstable();
        let earliest = *started_ats.first().unwrap();
        let latest = *started_ats.last().unwrap();
        assert_eq!(
            latest - earliest,
            Duration::days(i64::from(days - 1)),
            "seeded mint started_at values should span one day per seeded day",
        );
    }

    #[tokio::test]
    async fn usdc_rebalance_history_reaches_the_correct_terminal_per_direction() {
        let pool = setup_test_db().await;
        let now = Utc.with_ymd_and_hms(2026, 7, 1, 18, 0, 0).unwrap();
        let days = 6;

        seed_simulated_usdc_rebalance_history(&pool, now, days)
            .await
            .unwrap();

        for day in 0..days {
            let id = UsdcRebalanceId(simulated_transfer_uuid("usdc-rebalance", day));
            let rebalance = load_entity::<UsdcRebalance>(&pool, &id)
                .await
                .unwrap()
                .unwrap_or_else(|| panic!("usdc rebalance for day {day} was never initialized"));

            if day % 2 == 0 {
                assert!(
                    matches!(
                        rebalance,
                        UsdcRebalance::DepositConfirmed {
                            direction: RebalanceDirection::AlpacaToBase,
                            ..
                        }
                    ),
                    "day {day} (AlpacaToBase) did not reach DepositConfirmed: {rebalance:?}",
                );
            } else {
                assert!(
                    matches!(
                        rebalance,
                        UsdcRebalance::ConversionComplete {
                            direction: RebalanceDirection::BaseToAlpaca,
                            ..
                        }
                    ),
                    "day {day} (BaseToAlpaca) did not reach ConversionComplete: {rebalance:?}",
                );
            }
        }

        let timings = load_rebalance_timings(&pool, &ReportRange::all_time())
            .await
            .unwrap();
        assert_eq!(timings.total_operations, days as usize);
        assert_eq!(timings.skipped_operations, 0);

        // Each seeded day's first `*At` command (`InitiateConversionAt` for
        // AlpacaToBase, `BeginWithdrawalAt` for BaseToAlpaca) backdates
        // `started_at` by one additional day. If any of the 12 `*At` handlers
        // silently fell back to `Utc::now()` instead of threading the
        // supplied timestamp, every operation would collapse onto
        // near-identical `started_at` values instead of spanning the full
        // seeded range.
        let mut started_ats: Vec<DateTime<Utc>> = timings
            .operations
            .iter()
            .map(|operation| {
                operation.started_at.unwrap_or_else(|| {
                    panic!("rebalance operation missing started_at: {operation:?}")
                })
            })
            .collect();
        started_ats.sort_unstable();
        let earliest = *started_ats.first().unwrap();
        let latest = *started_ats.last().unwrap();
        assert_eq!(
            latest - earliest,
            Duration::days(i64::from(days - 1)),
            "seeded rebalance started_at values should span one day per seeded day",
        );
    }

    #[tokio::test]
    async fn equity_redemption_history_reaches_completed_for_every_seeded_day() {
        let pool = setup_test_db().await;
        let now = Utc.with_ymd_and_hms(2026, 7, 1, 18, 0, 0).unwrap();
        let days = 6;

        seed_simulated_equity_redemption_history(&pool, now, days)
            .await
            .unwrap();

        for day in 0..days {
            let id = RedemptionAggregateId(simulated_transfer_uuid("redemption", day));
            let redemption = load_entity::<EquityRedemption>(&pool, &id)
                .await
                .unwrap()
                .unwrap_or_else(|| panic!("redemption for day {day} was never initialized"));

            assert!(
                matches!(redemption, EquityRedemption::Completed { .. }),
                "day {day} redemption did not reach Completed: {redemption:?}",
            );
        }

        let timings = load_equity_timings(&pool, &ReportRange::all_time())
            .await
            .unwrap();
        assert_eq!(timings.total_operations, days as usize);
        assert_eq!(timings.skipped_operations, 0);
        assert!(
            timings
                .operations
                .iter()
                .all(|operation| operation.kind == EquityOperationKind::Redeem),
            "expected every seeded operation to be a redemption: {:?}",
            timings.operations,
        );
    }
}
