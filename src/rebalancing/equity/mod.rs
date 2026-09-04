//! Cross-venue equity transfer.
//!
//! [`CrossVenueEquityTransfer`] drives equity transfers in both directions
//! through `resume_equity_to_market_making` / `resume_equity_to_hedging`
//! entry points, each backed by an apalis job:
//!
//! - **Hedging -> Market-Making** (mint): requests tokenized equity from
//!   Alpaca and deposits it into a Raindex vault.
//! - **Market-Making -> Hedging** (redemption): withdraws tokenized equity
//!   from a Raindex vault and sends it to Alpaca for redemption.

mod authorization_job;
mod job;
mod resume_job;

pub(crate) use authorization_job::{
    DeliverMintAuthorization, DeliverMintAuthorizationCtx, DeliverMintAuthorizationJobQueue,
};
#[cfg(test)]
pub(crate) use job::{
    ResumeEquityToHedging, ResumeEquityToMarketMaking, TransferEquityToMarketMakingJobError,
};
pub(crate) use job::{
    TransferEquityToHedging, TransferEquityToHedgingCtx, TransferEquityToHedgingJobQueue,
    TransferEquityToMarketMaking, TransferEquityToMarketMakingCtx,
    TransferEquityToMarketMakingJobQueue,
};
pub(crate) use resume_job::{
    ResumeTokenizationAggregate, ResumeTokenizationCtx, ResumeTokenizationJobQueue,
    ResumeTokenizationTarget,
};

use alloy::hex::FromHexError;
use alloy::primitives::{Address, TxHash, U256};
use alloy::rpc::types::TransactionReceipt;
use async_trait::async_trait;
use sqlx::SqlitePool;
use std::collections::HashMap;
use std::sync::Arc;
use thiserror::Error;
use tracing::{debug, error, info, instrument, warn};

use st0x_event_sorcery::{AggregateError, LifecycleError, SendError, Store};
use st0x_evm::EvmError;
use st0x_execution::{FractionalShares, SharesConversionError, Symbol};
use st0x_issuance_dto::VaultModeTag;
use st0x_raindex::{Raindex, RaindexError, RaindexVaultId};
use st0x_tokenization::{
    AlpacaTokenizationError, IssuerRequestId, MintVerificationError, TokenizationRequest,
    TokenizationRequestId, TokenizationRequestIdError, TokenizationRequestStatus, Tokenizer,
    TokenizerError,
};
use st0x_wrapper::{
    UnderlyingPerWrapped, UnwrapConfirmation, WrapConfirmation, Wrapper, WrapperError,
};

use super::RebalancingService;
use super::trigger::RecoveryClaim;
use crate::bot_gas::redrive::BotGasFailureClassifier;
use crate::bot_gas::{
    BotGasEnqueueFailure, BotGasOperationCategory, BotGasReceiptCostEnqueuer,
    RecordBotGasReceiptCost,
};
use crate::conductor::job::QueuePushError;
use crate::equity_redemption::{
    DetectionFailure, EquityRedemption, EquityRedemptionCommand, EquityRedemptionError,
    RedemptionAggregateId,
};
use crate::mint_authorization::{ConfiguredMintAuthorizer, VaultModeCheckError, VaultModeReader};
use crate::native_gas::{
    ConfiguredGasReadiness, GasReadiness, GasReadinessFailure, TransferGasRoute,
};
use crate::tokenized_equity_mint::{
    TOKENIZED_EQUITY_DECIMALS, TokenizedEquityMint, TokenizedEquityMintCommand,
};
use crate::vault_lookup::{VaultLookup, VaultLookupError};

/// Data extracted from the TokensReceived aggregate state for
/// onchain verification and subsequent wrapping.
struct TokensReceivedData {
    shares_minted: U256,
    tx_hash: TxHash,
    symbol: Symbol,
    wallet: Address,
}

/// Result of re-checking a stuck transfer against the tokenization provider.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RecheckOutcome {
    /// The provider had settled the request; the aggregate was un-failed and
    /// the workflow resumed (or, for redemption, completed).
    Recovered,
    /// The aggregate was active (not failed); the normal workflow was resumed.
    Resumed,
    /// The aggregate was already in its terminal success state.
    AlreadyCompleted,
    /// The provider request is not yet completed; nothing changed.
    LeftUnchanged,
    /// The redemption tx has not been detected by the provider yet.
    NotDetectedYet,
    /// Another transfer for the same symbol is currently in progress, so
    /// recovery was refused: rebuilding tracking would overwrite the live
    /// transfer's in-flight balance. Retry once the symbol is free.
    Conflict,
    /// The failure happened past the recoverable stage (e.g. a mint that
    /// already received tokens, or a redemption that never sent them).
    /// Provider-completion recovery does not apply.
    NotRecoverable,
}

#[derive(Debug, Error)]
pub(crate) enum RecheckError {
    #[error(transparent)]
    Mint(#[from] MintError),
    #[error(transparent)]
    Redemption(#[from] RedemptionError),
    #[error(transparent)]
    Tokenizer(#[from] TokenizerError),
    #[error(transparent)]
    Rebalancing(#[from] super::trigger::RebalancingServiceError),
    #[error(transparent)]
    Database(#[from] sqlx::Error),
    #[error("completed provider request {0} is missing its onchain tx hash")]
    MissingTxHash(TokenizationRequestId),
    #[error("mint {0} has no accepted provider request to re-check")]
    NoAcceptedRequest(IssuerRequestId),
    #[error("mint {id} has an unparseable wallet address in its event history")]
    MalformedWallet {
        id: IssuerRequestId,
        #[source]
        source: FromHexError,
    },
    #[error("mint {id} has an empty tokenization request id in its event history")]
    MalformedTokenizationRequestId {
        id: IssuerRequestId,
        #[source]
        source: TokenizationRequestIdError,
    },
}

/// Context for re-checking a failed mint: the wallet and provider request
/// from the aggregate's event history, plus whether the mint progressed past
/// acceptance (in which case provider-completion recovery does not apply).
struct MintRecheckContext {
    wallet: Address,
    tokenization_request_id: TokenizationRequestId,
    received_tokens: bool,
}

async fn load_mint_recheck_context(
    pool: &SqlitePool,
    id: &IssuerRequestId,
) -> Result<MintRecheckContext, RecheckError> {
    let IssuerRequestId(raw_id) = id;

    let row: Option<(String, String, bool)> = sqlx::query_as(
        "SELECT \
             json_extract(requested.payload, '$.MintRequested.wallet'), \
             json_extract(accepted.payload, '$.MintAccepted.tokenization_request_id'), \
             EXISTS( \
                 SELECT 1 FROM events received \
                 WHERE received.aggregate_type = 'TokenizedEquityMint' \
                   AND received.aggregate_id = requested.aggregate_id \
                   AND received.event_type IN ( \
                       'TokenizedEquityMintEvent::TokensReceived', \
                       'TokenizedEquityMintEvent::ProviderCompletionRecovered' \
                   ) \
             ) \
         FROM events requested \
         INNER JOIN events accepted \
             ON accepted.aggregate_type = requested.aggregate_type \
            AND accepted.aggregate_id = requested.aggregate_id \
            AND accepted.event_type = 'TokenizedEquityMintEvent::MintAccepted' \
         WHERE requested.aggregate_type = 'TokenizedEquityMint' \
           AND requested.aggregate_id = ?1 \
           AND requested.event_type = 'TokenizedEquityMintEvent::MintRequested' \
         ORDER BY accepted.sequence DESC \
         LIMIT 1",
    )
    .bind(raw_id.to_string())
    .fetch_optional(pool)
    .await?;

    let Some((raw_wallet, raw_tokenization_request_id, received_tokens)) = row else {
        return Err(RecheckError::NoAcceptedRequest(id.clone()));
    };

    Ok(MintRecheckContext {
        wallet: raw_wallet
            .parse()
            .map_err(|source| RecheckError::MalformedWallet {
                id: id.clone(),
                source,
            })?,
        tokenization_request_id: TokenizationRequestId::try_new(&raw_tokenization_request_id)
            .map_err(|source| RecheckError::MalformedTokenizationRequestId {
                id: id.clone(),
                source,
            })?,
        received_tokens,
    })
}

/// Services shared by both equity transfer aggregates.
///
/// Both `TokenizedEquityMint` (hedging -> market-making) and
/// `EquityRedemption` (market-making -> hedging) need Raindex for
/// vault operations and Tokenizer for Alpaca API interactions.
#[derive(Clone)]
pub struct EquityTransferServices {
    pub raindex: Arc<dyn Raindex>,
    pub vault_lookup: Arc<dyn VaultLookup>,
    pub tokenizer: Arc<dyn Tokenizer>,
    pub wrapper: Arc<dyn Wrapper>,
    /// Enqueues bot-gas cost recording after `EquityRedemption`'s vault
    /// withdraw / unwrap confirmations succeed (ADR 0017).
    /// `TokenizedEquityMint`'s confirmations go through
    /// `CrossVenueEquityTransfer`'s own field instead (see that struct).
    pub bot_gas_enqueuer: BotGasReceiptCostEnqueuer,
    /// Signs MintAuthV1 recipient authorizations for orchestrator-mode
    /// mints (RAI-1243). `Disabled` while the config has no
    /// `[orchestrator]` section; `SignMintAuthorization` then fails
    /// loudly instead of guessing an orchestrator address.
    pub mint_authorizer: ConfiguredMintAuthorizer,
}

impl EquityTransferServices {
    /// Constructs a services instance whose methods all panic.
    ///
    /// Safe for sending commands that never invoke services (e.g., the
    /// `FailWrapping`, `FailAcceptance`, `FailRaindexDeposit`, `FailTransfer`,
    /// and `Reconcile` commands). Used by the CLI `transfer fail` and
    /// `transfer reconcile` subcommands where no real broker/RPC connection
    /// exists.
    pub fn panicking() -> Self {
        Self {
            raindex: Arc::new(PanickingRaindex),
            vault_lookup: Arc::new(PanickingVaultLookup),
            tokenizer: Arc::new(PanickingTokenizer),
            wrapper: Arc::new(PanickingWrapper),
            bot_gas_enqueuer: BotGasReceiptCostEnqueuer::Disabled,
            mint_authorizer: ConfiguredMintAuthorizer::Disabled,
        }
    }
}

/// Panicking Raindex stub for CLI-only use. All methods panic.
struct PanickingRaindex;

#[async_trait]
impl Raindex for PanickingRaindex {
    async fn withdraw(
        &self,
        _: Address,
        _: RaindexVaultId,
        _: U256,
        _: u8,
    ) -> Result<TxHash, RaindexError> {
        unimplemented!("PanickingRaindex: not available in CLI context")
    }

    async fn submit_deposit(
        &self,
        _: Address,
        _: RaindexVaultId,
        _: U256,
        _: u8,
    ) -> Result<TxHash, RaindexError> {
        unimplemented!("PanickingRaindex: not available in CLI context")
    }

    async fn submit_withdraw(
        &self,
        _: Address,
        _: RaindexVaultId,
        _: U256,
        _: u8,
    ) -> Result<TxHash, RaindexError> {
        unimplemented!("PanickingRaindex: not available in CLI context")
    }

    async fn confirm_tx_receipt(&self, _: TxHash) -> Result<TransactionReceipt, RaindexError> {
        unimplemented!("PanickingRaindex: not available in CLI context")
    }
}

/// Panicking VaultLookup stub for CLI-only use. All methods panic.
struct PanickingVaultLookup;

#[async_trait]
impl VaultLookup for PanickingVaultLookup {
    async fn vault_id_for_token(&self, _: Address) -> Result<RaindexVaultId, VaultLookupError> {
        unimplemented!("PanickingVaultLookup: not available in CLI context")
    }

    async fn vault_token_for_symbol(&self, _: &Symbol) -> Result<Address, VaultLookupError> {
        unimplemented!("PanickingVaultLookup: not available in CLI context")
    }
}

/// Panicking Tokenizer stub for CLI-only use. All methods panic.
struct PanickingTokenizer;

#[async_trait]
impl Tokenizer for PanickingTokenizer {
    async fn request_mint(
        &self,
        _: Symbol,
        _: FractionalShares,
        _: Address,
        _: IssuerRequestId,
    ) -> Result<TokenizationRequest, TokenizerError> {
        unimplemented!("PanickingTokenizer: not available in CLI context")
    }

    async fn find_mint_by_issuer_request_id(
        &self,
        _: &IssuerRequestId,
    ) -> Result<Option<TokenizationRequest>, TokenizerError> {
        unimplemented!("PanickingTokenizer: not available in CLI context")
    }

    async fn poll_mint_until_complete(
        &self,
        _: &TokenizationRequestId,
    ) -> Result<TokenizationRequest, TokenizerError> {
        unimplemented!("PanickingTokenizer: not available in CLI context")
    }

    async fn get_request(
        &self,
        _: &TokenizationRequestId,
    ) -> Result<TokenizationRequest, TokenizerError> {
        unimplemented!("PanickingTokenizer: not available in CLI context")
    }

    fn redemption_wallet(&self) -> Option<Address> {
        unimplemented!("PanickingTokenizer: not available in CLI context")
    }

    async fn wait_for_block(&self, _: u64) -> Result<(), EvmError> {
        unimplemented!("PanickingTokenizer: not available in CLI context")
    }

    async fn send_for_redemption(&self, _: Address, _: U256) -> Result<TxHash, TokenizerError> {
        unimplemented!("PanickingTokenizer: not available in CLI context")
    }

    async fn poll_for_redemption(&self, _: &TxHash) -> Result<TokenizationRequest, TokenizerError> {
        unimplemented!("PanickingTokenizer: not available in CLI context")
    }

    async fn find_redemption_by_tx(
        &self,
        _: &TxHash,
    ) -> Result<Option<TokenizationRequest>, TokenizerError> {
        unimplemented!("PanickingTokenizer: not available in CLI context")
    }

    async fn poll_redemption_until_complete(
        &self,
        _: &TokenizationRequestId,
    ) -> Result<TokenizationRequest, TokenizerError> {
        unimplemented!("PanickingTokenizer: not available in CLI context")
    }

    async fn verify_mint_tx(
        &self,
        _: TxHash,
        _: Address,
        _: Address,
        _: U256,
    ) -> Result<(), MintVerificationError> {
        unimplemented!("PanickingTokenizer: not available in CLI context")
    }

    async fn list_pending_requests(&self) -> Result<Vec<TokenizationRequest>, TokenizerError> {
        unimplemented!("PanickingTokenizer: not available in CLI context")
    }
}

/// Panicking Wrapper stub for CLI-only use. All methods panic.
struct PanickingWrapper;

#[async_trait]
impl Wrapper for PanickingWrapper {
    async fn get_ratio_for_symbol(&self, _: &Symbol) -> Result<UnderlyingPerWrapped, WrapperError> {
        unimplemented!("PanickingWrapper: not available in CLI context")
    }

    fn lookup_underlying(&self, _: &Symbol) -> Result<Address, WrapperError> {
        unimplemented!("PanickingWrapper: not available in CLI context")
    }

    fn lookup_derivative(&self, _: &Symbol) -> Result<Address, WrapperError> {
        unimplemented!("PanickingWrapper: not available in CLI context")
    }

    async fn to_wrapped(
        &self,
        _: Address,
        _: U256,
        _: Address,
    ) -> Result<(TxHash, U256), WrapperError> {
        unimplemented!("PanickingWrapper: not available in CLI context")
    }

    async fn to_underlying(
        &self,
        _: Address,
        _: U256,
        _: Address,
        _: Address,
    ) -> Result<(TxHash, U256), WrapperError> {
        unimplemented!("PanickingWrapper: not available in CLI context")
    }

    async fn donate(&self, _: Address, _: U256) -> Result<TxHash, WrapperError> {
        unimplemented!("PanickingWrapper: not available in CLI context")
    }

    async fn submit_wrap(&self, _: Address, _: U256, _: Address) -> Result<TxHash, WrapperError> {
        unimplemented!("PanickingWrapper: not available in CLI context")
    }

    async fn confirm_wrap(&self, _: Address, _: TxHash) -> Result<WrapConfirmation, WrapperError> {
        unimplemented!("PanickingWrapper: not available in CLI context")
    }

    async fn submit_unwrap(
        &self,
        _: Address,
        _: U256,
        _: Address,
        _: Address,
    ) -> Result<TxHash, WrapperError> {
        unimplemented!("PanickingWrapper: not available in CLI context")
    }

    async fn confirm_unwrap(
        &self,
        _: Address,
        _: TxHash,
    ) -> Result<UnwrapConfirmation, WrapperError> {
        unimplemented!("PanickingWrapper: not available in CLI context")
    }

    async fn wait_for_block(&self, _: u64) -> Result<(), WrapperError> {
        unimplemented!("PanickingWrapper: not available in CLI context")
    }

    fn owner(&self) -> Address {
        unimplemented!("PanickingWrapper: not available in CLI context")
    }
}

#[derive(Debug, Error)]
pub enum MintError {
    #[error(transparent)]
    GasReadiness(#[from] GasReadinessFailure),
    #[error("Aggregate error: {0}")]
    Aggregate(Box<SendError<TokenizedEquityMint>>),
    #[error("Wrapper error: {0}")]
    Wrapper(#[from] WrapperError),
    #[error("Raindex error: {0}")]
    Raindex(#[from] RaindexError),
    #[error("Vault lookup error: {0}")]
    VaultLookup(#[from] VaultLookupError),
    #[error("Onchain mint verification failed: {0}")]
    Verification(#[from] MintVerificationError),
    #[error(
        "Entity not found after command: expected {expected_state} \
         for {issuer_request_id}"
    )]
    EntityNotFound {
        issuer_request_id: IssuerRequestId,
        expected_state: &'static str,
    },
    #[error(
        "Unexpected mint state for {issuer_request_id}: expected \
         {expected_state}, got {entity:?}"
    )]
    UnexpectedState {
        issuer_request_id: IssuerRequestId,
        expected_state: &'static str,
        entity: Box<TokenizedEquityMint>,
    },
    /// Enqueueing the bot-gas receipt cost recording job failed after the
    /// confirming step (wrap or vault deposit) already succeeded onchain --
    /// a local SQLite write, safe to retry since the aggregate has not
    /// advanced past the confirmed state yet. Carries a [`BotGasEnqueueFailure`]
    /// (not a bare `QueuePushError`) so the tx hash the failed enqueue was
    /// for survives into any wrapper (e.g. `WrappedEquityRecoveryError`) that
    /// needs to re-surface it -- see [`crate::bot_gas::redrive`].
    #[error("Failed to enqueue bot-gas receipt cost recording: {0}")]
    BotGasEnqueue(BotGasEnqueueFailure),
    /// The asset's `vault_mode` could not be determined from issuance. The
    /// saga must not guess: assuming vault-direct would skip a required
    /// authorization and stall the mint at issuance's on-chain step. The
    /// mint stays `MintAccepted`; the resume path retries the check.
    #[error("Vault-mode check failed: {0}")]
    VaultModeCheck(#[from] VaultModeCheckError),
    /// `vault_mode` reads orchestrator but the symbol has no
    /// `[assets.equities.<symbol>] tokenized_equity` config entry to bind
    /// the authorization to.
    #[error("No tokenized-equity address configured for {symbol}")]
    UnknownTokenizedEquity { symbol: Symbol },
    /// Enqueueing the authorization delivery job failed -- a local SQLite
    /// write. The signed authorization is already persisted on the
    /// aggregate, so the resume path re-enqueues without re-signing.
    #[error("Failed to enqueue mint-authorization delivery: {0}")]
    AuthorizationEnqueue(#[from] QueuePushError),
}

/// Selector for `ERC20InsufficientBalance(address,uint256,uint256)`.
const ERC20_INSUFFICIENT_BALANCE_SELECTOR: &str = "0xe450d38c";

impl MintError {
    /// Returns `true` if the underlying error is an RPC-level
    /// `ERC20InsufficientBalance` revert, indicating the wallet has
    /// zero tokens because they were already deposited in a previous
    /// session.
    ///
    /// Matches both `Raindex` and `Wrapper` variants: currently only
    /// `Raindex` is reachable from `try_deposit_or_recover`, but the
    /// broader match keeps this predicate correct for any `MintError`
    /// regardless of call site.
    fn is_insufficient_balance_revert(&self) -> bool {
        let (Self::Raindex(RaindexError::Evm(EvmError::Transport(rpc_error)))
        | Self::Wrapper(WrapperError::Evm(EvmError::Transport(rpc_error)))) = self
        else {
            return false;
        };

        rpc_error.as_error_resp().is_some_and(|payload| {
            payload.data.as_ref().is_some_and(|data| {
                data.get()
                    .trim_matches('"')
                    .starts_with(ERC20_INSUFFICIENT_BALANCE_SELECTOR)
            })
        })
    }
}

impl From<SendError<TokenizedEquityMint>> for MintError {
    fn from(error: SendError<TokenizedEquityMint>) -> Self {
        Self::Aggregate(Box::new(error))
    }
}

impl BotGasFailureClassifier for MintError {
    fn is_bot_gas_enqueue_failure(&self) -> bool {
        match self {
            Self::BotGasEnqueue(_) => true,
            Self::GasReadiness(_)
            | Self::Aggregate(_)
            | Self::Wrapper(_)
            | Self::Raindex(_)
            | Self::VaultLookup(_)
            | Self::Verification(_)
            | Self::EntityNotFound { .. }
            | Self::UnexpectedState { .. }
            | Self::VaultModeCheck(_)
            | Self::UnknownTokenizedEquity { .. }
            | Self::AuthorizationEnqueue(_) => false,
        }
    }
}

/// Distinguishes mint failures before vs after tokens were received from
/// Alpaca. Post-receipt failures must NOT clear the in-progress guard
/// because real tokens exist in the wallet and startup recovery will
/// resume them.
#[derive(Debug, Error)]
pub enum MintTransferError {
    /// Failure before Alpaca delivered tokens. Safe to clear guard and
    /// retry from scratch.
    #[error(transparent)]
    PreReceipt(MintError),

    /// Failure after tokens were received (verify/wrap/deposit stage).
    /// Tokens exist in the wallet; guard must stay set for recovery.
    #[error(transparent)]
    PostReceipt(MintError),
}

impl MintTransferError {
    pub(crate) fn gas_readiness_retry_interval(&self) -> Option<std::time::Duration> {
        match self {
            Self::PreReceipt(MintError::GasReadiness(failure)) => failure.retry_interval(),
            Self::PreReceipt(_) | Self::PostReceipt(_) => None,
        }
    }
}

impl BotGasFailureClassifier for MintTransferError {
    fn is_bot_gas_enqueue_failure(&self) -> bool {
        match self {
            Self::PreReceipt(inner) | Self::PostReceipt(inner) => {
                inner.is_bot_gas_enqueue_failure()
            }
        }
    }
}

fn mint_reached_post_receipt(entity: &TokenizedEquityMint) -> bool {
    matches!(
        entity,
        TokenizedEquityMint::TokensReceived { .. }
            | TokenizedEquityMint::WrapSubmitted { .. }
            | TokenizedEquityMint::TokensWrapped { .. }
            | TokenizedEquityMint::VaultDepositSubmitted { .. }
            | TokenizedEquityMint::DepositedIntoRaindex { .. }
    )
}

fn classify_mint_resume_error(
    reached: Result<TokenizedEquityMint, MintError>,
    error: MintError,
) -> MintTransferError {
    match reached {
        Ok(entity) if mint_reached_post_receipt(&entity) => MintTransferError::PostReceipt(error),
        Ok(_) | Err(_) => MintTransferError::PreReceipt(error),
    }
}

#[derive(Debug, Error)]
pub enum RedemptionError {
    #[error(transparent)]
    GasReadiness(#[from] GasReadinessFailure),
    #[error(transparent)]
    Send(#[from] SendError<EquityRedemption>),
    #[error(transparent)]
    Raindex(#[from] RaindexError),
    #[error(transparent)]
    VaultLookup(#[from] VaultLookupError),
    #[error(transparent)]
    Alpaca(#[from] AlpacaTokenizationError),
    #[error(transparent)]
    Tokenizer(#[from] TokenizerError),
    #[error(transparent)]
    SharesConversion(#[from] SharesConversionError),
    #[error("Entity not found after command: {aggregate_id}")]
    EntityNotFound { aggregate_id: RedemptionAggregateId },
    #[error("Token send to Alpaca failed: {entity:?}")]
    SendFailed { entity: EquityRedemption },
    #[error("Unexpected entity: {entity:?}")]
    UnexpectedEntity { entity: EquityRedemption },
    #[error("Unexpected tokenization status: still pending after polling")]
    UnexpectedPendingStatus,
    #[error("Redemption was rejected by Alpaca")]
    Rejected,
}

impl RedemptionError {
    pub(crate) fn gas_readiness_retry_interval(&self) -> Option<std::time::Duration> {
        match self {
            Self::GasReadiness(failure) => failure.retry_interval(),
            _ => None,
        }
    }
}

impl BotGasFailureClassifier for RedemptionError {
    fn is_bot_gas_enqueue_failure(&self) -> bool {
        match self {
            Self::Send(AggregateError::UserError(LifecycleError::Apply(
                EquityRedemptionError::BotGasEnqueueFailed(_),
            ))) => true,
            Self::GasReadiness(_)
            | Self::Send(_)
            | Self::Raindex(_)
            | Self::VaultLookup(_)
            | Self::Alpaca(_)
            | Self::Tokenizer(_)
            | Self::SharesConversion(_)
            | Self::EntityNotFound { .. }
            | Self::SendFailed { .. }
            | Self::UnexpectedEntity { .. }
            | Self::UnexpectedPendingStatus
            | Self::Rejected => false,
        }
    }
}

/// Result of wrapping received mint tokens into ERC-4626 shares.
///
/// Returned by [`CrossVenueEquityTransfer::wrap_received_mint`] to give
/// each element a clear domain name and avoid positional ambiguity in the
/// `(Address, U256, u64)` tuple it replaces.
struct WrappedMintResult {
    /// ERC-4626 derivative token address (the vault).
    token: Address,
    /// Number of ERC-4626 shares minted by the wrap.
    shares: U256,
    /// Block number in which the wrap transaction was confirmed.
    block: u64,
}

/// Orchestrates equity transfers between Raindex and Alpaca.
///
/// Holds CQRS stores for both directions and domain service traits for
/// vault operations and tokenization. External code drives transfers only
/// through [`Self::resume_equity_to_market_making`] and
/// [`Self::resume_equity_to_hedging`].
pub struct CrossVenueEquityTransfer {
    raindex: Arc<dyn Raindex>,
    vault_lookup: Arc<dyn VaultLookup>,
    tokenizer: Arc<dyn Tokenizer>,
    wrapper: Arc<dyn Wrapper>,
    wallet: Address,
    mint_store: Arc<Store<TokenizedEquityMint>>,
    redemption_store: Arc<Store<EquityRedemption>>,
    /// Enqueues bot-gas cost recording after vault deposit / wrap
    /// confirmations succeed (ADR 0017). Defaults to `Disabled`;
    /// production wiring opts in via [`Self::with_bot_gas_enqueuer`].
    bot_gas_enqueuer: BotGasReceiptCostEnqueuer,
    /// Mint-authorization capability for orchestrator-mode assets
    /// (RAI-1243). Defaults to [`ConfiguredMintAuthorization::VaultDirectOnly`]
    /// (an explicit assertion, mirroring `BotGasReceiptCostEnqueuer`'s
    /// explicit-absence shape); production opts into
    /// [`ConfiguredMintAuthorization::Wired`] via
    /// [`Self::with_mint_authorization`].
    mint_authorization: ConfiguredMintAuthorization,
    gas_readiness: ConfiguredGasReadiness,
}

/// Whether this transfer can produce and deliver MintAuthV1 recipient
/// authorizations. Absence is a stated capability, not an inferred one: a
/// construction site choosing `VaultDirectOnly` asserts that every asset it
/// mints is vault-direct, rather than silently defaulting there.
pub(crate) enum ConfiguredMintAuthorization {
    /// Full orchestrator-mode wiring -- the server path, which always has
    /// the issuance client and job queue to build it.
    Wired(Box<MintAuthorizationWiring>),
    /// No wiring: the caller (CLI transfer subcommands, recovery jobs,
    /// tests) asserts the assets it mints are vault-direct. Without the
    /// wiring `vault_mode` cannot be read at all, so an orchestrator-mode
    /// mint through such a path proceeds unauthorized and stalls at
    /// polling -- surfaced by a warning per mint.
    VaultDirectOnly,
}

/// Everything the mint saga needs to produce and deliver a MintAuthV1
/// recipient authorization for orchestrator-mode assets (RAI-1243).
pub(crate) struct MintAuthorizationWiring {
    /// Reads each asset's `vault_mode` from issuance -- the single source
    /// of truth for which assets need an authorization; the bot keeps no
    /// asset-mode list of its own.
    pub(crate) vault_mode_reader: Arc<dyn VaultModeReader>,
    /// Tokenized-equity ERC-20 per symbol, from
    /// `[assets.equities.<SYM>] tokenized_equity` config -- the `token`
    /// the EIP-712 MintAuth binds.
    pub(crate) token_addresses: HashMap<Symbol, Address>,
    /// Delivery job queue; enqueued idempotently per issuer request id.
    pub(crate) delivery_queue: DeliverMintAuthorizationJobQueue,
}

impl CrossVenueEquityTransfer {
    pub fn new(
        raindex: Arc<dyn Raindex>,
        vault_lookup: Arc<dyn VaultLookup>,
        tokenizer: Arc<dyn Tokenizer>,
        wrapper: Arc<dyn Wrapper>,
        wallet: Address,
        mint_store: Arc<Store<TokenizedEquityMint>>,
        redemption_store: Arc<Store<EquityRedemption>>,
    ) -> Self {
        Self {
            raindex,
            vault_lookup,
            tokenizer,
            wrapper,
            wallet,
            mint_store,
            redemption_store,
            bot_gas_enqueuer: BotGasReceiptCostEnqueuer::Disabled,
            mint_authorization: ConfiguredMintAuthorization::VaultDirectOnly,
            gas_readiness: ConfiguredGasReadiness::default(),
        }
    }

    /// Uses the supplied native-gas readiness check before starting a transfer.
    #[must_use]
    pub fn with_gas_readiness(mut self, readiness: Arc<GasReadiness>) -> Self {
        self.gas_readiness = ConfiguredGasReadiness::Wired(readiness);
        self
    }

    /// Opts this transfer into bot-gas cost recording. Called at the
    /// production wiring site only; CLI and test construction leave the
    /// `Disabled` default from [`Self::new`].
    pub(crate) fn with_bot_gas_enqueuer(mut self, enqueuer: BotGasReceiptCostEnqueuer) -> Self {
        self.bot_gas_enqueuer = enqueuer;
        self
    }

    /// Opts this transfer into orchestrator-mode mint authorization.
    /// Called at the production wiring site only.
    pub(crate) fn with_mint_authorization(mut self, wiring: MintAuthorizationWiring) -> Self {
        self.mint_authorization = ConfiguredMintAuthorization::Wired(Box::new(wiring));
        self
    }

    /// Ensures an orchestrator-mode mint has its recipient authorization
    /// signed and its delivery enqueued before polling begins; a no-op for
    /// vault-direct assets. Idempotent end to end: signing no-ops once an
    /// authorization exists (the nonce is the mint's on-chain idempotency
    /// key and must never be re-minted), and the delivery enqueue is keyed
    /// on the issuer request id.
    async fn ensure_mint_authorization(
        &self,
        issuer_request_id: &IssuerRequestId,
        symbol: &Symbol,
    ) -> Result<(), MintError> {
        let wiring = match &self.mint_authorization {
            ConfiguredMintAuthorization::Wired(wiring) => wiring,
            // The construction site asserted vault-direct-only; without the
            // wiring `vault_mode` cannot be read, so the assertion cannot
            // be verified -- surface it per mint.
            ConfiguredMintAuthorization::VaultDirectOnly => {
                warn!(
                    target: "rebalance",
                    %issuer_request_id,
                    %symbol,
                    "Transfer constructed VaultDirectOnly; minting without a \
                     vault-mode check (an orchestrator-mode asset would \
                     stall at polling -- such mints must run through the \
                     server, which always wires authorization)"
                );
                return Ok(());
            }
        };

        // Fails closed: an indeterminate mode must not be guessed as
        // vault-direct, or a required authorization would be skipped and
        // the mint would stall at issuance's on-chain step.
        match wiring.vault_mode_reader.vault_mode(symbol).await? {
            VaultModeTag::VaultDirect => Ok(()),
            VaultModeTag::Orchestrator => {
                let token = wiring.token_addresses.get(symbol).copied().ok_or_else(|| {
                    MintError::UnknownTokenizedEquity {
                        symbol: symbol.clone(),
                    }
                })?;

                self.mint_store
                    .send(
                        issuer_request_id,
                        TokenizedEquityMintCommand::SignMintAuthorization { token },
                    )
                    .await?;

                // Bounds delivery to ONE live chain per mint: redrive
                // successors carry no idempotency key, so without this
                // check a resume during a delayed-redrive window would
                // start a second chain. Fails open on a guard read error
                // (warn + push): a duplicate chain delivers byte-identical
                // payloads and is benign, while a skipped push would stall
                // the delivery.
                let delivery_live = authorization_job::has_live_delivery_job(
                    &wiring.delivery_queue,
                    issuer_request_id,
                )
                .await
                .unwrap_or_else(|error| {
                    warn!(
                        target: "rebalance",
                        %issuer_request_id,
                        ?error,
                        "Live-delivery guard read failed; enqueueing anyway \
                         (duplicate chains are benign)"
                    );
                    false
                });
                if delivery_live {
                    info!(
                        target: "rebalance",
                        %issuer_request_id,
                        %symbol,
                        "Mint authorization delivery already live; not \
                         enqueueing another"
                    );
                    return Ok(());
                }

                let mut delivery_queue = wiring.delivery_queue.clone();
                delivery_queue
                    .push_idempotent(
                        &issuer_request_id.to_string(),
                        DeliverMintAuthorization {
                            issuer_request_id: issuer_request_id.clone(),
                            redrive_attempts: 0,
                        },
                    )
                    .await?;

                info!(
                    target: "rebalance",
                    %issuer_request_id,
                    %symbol,
                    "Mint authorization signed and delivery enqueued"
                );
                Ok(())
            }
        }
    }

    /// Enqueues bot-gas cost recording for a confirmed mint-side tx (vault
    /// deposit or wrap, always on Base). Converts a push failure into
    /// `MintError::BotGasEnqueue` carrying a `BotGasEnqueueFailure` -- built
    /// here (not via `#[from]`) so the tx hash the failed enqueue was for is
    /// captured while it's still in scope; `QueuePushError` alone can't
    /// carry it back out through `MintError`.
    async fn enqueue_bot_gas_cost(
        &self,
        tx_hash: TxHash,
        category: BotGasOperationCategory,
        symbol: Symbol,
    ) -> Result<(), MintError> {
        self.bot_gas_enqueuer
            .enqueue(RecordBotGasReceiptCost::for_base_tx(
                tx_hash, category, symbol,
            ))
            .await
            .map_err(|error| {
                MintError::BotGasEnqueue(BotGasEnqueueFailure::from_queue_push_error(
                    tx_hash, &error,
                ))
            })
    }

    /// Loads the aggregate after Poll and extracts fields from the
    /// TokensReceived state needed for verification and wrapping.
    async fn load_tokens_received(
        &self,
        issuer_request_id: &IssuerRequestId,
    ) -> Result<TokensReceivedData, MintError> {
        let entity = self
            .mint_store
            .load(issuer_request_id)
            .await?
            .ok_or_else(|| MintError::EntityNotFound {
                issuer_request_id: issuer_request_id.clone(),
                expected_state: "TokensReceived",
            })?;

        match entity {
            TokenizedEquityMint::TokensReceived {
                shares_minted,
                tx_hash,
                symbol,
                wallet,
                ..
            } => Ok(TokensReceivedData {
                shares_minted,
                tx_hash,
                symbol,
                wallet,
            }),
            other => Err(MintError::UnexpectedState {
                issuer_request_id: issuer_request_id.clone(),
                expected_state: "TokensReceived",
                entity: Box::new(other),
            }),
        }
    }

    async fn load_mint_entity(
        &self,
        issuer_request_id: &IssuerRequestId,
    ) -> Result<TokenizedEquityMint, MintError> {
        self.mint_store
            .load(issuer_request_id)
            .await?
            .ok_or_else(|| MintError::EntityNotFound {
                issuer_request_id: issuer_request_id.clone(),
                expected_state: "active mint state",
            })
    }

    async fn finalize_received_mint(
        &self,
        issuer_request_id: &IssuerRequestId,
        tokens_received: TokensReceivedData,
    ) -> Result<(), MintError> {
        self.verify_received_mint(&tokens_received).await?;

        let WrappedMintResult {
            token,
            shares,
            block,
        } = self
            .wrap_received_mint(issuer_request_id, &tokens_received)
            .await?;

        // Wait for the RPC node to catch up to the block where the wrap tx
        // confirmed before depositing. Without this, a load-balanced backend
        // that hasn't indexed the wrap block yet sees the wrapped-token balance
        // as zero and the vault deposit reverts with ERC20InsufficientBalance.
        self.wrapper.wait_for_block(block).await?;

        self.deposit_wrapped_mint(issuer_request_id, &tokens_received.symbol, token, shares)
            .await
    }

    async fn deposit_wrapped_mint(
        &self,
        issuer_request_id: &IssuerRequestId,
        symbol: &Symbol,
        wrapped_token: Address,
        wrapped_shares: U256,
    ) -> Result<(), MintError> {
        let vault_id = self.vault_lookup.vault_id_for_token(wrapped_token).await?;

        let vault_deposit_tx_hash = self
            .raindex
            .submit_deposit(
                wrapped_token,
                vault_id,
                wrapped_shares,
                TOKENIZED_EQUITY_DECIMALS,
            )
            .await?;

        self.mint_store
            .send(
                issuer_request_id,
                TokenizedEquityMintCommand::SubmitVaultDeposit {
                    vault_deposit_tx_hash,
                },
            )
            .await?;

        self.raindex.confirm_tx(vault_deposit_tx_hash).await?;
        self.enqueue_bot_gas_cost(
            vault_deposit_tx_hash,
            BotGasOperationCategory::VaultDeposit,
            symbol.clone(),
        )
        .await?;

        self.mint_store
            .send(
                issuer_request_id,
                TokenizedEquityMintCommand::DepositToVault {
                    vault_deposit_tx_hash,
                },
            )
            .await?;

        info!(target: "rebalance", %symbol, %vault_deposit_tx_hash, "Mint workflow completed");
        Ok(())
    }

    /// Attempts vault deposit; if it reverts with `ERC20InsufficientBalance`,
    /// the deposit already landed in a previous session -- advance the
    /// aggregate to terminal state. Used by both the `TokensWrapped` and
    /// `WrapSubmitted` resume paths.
    ///
    /// # Invariant: tokens only leave the wallet via vault deposit
    ///
    /// This recovery assumes that the *only* way wrapped tokens leave the
    /// wallet is through a successful Raindex vault deposit. Under this
    /// invariant, a zero balance at retry time proves the deposit landed
    /// in a prior session that crashed before persisting the CQRS event.
    ///
    /// If this invariant is violated (e.g. manual token transfer, bug in
    /// another code path), the recovery would incorrectly mark the mint
    /// complete while tokens are lost. The invariant holds today because
    /// the wrap -> deposit lifecycle is the sole consumer of these tokens
    /// and runs atomically within a single task.
    async fn try_deposit_or_recover(
        &self,
        issuer_request_id: &IssuerRequestId,
        symbol: &Symbol,
        wrapped_token: Address,
        wrapped_shares: U256,
    ) -> Result<(), MintError> {
        match self
            .deposit_wrapped_mint(issuer_request_id, symbol, wrapped_token, wrapped_shares)
            .await
        {
            Ok(()) => Ok(()),
            Err(error) if error.is_insufficient_balance_revert() => {
                warn!(
                    target: "rebalance",
                    %issuer_request_id,
                    %symbol,
                    ?error,
                    "Vault deposit reverted on resume -- deposit likely \
                    already landed in a previous session; closing operation"
                );

                // The real deposit tx hash is unknown (see the TxHash::ZERO
                // comment below), so its bot-gas cost can never be enqueued
                // here -- there is no tx hash to enqueue a
                // RecordBotGasReceiptCost job against. This is a real,
                // narrow financial-data gap (a confirmed bot-signed tx whose
                // gas cost will never be recorded), so it is logged loudly
                // rather than silently skipped -- see AGENTS.md's "CRITICAL:
                // Financial Data Integrity" and SPEC.md's bot-gas "Known
                // gaps".
                warn!(
                    target: "rebalance",
                    %issuer_request_id,
                    %symbol,
                    "Bot-gas receipt cost: the real vault-deposit tx hash for this \
                     crash-recovered mint is unknown (TxHash::ZERO sentinel); its gas \
                     cost cannot be recorded and this cost fact is permanently lost"
                );

                // TxHash::ZERO signals that the real deposit TX hash is
                // unknown -- the deposit succeeded in a previous session
                // that crashed before persisting the CQRS event.
                self.mint_store
                    .send(
                        issuer_request_id,
                        TokenizedEquityMintCommand::DepositToVault {
                            vault_deposit_tx_hash: TxHash::ZERO,
                        },
                    )
                    .await?;

                Ok(())
            }
            Err(error) => Err(error),
        }
    }

    async fn verify_received_mint(
        &self,
        tokens_received: &TokensReceivedData,
    ) -> Result<(), MintError> {
        info!(target: "rebalance",
            shares_minted = %tokens_received.shares_minted,
            tx_hash = %tokens_received.tx_hash,
            "Tokens received, verifying onchain"
        );

        let unwrapped_token = self.wrapper.lookup_underlying(&tokens_received.symbol)?;
        self.tokenizer
            .verify_mint_tx(
                tokens_received.tx_hash,
                unwrapped_token,
                tokens_received.wallet,
                tokens_received.shares_minted,
            )
            .await
            .inspect_err(|error| {
                warn!(target: "rebalance", %error, "Onchain mint verification failed");
            })?;

        Ok(())
    }

    async fn wrap_received_mint(
        &self,
        issuer_request_id: &IssuerRequestId,
        tokens_received: &TokensReceivedData,
    ) -> Result<WrappedMintResult, MintError> {
        info!(target: "rebalance", "Onchain verification passed, wrapping into ERC-4626 shares");

        let token = self.wrapper.lookup_derivative(&tokens_received.symbol)?;

        let wrap_tx_hash = self
            .wrapper
            .submit_wrap(token, tokens_received.shares_minted, self.wallet)
            .await?;

        self.mint_store
            .send(
                issuer_request_id,
                TokenizedEquityMintCommand::SubmitWrap { wrap_tx_hash },
            )
            .await?;

        let WrapConfirmation { shares, block } =
            self.wrapper.confirm_wrap(token, wrap_tx_hash).await?;
        self.enqueue_bot_gas_cost(
            wrap_tx_hash,
            BotGasOperationCategory::Wrap,
            tokens_received.symbol.clone(),
        )
        .await?;

        self.mint_store
            .send(
                issuer_request_id,
                TokenizedEquityMintCommand::WrapTokens {
                    wrap_tx_hash,
                    wrapped_shares: shares,
                    wrap_block: block,
                },
            )
            .await?;

        info!(target: "rebalance", %wrap_tx_hash, %shares, "Tokens wrapped, depositing to Raindex vault");
        Ok(WrappedMintResult {
            token,
            shares,
            block,
        })
    }

    #[allow(clippy::cognitive_complexity)]
    pub(crate) async fn resume_mint(
        &self,
        issuer_request_id: &IssuerRequestId,
    ) -> Result<(), MintError> {
        loop {
            match self.load_mint_entity(issuer_request_id).await? {
                TokenizedEquityMint::MintAccepted { symbol, .. } => {
                    info!(%issuer_request_id, "Resuming accepted mint");
                    // Idempotent: signing no-ops once an authorization
                    // exists and the delivery enqueue is keyed on the
                    // issuer request id, so a resumed orchestrator-mode
                    // mint re-delivers its PERSISTED authorization rather
                    // than minting a fresh nonce.
                    self.ensure_mint_authorization(issuer_request_id, &symbol)
                        .await?;
                    self.mint_store
                        .send(issuer_request_id, TokenizedEquityMintCommand::Poll)
                        .await?;
                }
                TokenizedEquityMint::TokensReceived { .. } => {
                    info!(%issuer_request_id, "Resuming received mint");
                    let tokens_received = self.load_tokens_received(issuer_request_id).await?;
                    return self
                        .finalize_received_mint(issuer_request_id, tokens_received)
                        .await;
                }
                TokenizedEquityMint::WrapSubmitted {
                    wrap_tx_hash,
                    symbol,
                    ..
                } => {
                    info!(%issuer_request_id, %wrap_tx_hash, "Resuming submitted wrap");
                    let wrapped_token = self.wrapper.lookup_derivative(&symbol)?;
                    let WrapConfirmation {
                        shares: wrapped_shares,
                        block: wrap_block,
                    } = self
                        .wrapper
                        .confirm_wrap(wrapped_token, wrap_tx_hash)
                        .await?;
                    self.enqueue_bot_gas_cost(
                        wrap_tx_hash,
                        BotGasOperationCategory::Wrap,
                        symbol.clone(),
                    )
                    .await?;

                    self.mint_store
                        .send(
                            issuer_request_id,
                            TokenizedEquityMintCommand::WrapTokens {
                                wrap_tx_hash,
                                wrapped_shares,
                                wrap_block,
                            },
                        )
                        .await?;

                    self.wrapper.wait_for_block(wrap_block).await?;

                    info!(target: "rebalance", %wrap_tx_hash, %wrapped_shares, "Wrap confirmed on resume, depositing to Raindex vault");
                    return self
                        .try_deposit_or_recover(
                            issuer_request_id,
                            &symbol,
                            wrapped_token,
                            wrapped_shares,
                        )
                        .await;
                }
                TokenizedEquityMint::TokensWrapped {
                    symbol,
                    wrapped_shares,
                    wrap_block,
                    ..
                } => {
                    info!(%issuer_request_id, "Resuming wrapped mint");
                    let wrapped_token = self.wrapper.lookup_derivative(&symbol)?;

                    // Skip the wait for legacy aggregates persisted before wrap_block was added.
                    if let Some(block) = wrap_block {
                        self.wrapper.wait_for_block(block).await?;
                    }

                    return self
                        .try_deposit_or_recover(
                            issuer_request_id,
                            &symbol,
                            wrapped_token,
                            wrapped_shares,
                        )
                        .await;
                }
                TokenizedEquityMint::VaultDepositSubmitted {
                    vault_deposit_tx_hash,
                    symbol,
                    ..
                } => {
                    info!(%issuer_request_id, %vault_deposit_tx_hash, "Resuming submitted vault deposit");
                    self.raindex.confirm_tx(vault_deposit_tx_hash).await?;
                    self.enqueue_bot_gas_cost(
                        vault_deposit_tx_hash,
                        BotGasOperationCategory::VaultDeposit,
                        symbol.clone(),
                    )
                    .await?;

                    self.mint_store
                        .send(
                            issuer_request_id,
                            TokenizedEquityMintCommand::DepositToVault {
                                vault_deposit_tx_hash,
                            },
                        )
                        .await?;

                    info!(target: "rebalance", %symbol, %vault_deposit_tx_hash, "Vault deposit confirmed on resume");
                    return Ok(());
                }
                TokenizedEquityMint::DepositedIntoRaindex { .. }
                | TokenizedEquityMint::Failed { .. }
                | TokenizedEquityMint::Reconciled { .. } => return Ok(()),
                TokenizedEquityMint::MintRequested { .. } => {
                    info!(%issuer_request_id, "Reconciling requested mint");
                    self.mint_store
                        .send(
                            issuer_request_id,
                            TokenizedEquityMintCommand::ReconcileMintRequest {
                                issuer_request_id: issuer_request_id.clone(),
                            },
                        )
                        .await?;
                }
            }
        }
    }

    /// Sends the Redeem command to submit vault withdrawal, then
    /// ConfirmWithdraw to wait for confirmation.
    async fn withdraw_from_raindex(
        &self,
        aggregate_id: &RedemptionAggregateId,
        symbol: &Symbol,
        quantity: FractionalShares,
        token: Address,
        amount: U256,
    ) -> Result<(), RedemptionError> {
        self.redemption_store
            .send(
                aggregate_id,
                EquityRedemptionCommand::Redeem {
                    symbol: symbol.clone(),
                    quantity: quantity.inner(),
                    token,
                    amount,
                },
            )
            .await?;

        self.redemption_store
            .send(aggregate_id, EquityRedemptionCommand::SubmitWithdraw)
            .await?;

        self.redemption_store
            .send(aggregate_id, EquityRedemptionCommand::ConfirmWithdraw)
            .await?;

        Ok(())
    }

    /// Unwraps ERC-4626 tokens and sends to Alpaca, returning the
    /// redemption tx hash.
    async fn unwrap_and_send(
        &self,
        aggregate_id: &RedemptionAggregateId,
    ) -> Result<TxHash, RedemptionError> {
        self.redemption_store
            .send(aggregate_id, EquityRedemptionCommand::UnwrapTokens)
            .await?;

        self.redemption_store
            .send(aggregate_id, EquityRedemptionCommand::SubmitUnwrap)
            .await?;

        self.redemption_store
            .send(aggregate_id, EquityRedemptionCommand::ConfirmUnwrap)
            .await?;

        info!(target: "rebalance", %aggregate_id, "Tokens unwrapped, sending to Alpaca");

        self.redemption_store
            .send(aggregate_id, EquityRedemptionCommand::PrepareSend)
            .await?;

        self.redemption_store
            .send(aggregate_id, EquityRedemptionCommand::SendTokens)
            .await?;

        let entity = self.redemption_store.load(aggregate_id).await?.ok_or(
            RedemptionError::EntityNotFound {
                aggregate_id: aggregate_id.clone(),
            },
        )?;

        match entity {
            EquityRedemption::TokensSent { redemption_tx, .. } => Ok(redemption_tx),
            entity @ EquityRedemption::Failed { .. } => Err(RedemptionError::SendFailed { entity }),
            entity => Err(RedemptionError::UnexpectedEntity { entity }),
        }
    }

    /// Polls for redemption detection and records it.
    async fn poll_detection(
        &self,
        aggregate_id: &RedemptionAggregateId,
        tx_hash: &TxHash,
    ) -> Result<TokenizationRequestId, RedemptionError> {
        let detected = match self.tokenizer.poll_for_redemption(tx_hash).await {
            Ok(req) => req,
            Err(error) => {
                warn!(target: "rebalance", %error, %tx_hash, "Polling for redemption detection failed");
                let failure = match &error {
                    TokenizerError::Alpaca(AlpacaTokenizationError::PollTimeout { .. }) => {
                        DetectionFailure::Timeout
                    }
                    TokenizerError::Alpaca(other) => DetectionFailure::ApiError {
                        status_code: other.status_code().map(|status| status.as_u16()),
                    },
                    TokenizerError::MintVerification(verification_error) => {
                        warn!(target: "rebalance",
                            %verification_error,
                            %tx_hash,
                            "Unexpected MintVerification error during redemption detection"
                        );
                        DetectionFailure::ApiError { status_code: None }
                    }
                };

                self.redemption_store
                    .send(
                        aggregate_id,
                        EquityRedemptionCommand::FailDetection { failure },
                    )
                    .await?;

                return Err(error.into());
            }
        };

        self.redemption_store
            .send(
                aggregate_id,
                EquityRedemptionCommand::Detect {
                    tokenization_request_id: detected.id.clone(),
                },
            )
            .await?;

        Ok(detected.id)
    }

    /// Polls for completion and finalizes the redemption.
    async fn poll_completion(
        &self,
        aggregate_id: &RedemptionAggregateId,
        request_id: &TokenizationRequestId,
    ) -> Result<(), RedemptionError> {
        let completed = match self
            .tokenizer
            .poll_redemption_until_complete(request_id)
            .await
        {
            Ok(req) => req,
            Err(error) => {
                warn!(target: "rebalance", %error, %request_id, "Polling for completion failed");
                self.redemption_store
                    .send(
                        aggregate_id,
                        EquityRedemptionCommand::RejectRedemption {
                            reason: error.to_string(),
                        },
                    )
                    .await?;
                return Err(error.into());
            }
        };

        match completed.status {
            TokenizationRequestStatus::Completed => {
                self.redemption_store
                    .send(aggregate_id, EquityRedemptionCommand::Complete)
                    .await?;
                Ok(())
            }
            TokenizationRequestStatus::Rejected => {
                self.redemption_store
                    .send(
                        aggregate_id,
                        EquityRedemptionCommand::RejectRedemption {
                            reason: "Alpaca rejected the redemption request".to_string(),
                        },
                    )
                    .await?;
                Err(RedemptionError::Rejected)
            }
            TokenizationRequestStatus::Pending => {
                warn!(target: "rebalance", %request_id, "poll_redemption_until_complete returned Pending status");
                Err(RedemptionError::UnexpectedPendingStatus)
            }
        }
    }

    async fn load_redemption_entity(
        &self,
        aggregate_id: &RedemptionAggregateId,
    ) -> Result<EquityRedemption, RedemptionError> {
        self.redemption_store
            .load(aggregate_id)
            .await?
            .ok_or_else(|| RedemptionError::EntityNotFound {
                aggregate_id: aggregate_id.clone(),
            })
    }

    #[allow(clippy::cognitive_complexity)]
    pub(crate) async fn resume_redemption(
        &self,
        aggregate_id: &RedemptionAggregateId,
    ) -> Result<(), RedemptionError> {
        loop {
            match self.load_redemption_entity(aggregate_id).await? {
                EquityRedemption::VaultWithdrawPending { .. } => {
                    info!(%aggregate_id, "Resuming pending vault withdrawal");
                    self.redemption_store
                        .send(aggregate_id, EquityRedemptionCommand::SubmitWithdraw)
                        .await?;
                }
                EquityRedemption::VaultWithdrawSubmitted { .. } => {
                    info!(%aggregate_id, "Resuming submitted vault withdrawal");
                    self.redemption_store
                        .send(aggregate_id, EquityRedemptionCommand::ConfirmWithdraw)
                        .await?;
                }
                EquityRedemption::WithdrawnFromRaindex { .. } => {
                    self.resume_withdrawn_redemption(aggregate_id).await?;
                }
                EquityRedemption::UnwrapPending { .. } => {
                    info!(%aggregate_id, "Resuming pending unwrap");
                    self.redemption_store
                        .send(aggregate_id, EquityRedemptionCommand::SubmitUnwrap)
                        .await?;
                }
                EquityRedemption::UnwrapSubmitted { .. } => {
                    info!(%aggregate_id, "Resuming submitted unwrap");
                    self.redemption_store
                        .send(aggregate_id, EquityRedemptionCommand::ConfirmUnwrap)
                        .await?;
                }
                EquityRedemption::TokensUnwrapped { .. } => {
                    self.resume_unwrapped_redemption(aggregate_id).await?;
                }
                EquityRedemption::SendPending { .. } => {
                    info!(%aggregate_id, "Resuming pending send");
                    self.redemption_store
                        .send(aggregate_id, EquityRedemptionCommand::SendTokens)
                        .await?;
                }
                EquityRedemption::TokensSent { redemption_tx, .. } => {
                    self.resume_sent_redemption(aggregate_id, &redemption_tx)
                        .await?;
                }
                EquityRedemption::Pending {
                    tokenization_request_id,
                    ..
                } => {
                    return self
                        .resume_pending_redemption(aggregate_id, &tokenization_request_id)
                        .await;
                }
                EquityRedemption::Completed { .. }
                | EquityRedemption::Failed { .. }
                | EquityRedemption::Reconciled { .. } => {
                    return Ok(());
                }
            }
        }
    }

    async fn resume_withdrawn_redemption(
        &self,
        aggregate_id: &RedemptionAggregateId,
    ) -> Result<(), RedemptionError> {
        info!(%aggregate_id, "Resuming withdrawn redemption");
        self.redemption_store
            .send(aggregate_id, EquityRedemptionCommand::UnwrapTokens)
            .await?;
        Ok(())
    }

    async fn resume_unwrapped_redemption(
        &self,
        aggregate_id: &RedemptionAggregateId,
    ) -> Result<(), RedemptionError> {
        info!(%aggregate_id, "Resuming unwrapped redemption");
        self.redemption_store
            .send(aggregate_id, EquityRedemptionCommand::PrepareSend)
            .await?;
        Ok(())
    }

    async fn resume_sent_redemption(
        &self,
        aggregate_id: &RedemptionAggregateId,
        redemption_tx: &TxHash,
    ) -> Result<(), RedemptionError> {
        info!(%aggregate_id, "Resuming sent redemption");
        match self.poll_detection(aggregate_id, redemption_tx).await {
            Ok(_) => Ok(()),
            Err(error) => {
                self.ignore_redemption_error_if_terminal(aggregate_id, error)
                    .await
            }
        }
    }

    async fn resume_pending_redemption(
        &self,
        aggregate_id: &RedemptionAggregateId,
        tokenization_request_id: &TokenizationRequestId,
    ) -> Result<(), RedemptionError> {
        info!(%aggregate_id, "Resuming detected redemption");
        match self
            .poll_completion(aggregate_id, tokenization_request_id)
            .await
        {
            Ok(()) => Ok(()),
            Err(error) => {
                self.ignore_redemption_error_if_terminal(aggregate_id, error)
                    .await
            }
        }
    }

    async fn ignore_redemption_error_if_terminal(
        &self,
        aggregate_id: &RedemptionAggregateId,
        error: RedemptionError,
    ) -> Result<(), RedemptionError> {
        if self.redemption_is_terminal(aggregate_id).await? {
            Ok(())
        } else {
            Err(error)
        }
    }

    async fn redemption_is_terminal(
        &self,
        aggregate_id: &RedemptionAggregateId,
    ) -> Result<bool, RedemptionError> {
        Ok(matches!(
            self.load_redemption_entity(aggregate_id).await?,
            EquityRedemption::Completed { .. }
                | EquityRedemption::Failed { .. }
                | EquityRedemption::Reconciled { .. }
        ))
    }

    /// Re-checks a stuck mint against the tokenization provider and, if the
    /// provider has settled it, un-fails the aggregate and resumes the
    /// wrap/deposit workflow. Active (non-failed) mints simply resume.
    ///
    /// Dispatches `RecoverProviderCompletion` through the reactor-wired mint
    /// store after rebuilding tracking, so the live inventory view is
    /// corrected. Must run inside the bot process for the reactor to fire.
    pub(crate) async fn recover_mint(
        &self,
        issuer_request_id: &IssuerRequestId,
        pool: &SqlitePool,
        rebalancing: &RebalancingService,
    ) -> Result<RecheckOutcome, RecheckError> {
        let entity = self.load_mint_entity(issuer_request_id).await?;

        let (symbol, quantity) = match &entity {
            // Both terminals are already settled: nothing to recheck.
            TokenizedEquityMint::DepositedIntoRaindex { .. }
            | TokenizedEquityMint::Reconciled { .. } => {
                return Ok(RecheckOutcome::AlreadyCompleted);
            }
            TokenizedEquityMint::Failed {
                symbol, quantity, ..
            } => (symbol.clone(), FractionalShares::new(*quantity)),
            _ => {
                self.resume_mint(issuer_request_id).await?;
                return Ok(RecheckOutcome::Resumed);
            }
        };

        let context = load_mint_recheck_context(pool, issuer_request_id).await?;

        // Provider-completion recovery only applies to mints that failed at
        // acceptance (tokens never received). A mint that already received
        // tokens and then failed while wrapping/depositing must not be reset
        // to TokensReceived and re-wrapped against tokens that already moved.
        if context.received_tokens {
            warn!(
                target: "rebalance",
                %issuer_request_id,
                "Mint failed after receiving tokens; provider-completion recovery does not apply"
            );
            return Ok(RecheckOutcome::NotRecoverable);
        }

        let request = match self
            .tokenizer
            .get_request(&context.tokenization_request_id)
            .await
        {
            Ok(request) => request,
            // A missing provider request is "left unchanged", not a hard error:
            // the request may not exist or may not be visible yet. Other tokenizer
            // failures (transient, verification) still propagate.
            Err(TokenizerError::Alpaca(AlpacaTokenizationError::RequestNotFound { id })) => {
                warn!(
                    target: "rebalance",
                    %issuer_request_id,
                    %id,
                    "Provider request not found; leaving mint unchanged"
                );
                return Ok(RecheckOutcome::LeftUnchanged);
            }
            Err(error) => return Err(error.into()),
        };

        if request.status != TokenizationRequestStatus::Completed {
            return Ok(RecheckOutcome::LeftUnchanged);
        }

        let tx_hash = request
            .tx_hash
            .ok_or_else(|| RecheckError::MissingTxHash(request.id.clone()))?;

        // Rebuild tracking (and restore the canonical in-flight inventory shape,
        // clearing any timeout tombstone) before dispatching so the reactor
        // applies the ProviderCompletionRecovered inventory effect when it
        // processes the event below.
        //
        // The rollback below covers a *persistence* failure (store.send returns
        // Err -> the event was never committed and the reactor never ran). It
        // does NOT cover a reactor-application failure: cqrs-es dispatches the
        // reactor after persisting, and the bridge logs reactor errors without
        // propagating them, so send still returns Ok. A reactor failure here
        // would leave the event persisted but inventory un-completed until the
        // next inventory poll/restart reconciles it.
        // Compare-and-claim: refuse recovery while a *different* mint for this
        // symbol is live (rebuilding would overwrite its in-flight balance, since
        // `set_inflight` replaces rather than adds). A slot still owned by this
        // same mint -- e.g. the live process never observed its failure, as with
        // a reactor-less failure injection -- is not a conflict; that is exactly
        // the stale state recovery reconciles. The check is atomic with the
        // in-flight restore so a concurrent mint cannot claim the slot in between.
        let rollback = match rebalancing
            .rebuild_mint_tracking_for_recovery(issuer_request_id, &entity, request.id.clone())
            .await?
        {
            RecoveryClaim::Conflict => return Ok(RecheckOutcome::Conflict),
            RecoveryClaim::Claimed(rollback) => rollback,
        };

        if let Err(error) = self
            .mint_store
            .send(
                issuer_request_id,
                TokenizedEquityMintCommand::RecoverProviderCompletion {
                    issuer_request_id: issuer_request_id.clone(),
                    wallet: context.wallet,
                    tokenization_request_id: request.id,
                    tx_hash,
                    fees: request.fees,
                },
            )
            .await
        {
            // The recovery event was not persisted, so the reactor never ran to
            // complete the in-flight that the rebuild restored. Undo the rebuild
            // so the live inventory does not show a phantom in-flight transfer or
            // a symbol locked in-progress until the next restart.
            if let Err(rollback_error) = rebalancing
                .rollback_mint_tracking_for_recovery(issuer_request_id, &symbol, quantity, rollback)
                .await
            {
                error!(
                    target: "rebalance",
                    %issuer_request_id,
                    ?rollback_error,
                    "Failed to roll back mint recovery state after dispatch failure"
                );
            }

            return Err(MintError::from(error).into());
        }

        // The recovery event is committed and the reactor has corrected
        // inventory. A resume failure now (e.g. a transient RPC error during
        // wrapping) must not be fatal: the aggregate is recovered and startup
        // recovery will finish the workflow. Clear the in-progress guard so the
        // symbol is not locked out of rebalancing until the next restart.
        if let Err(error) = self.resume_mint(issuer_request_id).await {
            warn!(
                target: "rebalance",
                %issuer_request_id,
                ?error,
                "Mint recovered but resume failed; cleared in-progress guard, workflow resumes on next startup"
            );
            rebalancing
                .abandon_mint_recovery_guard(issuer_request_id, &symbol)
                .await;
        }

        Ok(RecheckOutcome::Recovered)
    }

    /// Re-checks a stuck redemption against the tokenization provider and, if
    /// the provider has settled it, un-fails the aggregate to `Completed`.
    /// Active (non-failed) redemptions simply resume.
    ///
    /// Dispatches `RecoverProviderCompletion` through the reactor-wired
    /// redemption store after rebuilding tracking, so the in-flight transfer
    /// is completed in the live inventory view.
    pub(crate) async fn recover_redemption(
        &self,
        aggregate_id: &RedemptionAggregateId,
        rebalancing: &RebalancingService,
    ) -> Result<RecheckOutcome, RecheckError> {
        let entity = self.load_redemption_entity(aggregate_id).await?;

        let (symbol, tokenization_request_id, redemption_tx) = match &entity {
            // Both terminals are already settled: nothing to recheck.
            EquityRedemption::Completed { .. } | EquityRedemption::Reconciled { .. } => {
                return Ok(RecheckOutcome::AlreadyCompleted);
            }
            EquityRedemption::Failed {
                symbol,
                redemption_tx: Some(redemption_tx),
                tokenization_request_id,
                ..
            } => (
                symbol.clone(),
                tokenization_request_id.clone(),
                *redemption_tx,
            ),
            // A redemption that failed before sending tokens has nothing for
            // the provider to have settled.
            EquityRedemption::Failed { .. } => return Ok(RecheckOutcome::NotRecoverable),
            _ => {
                self.resume_redemption(aggregate_id).await?;
                return Ok(RecheckOutcome::Resumed);
            }
        };

        let request = match &tokenization_request_id {
            // A missing provider request leaves the redemption unchanged rather
            // than failing the operator command; transient/other errors propagate.
            Some(request_id) => match self.tokenizer.get_request(request_id).await {
                Ok(request) => request,
                Err(TokenizerError::Alpaca(AlpacaTokenizationError::RequestNotFound { id })) => {
                    warn!(
                        target: "rebalance",
                        %aggregate_id,
                        %id,
                        "Provider request not found; leaving redemption unchanged"
                    );
                    return Ok(RecheckOutcome::LeftUnchanged);
                }
                Err(error) => return Err(error.into()),
            },
            None => match self.tokenizer.find_redemption_by_tx(&redemption_tx).await? {
                Some(request) => request,
                None => return Ok(RecheckOutcome::NotDetectedYet),
            },
        };

        if request.status != TokenizationRequestStatus::Completed {
            return Ok(RecheckOutcome::LeftUnchanged);
        }

        // Compare-and-claim: refuse recovery while a *different* redemption for
        // this symbol is live (see recover_mint for the rationale). A slot still
        // owned by this same redemption is not a conflict. The check is atomic
        // with the in-flight restore.
        let rollback = match rebalancing
            .rebuild_redemption_tracking_for_recovery(aggregate_id, &entity)
            .await?
        {
            RecoveryClaim::Conflict => return Ok(RecheckOutcome::Conflict),
            RecoveryClaim::Claimed(rollback) => rollback,
        };

        if let Err(error) = self
            .redemption_store
            .send(
                aggregate_id,
                EquityRedemptionCommand::RecoverProviderCompletion {
                    tokenization_request_id: request.id,
                },
            )
            .await
        {
            // The recovery event was not persisted, so the reactor never ran to
            // complete the in-flight that the rebuild restored. Undo the rebuild
            // so the live inventory does not show a phantom in-flight transfer or
            // a symbol locked in-progress until the next restart.
            if let Err(rollback_error) = rebalancing
                .rollback_redemption_tracking_for_recovery(aggregate_id, &symbol, rollback)
                .await
            {
                error!(
                    target: "rebalance",
                    %aggregate_id,
                    ?rollback_error,
                    "Failed to roll back redemption recovery state after dispatch failure"
                );
            }

            return Err(RedemptionError::from(error).into());
        }

        Ok(RecheckOutcome::Recovered)
    }
}

impl CrossVenueEquityTransfer {
    /// Hedging -> Market-Making: drives the mint lifecycle (tokenize equity
    /// on Alpaca, wrap, deposit into the Raindex vault) for the given
    /// `issuer_request_id`, whether fresh or interrupted.
    ///
    /// The id is chosen by the caller at enqueue time so apalis retries (and
    /// bot restarts that re-pick the job row) re-enter the same aggregate: an
    /// absent aggregate starts a new mint, an in-flight one resumes from its
    /// persisted state, and a terminal one is a no-op.
    #[instrument(target = "rebalance", skip_all, fields(%issuer_request_id, %symbol, %quantity))]
    pub async fn resume_equity_to_market_making(
        &self,
        issuer_request_id: &IssuerRequestId,
        symbol: &Symbol,
        quantity: FractionalShares,
    ) -> Result<(), MintTransferError> {
        let existing = self
            .mint_store
            .load(issuer_request_id)
            .await
            .map_err(|error| MintTransferError::PreReceipt(error.into()))?;

        match existing {
            None => self.start_mint(issuer_request_id, symbol, quantity).await,
            Some(
                TokenizedEquityMint::MintRequested { .. }
                | TokenizedEquityMint::MintAccepted { .. },
            ) => {
                if let Err(error) = self.resume_mint(issuer_request_id).await {
                    let reached = self.load_mint_entity(issuer_request_id).await;
                    return Err(classify_mint_resume_error(reached, error));
                }

                Ok(())
            }
            Some(_) => self
                .resume_mint(issuer_request_id)
                .await
                .map_err(MintTransferError::PostReceipt),
        }
    }

    async fn start_mint(
        &self,
        issuer_request_id: &IssuerRequestId,
        symbol: &Symbol,
        quantity: FractionalShares,
    ) -> Result<(), MintTransferError> {
        self.gas_readiness
            .ensure_ready(TransferGasRoute::Equity)
            .await
            .map_err(MintError::from)
            .map_err(MintTransferError::PreReceipt)?;

        debug!(target: "rebalance", %issuer_request_id, wallet = %self.wallet, "Requesting mint");

        // Pre-receipt: no tokens exist yet, safe to retry on failure.
        self.mint_store
            .send(
                issuer_request_id,
                TokenizedEquityMintCommand::RequestMint {
                    issuer_request_id: issuer_request_id.clone(),
                    symbol: symbol.clone(),
                    quantity: quantity.inner(),
                    wallet: self.wallet,
                },
            )
            .await
            .map_err(|error| MintTransferError::PreReceipt(error.into()))?;

        self.mint_store
            .send(
                issuer_request_id,
                TokenizedEquityMintCommand::SubmitMintRequest {
                    issuer_request_id: issuer_request_id.clone(),
                },
            )
            .await
            .map_err(|error| MintTransferError::PreReceipt(error.into()))?;

        let submitted = self
            .load_mint_entity(issuer_request_id)
            .await
            .map_err(MintTransferError::PreReceipt)?;
        match submitted {
            TokenizedEquityMint::Failed { .. } => return Ok(()),
            TokenizedEquityMint::MintAccepted { .. } => {}
            entity => {
                return Err(MintTransferError::PreReceipt(MintError::UnexpectedState {
                    issuer_request_id: issuer_request_id.clone(),
                    expected_state: "MintAccepted or Failed",
                    entity: Box::new(entity),
                }));
            }
        }

        // Orchestrator-mode assets need the recipient authorization signed
        // and on its way to issuance BEFORE polling: issuance will not mint
        // (and the poll cannot complete) until the authorization arrives.
        // Still pre-receipt -- a failure here leaves the mint resumable.
        self.ensure_mint_authorization(issuer_request_id, symbol)
            .await
            .map_err(MintTransferError::PreReceipt)?;

        info!(target: "rebalance", "Mint request accepted, polling for completion");

        self.mint_store
            .send(issuer_request_id, TokenizedEquityMintCommand::Poll)
            .await
            .map_err(|error| MintTransferError::PreReceipt(error.into()))?;

        // Post-receipt: tokens exist in wallet from this point on.
        let tokens_received = self
            .load_tokens_received(issuer_request_id)
            .await
            .map_err(MintTransferError::PostReceipt)?;

        self.finalize_received_mint(issuer_request_id, tokens_received)
            .await
            .map_err(MintTransferError::PostReceipt)
    }
}

impl CrossVenueEquityTransfer {
    /// Market-Making -> Hedging: drives the redemption lifecycle (withdraw
    /// tokenized equity from the Raindex vault, unwrap, send to Alpaca for
    /// redemption) for the given `aggregate_id`, whether fresh or
    /// interrupted.
    ///
    /// The id is chosen by the caller at enqueue time so apalis retries (and
    /// bot restarts that re-pick the job row) re-enter the same aggregate: an
    /// absent aggregate starts a new redemption, an in-flight one resumes
    /// from its persisted state, and a terminal one is a no-op.
    #[instrument(target = "rebalance", skip_all, fields(%aggregate_id, %symbol, %quantity))]
    pub async fn resume_equity_to_hedging(
        &self,
        aggregate_id: &RedemptionAggregateId,
        symbol: &Symbol,
        quantity: FractionalShares,
    ) -> Result<(), RedemptionError> {
        let existing = self.redemption_store.load(aggregate_id).await?;

        match existing {
            None => self.start_redemption(aggregate_id, symbol, quantity).await,
            Some(_) => self.resume_redemption(aggregate_id).await,
        }
    }

    async fn start_redemption(
        &self,
        aggregate_id: &RedemptionAggregateId,
        symbol: &Symbol,
        quantity: FractionalShares,
    ) -> Result<(), RedemptionError> {
        self.gas_readiness
            .ensure_ready(TransferGasRoute::Equity)
            .await?;

        let token = self.vault_lookup.vault_token_for_symbol(symbol).await?;
        let amount = quantity.to_u256_18_decimals()?;

        info!(target: "rebalance", %token, %amount, %aggregate_id, "Starting equity transfer to hedging venue");

        self.withdraw_from_raindex(aggregate_id, symbol, quantity, token, amount)
            .await?;

        info!(target: "rebalance", "Withdrawn from Raindex, unwrapping and sending to Alpaca");

        let redemption_tx = self.unwrap_and_send(aggregate_id).await?;

        info!(target: "rebalance", %redemption_tx, "Tokens sent, polling for detection");
        let request_id = self.poll_detection(aggregate_id, &redemption_tx).await?;

        info!(target: "rebalance", %request_id, "Redemption detected, awaiting completion");
        self.poll_completion(aggregate_id, &request_id).await?;

        info!(target: "rebalance", "Equity transfer to hedging venue completed successfully");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::inventory::PollFreshness;
    use alloy::primitives::{Address, B256, address};
    use chrono::Utc;
    use sqlx::SqlitePool;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::broadcast;

    use st0x_config::ChainAssets;
    use st0x_dto::Statement;
    use st0x_event_sorcery::{AggregateError, LifecycleError, StoreBuilder, test_store};
    use st0x_evm::Chain;
    use st0x_execution::{FractionalShares, Symbol};
    use st0x_float_macro::float;
    use st0x_tokenization::issuer_request_id;
    use st0x_tokenization::mock::{
        MockCompletionOutcome, MockDetectionOutcome, MockMintRequestOutcome, MockTokenizer,
        MockVerificationOutcome,
    };
    use st0x_tokenization::tokenization_request_id;
    use st0x_wrapper::MockWrapper;

    use super::*;
    use crate::bot_gas::{RecordBotGasReceiptCostJobQueue, pending_bot_gas_jobs};
    use crate::equity_redemption::{EquityRedemptionError, redemption_aggregate_id};
    use crate::inventory::{
        BroadcastingInventory, ImbalanceThreshold, Inventory, InventoryView, Venue,
    };
    use crate::mint_authorization::{MockMintAuthorizer, StubVaultModeReader};
    use crate::onchain::mock::{DepositBehavior, MockRaindex};
    use crate::rebalancing::{RebalancingSchedulers, RebalancingServiceConfig};
    use crate::tokenized_equity_mint::TokenizedEquityMintEvent;
    use crate::usdc_rebalance::UsdcRebalance;
    use crate::vault_lookup::MockVaultLookup;
    use crate::vault_registry::{VaultRegistry, VaultRegistryId};

    fn mock_vault_lookup() -> MockVaultLookup {
        MockVaultLookup::new()
            .with_symbol_token(Symbol::new("TEST").unwrap(), Address::ZERO)
            .with_vault(Address::ZERO, RaindexVaultId(B256::ZERO))
            .with_default_vault(RaindexVaultId(B256::ZERO))
    }

    fn mock_services() -> EquityTransferServices {
        EquityTransferServices {
            raindex: Arc::new(MockRaindex::new()),
            vault_lookup: Arc::new(mock_vault_lookup()),
            tokenizer: Arc::new(MockTokenizer::new()),
            wrapper: Arc::new(MockWrapper::new()),
            bot_gas_enqueuer: BotGasReceiptCostEnqueuer::Disabled,
            mint_authorizer: ConfiguredMintAuthorizer::Disabled,
        }
    }

    async fn submit_requested_mint(transfer: &CrossVenueEquityTransfer, id: &IssuerRequestId) {
        transfer
            .mint_store
            .send(
                id,
                TokenizedEquityMintCommand::SubmitMintRequest {
                    issuer_request_id: id.clone(),
                },
            )
            .await
            .unwrap();
    }

    async fn insert_mint_event(
        pool: &SqlitePool,
        id: &IssuerRequestId,
        sequence: i64,
        event_type: &str,
        payload: &str,
    ) {
        let IssuerRequestId(raw) = id;
        sqlx::query(
            "INSERT INTO events \
             (aggregate_type, aggregate_id, sequence, event_type, event_version, payload, metadata) \
             VALUES ('TokenizedEquityMint', ?, ?, ?, '1.0', ?, '{}')",
        )
        .bind(raw.to_string())
        .bind(sequence)
        .bind(event_type)
        .bind(payload)
        .execute(pool)
        .await
        .unwrap();
    }

    fn mint_accepted_payload(id: &IssuerRequestId) -> String {
        format!(
            r#"{{"MintAccepted":{{"issuer_request_id":"{id}","tokenization_request_id":"tok-1","accepted_at":"2026-01-01T00:00:01Z"}}}}"#
        )
    }

    fn provider_completion_recovered_payload(id: &IssuerRequestId) -> String {
        format!(
            r#"{{"ProviderCompletionRecovered":{{"issuer_request_id":"{id}","wallet":"0x0000000000000000000000000000000000000001","tokenization_request_id":"tok-1","tx_hash":"0x1111111111111111111111111111111111111111111111111111111111111111","shares_minted":"10000000000000000000","fees":null,"recovered_at":"2026-01-01T00:02:00Z"}}}}"#
        )
    }

    #[tokio::test]
    async fn recheck_context_treats_recovered_mint_as_having_received_tokens() {
        // A mint recovered once via ProviderCompletionRecovered evolves into the
        // TokensReceived state without writing a literal TokensReceived event. If
        // it then re-fails at wrapping, a second recheck must NOT recover it again
        // (which would re-wrap already-moved tokens), so received_tokens must be
        // true even though no TokensReceived event exists.
        let pool = crate::test_utils::setup_test_db().await;
        let id = issuer_request_id("mint-rerecover");

        insert_mint_event(
            &pool,
            &id,
            0,
            "TokenizedEquityMintEvent::MintRequested",
            r#"{"MintRequested":{"symbol":"AAPL","quantity":"10","wallet":"0x0000000000000000000000000000000000000001","requested_at":"2026-01-01T00:00:00Z"}}"#,
        )
        .await;
        insert_mint_event(
            &pool,
            &id,
            1,
            "TokenizedEquityMintEvent::MintAccepted",
            &mint_accepted_payload(&id),
        )
        .await;
        insert_mint_event(
            &pool,
            &id,
            2,
            "TokenizedEquityMintEvent::ProviderCompletionRecovered",
            &provider_completion_recovered_payload(&id),
        )
        .await;

        let context = load_mint_recheck_context(&pool, &id).await.unwrap();

        assert!(
            context.received_tokens,
            "a mint already recovered via ProviderCompletionRecovered must count as having received tokens"
        );
    }

    /// Reproduces the production recovery path end to end: a mint that failed
    /// at acceptance (injected reactor-less, exactly as the simulate-failures
    /// harness does) is recovered by dispatching `RecoverProviderCompletion`
    /// through the reactor-wired store. The recovered quantity must leave the
    /// Hedging in-flight balance (the dashboard's "Inflight" column) and land in
    /// MarketMaking available -- not stay stuck in-flight forever.
    #[tokio::test]
    async fn recover_mint_clears_hedging_inflight() {
        let (pool, apalis_pool) = crate::test_utils::setup_test_pools().await;
        let symbol = Symbol::new("AAPL").unwrap();
        let id = issuer_request_id("mint-recover-inflight");

        // Reactor-less failure injection: the live reactor never observes these
        // events, so its inventory holds the full Hedging balance with nothing
        // in-flight -- the stale state recovery must reconcile.
        insert_mint_event(
            &pool,
            &id,
            1,
            "TokenizedEquityMintEvent::MintRequested",
            r#"{"MintRequested":{"symbol":"AAPL","quantity":"10","wallet":"0x0000000000000000000000000000000000000001","requested_at":"2026-01-01T00:00:00Z"}}"#,
        )
        .await;
        insert_mint_event(
            &pool,
            &id,
            2,
            "TokenizedEquityMintEvent::MintAccepted",
            &mint_accepted_payload(&id),
        )
        .await;
        insert_mint_event(
            &pool,
            &id,
            3,
            "TokenizedEquityMintEvent::MintAcceptanceFailed",
            r#"{"MintAcceptanceFailed":{"reason":"simulate: timeout","failed_at":"2026-01-01T00:00:02Z"}}"#,
        )
        .await;

        let (event_sender, _event_receiver) = broadcast::channel::<Statement>(16);
        let inventory = Arc::new(BroadcastingInventory::new(
            InventoryView::default().with_equity(
                symbol.clone(),
                FractionalShares::ZERO,
                FractionalShares::new(float!(100)),
            ),
            event_sender,
        ));

        let service = Arc::new(RebalancingService::new(
            RebalancingServiceConfig {
                poll_freshness: PollFreshness::always_fresh(),
                inventory_staleness_bound: Duration::from_secs(300),
                cash_reserved: None,
                equity: ImbalanceThreshold {
                    target: float!(0.5),
                    deviation: float!(0.2),
                },
                usdc: None,
                transfer_timeout: Duration::from_secs(1800),
                assets: ChainAssets::default(),
            },
            Arc::new(test_store::<VaultRegistry>(pool.clone(), ())),
            VaultRegistryId {
                orderbook: address!("0x0000000000000000000000000000000000000001"),
                owner: address!("0x0000000000000000000000000000000000000002"),
            },
            inventory.clone(),
            Arc::new(MockWrapper::new()),
            RebalancingSchedulers::new(&apalis_pool),
            Arc::new(crate::alerts::LogNotifier),
        ));

        // Reactor-wired stores -- the production wiring that dispatches committed
        // events to the reactor's `on_mint`.
        let mint_store = StoreBuilder::<TokenizedEquityMint>::new(pool.clone())
            .with(service.clone())
            .build(mock_services())
            .await
            .unwrap();
        let redemption_store = StoreBuilder::<EquityRedemption>::new(pool.clone())
            .with(service.clone())
            .build(mock_services())
            .await
            .unwrap();
        service
            .set_stores(
                mint_store.clone(),
                redemption_store.clone(),
                Arc::new(test_store::<UsdcRebalance>(pool.clone(), ())),
            )
            .await;

        // The provider reports the request settled: get_request must find a
        // Completed request (with a tx_hash) under the aggregate's
        // tokenization_request_id ("tok-1").
        let mut completed_request = TokenizationRequest::mock_completed();
        completed_request.id = tokenization_request_id("tok-1");
        let tokenizer =
            Arc::new(MockTokenizer::new().with_pending_requests(vec![completed_request]));

        let transfer = CrossVenueEquityTransfer::new(
            Arc::new(MockRaindex::new()),
            Arc::new(MockVaultLookup::new()),
            tokenizer,
            Arc::new(MockWrapper::new()),
            address!("0x0000000000000000000000000000000000000001"),
            mint_store,
            redemption_store,
        );

        let outcome = transfer.recover_mint(&id, &pool, &service).await.unwrap();
        assert!(
            matches!(outcome, RecheckOutcome::Recovered),
            "expected Recovered, got {outcome:?}"
        );

        let (hedging_inflight, market_making_available) = {
            let view = inventory.read().await;

            (
                view.equity_inflight(&symbol, Venue::Hedging),
                view.equity_available(&symbol, Venue::MarketMaking),
            )
        };
        assert_eq!(
            hedging_inflight,
            Some(FractionalShares::ZERO),
            "recovered mint left its quantity stuck in Hedging in-flight"
        );
        assert_eq!(
            market_making_available,
            Some(FractionalShares::new(float!(10))),
            "recovered quantity should land in MarketMaking available"
        );
    }

    /// Recovery must not double-count an in-flight that is ALREADY established.
    ///
    /// The realistic stuck state -- what the simulate-failures harness and the
    /// CLI `transfer fail` ops tool both produce -- is: `MintAccepted` ran
    /// `start` so the quantity sits in Hedging in-flight, but the failure was
    /// recorded out-of-process so the reactor never ran `cancel`. Recovery then
    /// runs against an in-flight already at the mint quantity; re-establishing
    /// it with a `Start` double-counts (qty -> 2*qty), and the completion
    /// removes only one copy, leaving the quantity stuck in-flight. Recovery
    /// must reconcile it to zero idempotently.
    #[tokio::test]
    async fn recover_mint_does_not_double_count_existing_inflight() {
        let (pool, apalis_pool) = crate::test_utils::setup_test_pools().await;
        let symbol = Symbol::new("AAPL").unwrap();
        let id = issuer_request_id("mint-double-count");

        insert_mint_event(
            &pool,
            &id,
            1,
            "TokenizedEquityMintEvent::MintRequested",
            r#"{"MintRequested":{"symbol":"AAPL","quantity":"10","wallet":"0x0000000000000000000000000000000000000001","requested_at":"2026-01-01T00:00:00Z"}}"#,
        )
        .await;
        insert_mint_event(
            &pool,
            &id,
            2,
            "TokenizedEquityMintEvent::MintAccepted",
            &mint_accepted_payload(&id),
        )
        .await;
        insert_mint_event(
            &pool,
            &id,
            3,
            "TokenizedEquityMintEvent::MintAcceptanceFailed",
            r#"{"MintAcceptanceFailed":{"reason":"simulate: timeout","failed_at":"2026-01-01T00:00:02Z"}}"#,
        )
        .await;

        let (event_sender, _event_receiver) = broadcast::channel::<Statement>(16);

        // MintAccepted's `start` already moved the quantity into Hedging
        // in-flight (available 100 -> 90, in-flight 10); the out-of-process
        // failure never ran `cancel`, so the in-flight is still established.
        let seeded = InventoryView::default()
            .with_equity(
                symbol.clone(),
                FractionalShares::ZERO,
                FractionalShares::new(float!(90)),
            )
            .update_equity(
                &symbol,
                Inventory::set_inflight(Venue::Hedging, FractionalShares::new(float!(10))),
                Utc::now(),
            )
            .unwrap();
        let inventory = Arc::new(BroadcastingInventory::new(seeded, event_sender));

        let service = Arc::new(RebalancingService::new(
            RebalancingServiceConfig {
                poll_freshness: PollFreshness::always_fresh(),
                inventory_staleness_bound: Duration::from_secs(300),
                cash_reserved: None,
                equity: ImbalanceThreshold {
                    target: float!(0.5),
                    deviation: float!(0.2),
                },
                usdc: None,
                transfer_timeout: Duration::from_secs(1800),
                assets: ChainAssets::default(),
            },
            Arc::new(test_store::<VaultRegistry>(pool.clone(), ())),
            VaultRegistryId {
                orderbook: address!("0x0000000000000000000000000000000000000001"),
                owner: address!("0x0000000000000000000000000000000000000002"),
            },
            inventory.clone(),
            Arc::new(MockWrapper::new()),
            RebalancingSchedulers::new(&apalis_pool),
            Arc::new(crate::alerts::LogNotifier),
        ));

        let mint_store = StoreBuilder::<TokenizedEquityMint>::new(pool.clone())
            .with(service.clone())
            .build(mock_services())
            .await
            .unwrap();
        let redemption_store = StoreBuilder::<EquityRedemption>::new(pool.clone())
            .with(service.clone())
            .build(mock_services())
            .await
            .unwrap();
        service
            .set_stores(
                mint_store.clone(),
                redemption_store.clone(),
                Arc::new(test_store::<UsdcRebalance>(pool.clone(), ())),
            )
            .await;

        let mut completed_request = TokenizationRequest::mock_completed();
        completed_request.id = tokenization_request_id("tok-1");
        let tokenizer =
            Arc::new(MockTokenizer::new().with_pending_requests(vec![completed_request]));

        let transfer = CrossVenueEquityTransfer::new(
            Arc::new(MockRaindex::new()),
            Arc::new(MockVaultLookup::new()),
            tokenizer,
            Arc::new(MockWrapper::new()),
            address!("0x0000000000000000000000000000000000000001"),
            mint_store,
            redemption_store,
        );

        let outcome = transfer.recover_mint(&id, &pool, &service).await.unwrap();
        assert!(
            matches!(outcome, RecheckOutcome::Recovered),
            "expected Recovered, got {outcome:?}"
        );

        let (hedging_inflight, market_making_available) = {
            let view = inventory.read().await;

            (
                view.equity_inflight(&symbol, Venue::Hedging),
                view.equity_available(&symbol, Venue::MarketMaking),
            )
        };
        assert_eq!(
            hedging_inflight,
            Some(FractionalShares::ZERO),
            "recovery double-counted the already-established in-flight, leaving it stuck"
        );
        assert_eq!(
            market_making_available,
            Some(FractionalShares::new(float!(10))),
            "recovered quantity should land in MarketMaking available"
        );
    }

    /// A reconciled mint is terminal: `resume_mint` must be a clean no-op for
    /// an apalis retry, leaving the aggregate in `Reconciled`.
    #[tokio::test]
    async fn resume_mint_is_noop_on_reconciled_mint() {
        let transfer = create_equity_transfer(
            Arc::new(MockTokenizer::new()),
            Arc::new(MockRaindex::new()),
            Arc::new(MockWrapper::new()),
        )
        .await;

        let id = issuer_request_id("ISS-RECONCILED");
        transfer
            .mint_store
            .send(
                &id,
                TokenizedEquityMintCommand::RequestMint {
                    issuer_request_id: id.clone(),
                    symbol: Symbol::new("AAPL").unwrap(),
                    quantity: float!(10),
                    wallet: transfer.wallet,
                },
            )
            .await
            .unwrap();
        transfer
            .mint_store
            .send(
                &id,
                TokenizedEquityMintCommand::FailAcceptance {
                    reason: "stranded mid-flight".to_string(),
                },
            )
            .await
            .unwrap();
        transfer
            .mint_store
            .send(
                &id,
                TokenizedEquityMintCommand::Reconcile {
                    reason: "wrapped manually via wrap-equity".to_string(),
                },
            )
            .await
            .unwrap();

        transfer
            .resume_mint(&id)
            .await
            .expect("a reconciled mint must be a clean no-op for the job retry");

        let entity = transfer.mint_store.load(&id).await.unwrap().unwrap();
        assert!(
            matches!(entity, TokenizedEquityMint::Reconciled { .. }),
            "resume must leave a reconciled mint terminal, got: {entity:?}"
        );
    }

    /// A reconciled redemption is terminal: `resume_redemption` must be a clean
    /// no-op for an apalis retry, leaving the aggregate in `Reconciled`.
    #[tokio::test]
    async fn resume_redemption_is_noop_on_reconciled_redemption() {
        let transfer = create_equity_transfer(
            Arc::new(MockTokenizer::new()),
            Arc::new(MockRaindex::new()),
            Arc::new(MockWrapper::new()),
        )
        .await;

        let id = redemption_aggregate_id("redeem-reconciled");
        let symbol = Symbol::new("TEST").unwrap();
        let token = transfer
            .vault_lookup
            .vault_token_for_symbol(&symbol)
            .await
            .unwrap();
        let amount = FractionalShares::new(float!(50))
            .to_u256_18_decimals()
            .unwrap();

        transfer
            .withdraw_from_raindex(
                &id,
                &symbol,
                FractionalShares::new(float!(50)),
                token,
                amount,
            )
            .await
            .unwrap();
        transfer.unwrap_and_send(&id).await.unwrap();
        transfer
            .redemption_store
            .send(
                &id,
                EquityRedemptionCommand::FailDetection {
                    failure: DetectionFailure::Timeout,
                },
            )
            .await
            .unwrap();
        transfer
            .redemption_store
            .send(
                &id,
                EquityRedemptionCommand::Reconcile {
                    reason: "deposited manually via vault-deposit".to_string(),
                },
            )
            .await
            .unwrap();

        transfer
            .resume_redemption(&id)
            .await
            .expect("a reconciled redemption must be a clean no-op for the job retry");

        let entity = transfer.redemption_store.load(&id).await.unwrap().unwrap();
        assert!(
            matches!(entity, EquityRedemption::Reconciled { .. }),
            "resume must leave a reconciled redemption terminal, got: {entity:?}"
        );
    }

    /// A reconciled mint is already settled, so the operator recheck path
    /// reports `AlreadyCompleted` without attempting provider recovery.
    #[tokio::test]
    async fn recover_mint_reports_already_completed_on_reconciled_mint() {
        let (transfer, service, pool) = transfer_with_rebalancing_service().await;

        let id = issuer_request_id("mint-recover-reconciled");
        transfer
            .mint_store
            .send(
                &id,
                TokenizedEquityMintCommand::RequestMint {
                    issuer_request_id: id.clone(),
                    symbol: Symbol::new("AAPL").unwrap(),
                    quantity: float!(10),
                    wallet: transfer.wallet,
                },
            )
            .await
            .unwrap();
        transfer
            .mint_store
            .send(
                &id,
                TokenizedEquityMintCommand::FailAcceptance {
                    reason: "stranded mid-flight".to_string(),
                },
            )
            .await
            .unwrap();
        transfer
            .mint_store
            .send(
                &id,
                TokenizedEquityMintCommand::Reconcile {
                    reason: "wrapped manually via wrap-equity".to_string(),
                },
            )
            .await
            .unwrap();

        let outcome = transfer.recover_mint(&id, &pool, &service).await.unwrap();
        assert!(
            matches!(outcome, RecheckOutcome::AlreadyCompleted),
            "a reconciled mint must recheck as AlreadyCompleted, got {outcome:?}"
        );
    }

    /// A reconciled redemption is already settled, so the operator recheck path
    /// reports `AlreadyCompleted` without attempting provider recovery.
    #[tokio::test]
    async fn recover_redemption_reports_already_completed_on_reconciled_redemption() {
        let (transfer, service, _pool) = transfer_with_rebalancing_service().await;

        let id = redemption_aggregate_id("redeem-recover-reconciled");
        let symbol = Symbol::new("TEST").unwrap();
        let token = transfer
            .vault_lookup
            .vault_token_for_symbol(&symbol)
            .await
            .unwrap();
        let amount = FractionalShares::new(float!(50))
            .to_u256_18_decimals()
            .unwrap();

        transfer
            .withdraw_from_raindex(
                &id,
                &symbol,
                FractionalShares::new(float!(50)),
                token,
                amount,
            )
            .await
            .unwrap();
        transfer.unwrap_and_send(&id).await.unwrap();
        transfer
            .redemption_store
            .send(
                &id,
                EquityRedemptionCommand::FailDetection {
                    failure: DetectionFailure::Timeout,
                },
            )
            .await
            .unwrap();
        transfer
            .redemption_store
            .send(
                &id,
                EquityRedemptionCommand::Reconcile {
                    reason: "deposited manually via vault-deposit".to_string(),
                },
            )
            .await
            .unwrap();

        let outcome = transfer.recover_redemption(&id, &service).await.unwrap();
        assert!(
            matches!(outcome, RecheckOutcome::AlreadyCompleted),
            "a reconciled redemption must recheck as AlreadyCompleted, got {outcome:?}"
        );
    }

    /// Builds a transfer wired to a real `RebalancingService` sharing the same
    /// command stores, so a seeded aggregate is visible to the `recover_*`
    /// recheck entry points.
    async fn transfer_with_rebalancing_service() -> (
        CrossVenueEquityTransfer,
        Arc<RebalancingService>,
        SqlitePool,
    ) {
        let (pool, apalis_pool) = crate::test_utils::setup_test_pools().await;
        let (event_sender, _event_receiver) = broadcast::channel::<Statement>(16);
        let inventory = Arc::new(BroadcastingInventory::new(
            InventoryView::default(),
            event_sender,
        ));

        let service = Arc::new(RebalancingService::new(
            RebalancingServiceConfig {
                poll_freshness: PollFreshness::always_fresh(),
                inventory_staleness_bound: Duration::from_secs(300),
                cash_reserved: None,
                equity: ImbalanceThreshold {
                    target: float!(0.5),
                    deviation: float!(0.2),
                },
                usdc: None,
                transfer_timeout: Duration::from_secs(1800),
                assets: ChainAssets::default(),
            },
            Arc::new(test_store::<VaultRegistry>(pool.clone(), ())),
            VaultRegistryId {
                orderbook: address!("0x0000000000000000000000000000000000000001"),
                owner: address!("0x0000000000000000000000000000000000000002"),
            },
            inventory,
            Arc::new(MockWrapper::new()),
            RebalancingSchedulers::new(&apalis_pool),
            Arc::new(crate::alerts::LogNotifier),
        ));

        let mint_store = StoreBuilder::<TokenizedEquityMint>::new(pool.clone())
            .with(service.clone())
            .build(mock_services())
            .await
            .unwrap();
        let redemption_store = StoreBuilder::<EquityRedemption>::new(pool.clone())
            .with(service.clone())
            .build(mock_services())
            .await
            .unwrap();
        service
            .set_stores(
                mint_store.clone(),
                redemption_store.clone(),
                Arc::new(test_store::<UsdcRebalance>(pool.clone(), ())),
            )
            .await;

        let transfer = CrossVenueEquityTransfer::new(
            Arc::new(MockRaindex::new()),
            Arc::new(mock_vault_lookup()),
            Arc::new(MockTokenizer::new()),
            Arc::new(MockWrapper::new()),
            address!("0x0000000000000000000000000000000000000001"),
            mint_store,
            redemption_store,
        );

        (transfer, service, pool)
    }

    async fn create_equity_transfer(
        tokenizer: Arc<dyn Tokenizer>,
        raindex: Arc<dyn Raindex>,
        wrapper: Arc<dyn Wrapper>,
    ) -> CrossVenueEquityTransfer {
        let (transfer, _pool) = create_equity_transfer_with_pool(tokenizer, raindex, wrapper).await;
        transfer
    }

    /// Like [`create_equity_transfer`] but also returns the backing pool, so a
    /// test can seed legacy events directly (e.g. a `TokensWrapped` event
    /// persisted before the `wrap_block` field existed).
    async fn create_equity_transfer_with_pool(
        tokenizer: Arc<dyn Tokenizer>,
        raindex: Arc<dyn Raindex>,
        wrapper: Arc<dyn Wrapper>,
    ) -> (CrossVenueEquityTransfer, SqlitePool) {
        let pool = SqlitePool::connect(":memory:").await.unwrap();
        sqlx::migrate!().run(&pool).await.unwrap();
        let vault_lookup = Arc::new(mock_vault_lookup());
        let services = EquityTransferServices {
            raindex: raindex.clone(),
            vault_lookup: vault_lookup.clone(),
            tokenizer: tokenizer.clone(),
            wrapper: wrapper.clone(),
            bot_gas_enqueuer: BotGasReceiptCostEnqueuer::Disabled,
            mint_authorizer: ConfiguredMintAuthorizer::Disabled,
        };

        let mint_store = Arc::new(test_store(pool.clone(), services.clone()));
        let redemption_store = Arc::new(test_store(pool.clone(), services));

        let transfer = CrossVenueEquityTransfer::new(
            raindex,
            vault_lookup,
            tokenizer,
            wrapper,
            Address::random(),
            mint_store,
            redemption_store,
        );

        (transfer, pool)
    }

    #[tokio::test]
    async fn mint_transfer_sends_mint_and_deposit_commands() {
        let transfer = create_equity_transfer(
            Arc::new(MockTokenizer::new()),
            Arc::new(MockRaindex::new()),
            Arc::new(MockWrapper::new()),
        )
        .await;

        transfer
            .resume_equity_to_market_making(
                &issuer_request_id("ISS-TEST"),
                &Symbol::new("AAPL").unwrap(),
                FractionalShares::new(float!(100.0)),
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn definitive_mint_rejection_completes_without_later_steps() {
        let tokenizer = Arc::new(
            MockTokenizer::new().with_mint_request_outcome(MockMintRequestOutcome::DefinitiveError),
        );
        let transfer = create_equity_transfer(
            tokenizer.clone(),
            Arc::new(MockRaindex::new()),
            Arc::new(MockWrapper::new()),
        )
        .await;
        let id = issuer_request_id("ISS-DEFINITIVE-REJECTION");

        transfer
            .resume_equity_to_market_making(
                &id,
                &Symbol::new("AAPL").unwrap(),
                FractionalShares::new(float!(100.0)),
            )
            .await
            .unwrap();

        assert_eq!(tokenizer.mint_request_call_count(), 1);
        let entity = transfer.mint_store.load(&id).await.unwrap().unwrap();
        assert!(matches!(entity, TokenizedEquityMint::Failed { .. }));
    }

    #[tokio::test]
    async fn fresh_mint_refuses_low_base_gas_before_creating_aggregate() {
        let transfer = create_equity_transfer(
            Arc::new(MockTokenizer::new()),
            Arc::new(MockRaindex::new()),
            Arc::new(MockWrapper::new()),
        )
        .await
        .with_gas_readiness(crate::native_gas::GasReadiness::for_test(
            U256::ZERO,
            U256::from(1_u64),
            U256::MAX,
            U256::from(1_u64),
        ));
        let id = issuer_request_id("ISS-LOW-GAS");

        let error = transfer
            .resume_equity_to_market_making(
                &id,
                &Symbol::new("AAPL").unwrap(),
                FractionalShares::new(float!(100.0)),
            )
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            MintTransferError::PreReceipt(MintError::GasReadiness(_))
        ));
        assert!(
            transfer.mint_store.load(&id).await.unwrap().is_none(),
            "gas refusal must happen before RequestMint creates aggregate state"
        );
    }

    /// Acceptance criterion: a full mint (RequestMint through
    /// DepositToVault) enqueues exactly one `Wrap` and one `VaultDeposit`
    /// bot-gas job, each carrying the symbol and Base chain.
    #[tokio::test]
    async fn mint_transfer_enqueues_wrap_and_vault_deposit_bot_gas_jobs() {
        let (pool, apalis_pool) = crate::test_utils::setup_test_pools().await;
        let services = mock_services();
        let mint_store = Arc::new(test_store(pool.clone(), services.clone()));
        let redemption_store = Arc::new(test_store(pool, services));
        let queue = RecordBotGasReceiptCostJobQueue::new(&apalis_pool);

        let transfer = CrossVenueEquityTransfer::new(
            Arc::new(MockRaindex::new()),
            Arc::new(mock_vault_lookup()),
            Arc::new(MockTokenizer::new()),
            Arc::new(MockWrapper::new()),
            Address::random(),
            mint_store,
            redemption_store,
        )
        .with_bot_gas_enqueuer(BotGasReceiptCostEnqueuer::Enabled(queue));

        let symbol = Symbol::new("AAPL").unwrap();
        transfer
            .resume_equity_to_market_making(
                &issuer_request_id("ISS-BOT-GAS"),
                &symbol,
                FractionalShares::new(float!(100.0)),
            )
            .await
            .unwrap();

        let jobs = pending_bot_gas_jobs(&apalis_pool).await;
        assert_eq!(
            jobs.len(),
            2,
            "expected exactly one Wrap and one VaultDeposit job"
        );
        let wrap_jobs: Vec<_> = jobs
            .iter()
            .filter(|job| job.category == BotGasOperationCategory::Wrap)
            .collect();
        let deposit_jobs: Vec<_> = jobs
            .iter()
            .filter(|job| job.category == BotGasOperationCategory::VaultDeposit)
            .collect();
        assert_eq!(wrap_jobs.len(), 1);
        assert_eq!(deposit_jobs.len(), 1);
        for job in [wrap_jobs[0], deposit_jobs[0]] {
            assert_eq!(job.chain, Chain::Base);
            assert_eq!(job.symbol, Some(symbol.clone()));
        }
    }

    /// Acceptance criterion: an enqueue failure after a confirmed
    /// wrap propagates as a hard error rather than being swallowed. A fresh
    /// mint reaches `wrap_received_mint`'s enqueue call (after `confirm_wrap`)
    /// strictly before `deposit_wrapped_mint`'s, so this test exercises the
    /// WRAP-side enqueue only; see `deposit_enqueue_failure_propagates` below
    /// for the deposit side.
    #[tokio::test]
    async fn wrap_enqueue_failure_propagates() {
        let (pool, apalis_pool) = crate::test_utils::setup_test_pools().await;
        let services = mock_services();
        let mint_store = Arc::new(test_store(pool.clone(), services.clone()));
        let redemption_store = Arc::new(test_store(pool, services));
        let queue = RecordBotGasReceiptCostJobQueue::new(&apalis_pool);
        apalis_pool.close().await;

        let transfer = CrossVenueEquityTransfer::new(
            Arc::new(MockRaindex::new()),
            Arc::new(mock_vault_lookup()),
            Arc::new(MockTokenizer::new()),
            Arc::new(MockWrapper::new()),
            Address::random(),
            mint_store,
            redemption_store,
        )
        .with_bot_gas_enqueuer(BotGasReceiptCostEnqueuer::Enabled(queue));

        let error = transfer
            .resume_equity_to_market_making(
                &issuer_request_id("ISS-BOT-GAS-FAIL"),
                &Symbol::new("AAPL").unwrap(),
                FractionalShares::new(float!(100.0)),
            )
            .await
            .unwrap_err();

        assert!(
            matches!(error, MintTransferError::PostReceipt(inner) if matches!(inner, MintError::BotGasEnqueue(_)))
        );
    }

    /// Isolates the DEPOSIT-side enqueue call (`deposit_wrapped_mint`'s,
    /// reached again on resume from `VaultDepositSubmitted`): seeds the
    /// aggregate straight to `VaultDepositSubmitted` via direct commands (so
    /// the wrap-side enqueue, which runs earlier in a fresh mint, never
    /// fires), then resumes with a closed apalis pool. The failure must leave
    /// the aggregate un-advanced -- still `VaultDepositSubmitted`, not
    /// `DepositedIntoRaindex` -- so a retry re-attempts both `confirm_tx` and
    /// the enqueue.
    #[tokio::test]
    async fn deposit_enqueue_failure_propagates() {
        let (pool, apalis_pool) = crate::test_utils::setup_test_pools().await;
        let services = mock_services();
        let mint_store = Arc::new(test_store(pool.clone(), services.clone()));
        let redemption_store = Arc::new(test_store(pool, services));
        let queue = RecordBotGasReceiptCostJobQueue::new(&apalis_pool);

        let transfer = CrossVenueEquityTransfer::new(
            Arc::new(MockRaindex::new()),
            Arc::new(mock_vault_lookup()),
            Arc::new(MockTokenizer::new()),
            Arc::new(MockWrapper::new()),
            Address::random(),
            mint_store,
            redemption_store,
        )
        .with_bot_gas_enqueuer(BotGasReceiptCostEnqueuer::Enabled(queue));

        let id = issuer_request_id("ISS-BOT-GAS-DEPOSIT-FAIL");
        transfer
            .mint_store
            .send(
                &id,
                TokenizedEquityMintCommand::RequestMint {
                    issuer_request_id: id.clone(),
                    symbol: Symbol::new("AAPL").unwrap(),
                    quantity: float!(10),
                    wallet: transfer.wallet,
                },
            )
            .await
            .unwrap();
        submit_requested_mint(&transfer, &id).await;
        transfer
            .mint_store
            .send(&id, TokenizedEquityMintCommand::Poll)
            .await
            .unwrap();
        let wrap_tx = TxHash::random();
        transfer
            .mint_store
            .send(
                &id,
                TokenizedEquityMintCommand::SubmitWrap {
                    wrap_tx_hash: wrap_tx,
                },
            )
            .await
            .unwrap();
        transfer
            .mint_store
            .send(
                &id,
                TokenizedEquityMintCommand::WrapTokens {
                    wrap_tx_hash: wrap_tx,
                    wrapped_shares: U256::from(10_000_000_000_000_000_000u128),
                    wrap_block: 1,
                },
            )
            .await
            .unwrap();
        let vault_deposit_tx_hash = TxHash::random();
        transfer
            .mint_store
            .send(
                &id,
                TokenizedEquityMintCommand::SubmitVaultDeposit {
                    vault_deposit_tx_hash,
                },
            )
            .await
            .unwrap();

        let entity = transfer.mint_store.load(&id).await.unwrap().unwrap();
        assert!(
            matches!(entity, TokenizedEquityMint::VaultDepositSubmitted { .. }),
            "expected VaultDepositSubmitted, got: {entity:?}"
        );

        apalis_pool.close().await;

        let error = transfer.resume_mint(&id).await.unwrap_err();

        assert!(
            matches!(error, MintError::BotGasEnqueue(_)),
            "expected BotGasEnqueue, got: {error:?}"
        );

        let entity = transfer.mint_store.load(&id).await.unwrap().unwrap();
        assert!(
            matches!(entity, TokenizedEquityMint::VaultDepositSubmitted { .. }),
            "aggregate must remain in VaultDepositSubmitted (un-advanced) so the retry \
             re-attempts confirm_tx and the enqueue; got: {entity:?}"
        );
    }

    #[tokio::test]
    async fn resume_mint_from_accepted_completes_workflow() {
        let transfer = create_equity_transfer(
            Arc::new(MockTokenizer::new()),
            Arc::new(MockRaindex::new()),
            Arc::new(MockWrapper::new()),
        )
        .await;

        let id = issuer_request_id("ISS-RESUME");
        transfer
            .mint_store
            .send(
                &id,
                TokenizedEquityMintCommand::RequestMint {
                    issuer_request_id: id.clone(),
                    symbol: Symbol::new("AAPL").unwrap(),
                    quantity: float!(10),
                    wallet: transfer.wallet,
                },
            )
            .await
            .unwrap();
        submit_requested_mint(&transfer, &id).await;

        transfer.resume_mint(&id).await.unwrap();

        let entity = transfer.mint_store.load(&id).await.unwrap().unwrap();
        assert!(
            matches!(entity, TokenizedEquityMint::DepositedIntoRaindex { .. }),
            "Expected deposited mint after resume, got: {entity:?}"
        );
    }

    /// Builds a transfer with full mint-authorization wiring: a stubbed
    /// vault-mode reader, a token map for AAPL, an Enabled mock authorizer
    /// in the aggregate services, and a real (in-memory) delivery queue.
    /// Returns the event pool and the apalis pool for assertions.
    async fn create_authorization_wired_transfer(
        mode: VaultModeTag,
    ) -> (
        CrossVenueEquityTransfer,
        SqlitePool,
        apalis_sqlite::SqlitePool,
    ) {
        let (pool, apalis_pool) = crate::test_utils::setup_test_pools().await;
        let services = EquityTransferServices {
            mint_authorizer: ConfiguredMintAuthorizer::Enabled(Arc::new(MockMintAuthorizer)),
            ..mock_services()
        };

        let mint_store = Arc::new(test_store(pool.clone(), services.clone()));
        let redemption_store = Arc::new(test_store(pool.clone(), services));

        let mut token_addresses = HashMap::new();
        token_addresses.insert(Symbol::new("AAPL").unwrap(), Address::repeat_byte(0x11));

        let transfer = CrossVenueEquityTransfer::new(
            Arc::new(MockRaindex::new()),
            Arc::new(mock_vault_lookup()),
            Arc::new(MockTokenizer::new()),
            Arc::new(MockWrapper::new()),
            Address::random(),
            mint_store,
            redemption_store,
        )
        .with_mint_authorization(MintAuthorizationWiring {
            vault_mode_reader: Arc::new(StubVaultModeReader(mode)),
            token_addresses,
            delivery_queue: DeliverMintAuthorizationJobQueue::new(&apalis_pool),
        });

        (transfer, pool, apalis_pool)
    }

    async fn count_events(pool: &SqlitePool, event_type: &str) -> i64 {
        sqlx::query_scalar("SELECT COUNT(*) FROM events WHERE event_type = ?")
            .bind(event_type)
            .fetch_one(pool)
            .await
            .unwrap()
    }

    async fn count_delivery_jobs(apalis_pool: &apalis_sqlite::SqlitePool) -> i64 {
        sqlx_apalis::query_scalar::<_, i64>("SELECT COUNT(*) FROM Jobs WHERE job_type = ?")
            .bind(std::any::type_name::<DeliverMintAuthorization>())
            .fetch_one(apalis_pool)
            .await
            .unwrap()
    }

    /// An orchestrator-mode mint resumed from `MintAccepted` signs its
    /// authorization, enqueues exactly one delivery, and still completes
    /// the workflow -- and a SECOND resume never signs a fresh nonce or
    /// duplicates the delivery job (the resume idempotency invariant).
    #[tokio::test]
    async fn resume_of_orchestrator_mint_signs_once_and_enqueues_delivery() {
        let (transfer, pool, apalis_pool) =
            create_authorization_wired_transfer(VaultModeTag::Orchestrator).await;

        let id = issuer_request_id("ISS-ORCH-RESUME");
        transfer
            .mint_store
            .send(
                &id,
                TokenizedEquityMintCommand::RequestMint {
                    issuer_request_id: id.clone(),
                    symbol: Symbol::new("AAPL").unwrap(),
                    quantity: float!(10),
                    wallet: transfer.wallet,
                },
            )
            .await
            .unwrap();
        submit_requested_mint(&transfer, &id).await;

        transfer.resume_mint(&id).await.unwrap();

        let entity = transfer.mint_store.load(&id).await.unwrap().unwrap();
        assert!(
            matches!(entity, TokenizedEquityMint::DepositedIntoRaindex { .. }),
            "Expected deposited mint after resume, got: {entity:?}"
        );
        assert_eq!(
            count_events(&pool, "TokenizedEquityMintEvent::MintAuthorizationSigned").await,
            1
        );
        assert_eq!(count_delivery_jobs(&apalis_pool).await, 1);

        // Second resume: the mint is DepositedIntoRaindex, so resume_mint
        // short-circuits on the terminal state before ever reaching
        // authorization -- the unchanged counts pin that short-circuit.
        // (Signing idempotency itself is pinned by
        // `re_signing_keeps_the_original_nonce` in tokenized_equity_mint.)
        transfer.resume_mint(&id).await.unwrap();
        assert_eq!(
            count_events(&pool, "TokenizedEquityMintEvent::MintAuthorizationSigned").await,
            1
        );
        assert_eq!(count_delivery_jobs(&apalis_pool).await, 1);
    }

    /// A restart during a delayed-redrive window leaves an UNKEYED pending
    /// successor row (`push_with_delay` records no idempotency key), which
    /// `push_idempotent` alone cannot see. The live-delivery guard must
    /// bound the mint to one chain: re-ensuring the authorization enqueues
    /// nothing while that successor is live.
    #[tokio::test]
    async fn ensure_does_not_start_a_second_delivery_chain_over_a_redrive_successor() {
        let (transfer, _pool, apalis_pool) =
            create_authorization_wired_transfer(VaultModeTag::Orchestrator).await;

        let id = issuer_request_id("ISS-ORCH-REDRIVE");
        let symbol = Symbol::new("AAPL").unwrap();
        transfer
            .mint_store
            .send(
                &id,
                TokenizedEquityMintCommand::RequestMint {
                    issuer_request_id: id.clone(),
                    symbol: symbol.clone(),
                    quantity: float!(10),
                    wallet: transfer.wallet,
                },
            )
            .await
            .unwrap();
        submit_requested_mint(&transfer, &id).await;
        transfer
            .mint_store
            .send(
                &id,
                TokenizedEquityMintCommand::SignMintAuthorization {
                    token: Address::repeat_byte(0x11),
                },
            )
            .await
            .unwrap();

        // The state a crash mid-redrive leaves behind: one delayed,
        // UNKEYED successor row.
        let mut queue = DeliverMintAuthorizationJobQueue::new(&apalis_pool);
        queue
            .push_with_delay(
                DeliverMintAuthorization {
                    issuer_request_id: id.clone(),
                    redrive_attempts: 3,
                },
                std::time::Duration::from_secs(30),
            )
            .await
            .unwrap();
        assert_eq!(count_delivery_jobs(&apalis_pool).await, 1);

        transfer
            .ensure_mint_authorization(&id, &symbol)
            .await
            .unwrap();

        assert_eq!(
            count_delivery_jobs(&apalis_pool).await,
            1,
            "the live-delivery guard must not start a second chain over a \
             pending redrive successor"
        );
    }

    /// A vault-direct asset takes the pre-authorization path untouched: no
    /// signing event, no delivery job.
    #[tokio::test]
    async fn resume_of_vault_direct_mint_signs_nothing() {
        let (transfer, pool, apalis_pool) =
            create_authorization_wired_transfer(VaultModeTag::VaultDirect).await;

        let id = issuer_request_id("ISS-DIRECT-RESUME");
        transfer
            .mint_store
            .send(
                &id,
                TokenizedEquityMintCommand::RequestMint {
                    issuer_request_id: id.clone(),
                    symbol: Symbol::new("AAPL").unwrap(),
                    quantity: float!(10),
                    wallet: transfer.wallet,
                },
            )
            .await
            .unwrap();
        submit_requested_mint(&transfer, &id).await;

        transfer.resume_mint(&id).await.unwrap();

        let entity = transfer.mint_store.load(&id).await.unwrap().unwrap();
        assert!(
            matches!(entity, TokenizedEquityMint::DepositedIntoRaindex { .. }),
            "Expected deposited mint after resume, got: {entity:?}"
        );
        assert_eq!(
            count_events(&pool, "TokenizedEquityMintEvent::MintAuthorizationSigned").await,
            0
        );
        assert_eq!(count_delivery_jobs(&apalis_pool).await, 0);
    }

    /// An orchestrator-mode asset with no `[assets.equities.<SYM>]`
    /// tokenized-equity entry cannot bind the EIP-712 MintAuth to a token:
    /// the mint must fail with the exact variant, never proceed
    /// unauthorized.
    #[tokio::test]
    async fn orchestrator_asset_without_token_address_fails_before_signing() {
        let (transfer, pool, apalis_pool) =
            create_authorization_wired_transfer(VaultModeTag::Orchestrator).await;

        // TSLA is absent from the helper's AAPL-only token map.
        let tsla = Symbol::new("TSLA").unwrap();
        let error = transfer
            .ensure_mint_authorization(&issuer_request_id("ISS-NO-TOKEN"), &tsla)
            .await
            .unwrap_err();

        assert!(
            matches!(
                error,
                MintError::UnknownTokenizedEquity { symbol } if symbol == tsla
            ),
            "expected UnknownTokenizedEquity for the unmapped symbol"
        );
        assert_eq!(
            count_events(&pool, "TokenizedEquityMintEvent::MintAuthorizationSigned").await,
            0,
            "the failed lookup must precede any signing"
        );
        assert_eq!(count_delivery_jobs(&apalis_pool).await, 0);
    }

    /// A transfer built without `with_mint_authorization` carries the
    /// explicit `VaultDirectOnly` assertion: `ensure_mint_authorization`
    /// cannot read `vault_mode` at all, so it must sign nothing and
    /// enqueue nothing -- pinned so the assertion stays a stated
    /// capability rather than drifting into silent behavior.
    #[tokio::test]
    async fn vault_direct_only_transfer_signs_nothing_and_enqueues_nothing() {
        let (pool, apalis_pool) = crate::test_utils::setup_test_pools().await;
        let services = EquityTransferServices {
            mint_authorizer: ConfiguredMintAuthorizer::Enabled(Arc::new(MockMintAuthorizer)),
            ..mock_services()
        };
        let mint_store = Arc::new(test_store(pool.clone(), services.clone()));
        let redemption_store = Arc::new(test_store(pool.clone(), services));
        let transfer = CrossVenueEquityTransfer::new(
            Arc::new(MockRaindex::new()),
            Arc::new(mock_vault_lookup()),
            Arc::new(MockTokenizer::new()),
            Arc::new(MockWrapper::new()),
            Address::random(),
            mint_store,
            redemption_store,
        );

        let id = issuer_request_id("ISS-VAULT-DIRECT-ONLY");
        let symbol = Symbol::new("AAPL").unwrap();
        transfer
            .mint_store
            .send(
                &id,
                TokenizedEquityMintCommand::RequestMint {
                    issuer_request_id: id.clone(),
                    symbol: symbol.clone(),
                    quantity: float!(10),
                    wallet: transfer.wallet,
                },
            )
            .await
            .unwrap();
        submit_requested_mint(&transfer, &id).await;

        transfer
            .ensure_mint_authorization(&id, &symbol)
            .await
            .unwrap();

        assert_eq!(
            count_events(&pool, "TokenizedEquityMintEvent::MintAuthorizationSigned").await,
            0,
            "a VaultDirectOnly transfer must never sign an authorization"
        );
        assert_eq!(count_delivery_jobs(&apalis_pool).await, 0);
    }

    /// Re-running the job entry point with an id whose aggregate already
    /// exists must resume from the persisted state (the crash-recovery path)
    /// rather than re-requesting the mint from Alpaca.
    #[tokio::test]
    async fn resume_equity_to_market_making_resumes_existing_aggregate_despite_low_gas() {
        let transfer = create_equity_transfer(
            Arc::new(MockTokenizer::new()),
            Arc::new(MockRaindex::new()),
            Arc::new(MockWrapper::new()),
        )
        .await
        .with_gas_readiness(crate::native_gas::GasReadiness::for_test(
            U256::ZERO,
            U256::from(1_u64),
            U256::MAX,
            U256::from(1_u64),
        ));

        let id = issuer_request_id("ISS-CRASH-RESUME");
        let symbol = Symbol::new("AAPL").unwrap();

        // First attempt crashes after the mint request was accepted: the
        // aggregate is persisted in MintAccepted.
        transfer
            .mint_store
            .send(
                &id,
                TokenizedEquityMintCommand::RequestMint {
                    issuer_request_id: id.clone(),
                    symbol: symbol.clone(),
                    quantity: float!(10),
                    wallet: transfer.wallet,
                },
            )
            .await
            .unwrap();
        submit_requested_mint(&transfer, &id).await;

        // The re-enqueued job runs the same entry point with the same id; a
        // fresh RequestMint here would fail with AlreadyInProgress, so
        // completing proves the resume path was taken.
        transfer
            .resume_equity_to_market_making(&id, &symbol, FractionalShares::new(float!(10)))
            .await
            .unwrap();

        let entity = transfer.mint_store.load(&id).await.unwrap().unwrap();
        assert!(
            matches!(entity, TokenizedEquityMint::DepositedIntoRaindex { .. }),
            "Expected deposited mint after job re-run, got: {entity:?}"
        );
    }

    /// A terminal aggregate is a no-op for the job entry point: apalis
    /// retries after a partial failure must not error once the aggregate
    /// has already completed.
    #[tokio::test]
    async fn resume_equity_to_market_making_is_noop_on_completed_mint() {
        let transfer = create_equity_transfer(
            Arc::new(MockTokenizer::new()),
            Arc::new(MockRaindex::new()),
            Arc::new(MockWrapper::new()),
        )
        .await;

        let id = issuer_request_id("ISS-DONE");
        let symbol = Symbol::new("AAPL").unwrap();

        transfer
            .resume_equity_to_market_making(&id, &symbol, FractionalShares::new(float!(10)))
            .await
            .unwrap();

        let entity = transfer.mint_store.load(&id).await.unwrap().unwrap();
        assert!(
            matches!(entity, TokenizedEquityMint::DepositedIntoRaindex { .. }),
            "Expected deposited mint, got: {entity:?}"
        );

        transfer
            .resume_equity_to_market_making(&id, &symbol, FractionalShares::new(float!(10)))
            .await
            .expect("a completed mint must be a clean no-op for the job retry");
    }

    #[tokio::test]
    async fn resume_redemption_from_tokens_sent_completes_workflow() {
        let tokenizer: Arc<dyn Tokenizer> = Arc::new(
            MockTokenizer::new()
                .with_detection_outcome(MockDetectionOutcome::Detected)
                .with_completion_outcome(MockCompletionOutcome::Completed),
        );
        let transfer = create_equity_transfer(
            tokenizer,
            Arc::new(MockRaindex::new()),
            Arc::new(MockWrapper::new()),
        )
        .await;

        let id = redemption_aggregate_id("redemption-resume");
        let symbol = Symbol::new("TEST").unwrap();
        let token = transfer
            .vault_lookup
            .vault_token_for_symbol(&symbol)
            .await
            .unwrap();
        let amount = FractionalShares::new(float!(50))
            .to_u256_18_decimals()
            .unwrap();

        transfer
            .withdraw_from_raindex(
                &id,
                &symbol,
                FractionalShares::new(float!(50)),
                token,
                amount,
            )
            .await
            .unwrap();
        transfer.unwrap_and_send(&id).await.unwrap();

        transfer.resume_redemption(&id).await.unwrap();

        let entity = transfer.redemption_store.load(&id).await.unwrap().unwrap();
        assert!(
            matches!(entity, EquityRedemption::Completed { .. }),
            "Expected completed redemption after resume, got: {entity:?}"
        );
    }

    /// A fresh id runs the full redemption flow through the job entry point.
    #[tokio::test]
    async fn resume_equity_to_hedging_starts_fresh_redemption() {
        let tokenizer: Arc<dyn Tokenizer> = Arc::new(
            MockTokenizer::new()
                .with_detection_outcome(MockDetectionOutcome::Detected)
                .with_completion_outcome(MockCompletionOutcome::Completed),
        );
        let transfer = create_equity_transfer(
            tokenizer,
            Arc::new(MockRaindex::new()),
            Arc::new(MockWrapper::new()),
        )
        .await;

        let id = redemption_aggregate_id("redeem-fresh");
        transfer
            .resume_equity_to_hedging(
                &id,
                &Symbol::new("TEST").unwrap(),
                FractionalShares::new(float!(50)),
            )
            .await
            .unwrap();

        let entity = transfer.redemption_store.load(&id).await.unwrap().unwrap();
        assert!(
            matches!(entity, EquityRedemption::Completed { .. }),
            "Expected completed redemption, got: {entity:?}"
        );
    }

    #[tokio::test]
    async fn fresh_redemption_refuses_low_base_gas_before_creating_aggregate() {
        let transfer = create_equity_transfer(
            Arc::new(MockTokenizer::new()),
            Arc::new(MockRaindex::new()),
            Arc::new(MockWrapper::new()),
        )
        .await
        .with_gas_readiness(crate::native_gas::GasReadiness::for_test(
            U256::ZERO,
            U256::from(1_u64),
            U256::MAX,
            U256::from(1_u64),
        ));
        let id = redemption_aggregate_id("redemption-low-gas");

        let error = transfer
            .resume_equity_to_hedging(
                &id,
                &Symbol::new("AAPL").unwrap(),
                FractionalShares::new(float!(50)),
            )
            .await
            .unwrap_err();

        assert!(matches!(error, RedemptionError::GasReadiness(_)));
        assert!(
            transfer.redemption_store.load(&id).await.unwrap().is_none(),
            "gas refusal must happen before the Raindex withdrawal creates aggregate state"
        );
    }

    /// Acceptance criterion: a full redemption (withdraw through the
    /// redemption-wallet send) enqueues exactly one `VaultWithdraw`, one
    /// `Unwrap`, and one `WalletTransfer` bot-gas job, each carrying the
    /// symbol and Base chain.
    #[tokio::test]
    async fn redemption_transfer_enqueues_vault_withdraw_and_unwrap_bot_gas_jobs() {
        let (pool, apalis_pool) = crate::test_utils::setup_test_pools().await;
        let queue = RecordBotGasReceiptCostJobQueue::new(&apalis_pool);
        let tokenizer: Arc<dyn Tokenizer> = Arc::new(
            MockTokenizer::new()
                .with_detection_outcome(MockDetectionOutcome::Detected)
                .with_completion_outcome(MockCompletionOutcome::Completed),
        );
        let services = EquityTransferServices {
            raindex: Arc::new(MockRaindex::new()),
            vault_lookup: Arc::new(mock_vault_lookup()),
            tokenizer: tokenizer.clone(),
            wrapper: Arc::new(MockWrapper::new()),
            bot_gas_enqueuer: BotGasReceiptCostEnqueuer::Enabled(queue),
            mint_authorizer: ConfiguredMintAuthorizer::Disabled,
        };
        let mint_store = Arc::new(test_store(pool.clone(), services.clone()));
        let redemption_store = Arc::new(test_store(pool, services));
        let transfer = CrossVenueEquityTransfer::new(
            Arc::new(MockRaindex::new()),
            Arc::new(mock_vault_lookup()),
            tokenizer,
            Arc::new(MockWrapper::new()),
            Address::random(),
            mint_store,
            redemption_store,
        );

        let symbol = Symbol::new("TEST").unwrap();
        let id = redemption_aggregate_id("redeem-bot-gas");
        transfer
            .resume_equity_to_hedging(&id, &symbol, FractionalShares::new(float!(50)))
            .await
            .unwrap();

        let jobs = pending_bot_gas_jobs(&apalis_pool).await;
        assert_eq!(
            jobs.len(),
            3,
            "expected exactly one VaultWithdraw, one Unwrap, and one WalletTransfer job"
        );
        let withdraw_jobs: Vec<_> = jobs
            .iter()
            .filter(|job| job.category == BotGasOperationCategory::VaultWithdraw)
            .collect();
        let unwrap_jobs: Vec<_> = jobs
            .iter()
            .filter(|job| job.category == BotGasOperationCategory::Unwrap)
            .collect();
        let wallet_transfer_jobs: Vec<_> = jobs
            .iter()
            .filter(|job| job.category == BotGasOperationCategory::WalletTransfer)
            .collect();
        assert_eq!(withdraw_jobs.len(), 1);
        assert_eq!(unwrap_jobs.len(), 1);
        assert_eq!(wallet_transfer_jobs.len(), 1);
        for job in [withdraw_jobs[0], unwrap_jobs[0], wallet_transfer_jobs[0]] {
            assert_eq!(job.chain, Chain::Base);
            assert_eq!(job.symbol, Some(symbol.clone()));
        }
    }

    /// Acceptance criterion: an enqueue failure after a confirmed
    /// vault withdraw propagates as a hard error rather than being swallowed.
    #[tokio::test]
    async fn withdraw_enqueue_failure_propagates() {
        let (pool, apalis_pool) = crate::test_utils::setup_test_pools().await;
        let queue = RecordBotGasReceiptCostJobQueue::new(&apalis_pool);
        apalis_pool.close().await;
        let services = EquityTransferServices {
            raindex: Arc::new(MockRaindex::new()),
            vault_lookup: Arc::new(mock_vault_lookup()),
            tokenizer: Arc::new(MockTokenizer::new()),
            wrapper: Arc::new(MockWrapper::new()),
            bot_gas_enqueuer: BotGasReceiptCostEnqueuer::Enabled(queue),
            mint_authorizer: ConfiguredMintAuthorizer::Disabled,
        };
        let redemption_store = Arc::new(test_store::<EquityRedemption>(pool, services));

        let symbol = Symbol::new("TEST").unwrap();
        let token = mock_vault_lookup()
            .vault_token_for_symbol(&symbol)
            .await
            .unwrap();
        let amount = FractionalShares::new(float!(50))
            .to_u256_18_decimals()
            .unwrap();
        let id = redemption_aggregate_id("redeem-bot-gas-fail");
        redemption_store
            .send(
                &id,
                EquityRedemptionCommand::Redeem {
                    symbol: symbol.clone(),
                    quantity: float!(50),
                    token,
                    amount,
                },
            )
            .await
            .unwrap();
        redemption_store
            .send(&id, EquityRedemptionCommand::SubmitWithdraw)
            .await
            .unwrap();

        let error = redemption_store
            .send(&id, EquityRedemptionCommand::ConfirmWithdraw)
            .await
            .unwrap_err();

        assert!(
            matches!(
                error,
                AggregateError::UserError(LifecycleError::Apply(
                    EquityRedemptionError::BotGasEnqueueFailed(_)
                ))
            ),
            "expected BotGasEnqueueFailed, got: {error:?}"
        );
    }

    /// Re-running the job entry point with an id whose aggregate already
    /// exists must resume from the persisted state (the crash-recovery path)
    /// rather than re-running the vault withdrawal from scratch.
    #[tokio::test]
    async fn resume_equity_to_hedging_resumes_existing_aggregate_despite_low_gas() {
        let tokenizer: Arc<dyn Tokenizer> = Arc::new(
            MockTokenizer::new()
                .with_detection_outcome(MockDetectionOutcome::Detected)
                .with_completion_outcome(MockCompletionOutcome::Completed),
        );
        let transfer = create_equity_transfer(
            tokenizer,
            Arc::new(MockRaindex::new()),
            Arc::new(MockWrapper::new()),
        )
        .await
        .with_gas_readiness(crate::native_gas::GasReadiness::for_test(
            U256::ZERO,
            U256::from(1_u64),
            U256::MAX,
            U256::from(1_u64),
        ));

        let id = redemption_aggregate_id("redeem-crash-resume");
        let symbol = Symbol::new("TEST").unwrap();
        let quantity = FractionalShares::new(float!(50));
        let token = transfer
            .vault_lookup
            .vault_token_for_symbol(&symbol)
            .await
            .unwrap();
        let amount = quantity.to_u256_18_decimals().unwrap();

        // First attempt crashes after the withdrawal: the aggregate is
        // persisted mid-flight.
        transfer
            .withdraw_from_raindex(&id, &symbol, quantity, token, amount)
            .await
            .unwrap();

        // The re-enqueued job runs the same entry point with the same id; a
        // fresh Redeem command here would fail on the already-initialized
        // aggregate, so completing proves the resume path was taken.
        transfer
            .resume_equity_to_hedging(&id, &symbol, quantity)
            .await
            .unwrap();

        let entity = transfer.redemption_store.load(&id).await.unwrap().unwrap();
        assert!(
            matches!(entity, EquityRedemption::Completed { .. }),
            "Expected completed redemption after job re-run, got: {entity:?}"
        );
    }

    /// A terminal aggregate is a no-op for the job entry point: apalis
    /// retries after a partial failure must not error once the aggregate has
    /// already completed.
    #[tokio::test]
    async fn resume_equity_to_hedging_is_noop_on_completed_redemption() {
        let tokenizer: Arc<dyn Tokenizer> = Arc::new(
            MockTokenizer::new()
                .with_detection_outcome(MockDetectionOutcome::Detected)
                .with_completion_outcome(MockCompletionOutcome::Completed),
        );
        let transfer = create_equity_transfer(
            tokenizer,
            Arc::new(MockRaindex::new()),
            Arc::new(MockWrapper::new()),
        )
        .await;

        let id = redemption_aggregate_id("redeem-done");
        let symbol = Symbol::new("TEST").unwrap();
        let quantity = FractionalShares::new(float!(50));

        transfer
            .resume_equity_to_hedging(&id, &symbol, quantity)
            .await
            .unwrap();

        transfer
            .resume_equity_to_hedging(&id, &symbol, quantity)
            .await
            .expect("a completed redemption must be a clean no-op for the job retry");
    }

    #[tokio::test]
    async fn redemption_transfer_full_workflow_succeeds() {
        let tokenizer: Arc<dyn Tokenizer> = Arc::new(
            MockTokenizer::new()
                .with_detection_outcome(MockDetectionOutcome::Detected)
                .with_completion_outcome(MockCompletionOutcome::Completed),
        );
        let raindex: Arc<dyn Raindex> = Arc::new(MockRaindex::new());

        let transfer =
            create_equity_transfer(tokenizer, raindex, Arc::new(MockWrapper::new())).await;

        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            transfer.resume_equity_to_hedging(
                &redemption_aggregate_id("redeem-workflow"),
                &Symbol::new("TEST").unwrap(),
                FractionalShares::new(float!(50)),
            ),
        )
        .await
        .expect("redemption transfer timed out")
        .unwrap();
    }

    #[tokio::test]
    async fn redemption_transfer_fails_on_detection_timeout() {
        let tokenizer: Arc<dyn Tokenizer> =
            Arc::new(MockTokenizer::new().with_detection_outcome(MockDetectionOutcome::Timeout));
        let raindex: Arc<dyn Raindex> = Arc::new(MockRaindex::new());

        let transfer =
            create_equity_transfer(tokenizer, raindex, Arc::new(MockWrapper::new())).await;

        let error = transfer
            .resume_equity_to_hedging(
                &redemption_aggregate_id("redeem-detection-timeout"),
                &Symbol::new("TEST").unwrap(),
                FractionalShares::new(float!(50)),
            )
            .await
            .unwrap_err();

        assert!(
            matches!(error, RedemptionError::Tokenizer(_)),
            "Expected Tokenizer error, got: {error:?}"
        );
    }

    #[tokio::test]
    async fn redemption_transfer_fails_on_detection_api_error() {
        let tokenizer: Arc<dyn Tokenizer> =
            Arc::new(MockTokenizer::new().with_detection_outcome(MockDetectionOutcome::ApiError));
        let raindex: Arc<dyn Raindex> = Arc::new(MockRaindex::new());

        let transfer =
            create_equity_transfer(tokenizer, raindex, Arc::new(MockWrapper::new())).await;

        let error = transfer
            .resume_equity_to_hedging(
                &redemption_aggregate_id("redeem-detection-api-error"),
                &Symbol::new("TEST").unwrap(),
                FractionalShares::new(float!(50)),
            )
            .await
            .unwrap_err();

        assert!(
            matches!(error, RedemptionError::Tokenizer(_)),
            "Expected Tokenizer error, got: {error:?}"
        );
    }

    #[tokio::test]
    async fn redemption_transfer_fails_on_completion_rejection() {
        let tokenizer: Arc<dyn Tokenizer> = Arc::new(
            MockTokenizer::new()
                .with_detection_outcome(MockDetectionOutcome::Detected)
                .with_completion_outcome(MockCompletionOutcome::Rejected),
        );
        let raindex: Arc<dyn Raindex> = Arc::new(MockRaindex::new());

        let transfer =
            create_equity_transfer(tokenizer, raindex, Arc::new(MockWrapper::new())).await;

        let error = transfer
            .resume_equity_to_hedging(
                &redemption_aggregate_id("redeem-completion-rejected"),
                &Symbol::new("TEST").unwrap(),
                FractionalShares::new(float!(50)),
            )
            .await
            .unwrap_err();

        assert!(
            matches!(error, RedemptionError::Rejected),
            "Expected Rejected error, got: {error:?}"
        );
    }

    #[tokio::test]
    async fn redemption_transfer_fails_on_pending_status() {
        let tokenizer: Arc<dyn Tokenizer> = Arc::new(
            MockTokenizer::new()
                .with_detection_outcome(MockDetectionOutcome::Detected)
                .with_completion_outcome(MockCompletionOutcome::Pending),
        );
        let raindex: Arc<dyn Raindex> = Arc::new(MockRaindex::new());

        let transfer =
            create_equity_transfer(tokenizer, raindex, Arc::new(MockWrapper::new())).await;

        let error = transfer
            .resume_equity_to_hedging(
                &redemption_aggregate_id("redeem-pending-status"),
                &Symbol::new("TEST").unwrap(),
                FractionalShares::new(float!(50)),
            )
            .await
            .unwrap_err();

        assert!(
            matches!(error, RedemptionError::UnexpectedPendingStatus),
            "Expected UnexpectedPendingStatus error, got: {error:?}"
        );
    }

    #[tokio::test]
    async fn mint_transfer_fails_when_wrapper_fails() {
        let transfer = create_equity_transfer(
            Arc::new(MockTokenizer::new()),
            Arc::new(MockRaindex::new()),
            Arc::new(MockWrapper::failing()),
        )
        .await;

        let error = transfer
            .resume_equity_to_market_making(
                &issuer_request_id("ISS-TEST"),
                &Symbol::new("AAPL").unwrap(),
                FractionalShares::new(float!(100.0)),
            )
            .await
            .unwrap_err();

        assert!(
            matches!(error, MintTransferError::PostReceipt(MintError::Wrapper(_))),
            "Expected PostReceipt(Wrapper) error, got: {error:?}"
        );
    }

    #[tokio::test]
    async fn mint_transfer_fails_when_raindex_deposit_fails() {
        let transfer = create_equity_transfer(
            Arc::new(MockTokenizer::new()),
            Arc::new(MockRaindex::new().with_deposit_behavior(DepositBehavior::FailGeneric)),
            Arc::new(MockWrapper::new()),
        )
        .await;

        let error = transfer
            .resume_equity_to_market_making(
                &issuer_request_id("ISS-TEST"),
                &Symbol::new("AAPL").unwrap(),
                FractionalShares::new(float!(100.0)),
            )
            .await
            .unwrap_err();

        assert!(
            matches!(error, MintTransferError::PostReceipt(MintError::Raindex(_))),
            "Expected PostReceipt(Raindex) error, got: {error:?}"
        );
    }

    #[tokio::test]
    async fn mint_transfer_fails_when_underlying_lookup_fails() {
        let transfer = create_equity_transfer(
            Arc::new(MockTokenizer::new()),
            Arc::new(MockRaindex::new()),
            Arc::new(MockWrapper::failing_lookup()),
        )
        .await;

        let error = transfer
            .resume_equity_to_market_making(
                &issuer_request_id("ISS-TEST"),
                &Symbol::new("AAPL").unwrap(),
                FractionalShares::new(float!(100)),
            )
            .await
            .unwrap_err();

        assert!(
            matches!(
                error,
                MintTransferError::PostReceipt(MintError::Wrapper(
                    WrapperError::SymbolNotConfigured(_)
                ))
            ),
            "Expected PostReceipt(Wrapper(SymbolNotConfigured)) error, got: {error:?}"
        );
    }

    #[tokio::test]
    async fn mint_transfer_fails_when_derivative_lookup_fails() {
        let transfer = create_equity_transfer(
            Arc::new(MockTokenizer::new()),
            Arc::new(MockRaindex::new()),
            Arc::new(MockWrapper::failing_derivative_lookup()),
        )
        .await;

        let error = transfer
            .resume_equity_to_market_making(
                &issuer_request_id("ISS-TEST"),
                &Symbol::new("AAPL").unwrap(),
                FractionalShares::new(float!(100)),
            )
            .await
            .unwrap_err();

        assert!(
            matches!(
                error,
                MintTransferError::PostReceipt(MintError::Wrapper(
                    WrapperError::SymbolNotConfigured(_)
                ))
            ),
            "Expected PostReceipt(Wrapper(SymbolNotConfigured)) error, got: {error:?}"
        );
    }

    /// Verifies that mint deposits the derivative token (from
    /// `lookup_derivative`) to the Raindex vault, not the
    /// base tokenized share (from `lookup_underlying`).
    #[tokio::test]
    async fn mint_deposits_derivative_token_not_base_tokenized_share() {
        let base_share = Address::random();
        let derivative = Address::random();

        let raindex = Arc::new(MockRaindex::new());
        let wrapper = Arc::new(
            MockWrapper::new()
                .with_tokenized_shares(base_share)
                .with_wrapped_token(derivative),
        );

        let transfer = create_equity_transfer(
            Arc::new(MockTokenizer::new()),
            Arc::clone(&raindex) as Arc<dyn Raindex>,
            wrapper,
        )
        .await;

        transfer
            .resume_equity_to_market_making(
                &issuer_request_id("ISS-TEST"),
                &Symbol::new("AAPL").unwrap(),
                FractionalShares::new(float!(100)),
            )
            .await
            .unwrap();

        let deposited = raindex.last_deposited_token().expect("deposit was called");
        assert_eq!(
            deposited, derivative,
            "Deposit should use the derivative token, not the base tokenized share"
        );
        assert_ne!(
            deposited, base_share,
            "Deposit must not use the base tokenized share"
        );
    }

    #[tokio::test]
    async fn mint_transfer_fails_when_receipt_not_found() {
        let tokenizer = MockTokenizer::new()
            .with_verification_outcome(MockVerificationOutcome::ReceiptNotFound);

        let transfer = create_equity_transfer(
            Arc::new(tokenizer),
            Arc::new(MockRaindex::new()),
            Arc::new(MockWrapper::new()),
        )
        .await;

        let error = transfer
            .resume_equity_to_market_making(
                &issuer_request_id("ISS-TEST"),
                &Symbol::new("AAPL").unwrap(),
                FractionalShares::new(float!(100.0)),
            )
            .await
            .unwrap_err();

        assert!(
            matches!(
                error,
                MintTransferError::PostReceipt(MintError::Verification(
                    MintVerificationError::ReceiptNotFound { .. }
                ))
            ),
            "Expected PostReceipt(Verification(ReceiptNotFound)) error, got: {error:?}"
        );
    }

    #[tokio::test]
    async fn mint_transfer_fails_when_transaction_reverted() {
        let tokenizer = MockTokenizer::new()
            .with_verification_outcome(MockVerificationOutcome::TransactionReverted);

        let transfer = create_equity_transfer(
            Arc::new(tokenizer),
            Arc::new(MockRaindex::new()),
            Arc::new(MockWrapper::new()),
        )
        .await;

        let error = transfer
            .resume_equity_to_market_making(
                &issuer_request_id("ISS-TEST"),
                &Symbol::new("AAPL").unwrap(),
                FractionalShares::new(float!(100.0)),
            )
            .await
            .unwrap_err();

        assert!(
            matches!(
                error,
                MintTransferError::PostReceipt(MintError::Verification(
                    MintVerificationError::TransactionReverted { .. }
                ))
            ),
            "Expected PostReceipt(Verification(TransactionReverted)) error, got: {error:?}"
        );
    }

    #[tokio::test]
    async fn mint_transfer_fails_when_no_matching_transfer() {
        let tokenizer = MockTokenizer::new()
            .with_verification_outcome(MockVerificationOutcome::NoMatchingTransfer);

        let transfer = create_equity_transfer(
            Arc::new(tokenizer),
            Arc::new(MockRaindex::new()),
            Arc::new(MockWrapper::new()),
        )
        .await;

        let error = transfer
            .resume_equity_to_market_making(
                &issuer_request_id("ISS-TEST"),
                &Symbol::new("AAPL").unwrap(),
                FractionalShares::new(float!(100.0)),
            )
            .await
            .unwrap_err();

        assert!(
            matches!(
                error,
                MintTransferError::PostReceipt(MintError::Verification(
                    MintVerificationError::NoMatchingTransfer { .. }
                ))
            ),
            "Expected PostReceipt(Verification(NoMatchingTransfer)) error, got: {error:?}"
        );
    }

    #[tokio::test]
    async fn mint_transfer_fails_when_insufficient_transfer_amount() {
        let tokenizer = MockTokenizer::new()
            .with_verification_outcome(MockVerificationOutcome::InsufficientTransferAmount);

        let transfer = create_equity_transfer(
            Arc::new(tokenizer),
            Arc::new(MockRaindex::new()),
            Arc::new(MockWrapper::new()),
        )
        .await;

        let error = transfer
            .resume_equity_to_market_making(
                &issuer_request_id("ISS-TEST"),
                &Symbol::new("AAPL").unwrap(),
                FractionalShares::new(float!(100.0)),
            )
            .await
            .unwrap_err();

        assert!(
            matches!(
                error,
                MintTransferError::PostReceipt(MintError::Verification(
                    MintVerificationError::InsufficientTransferAmount { .. }
                ))
            ),
            "Expected Verification(InsufficientTransferAmount) error, got: {error:?}"
        );
    }

    #[tokio::test]
    async fn resume_mint_recovers_when_deposit_reverts() {
        let transfer = create_equity_transfer(
            Arc::new(MockTokenizer::new()),
            Arc::new(
                MockRaindex::new().with_deposit_behavior(DepositBehavior::FailExecutionReverted),
            ),
            Arc::new(MockWrapper::new()),
        )
        .await;

        let id = issuer_request_id("ISS-REVERT-RECOVERY");

        // Advance the aggregate to TokensWrapped state manually:
        // RequestMint -> MintAccepted -> TokensReceived -> WrapSubmitted
        // -> TokensWrapped
        transfer
            .mint_store
            .send(
                &id,
                TokenizedEquityMintCommand::RequestMint {
                    issuer_request_id: id.clone(),
                    symbol: Symbol::new("AAPL").unwrap(),
                    quantity: float!(10),
                    wallet: transfer.wallet,
                },
            )
            .await
            .unwrap();
        submit_requested_mint(&transfer, &id).await;

        // Poll advances to TokensReceived
        transfer
            .mint_store
            .send(&id, TokenizedEquityMintCommand::Poll)
            .await
            .unwrap();

        let wrap_tx = TxHash::random();
        transfer
            .mint_store
            .send(
                &id,
                TokenizedEquityMintCommand::SubmitWrap {
                    wrap_tx_hash: wrap_tx,
                },
            )
            .await
            .unwrap();

        let wrapped_shares = U256::from(10_000_000_000_000_000_000u128);
        transfer
            .mint_store
            .send(
                &id,
                TokenizedEquityMintCommand::WrapTokens {
                    wrap_tx_hash: wrap_tx,
                    wrapped_shares,
                    wrap_block: 1,
                },
            )
            .await
            .unwrap();

        // Verify we're in TokensWrapped state
        let entity = transfer.mint_store.load(&id).await.unwrap().unwrap();
        assert!(
            matches!(entity, TokenizedEquityMint::TokensWrapped { .. }),
            "Expected TokensWrapped, got: {entity:?}"
        );

        // Resume should detect zero balance and advance to terminal
        transfer.resume_mint(&id).await.unwrap();

        let entity = transfer.mint_store.load(&id).await.unwrap().unwrap();
        let TokenizedEquityMint::DepositedIntoRaindex {
            vault_deposit_tx_hash,
            ..
        } = entity
        else {
            panic!("Expected DepositedIntoRaindex after revert recovery, got: {entity:?}");
        };
        assert_eq!(
            vault_deposit_tx_hash,
            TxHash::ZERO,
            "Recovered deposit must use TxHash::ZERO sentinel"
        );
    }

    #[tokio::test]
    async fn resume_mint_from_wrap_submitted_recovers_when_deposit_reverts() {
        let mock_wrapper = MockWrapper::new();
        let wrap_tx = TxHash::random();

        // Pre-seed the mock so confirm_wrap recognises the tx hash that the
        // aggregate will store in WrapSubmitted state.
        mock_wrapper.seed_submitted_amount(wrap_tx, U256::from(10_000_000_000_000_000_000u128));

        let transfer = create_equity_transfer(
            Arc::new(MockTokenizer::new()),
            Arc::new(
                MockRaindex::new().with_deposit_behavior(DepositBehavior::FailExecutionReverted),
            ),
            Arc::new(mock_wrapper),
        )
        .await;

        let id = issuer_request_id("ISS-WRAP-SUBMITTED-RECOVERY");

        // Advance aggregate to WrapSubmitted state:
        // RequestMint -> MintAccepted -> TokensReceived -> WrapSubmitted
        transfer
            .mint_store
            .send(
                &id,
                TokenizedEquityMintCommand::RequestMint {
                    issuer_request_id: id.clone(),
                    symbol: Symbol::new("AAPL").unwrap(),
                    quantity: float!(10),
                    wallet: transfer.wallet,
                },
            )
            .await
            .unwrap();
        submit_requested_mint(&transfer, &id).await;

        transfer
            .mint_store
            .send(&id, TokenizedEquityMintCommand::Poll)
            .await
            .unwrap();

        transfer
            .mint_store
            .send(
                &id,
                TokenizedEquityMintCommand::SubmitWrap {
                    wrap_tx_hash: wrap_tx,
                },
            )
            .await
            .unwrap();

        let entity = transfer.mint_store.load(&id).await.unwrap().unwrap();
        assert!(
            matches!(entity, TokenizedEquityMint::WrapSubmitted { .. }),
            "Expected WrapSubmitted, got: {entity:?}"
        );

        // Resume from WrapSubmitted: confirms wrap, then deposit reverts,
        // recovery should advance to terminal state.
        transfer.resume_mint(&id).await.unwrap();

        let entity = transfer.mint_store.load(&id).await.unwrap().unwrap();
        let TokenizedEquityMint::DepositedIntoRaindex {
            vault_deposit_tx_hash,
            ..
        } = entity
        else {
            panic!(
                "Expected DepositedIntoRaindex after WrapSubmitted revert recovery, got: {entity:?}"
            );
        };
        assert_eq!(
            vault_deposit_tx_hash,
            TxHash::ZERO,
            "Recovered deposit must use TxHash::ZERO sentinel"
        );
    }

    #[tokio::test]
    async fn resume_mint_from_tokens_wrapped_calls_wait_for_block_with_wrap_block() {
        // Keep a typed reference so we can inspect both wait_for_block and
        // deposit call records after resume. Ordering is verified by confirming
        // both happened: wait_for_block must be called (the guard) and the
        // deposit must also succeed (proving the guard did not abort the flow).
        // Strict sequential ordering (wait_for_block strictly before deposit)
        // cannot be asserted with the current separate-mock seam — the
        // existing test `mint_transfer_fails_when_wait_for_block_fails` covers
        // the abort path, proving the guard is on the critical path.
        let mock_wrapper: Arc<MockWrapper> = Arc::new(MockWrapper::new());
        let mock_raindex: Arc<MockRaindex> = Arc::new(MockRaindex::new());
        let wrap_tx = TxHash::random();
        let wrap_block = 9999u64;

        let transfer = create_equity_transfer(
            Arc::new(MockTokenizer::new()),
            Arc::clone(&mock_raindex) as Arc<dyn Raindex>,
            Arc::clone(&mock_wrapper) as Arc<dyn Wrapper>,
        )
        .await;

        let id = issuer_request_id("ISS-TOKENS-WRAPPED-WAIT");

        // Advance aggregate to TokensWrapped state manually.
        transfer
            .mint_store
            .send(
                &id,
                TokenizedEquityMintCommand::RequestMint {
                    issuer_request_id: id.clone(),
                    symbol: Symbol::new("AAPL").unwrap(),
                    quantity: float!(10),
                    wallet: transfer.wallet,
                },
            )
            .await
            .unwrap();
        submit_requested_mint(&transfer, &id).await;

        transfer
            .mint_store
            .send(&id, TokenizedEquityMintCommand::Poll)
            .await
            .unwrap();

        transfer
            .mint_store
            .send(
                &id,
                TokenizedEquityMintCommand::SubmitWrap {
                    wrap_tx_hash: wrap_tx,
                },
            )
            .await
            .unwrap();

        let wrapped_shares = U256::from(10_000_000_000_000_000_000u128);
        transfer
            .mint_store
            .send(
                &id,
                TokenizedEquityMintCommand::WrapTokens {
                    wrap_tx_hash: wrap_tx,
                    wrapped_shares,
                    wrap_block,
                },
            )
            .await
            .unwrap();

        let entity = transfer.mint_store.load(&id).await.unwrap().unwrap();
        assert!(
            matches!(entity, TokenizedEquityMint::TokensWrapped { .. }),
            "Expected TokensWrapped state before resume, got: {entity:?}"
        );

        transfer.resume_mint(&id).await.unwrap();

        let calls = mock_wrapper.wait_for_block_calls();

        assert_eq!(
            calls,
            vec![wrap_block],
            "wait_for_block must be called exactly once with wrap_block={wrap_block} on TokensWrapped resume"
        );

        // Confirm the deposit also ran — proving wait_for_block did not abort
        // the flow and the guard is on the critical path to deposit.
        // (strict sequential ordering is covered by the abort test
        // `resume_mint_from_tokens_wrapped_fails_when_wait_for_block_fails`)
        assert_eq!(
            mock_raindex.last_deposited_token(),
            Some(Address::ZERO),
            "submit_deposit must have been called with the derivative token after wait_for_block on TokensWrapped resume"
        );
    }

    /// Verifies that `resume_mint` from `TokensWrapped` state with `wrap_block:
    /// None` skips `wait_for_block` entirely (backward-compat path for aggregates
    /// persisted before the field was added).
    ///
    /// If the `if let Some(block) = wrap_block` guard is accidentally removed,
    /// this test catches it: `wait_for_block` would be called with a sentinel
    /// value (0 or similar) and the assertion below would fail.
    #[tokio::test]
    async fn resume_mint_from_tokens_wrapped_skips_wait_for_block_when_wrap_block_is_none() {
        let mock_wrapper: Arc<MockWrapper> = Arc::new(MockWrapper::new());
        let wrap_tx = TxHash::random();

        let (transfer, pool) = create_equity_transfer_with_pool(
            Arc::new(MockTokenizer::new()),
            Arc::new(MockRaindex::new()),
            Arc::clone(&mock_wrapper) as Arc<dyn Wrapper>,
        )
        .await;

        let id = issuer_request_id("ISS-TOKENS-WRAPPED-NO-BLOCK");

        transfer
            .mint_store
            .send(
                &id,
                TokenizedEquityMintCommand::RequestMint {
                    issuer_request_id: id.clone(),
                    symbol: Symbol::new("AAPL").unwrap(),
                    quantity: float!(10),
                    wallet: transfer.wallet,
                },
            )
            .await
            .unwrap();
        submit_requested_mint(&transfer, &id).await;

        transfer
            .mint_store
            .send(&id, TokenizedEquityMintCommand::Poll)
            .await
            .unwrap();

        transfer
            .mint_store
            .send(
                &id,
                TokenizedEquityMintCommand::SubmitWrap {
                    wrap_tx_hash: wrap_tx,
                },
            )
            .await
            .unwrap();

        // Simulate a legacy aggregate: a `TokensWrapped` event persisted before
        // the `wrap_block` field existed. The live `WrapTokens` command now
        // requires `wrap_block`, so the only way to reach a `wrap_block: None`
        // state is to replay an old event -- seeded here directly with the
        // field omitted, exercising the `#[serde(default)]` backward-compat path.
        let wrapped_shares = U256::from(10_000_000_000_000_000_000u128);
        let legacy_payload = {
            let mut value = serde_json::to_value(TokenizedEquityMintEvent::TokensWrapped {
                wrap_tx_hash: wrap_tx,
                wrapped_shares,
                wrapped_at: Utc::now(),
                wrap_block: None,
            })
            .unwrap();
            value["TokensWrapped"]
                .as_object_mut()
                .unwrap()
                .remove("wrap_block");
            value.to_string()
        };

        let IssuerRequestId(raw_id) = &id;
        let next_sequence: i64 = sqlx::query_scalar(
            "SELECT COALESCE(MAX(sequence), -1) + 1 FROM events WHERE aggregate_id = ?",
        )
        .bind(raw_id.to_string())
        .fetch_one(&pool)
        .await
        .unwrap();

        insert_mint_event(
            &pool,
            &id,
            next_sequence,
            "TokenizedEquityMintEvent::TokensWrapped",
            &legacy_payload,
        )
        .await;

        let entity = transfer.mint_store.load(&id).await.unwrap().unwrap();
        assert!(
            matches!(entity, TokenizedEquityMint::TokensWrapped { .. }),
            "Expected TokensWrapped state before resume, got: {entity:?}"
        );

        transfer.resume_mint(&id).await.unwrap();

        assert_eq!(
            mock_wrapper.wait_for_block_calls(),
            Vec::<u64>::new(),
            "wait_for_block must NOT be called when wrap_block is None (legacy aggregate)"
        );
    }

    /// Verifies that `resume_mint` from `TokensWrapped` state propagates a
    /// `wait_for_block` failure as `MintError::Wrapper(WrapperError::Evm(..))`,
    /// and that the deposit does NOT run when `wait_for_block` fails.
    ///
    /// This proves that `wait_for_block` is on the critical path before the
    /// deposit in the `TokensWrapped` resume branch. A refactor that swaps the
    /// order (deposit first, wait_for_block second) would make this test fail
    /// while the happy-path test still passes.
    #[tokio::test]
    async fn resume_mint_from_tokens_wrapped_fails_when_wait_for_block_fails() {
        let mock_raindex: Arc<MockRaindex> = Arc::new(MockRaindex::new());
        let wrap_tx = TxHash::random();

        let transfer = create_equity_transfer(
            Arc::new(MockTokenizer::new()),
            Arc::clone(&mock_raindex) as Arc<dyn Raindex>,
            Arc::new(MockWrapper::failing_wait_for_block()),
        )
        .await;

        let id = issuer_request_id("ISS-TOKENS-WRAPPED-WAIT-FAIL");

        transfer
            .mint_store
            .send(
                &id,
                TokenizedEquityMintCommand::RequestMint {
                    issuer_request_id: id.clone(),
                    symbol: Symbol::new("AAPL").unwrap(),
                    quantity: float!(10),
                    wallet: transfer.wallet,
                },
            )
            .await
            .unwrap();
        submit_requested_mint(&transfer, &id).await;

        transfer
            .mint_store
            .send(&id, TokenizedEquityMintCommand::Poll)
            .await
            .unwrap();

        transfer
            .mint_store
            .send(
                &id,
                TokenizedEquityMintCommand::SubmitWrap {
                    wrap_tx_hash: wrap_tx,
                },
            )
            .await
            .unwrap();

        let wrapped_shares = U256::from(10_000_000_000_000_000_000u128);
        transfer
            .mint_store
            .send(
                &id,
                TokenizedEquityMintCommand::WrapTokens {
                    wrap_tx_hash: wrap_tx,
                    wrapped_shares,
                    wrap_block: 9999u64,
                },
            )
            .await
            .unwrap();

        let entity = transfer.mint_store.load(&id).await.unwrap().unwrap();
        assert!(
            matches!(entity, TokenizedEquityMint::TokensWrapped { .. }),
            "expected TokensWrapped state before resume, got: {entity:?}"
        );

        let error = transfer
            .resume_mint(&id)
            .await
            .expect_err("wait_for_block failure must propagate from TokensWrapped resume");

        assert!(
            matches!(
                error,
                MintError::Wrapper(WrapperError::Evm(EvmError::NodeBehindRequiredBlock { .. }))
            ),
            "expected MintError::Wrapper wrapping NodeBehindRequiredBlock, got: {error:?}"
        );

        assert!(
            mock_raindex.last_deposited_token().is_none(),
            "deposit must NOT run when wait_for_block fails in TokensWrapped resume"
        );
    }

    /// Verifies that `resume_mint` from `WrapSubmitted` state propagates a
    /// `wait_for_block` failure as `MintError::Wrapper(WrapperError::Evm(..))`.
    ///
    /// The `WrapSubmitted` branch calls `wait_for_block` unconditionally (block
    /// comes from a freshly confirmed tx). A missing `?` or wrong error mapping
    /// would let the flow silently proceed to deposit against a stale node --
    /// the exact regression this PR was written to prevent.
    #[tokio::test]
    async fn resume_mint_from_wrap_submitted_fails_when_wait_for_block_fails() {
        let mock_wrapper = MockWrapper::failing_wait_for_block();
        let wrap_tx = TxHash::random();

        // Pre-seed so confirm_wrap recognises the tx hash stored in WrapSubmitted.
        mock_wrapper.seed_submitted_amount(wrap_tx, U256::from(10_000_000_000_000_000_000u128));

        let transfer = create_equity_transfer(
            Arc::new(MockTokenizer::new()),
            Arc::new(MockRaindex::new()),
            Arc::new(mock_wrapper),
        )
        .await;

        let id = issuer_request_id("ISS-WRAP-SUBMITTED-WAIT-FAIL");

        // Advance to WrapSubmitted: RequestMint -> Poll -> SubmitWrap
        transfer
            .mint_store
            .send(
                &id,
                TokenizedEquityMintCommand::RequestMint {
                    issuer_request_id: id.clone(),
                    symbol: Symbol::new("AAPL").unwrap(),
                    quantity: float!(10),
                    wallet: transfer.wallet,
                },
            )
            .await
            .unwrap();
        submit_requested_mint(&transfer, &id).await;

        transfer
            .mint_store
            .send(&id, TokenizedEquityMintCommand::Poll)
            .await
            .unwrap();

        transfer
            .mint_store
            .send(
                &id,
                TokenizedEquityMintCommand::SubmitWrap {
                    wrap_tx_hash: wrap_tx,
                },
            )
            .await
            .unwrap();

        let entity = transfer.mint_store.load(&id).await.unwrap().unwrap();
        assert!(
            matches!(entity, TokenizedEquityMint::WrapSubmitted { .. }),
            "expected WrapSubmitted state before resume, got: {entity:?}"
        );

        let error = transfer
            .resume_mint(&id)
            .await
            .expect_err("wait_for_block failure must propagate from WrapSubmitted resume");

        assert!(
            matches!(
                error,
                MintError::Wrapper(WrapperError::Evm(EvmError::NodeBehindRequiredBlock { .. }))
            ),
            "expected MintError::Wrapper wrapping NodeBehindRequiredBlock, got: {error:?}"
        );
    }

    /// Verifies that the mint happy-path propagates a `wait_for_block` failure
    /// as `MintTransferError::PostReceipt(MintError::Wrapper(..))`.
    ///
    /// `finalize_received_mint` calls `wait_for_block(wrap_block)` unconditionally
    /// after wrapping. A missing `?` or wrong error mapping would let the flow
    /// silently deposit against a stale node.
    #[tokio::test]
    async fn mint_transfer_fails_when_wait_for_block_fails() {
        let transfer = create_equity_transfer(
            Arc::new(MockTokenizer::new()),
            Arc::new(MockRaindex::new()),
            Arc::new(MockWrapper::failing_wait_for_block()),
        )
        .await;

        let error = transfer
            .resume_equity_to_market_making(
                &issuer_request_id("ISS-WAIT-BLOCK-FAIL"),
                &Symbol::new("AAPL").unwrap(),
                FractionalShares::new(float!(10.0)),
            )
            .await
            .expect_err("wait_for_block failure must propagate in the mint happy path");

        assert!(
            matches!(
                error,
                MintTransferError::PostReceipt(MintError::Wrapper(WrapperError::Evm(
                    EvmError::NodeBehindRequiredBlock { .. }
                )))
            ),
            "expected PostReceipt(Wrapper(NodeBehindRequiredBlock)), got: {error:?}"
        );
    }
}
