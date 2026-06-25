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

mod job;
mod resume_job;
mod step;

#[cfg(test)]
pub(crate) use job::TransferEquityToMarketMakingJobError;
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
use sqlx::SqlitePool;
use std::sync::Arc;
use thiserror::Error;
use tracing::{debug, error, info, instrument, warn};

use st0x_event_sorcery::{AggregateError, LifecycleError, SendError, Store};
use st0x_evm::EvmError;
use st0x_execution::{FractionalShares, SharesConversionError, Symbol};
use st0x_raindex::{Raindex, RaindexError};
use st0x_wrapper::{WrapConfirmation, Wrapper, WrapperError};

use super::RebalancingService;
use super::trigger::RecoveryClaim;
use crate::equity_redemption::{
    DetectionFailure, EquityRedemption, EquityRedemptionCommand, RedemptionAggregateId, SendOutcome,
};
use crate::tokenization::{
    AlpacaTokenizationError, MintVerificationError, TokenizationRequest, TokenizationRequestStatus,
    Tokenizer, TokenizerError,
};
use crate::tokenized_equity_mint::{
    IssuerRequestId, TokenizationRequestId, TokenizedEquityMint, TokenizedEquityMintCommand,
    TokenizedEquityMintError,
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
        tokenization_request_id: TokenizationRequestId(raw_tokenization_request_id),
        received_tokens,
    })
}

#[derive(Debug, Error)]
pub(crate) enum MintError {
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
    #[error("Tokenization provider error: {0}")]
    Tokenizer(#[from] TokenizerError),
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

impl From<step::EquityVaultStepError> for MintError {
    fn from(error: step::EquityVaultStepError) -> Self {
        match error {
            step::EquityVaultStepError::Wrapper(error) => Self::Wrapper(error),
            step::EquityVaultStepError::Raindex(error) => Self::Raindex(error),
            step::EquityVaultStepError::VaultLookup(error) => Self::VaultLookup(error),
            other => unreachable!("withdrawal-specific step error on mint path: {other:?}"),
        }
    }
}

/// Distinguishes mint failures before vs after tokens were received from
/// Alpaca. Post-receipt failures must NOT clear the in-progress guard
/// because real tokens exist in the wallet and startup recovery will
/// resume them.
#[derive(Debug, Error)]
pub(crate) enum MintTransferError {
    /// Failure before Alpaca delivered tokens. Safe to clear guard and
    /// retry from scratch.
    #[error(transparent)]
    PreReceipt(MintError),

    /// Failure after tokens were received (verify/wrap/deposit stage).
    /// Tokens exist in the wallet; guard must stay set for recovery.
    #[error(transparent)]
    PostReceipt(MintError),
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

/// Returns the rejected domain error when a `RecordTokensReceived` send failed
/// because the aggregate deterministically refused the provider's completion
/// payload (missing tx hash, or a token-symbol mismatch/absence). These are
/// permanent -- a retry re-polls the same payload -- so callers drive the mint
/// to `Failed` instead of looping. `None` means the failure is transient
/// (store/infra) and should propagate for a normal retry.
fn provider_completion_rejection(
    error: &SendError<TokenizedEquityMint>,
) -> Option<&TokenizedEquityMintError> {
    let AggregateError::UserError(LifecycleError::Apply(domain_error)) = error else {
        return None;
    };

    match domain_error {
        TokenizedEquityMintError::MissingTxHash
        | TokenizedEquityMintError::TokenSymbolMismatch { .. }
        | TokenizedEquityMintError::MissingTokenSymbol { .. } => Some(domain_error),
        _ => None,
    }
}

#[derive(Debug, Error)]
pub(crate) enum RedemptionError {
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
    #[error(transparent)]
    Step(#[from] step::EquityVaultStepError),
    #[error(transparent)]
    NodeSync(#[from] EvmError),
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
pub(crate) struct CrossVenueEquityTransfer {
    raindex: Arc<dyn Raindex>,
    vault_lookup: Arc<dyn VaultLookup>,
    tokenizer: Arc<dyn Tokenizer>,
    wrapper: Arc<dyn Wrapper>,
    wallet: Address,
    mint_store: Arc<Store<TokenizedEquityMint>>,
    redemption_store: Arc<Store<EquityRedemption>>,
}

impl CrossVenueEquityTransfer {
    pub(crate) fn new(
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
        }
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
        let vault_deposit_tx_hash = step::submit_vault_deposit(
            self.vault_lookup.as_ref(),
            self.raindex.as_ref(),
            wrapped_token,
            wrapped_shares,
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

        let step::SubmittedWrap {
            wrapped_token: token,
            wrap_tx_hash,
        } = step::submit_wrap(
            self.wrapper.as_ref(),
            &tokens_received.symbol,
            tokens_received.shares_minted,
            self.wallet,
        )
        .await?;

        self.mint_store
            .send(
                issuer_request_id,
                TokenizedEquityMintCommand::SubmitWrap { wrap_tx_hash },
            )
            .await?;

        let WrapConfirmation { shares, block } =
            self.wrapper.confirm_wrap(token, wrap_tx_hash).await?;

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
                TokenizedEquityMint::MintAccepted {
                    symbol,
                    tokenization_request_id,
                    ..
                } => {
                    info!(%issuer_request_id, "Polling accepted mint");
                    match self
                        .tokenizer
                        .poll_mint_until_complete(&tokenization_request_id)
                        .await
                    {
                        Ok(completed) => match completed.status {
                            TokenizationRequestStatus::Completed => {
                                self.record_completed_mint(issuer_request_id, &symbol, completed)
                                    .await?;
                            }
                            TokenizationRequestStatus::Rejected => {
                                warn!(target: "rebalance", %symbol, "Mint rejected by Alpaca after acceptance");
                                self.mint_store
                                    .send(
                                        issuer_request_id,
                                        TokenizedEquityMintCommand::FailAcceptance {
                                            reason: "Rejected by Alpaca after acceptance"
                                                .to_string(),
                                        },
                                    )
                                    .await?;
                            }
                            TokenizationRequestStatus::Pending => {
                                warn!(target: "rebalance", %symbol, "Unexpected pending status after polling");
                                self.mint_store
                                    .send(
                                        issuer_request_id,
                                        TokenizedEquityMintCommand::FailAcceptance {
                                            reason: "Unexpected Pending status after polling"
                                                .to_string(),
                                        },
                                    )
                                    .await?;
                            }
                        },
                        Err(error) => {
                            warn!(target: "rebalance", %error, %symbol, "Mint polling failed");
                            self.mint_store
                                .send(
                                    issuer_request_id,
                                    TokenizedEquityMintCommand::FailAcceptance {
                                        reason: format!("Polling failed: {error}"),
                                    },
                                )
                                .await?;
                        }
                    }
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
                entity @ TokenizedEquityMint::MintRequested { .. } => {
                    return Err(MintError::UnexpectedState {
                        issuer_request_id: issuer_request_id.clone(),
                        expected_state: "MintAccepted, TokensReceived, or TokensWrapped",
                        entity: Box::new(entity),
                    });
                }
            }
        }
    }

    /// Records the provider's completion payload against the mint aggregate.
    ///
    /// When the aggregate deterministically rejects the payload (missing tx
    /// hash, or a token-symbol mismatch/absence), retrying re-polls the same bad
    /// data and traps the job in an endless retry loop. Such rejections are
    /// terminal: drive the mint to `Failed` via `FailAcceptance` so the job
    /// completes. Transient store failures propagate unchanged for a retry.
    async fn record_completed_mint(
        &self,
        issuer_request_id: &IssuerRequestId,
        symbol: &Symbol,
        completed: TokenizationRequest,
    ) -> Result<(), MintError> {
        let outcome = self
            .mint_store
            .send(
                issuer_request_id,
                TokenizedEquityMintCommand::RecordTokensReceived {
                    tx_hash: completed.tx_hash,
                    token_symbol: completed.token_symbol,
                    fees: completed.fees,
                },
            )
            .await;

        let Err(error) = outcome else {
            return Ok(());
        };

        let Some(rejection) = provider_completion_rejection(&error) else {
            return Err(error.into());
        };

        warn!(
            target: "rebalance",
            %symbol,
            %rejection,
            "Provider returned an unusable completion payload; failing the mint terminally"
        );

        self.mint_store
            .send(
                issuer_request_id,
                TokenizedEquityMintCommand::FailAcceptance {
                    reason: format!("Invalid provider completion payload: {rejection}"),
                },
            )
            .await?;

        Ok(())
    }

    /// Sends Redeem, then drives the withdraw I/O in the orchestrator,
    /// recording the submitted tx and the confirmation via pure commands.
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

        self.submit_and_confirm_withdraw(aggregate_id, token, amount)
            .await
    }

    /// Submits the vault withdrawal, records it, then confirms it. Shared by the
    /// fresh-start path and resume from `VaultWithdrawPending`.
    async fn submit_and_confirm_withdraw(
        &self,
        aggregate_id: &RedemptionAggregateId,
        token: Address,
        wrapped_amount: U256,
    ) -> Result<(), RedemptionError> {
        let tx_hash = step::submit_vault_withdraw(
            self.vault_lookup.as_ref(),
            self.raindex.as_ref(),
            token,
            wrapped_amount,
        )
        .await?;

        self.redemption_store
            .send(
                aggregate_id,
                EquityRedemptionCommand::SubmitWithdraw { tx_hash },
            )
            .await?;

        self.confirm_withdraw(aggregate_id, token, tx_hash).await
    }

    /// Confirms a submitted withdrawal and records the decoded receipt. Shared
    /// by resume from `VaultWithdrawSubmitted`.
    async fn confirm_withdraw(
        &self,
        aggregate_id: &RedemptionAggregateId,
        token: Address,
        tx_hash: TxHash,
    ) -> Result<(), RedemptionError> {
        let step::ConfirmedWithdraw {
            actual_wrapped_amount,
            raindex_withdraw_block,
        } = step::confirm_vault_withdraw(
            self.raindex.as_ref(),
            self.wrapper.as_ref(),
            token,
            tx_hash,
        )
        .await?;

        self.redemption_store
            .send(
                aggregate_id,
                EquityRedemptionCommand::ConfirmWithdraw {
                    actual_wrapped_amount,
                    raindex_withdraw_block,
                },
            )
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

        let (symbol, token, wrapped_amount, raindex_withdraw_block) =
            match self.load_redemption_entity(aggregate_id).await? {
                EquityRedemption::UnwrapPending {
                    symbol,
                    token,
                    wrapped_amount,
                    raindex_withdraw_block,
                    ..
                } => (symbol, token, wrapped_amount, raindex_withdraw_block),
                entity => return Err(RedemptionError::UnexpectedEntity { entity }),
            };

        self.submit_and_confirm_unwrap(
            aggregate_id,
            &symbol,
            token,
            wrapped_amount,
            raindex_withdraw_block,
        )
        .await?;

        info!(target: "rebalance", %aggregate_id, "Tokens unwrapped, sending to Alpaca");

        self.redemption_store
            .send(aggregate_id, EquityRedemptionCommand::PrepareSend)
            .await?;

        self.send_redemption(aggregate_id).await?;

        match self.load_redemption_entity(aggregate_id).await? {
            EquityRedemption::TokensSent { redemption_tx, .. } => Ok(redemption_tx),
            entity @ EquityRedemption::Failed { .. } => Err(RedemptionError::SendFailed { entity }),
            entity => Err(RedemptionError::UnexpectedEntity { entity }),
        }
    }

    /// Submits the ERC-4626 unwrap, records it, then confirms it. Shared by the
    /// fresh-start path and resume from `UnwrapPending`.
    async fn submit_and_confirm_unwrap(
        &self,
        aggregate_id: &RedemptionAggregateId,
        symbol: &Symbol,
        token: Address,
        wrapped_amount: U256,
        raindex_withdraw_block: Option<u64>,
    ) -> Result<(), RedemptionError> {
        let unwrap_tx_hash = step::submit_token_unwrap(
            self.wrapper.as_ref(),
            token,
            wrapped_amount,
            raindex_withdraw_block,
        )
        .await?;

        self.redemption_store
            .send(
                aggregate_id,
                EquityRedemptionCommand::SubmitUnwrap { unwrap_tx_hash },
            )
            .await?;

        self.confirm_unwrap(aggregate_id, symbol, token, unwrap_tx_hash)
            .await
    }

    /// Confirms a submitted unwrap and records the result. Shared by resume from
    /// `UnwrapSubmitted`.
    async fn confirm_unwrap(
        &self,
        aggregate_id: &RedemptionAggregateId,
        symbol: &Symbol,
        token: Address,
        unwrap_tx_hash: TxHash,
    ) -> Result<(), RedemptionError> {
        let step::ConfirmedUnwrap {
            underlying_token,
            unwrapped_amount,
            unwrap_block,
        } = step::confirm_token_unwrap(self.wrapper.as_ref(), symbol, token, unwrap_tx_hash)
            .await?;

        self.redemption_store
            .send(
                aggregate_id,
                EquityRedemptionCommand::ConfirmUnwrap {
                    underlying_token,
                    unwrapped_amount,
                    unwrap_block,
                },
            )
            .await?;
        Ok(())
    }

    /// Performs the send-to-Alpaca step (reading the persisted `SendPending`
    /// state) and records its outcome. Shared by the fresh-start path and resume
    /// from `SendPending`.
    async fn send_redemption(
        &self,
        aggregate_id: &RedemptionAggregateId,
    ) -> Result<(), RedemptionError> {
        let (symbol, underlying_token, unwrapped_amount, unwrap_block) =
            match self.load_redemption_entity(aggregate_id).await? {
                EquityRedemption::SendPending {
                    symbol,
                    underlying_token,
                    unwrapped_amount,
                    unwrap_block,
                    ..
                } => (symbol, underlying_token, unwrapped_amount, unwrap_block),
                entity => return Err(RedemptionError::UnexpectedEntity { entity }),
            };

        let outcome = self
            .send_to_alpaca(&symbol, underlying_token, unwrapped_amount, unwrap_block)
            .await?;

        match outcome {
            SendOutcome::Sent {
                redemption_wallet,
                redemption_tx,
            } => {
                // Persist the broadcast tx immediately, before finalizing, so a
                // crash here resumes by confirming the recorded tx rather than
                // re-broadcasting an irreversible transfer.
                self.redemption_store
                    .send(
                        aggregate_id,
                        EquityRedemptionCommand::SubmitSend {
                            redemption_wallet,
                            redemption_tx,
                        },
                    )
                    .await?;
                self.confirm_send(aggregate_id).await
            }
            failure @ (SendOutcome::WalletNotConfigured | SendOutcome::SendFailed) => {
                self.redemption_store
                    .send(
                        aggregate_id,
                        EquityRedemptionCommand::RecordSendOutcome { outcome: failure },
                    )
                    .await?;
                Ok(())
            }
        }
    }

    /// Finalizes a broadcast redemption send to `TokensSent` without
    /// re-broadcasting. Shared by the fresh-send path and resume from
    /// `SendSubmitted`.
    async fn confirm_send(
        &self,
        aggregate_id: &RedemptionAggregateId,
    ) -> Result<(), RedemptionError> {
        self.redemption_store
            .send(aggregate_id, EquityRedemptionCommand::ConfirmSend)
            .await?;
        Ok(())
    }

    /// Sends unwrapped tokens to Alpaca's redemption wallet, mapping the result
    /// to a [`SendOutcome`]. A missing wallet or a send revert is a terminal
    /// outcome (the aggregate records `TransferFailed`); a node-sync wait
    /// failure is a retryable error so apalis retries the job.
    async fn send_to_alpaca(
        &self,
        symbol: &Symbol,
        token: Address,
        amount: U256,
        unwrap_block: Option<u64>,
    ) -> Result<SendOutcome, RedemptionError> {
        let Some(redemption_wallet) = Tokenizer::redemption_wallet(self.tokenizer.as_ref()) else {
            warn!(target: "rebalance", %symbol, "Redemption wallet not configured");
            return Ok(SendOutcome::WalletNotConfigured);
        };

        // Wait for the tokenizer's provider to index the unwrap block before
        // sending, so the transfer is not simulated against a stale zero
        // balance. A wait failure is retryable (NOT a terminal send failure).
        if let Some(block) = unwrap_block {
            self.tokenizer.wait_for_block(block).await?;
        }

        info!(target: "rebalance", %token, %amount, "Sending unwrapped tokens for redemption");

        match Tokenizer::send_for_redemption(self.tokenizer.as_ref(), token, amount).await {
            Ok(redemption_tx) => Ok(SendOutcome::Sent {
                redemption_wallet,
                redemption_tx,
            }),
            Err(error) => {
                warn!(target: "rebalance", %error, %token, %amount, "Send for redemption failed");
                Ok(SendOutcome::SendFailed)
            }
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
                EquityRedemption::VaultWithdrawPending {
                    token,
                    wrapped_amount,
                    ..
                } => {
                    info!(%aggregate_id, "Resuming pending vault withdrawal");
                    self.submit_and_confirm_withdraw(aggregate_id, token, wrapped_amount)
                        .await?;
                }
                EquityRedemption::VaultWithdrawSubmitted { token, tx_hash, .. } => {
                    info!(%aggregate_id, "Resuming submitted vault withdrawal");
                    self.confirm_withdraw(aggregate_id, token, tx_hash).await?;
                }
                EquityRedemption::WithdrawnFromRaindex { .. } => {
                    self.resume_withdrawn_redemption(aggregate_id).await?;
                }
                EquityRedemption::UnwrapPending {
                    symbol,
                    token,
                    wrapped_amount,
                    raindex_withdraw_block,
                    ..
                } => {
                    info!(%aggregate_id, "Resuming pending unwrap");
                    self.submit_and_confirm_unwrap(
                        aggregate_id,
                        &symbol,
                        token,
                        wrapped_amount,
                        raindex_withdraw_block,
                    )
                    .await?;
                }
                EquityRedemption::UnwrapSubmitted {
                    symbol,
                    token,
                    unwrap_tx_hash,
                    ..
                } => {
                    info!(%aggregate_id, "Resuming submitted unwrap");
                    self.confirm_unwrap(aggregate_id, &symbol, token, unwrap_tx_hash)
                        .await?;
                }
                EquityRedemption::TokensUnwrapped { .. } => {
                    self.resume_unwrapped_redemption(aggregate_id).await?;
                }
                EquityRedemption::SendPending { .. } => {
                    info!(%aggregate_id, "Resuming pending send");
                    self.send_redemption(aggregate_id).await?;
                }
                EquityRedemption::SendSubmitted { .. } => {
                    info!(%aggregate_id, "Resuming submitted send");
                    self.confirm_send(aggregate_id).await?;
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
    pub(crate) async fn resume_equity_to_market_making(
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
            ) => self.resume_mint_classified(issuer_request_id).await,
            Some(_) => self
                .resume_mint(issuer_request_id)
                .await
                .map_err(MintTransferError::PostReceipt),
        }
    }

    /// Runs `resume_mint` and, on failure, reloads the aggregate to bucket the
    /// error as pre- or post-receipt. `resume_mint`'s error is only actionable
    /// once classified by how far the aggregate advanced, so the resume and the
    /// classification are coupled here rather than repeated at each call site.
    async fn resume_mint_classified(
        &self,
        issuer_request_id: &IssuerRequestId,
    ) -> Result<(), MintTransferError> {
        if let Err(error) = self.resume_mint(issuer_request_id).await {
            let reached = self.load_mint_entity(issuer_request_id).await;
            return Err(classify_mint_resume_error(reached, error));
        }

        Ok(())
    }

    async fn start_mint(
        &self,
        issuer_request_id: &IssuerRequestId,
        symbol: &Symbol,
        quantity: FractionalShares,
    ) -> Result<(), MintTransferError> {
        debug!(target: "rebalance", %issuer_request_id, wallet = %self.wallet, "Requesting mint");

        // Pre-receipt I/O: no tokens exist yet, so this stage is safe to retry.
        // issuer_request_id is the provider-side dedup key: if the process
        // crashes after request_mint returns but before the RecordMintRequested
        // commit, the retry re-enters start_mint and re-calls request_mint with
        // the same id, and Alpaca deduplicates on issuer_request_id so no
        // duplicate mint request is created.
        let request = self
            .tokenizer
            .request_mint(
                symbol.clone(),
                quantity,
                self.wallet,
                issuer_request_id.clone(),
            )
            .await
            .map_err(|error| MintTransferError::PreReceipt(error.into()))?;

        if matches!(request.status, TokenizationRequestStatus::Rejected) {
            warn!(target: "rebalance", %symbol, "Mint request rejected by Alpaca");
            self.mint_store
                .send(
                    issuer_request_id,
                    TokenizedEquityMintCommand::RejectMintRequest {
                        symbol: symbol.clone(),
                        quantity: quantity.inner(),
                        wallet: self.wallet,
                        reason: "Rejected by Alpaca".to_string(),
                    },
                )
                .await
                .map_err(|error| MintTransferError::PreReceipt(error.into()))?;

            // Aggregate is now Failed; nothing further to drive.
            return Ok(());
        }

        info!(target: "rebalance", %symbol, request_id = %request.id.0, "Mint request accepted, polling for completion");

        // A concurrent worker (or a retry that overlapped a prior pass) may have
        // initialized this aggregate while request_mint was in flight. Recording
        // again would either be a redundant no-op (same tokenization_request_id)
        // or wedge on AlreadyInProgress (a second, non-deduplicated Alpaca id),
        // so when the aggregate is already accepted, resume the persisted request
        // instead of re-recording.
        if let Some(TokenizedEquityMint::MintAccepted {
            tokenization_request_id: accepted,
            ..
        }) = self
            .mint_store
            .load(issuer_request_id)
            .await
            .map_err(|error| MintTransferError::PreReceipt(error.into()))?
        {
            if accepted != request.id {
                error!(
                    target: "rebalance",
                    %symbol,
                    persisted_request_id = %accepted.0,
                    duplicate_request_id = %request.id.0,
                    "Alpaca returned a second tokenization_request_id for an already-accepted \
                     mint; resuming the persisted request. The duplicate needs operator cleanup."
                );
            }

            return self.resume_mint_classified(issuer_request_id).await;
        }

        self.mint_store
            .send(
                issuer_request_id,
                TokenizedEquityMintCommand::RecordMintRequested {
                    issuer_request_id: issuer_request_id.clone(),
                    symbol: symbol.clone(),
                    quantity: quantity.inner(),
                    wallet: self.wallet,
                    tokenization_request_id: request.id,
                },
            )
            .await
            .map_err(|error| MintTransferError::PreReceipt(error.into()))?;

        // resume_mint drives poll -> finalize; resume_mint_classified buckets
        // its failure by how far the aggregate advanced (pre- vs post-receipt).
        self.resume_mint_classified(issuer_request_id).await
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
    pub(crate) async fn resume_equity_to_hedging(
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
    use alloy::primitives::{Address, B256, address};
    use chrono::Utc;
    use sqlx::SqlitePool;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::broadcast;

    use st0x_dto::Statement;
    use st0x_event_sorcery::{StoreBuilder, test_store};
    use st0x_execution::{FractionalShares, Symbol};
    use st0x_float_macro::float;

    use st0x_config::{AssetsConfig, EquitiesConfig};
    use st0x_raindex::RaindexVaultId;
    use st0x_wrapper::MockWrapper;

    use super::*;
    use crate::equity_redemption::redemption_aggregate_id;
    use crate::inventory::{
        BroadcastingInventory, ImbalanceThreshold, Inventory, InventoryView, Venue,
    };
    use crate::onchain::mock::{DepositBehavior, MockRaindex};
    use crate::rebalancing::{RebalancingSchedulers, RebalancingServiceConfig};
    use crate::tokenization::TokenizationRequest;
    use crate::tokenization::mock::{
        MockCompletionOutcome, MockDetectionOutcome, MockMintPollOutcome, MockMintRequestOutcome,
        MockTokenizer, MockVerificationOutcome,
    };
    use crate::tokenized_equity_mint::{TokenizedEquityMintEvent, issuer_request_id};
    use crate::usdc_rebalance::UsdcRebalance;
    use crate::vault_lookup::MockVaultLookup;
    use crate::vault_registry::VaultRegistry;

    fn mock_vault_lookup() -> MockVaultLookup {
        MockVaultLookup::new()
            .with_symbol_token(Symbol::new("TEST").unwrap(), Address::ZERO)
            .with_vault(Address::ZERO, RaindexVaultId(B256::ZERO))
            .with_default_vault(RaindexVaultId(B256::ZERO))
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
                equity: ImbalanceThreshold {
                    target: float!(0.5),
                    deviation: float!(0.2),
                },
                usdc: None,
                transfer_timeout: Duration::from_secs(1800),
                assets: AssetsConfig {
                    equities: EquitiesConfig::default(),
                    cash: None,
                },
            },
            Arc::new(test_store::<VaultRegistry>(pool.clone(), ())),
            address!("0x0000000000000000000000000000000000000001"),
            address!("0x0000000000000000000000000000000000000002"),
            inventory.clone(),
            Arc::new(MockWrapper::new()),
            RebalancingSchedulers::new(&apalis_pool),
            Arc::new(crate::alerts::NoopNotifier),
        ));

        // Reactor-wired stores -- the production wiring that dispatches committed
        // events to the reactor's `on_mint`.
        let mint_store = StoreBuilder::<TokenizedEquityMint>::new(pool.clone())
            .with(service.clone())
            .build(())
            .await
            .unwrap();
        let redemption_store = StoreBuilder::<EquityRedemption>::new(pool.clone())
            .with(service.clone())
            .build(())
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
        completed_request.id = TokenizationRequestId("tok-1".to_string());
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
                equity: ImbalanceThreshold {
                    target: float!(0.5),
                    deviation: float!(0.2),
                },
                usdc: None,
                transfer_timeout: Duration::from_secs(1800),
                assets: AssetsConfig {
                    equities: EquitiesConfig::default(),
                    cash: None,
                },
            },
            Arc::new(test_store::<VaultRegistry>(pool.clone(), ())),
            address!("0x0000000000000000000000000000000000000001"),
            address!("0x0000000000000000000000000000000000000002"),
            inventory.clone(),
            Arc::new(MockWrapper::new()),
            RebalancingSchedulers::new(&apalis_pool),
            Arc::new(crate::alerts::NoopNotifier),
        ));

        let mint_store = StoreBuilder::<TokenizedEquityMint>::new(pool.clone())
            .with(service.clone())
            .build(())
            .await
            .unwrap();
        let redemption_store = StoreBuilder::<EquityRedemption>::new(pool.clone())
            .with(service.clone())
            .build(())
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
        completed_request.id = TokenizationRequestId("tok-1".to_string());
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
                TokenizedEquityMintCommand::RecordMintRequested {
                    issuer_request_id: id.clone(),
                    symbol: Symbol::new("AAPL").unwrap(),
                    quantity: float!(10),
                    wallet: transfer.wallet,
                    tokenization_request_id: TokenizationRequestId("test-req-id".to_string()),
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
                TokenizedEquityMintCommand::RecordMintRequested {
                    issuer_request_id: id.clone(),
                    symbol: Symbol::new("AAPL").unwrap(),
                    quantity: float!(10),
                    wallet: transfer.wallet,
                    tokenization_request_id: TokenizationRequestId("test-req-id".to_string()),
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
                equity: ImbalanceThreshold {
                    target: float!(0.5),
                    deviation: float!(0.2),
                },
                usdc: None,
                transfer_timeout: Duration::from_secs(1800),
                assets: AssetsConfig {
                    equities: EquitiesConfig::default(),
                    cash: None,
                },
            },
            Arc::new(test_store::<VaultRegistry>(pool.clone(), ())),
            address!("0x0000000000000000000000000000000000000001"),
            address!("0x0000000000000000000000000000000000000002"),
            inventory,
            Arc::new(MockWrapper::new()),
            RebalancingSchedulers::new(&apalis_pool),
            Arc::new(crate::alerts::NoopNotifier),
        ));

        let mint_store = StoreBuilder::<TokenizedEquityMint>::new(pool.clone())
            .with(service.clone())
            .build(())
            .await
            .unwrap();
        let redemption_store = StoreBuilder::<EquityRedemption>::new(pool.clone())
            .with(service.clone())
            .build(())
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
        let mint_store = Arc::new(test_store::<TokenizedEquityMint>(pool.clone(), ()));
        let redemption_store = Arc::new(test_store::<EquityRedemption>(pool.clone(), ()));
        let vault_lookup = mock_vault_lookup();

        let transfer = CrossVenueEquityTransfer::new(
            raindex,
            Arc::new(vault_lookup),
            tokenizer,
            wrapper,
            Address::random(),
            mint_store,
            redemption_store,
        );

        (transfer, pool)
    }

    #[tokio::test]
    async fn send_to_alpaca_returns_wallet_not_configured_when_no_wallet() {
        let transfer = create_equity_transfer(
            Arc::new(MockTokenizer::new().with_no_redemption_wallet()),
            Arc::new(MockRaindex::new()),
            Arc::new(MockWrapper::new()),
        )
        .await;

        let outcome = transfer
            .send_to_alpaca(
                &Symbol::new("AAPL").unwrap(),
                Address::ZERO,
                U256::from(1_000_000_000_000_000_000_u64),
                None,
            )
            .await
            .unwrap();

        assert!(
            matches!(outcome, SendOutcome::WalletNotConfigured),
            "expected WalletNotConfigured, got {outcome:?}"
        );
    }

    #[tokio::test]
    async fn send_to_alpaca_returns_send_failed_on_send_error() {
        let transfer = create_equity_transfer(
            Arc::new(MockTokenizer::new().with_send_failure()),
            Arc::new(MockRaindex::new()),
            Arc::new(MockWrapper::new()),
        )
        .await;

        let outcome = transfer
            .send_to_alpaca(
                &Symbol::new("AAPL").unwrap(),
                Address::ZERO,
                U256::from(1_000_000_000_000_000_000_u64),
                None,
            )
            .await
            .unwrap();

        assert!(
            matches!(outcome, SendOutcome::SendFailed),
            "expected SendFailed when send_for_redemption errors, got {outcome:?}"
        );
    }

    #[tokio::test]
    async fn send_to_alpaca_waits_for_unwrap_block_then_sends() {
        let unwrap_block = 4242u64;
        let tokenizer = Arc::new(MockTokenizer::new());
        let transfer = create_equity_transfer(
            tokenizer.clone(),
            Arc::new(MockRaindex::new()),
            Arc::new(MockWrapper::new()),
        )
        .await;

        let outcome = transfer
            .send_to_alpaca(
                &Symbol::new("AAPL").unwrap(),
                Address::ZERO,
                U256::from(1_000_000_000_000_000_000_u64),
                Some(unwrap_block),
            )
            .await
            .unwrap();

        assert!(
            matches!(outcome, SendOutcome::Sent { .. }),
            "expected Sent, got {outcome:?}"
        );
        assert_eq!(
            tokenizer.wait_for_block_calls(),
            vec![unwrap_block],
            "send must wait for the unwrap block before sending"
        );
    }

    #[tokio::test]
    async fn send_to_alpaca_skips_wait_when_unwrap_block_is_none() {
        let tokenizer = Arc::new(MockTokenizer::new());
        let transfer = create_equity_transfer(
            tokenizer.clone(),
            Arc::new(MockRaindex::new()),
            Arc::new(MockWrapper::new()),
        )
        .await;

        let outcome = transfer
            .send_to_alpaca(
                &Symbol::new("AAPL").unwrap(),
                Address::ZERO,
                U256::from(1_000_000_000_000_000_000_u64),
                None,
            )
            .await
            .unwrap();

        assert!(matches!(outcome, SendOutcome::Sent { .. }));
        assert_eq!(
            tokenizer.wait_for_block_calls(),
            Vec::<u64>::new(),
            "send must NOT wait for a block when unwrap_block is None"
        );
    }

    #[tokio::test]
    async fn send_to_alpaca_returns_node_sync_error_when_wait_fails() {
        let tokenizer = Arc::new(MockTokenizer::new().failing_wait_for_block());
        let transfer = create_equity_transfer(
            tokenizer.clone(),
            Arc::new(MockRaindex::new()),
            Arc::new(MockWrapper::new()),
        )
        .await;

        let error = transfer
            .send_to_alpaca(
                &Symbol::new("AAPL").unwrap(),
                Address::ZERO,
                U256::from(1_000_000_000_000_000_000_u64),
                Some(99),
            )
            .await
            .unwrap_err();

        assert!(
            matches!(
                error,
                RedemptionError::NodeSync(EvmError::NodeBehindRequiredBlock { .. })
            ),
            "a wait-for-block failure must surface as a retryable NodeSync error, got {error:?}"
        );
        assert_eq!(
            tokenizer.redemption_send_count(),
            0,
            "tokens must NOT be sent when the node-sync wait fails (wait happens first)"
        );
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
    async fn start_mint_request_api_error_is_pre_receipt() {
        let transfer = create_equity_transfer(
            Arc::new(
                MockTokenizer::new().with_mint_request_outcome(MockMintRequestOutcome::ApiError),
            ),
            Arc::new(MockRaindex::new()),
            Arc::new(MockWrapper::new()),
        )
        .await;

        let error = transfer
            .resume_equity_to_market_making(
                &issuer_request_id("ISS-REQ-ERR"),
                &Symbol::new("AAPL").unwrap(),
                FractionalShares::new(float!(10.0)),
            )
            .await
            .unwrap_err();

        assert!(
            matches!(
                error,
                MintTransferError::PreReceipt(MintError::Tokenizer(_))
            ),
            "request_mint failure must be pre-receipt, got: {error:?}"
        );
    }

    #[tokio::test]
    async fn start_mint_alpaca_rejection_records_failed_and_returns_ok() {
        let transfer = create_equity_transfer(
            Arc::new(
                MockTokenizer::new().with_mint_request_outcome(MockMintRequestOutcome::Rejected),
            ),
            Arc::new(MockRaindex::new()),
            Arc::new(MockWrapper::new()),
        )
        .await;

        let id = issuer_request_id("ISS-REQ-REJECT");
        // A fresh Alpaca rejection is terminal, not retryable: the job entry
        // point returns Ok so apalis marks it done rather than retrying forever.
        transfer
            .resume_equity_to_market_making(
                &id,
                &Symbol::new("AAPL").unwrap(),
                FractionalShares::new(float!(10.0)),
            )
            .await
            .unwrap();

        let entity = transfer.mint_store.load(&id).await.unwrap().unwrap();
        assert!(
            matches!(entity, TokenizedEquityMint::Failed { .. }),
            "rejected mint must be Failed, got: {entity:?}"
        );
    }

    /// The issuer_request_id is the provider-side dedup key: a job retry must
    /// resume the persisted aggregate rather than issue a second Alpaca mint
    /// request for the same transfer. Proven by re-entering the entry point with
    /// the same id and asserting request_mint ran exactly once.
    #[tokio::test]
    async fn retry_resumes_without_a_second_mint_request() {
        let tokenizer = Arc::new(MockTokenizer::new());
        let transfer = create_equity_transfer(
            tokenizer.clone(),
            Arc::new(MockRaindex::new()),
            Arc::new(MockWrapper::new()),
        )
        .await;

        let id = issuer_request_id("ISS-DEDUP");
        let symbol = Symbol::new("AAPL").unwrap();

        // First pass: a fresh transfer issues exactly one mint request, keyed by
        // the issuer_request_id, and drives the mint to completion.
        transfer
            .resume_equity_to_market_making(&id, &symbol, FractionalShares::new(float!(10.0)))
            .await
            .unwrap();

        assert_eq!(tokenizer.request_mint_count(), 1);
        assert_eq!(tokenizer.last_issuer_request_id(), Some(id.clone()));

        // Retry with the same id: the resume path takes over and must NOT issue a
        // second Alpaca mint request -- otherwise the dedup key bought nothing.
        transfer
            .resume_equity_to_market_making(&id, &symbol, FractionalShares::new(float!(10.0)))
            .await
            .unwrap();

        assert_eq!(
            tokenizer.request_mint_count(),
            1,
            "a retry must resume the persisted mint, not request a duplicate"
        );
    }

    /// A concurrent worker (or overlapping retry) can initialize the aggregate
    /// while request_mint is in flight. If Alpaca then returns a different,
    /// non-deduplicated tokenization_request_id, start_mint must resume the
    /// persisted request rather than wedge on AlreadyInProgress.
    #[tokio::test]
    async fn start_mint_with_conflicting_request_id_resumes_persisted_mint() {
        let transfer = create_equity_transfer(
            Arc::new(MockTokenizer::new()),
            Arc::new(MockRaindex::new()),
            Arc::new(MockWrapper::new()),
        )
        .await;

        let id = issuer_request_id("ISS-CONFLICT-ID");
        let symbol = Symbol::new("AAPL").unwrap();

        // Stand in for the concurrent worker: the aggregate is already accepted
        // under a different tokenization_request_id than the mock's request_mint
        // hands back (MOCK_REQ_ID).
        transfer
            .mint_store
            .send(
                &id,
                TokenizedEquityMintCommand::RecordMintRequested {
                    issuer_request_id: id.clone(),
                    symbol: symbol.clone(),
                    quantity: float!(10),
                    wallet: transfer.wallet,
                    tokenization_request_id: TokenizationRequestId("ORIGINAL_REQ".to_string()),
                },
            )
            .await
            .unwrap();

        // start_mint requests a (duplicate) mint, sees the already-accepted
        // aggregate, and resumes the persisted request rather than re-recording.
        transfer
            .start_mint(&id, &symbol, FractionalShares::new(float!(10)))
            .await
            .expect("a conflicting request id must resume, not wedge");

        let entity = transfer.mint_store.load(&id).await.unwrap().unwrap();
        let TokenizedEquityMint::DepositedIntoRaindex {
            tokenization_request_id,
            ..
        } = entity
        else {
            panic!("Expected the persisted mint to complete, got: {entity:?}");
        };
        assert_eq!(
            tokenization_request_id,
            TokenizationRequestId("ORIGINAL_REQ".to_string()),
            "the persisted request id must survive -- the duplicate must not overwrite it"
        );
    }

    #[tokio::test]
    async fn resume_mint_poll_rejected_records_failed() {
        let transfer = create_equity_transfer(
            Arc::new(MockTokenizer::new().with_mint_poll_outcome(MockMintPollOutcome::Rejected)),
            Arc::new(MockRaindex::new()),
            Arc::new(MockWrapper::new()),
        )
        .await;

        let id = issuer_request_id("ISS-POLL-REJECT");
        transfer
            .resume_equity_to_market_making(
                &id,
                &Symbol::new("AAPL").unwrap(),
                FractionalShares::new(float!(10.0)),
            )
            .await
            .unwrap();

        let entity = transfer.mint_store.load(&id).await.unwrap().unwrap();
        assert!(
            matches!(entity, TokenizedEquityMint::Failed { .. }),
            "poll rejection must mark the mint Failed, got: {entity:?}"
        );
    }

    #[tokio::test]
    async fn resume_mint_poll_pending_records_failed() {
        let transfer = create_equity_transfer(
            Arc::new(MockTokenizer::new().with_mint_poll_outcome(MockMintPollOutcome::Pending)),
            Arc::new(MockRaindex::new()),
            Arc::new(MockWrapper::new()),
        )
        .await;

        let id = issuer_request_id("ISS-POLL-PENDING");
        transfer
            .resume_equity_to_market_making(
                &id,
                &Symbol::new("AAPL").unwrap(),
                FractionalShares::new(float!(10.0)),
            )
            .await
            .unwrap();

        let entity = transfer.mint_store.load(&id).await.unwrap().unwrap();
        assert!(
            matches!(entity, TokenizedEquityMint::Failed { .. }),
            "unexpected pending poll must mark the mint Failed, got: {entity:?}"
        );
    }

    #[tokio::test]
    async fn resume_mint_poll_error_records_failed() {
        let transfer = create_equity_transfer(
            Arc::new(MockTokenizer::new().with_mint_poll_outcome(MockMintPollOutcome::PollError)),
            Arc::new(MockRaindex::new()),
            Arc::new(MockWrapper::new()),
        )
        .await;

        let id = issuer_request_id("ISS-POLL-ERR");
        transfer
            .resume_equity_to_market_making(
                &id,
                &Symbol::new("AAPL").unwrap(),
                FractionalShares::new(float!(10.0)),
            )
            .await
            .unwrap();

        let entity = transfer.mint_store.load(&id).await.unwrap().unwrap();
        assert!(
            matches!(entity, TokenizedEquityMint::Failed { .. }),
            "poll error must mark the mint Failed, got: {entity:?}"
        );
    }

    /// After a poll rejection drives the mint to `Failed`, the apalis retry
    /// re-enters the job entry point. A terminal aggregate must be a clean
    /// no-op so the retry does not loop on the rejected mint.
    #[tokio::test]
    async fn resume_equity_to_market_making_is_noop_on_failed_mint() {
        let transfer = create_equity_transfer(
            Arc::new(MockTokenizer::new().with_mint_poll_outcome(MockMintPollOutcome::Rejected)),
            Arc::new(MockRaindex::new()),
            Arc::new(MockWrapper::new()),
        )
        .await;

        let id = issuer_request_id("ISS-POLL-REJECT-NOOP");
        let symbol = Symbol::new("AAPL").unwrap();

        transfer
            .resume_equity_to_market_making(&id, &symbol, FractionalShares::new(float!(10.0)))
            .await
            .unwrap();

        let entity = transfer.mint_store.load(&id).await.unwrap().unwrap();
        assert!(
            matches!(entity, TokenizedEquityMint::Failed { .. }),
            "poll rejection must mark the mint Failed, got: {entity:?}"
        );

        transfer
            .resume_equity_to_market_making(&id, &symbol, FractionalShares::new(float!(10.0)))
            .await
            .expect("a Failed mint must be a clean no-op for the job retry");
    }

    #[tokio::test]
    async fn resume_mint_poll_wrong_token_symbol_fails_terminally() {
        let transfer = create_equity_transfer(
            Arc::new(MockTokenizer::new().with_token_symbol_override("tGME")),
            Arc::new(MockRaindex::new()),
            Arc::new(MockWrapper::new()),
        )
        .await;

        let id = issuer_request_id("ISS-WRONG-SYM");
        let symbol = Symbol::new("AAPL").unwrap();

        // A mismatched token symbol is a permanent provider-payload rejection:
        // re-polling returns the same bad symbol, so the job must complete (Ok)
        // with the mint driven to Failed rather than retrying forever.
        transfer
            .resume_equity_to_market_making(&id, &symbol, FractionalShares::new(float!(10.0)))
            .await
            .expect("a mismatched token symbol must terminally fail, not error for retry");

        let entity = transfer.mint_store.load(&id).await.unwrap().unwrap();
        assert!(
            matches!(entity, TokenizedEquityMint::Failed { .. }),
            "mismatched token symbol must drive the mint to Failed, got: {entity:?}"
        );

        // The retry is a clean no-op: the terminal aggregate is not re-driven.
        transfer
            .resume_equity_to_market_making(&id, &symbol, FractionalShares::new(float!(10.0)))
            .await
            .expect("a Failed mint must be a clean no-op for the job retry");
    }

    #[tokio::test]
    async fn resume_mint_poll_missing_token_symbol_fails_terminally() {
        let transfer = create_equity_transfer(
            Arc::new(MockTokenizer::new().with_no_token_symbol()),
            Arc::new(MockRaindex::new()),
            Arc::new(MockWrapper::new()),
        )
        .await;

        let id = issuer_request_id("ISS-MISSING-SYM");
        let symbol = Symbol::new("AAPL").unwrap();

        // A missing token symbol is a permanent provider-payload rejection, so
        // the job must complete (Ok) with the mint driven to Failed.
        transfer
            .resume_equity_to_market_making(&id, &symbol, FractionalShares::new(float!(10.0)))
            .await
            .expect("a missing token symbol must terminally fail, not error for retry");

        let entity = transfer.mint_store.load(&id).await.unwrap().unwrap();
        assert!(
            matches!(entity, TokenizedEquityMint::Failed { .. }),
            "missing token symbol must drive the mint to Failed, got: {entity:?}"
        );

        // The retry is a clean no-op: the terminal aggregate is not re-driven.
        transfer
            .resume_equity_to_market_making(&id, &symbol, FractionalShares::new(float!(10.0)))
            .await
            .expect("a Failed mint must be a clean no-op for the job retry");
    }

    #[tokio::test]
    async fn resume_mint_records_provider_fees() {
        // Wrap fails so the flow halts right after TokensReceived is recorded,
        // letting us inspect the persisted fees forwarded from the provider.
        let transfer = create_equity_transfer(
            Arc::new(MockTokenizer::new().with_fees(float!(0.25))),
            Arc::new(MockRaindex::new()),
            Arc::new(MockWrapper::failing()),
        )
        .await;

        let id = issuer_request_id("ISS-FEES");
        transfer
            .resume_equity_to_market_making(
                &id,
                &Symbol::new("AAPL").unwrap(),
                FractionalShares::new(float!(10.0)),
            )
            .await
            .unwrap_err();

        let entity = transfer.mint_store.load(&id).await.unwrap().unwrap();
        let TokenizedEquityMint::TokensReceived {
            fees: Some(fees), ..
        } = entity
        else {
            panic!("expected TokensReceived carrying fees, got: {entity:?}");
        };
        assert!(
            fees.eq(float!(0.25)).unwrap(),
            "provider fees must propagate to TokensReceived"
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
                TokenizedEquityMintCommand::RecordMintRequested {
                    issuer_request_id: id.clone(),
                    symbol: Symbol::new("AAPL").unwrap(),
                    quantity: float!(10),
                    wallet: transfer.wallet,
                    tokenization_request_id: TokenizationRequestId("tok-1".to_string()),
                },
            )
            .await
            .unwrap();

        transfer.resume_mint(&id).await.unwrap();

        let entity = transfer.mint_store.load(&id).await.unwrap().unwrap();
        assert!(
            matches!(entity, TokenizedEquityMint::DepositedIntoRaindex { .. }),
            "Expected deposited mint after resume, got: {entity:?}"
        );
    }

    /// Re-running the job entry point with an id whose aggregate already
    /// exists must resume from the persisted state (the crash-recovery path)
    /// rather than re-requesting the mint from Alpaca.
    #[tokio::test]
    async fn resume_equity_to_market_making_resumes_existing_aggregate() {
        let transfer = create_equity_transfer(
            Arc::new(MockTokenizer::new()),
            Arc::new(MockRaindex::new()),
            Arc::new(MockWrapper::new()),
        )
        .await;

        let id = issuer_request_id("ISS-CRASH-RESUME");
        let symbol = Symbol::new("AAPL").unwrap();

        // First attempt crashes after the mint request was accepted: the
        // aggregate is persisted in MintAccepted.
        transfer
            .mint_store
            .send(
                &id,
                TokenizedEquityMintCommand::RecordMintRequested {
                    issuer_request_id: id.clone(),
                    symbol: symbol.clone(),
                    quantity: float!(10),
                    wallet: transfer.wallet,
                    tokenization_request_id: TokenizationRequestId("tok-1".to_string()),
                },
            )
            .await
            .unwrap();

        // The re-enqueued job runs the same entry point with the same id; a
        // fresh RecordMintRequested here would fail with AlreadyInProgress, so
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

    /// Resume from `VaultWithdrawSubmitted` re-confirms the persisted withdraw tx
    /// without re-broadcasting it -- the crash-recovery property the submit/confirm
    /// split exists to deliver.
    #[tokio::test]
    async fn resume_from_vault_withdraw_submitted_confirms_without_resubmitting() {
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

        let id = redemption_aggregate_id("resume-withdraw-submitted");
        let symbol = Symbol::new("TEST").unwrap();
        let quantity = FractionalShares::new(float!(50));
        let token = transfer
            .vault_lookup
            .vault_token_for_symbol(&symbol)
            .await
            .unwrap();
        let amount = quantity.to_u256_18_decimals().unwrap();

        // Seed `VaultWithdrawSubmitted`: the withdraw tx was broadcast and
        // recorded, then the process crashed before confirmation.
        transfer
            .redemption_store
            .send(
                &id,
                EquityRedemptionCommand::Redeem {
                    symbol: symbol.clone(),
                    quantity: quantity.inner(),
                    token,
                    amount,
                },
            )
            .await
            .unwrap();
        let tx_hash = step::submit_vault_withdraw(
            transfer.vault_lookup.as_ref(),
            transfer.raindex.as_ref(),
            token,
            amount,
        )
        .await
        .unwrap();
        transfer
            .redemption_store
            .send(&id, EquityRedemptionCommand::SubmitWithdraw { tx_hash })
            .await
            .unwrap();

        let seeded = transfer.redemption_store.load(&id).await.unwrap().unwrap();
        assert!(
            matches!(seeded, EquityRedemption::VaultWithdrawSubmitted { .. }),
            "test setup must seed VaultWithdrawSubmitted, got: {seeded:?}"
        );

        // Resume takes the confirm-only arm. Re-entering the submit path would
        // re-send `SubmitWithdraw` from an already-submitted aggregate, which the
        // state machine rejects -- so reaching `Completed` proves the persisted
        // withdraw was confirmed, never re-broadcast.
        transfer.resume_redemption(&id).await.unwrap();

        let entity = transfer.redemption_store.load(&id).await.unwrap().unwrap();
        assert!(
            matches!(entity, EquityRedemption::Completed { .. }),
            "Expected Completed after resume from VaultWithdrawSubmitted, got: {entity:?}"
        );
    }

    /// Resume from `UnwrapSubmitted` re-confirms the persisted unwrap tx without
    /// re-broadcasting it.
    #[tokio::test]
    async fn resume_from_unwrap_submitted_confirms_without_resubmitting() {
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

        let id = redemption_aggregate_id("resume-unwrap-submitted");
        let symbol = Symbol::new("TEST").unwrap();
        let quantity = FractionalShares::new(float!(50));
        let token = transfer
            .vault_lookup
            .vault_token_for_symbol(&symbol)
            .await
            .unwrap();
        let amount = quantity.to_u256_18_decimals().unwrap();

        // Drive through the confirmed withdrawal to `UnwrapPending`.
        transfer
            .withdraw_from_raindex(&id, &symbol, quantity, token, amount)
            .await
            .unwrap();
        transfer
            .redemption_store
            .send(&id, EquityRedemptionCommand::UnwrapTokens)
            .await
            .unwrap();

        // Seed `UnwrapSubmitted`: the unwrap tx was broadcast and recorded, then
        // the process crashed before confirmation.
        let (unwrap_token, wrapped_amount, raindex_withdraw_block) =
            match transfer.redemption_store.load(&id).await.unwrap().unwrap() {
                EquityRedemption::UnwrapPending {
                    token,
                    wrapped_amount,
                    raindex_withdraw_block,
                    ..
                } => (token, wrapped_amount, raindex_withdraw_block),
                other => panic!("expected UnwrapPending, got: {other:?}"),
            };
        let unwrap_tx_hash = step::submit_token_unwrap(
            transfer.wrapper.as_ref(),
            unwrap_token,
            wrapped_amount,
            raindex_withdraw_block,
        )
        .await
        .unwrap();
        transfer
            .redemption_store
            .send(
                &id,
                EquityRedemptionCommand::SubmitUnwrap { unwrap_tx_hash },
            )
            .await
            .unwrap();

        let seeded = transfer.redemption_store.load(&id).await.unwrap().unwrap();
        assert!(
            matches!(seeded, EquityRedemption::UnwrapSubmitted { .. }),
            "test setup must seed UnwrapSubmitted, got: {seeded:?}"
        );

        // Resume takes the confirm-only arm; re-entering submit would re-send
        // `SubmitUnwrap` from an already-submitted aggregate (rejected), so
        // reaching `Completed` proves the unwrap was confirmed, not re-broadcast.
        transfer.resume_redemption(&id).await.unwrap();

        let entity = transfer.redemption_store.load(&id).await.unwrap().unwrap();
        assert!(
            matches!(entity, EquityRedemption::Completed { .. }),
            "Expected Completed after resume from UnwrapSubmitted, got: {entity:?}"
        );
    }

    /// A partial vault fill collapses the withdrawn amount; the orchestrator must
    /// unwrap that actual amount, not the originally requested one.
    #[tokio::test]
    async fn unwrap_uses_actual_partial_withdraw_amount_through_orchestrator() {
        let quantity = FractionalShares::new(float!(37.143292455));
        let requested = quantity.to_u256_18_decimals().unwrap();
        let actual = U256::from(33_681_456_848_531_939_569_u128);

        let tokenizer: Arc<dyn Tokenizer> = Arc::new(MockTokenizer::new());
        let wrapper = Arc::new(MockWrapper::new());
        let transfer = create_equity_transfer(
            tokenizer,
            Arc::new(MockRaindex::new().with_withdraw_actual_amount(actual)),
            wrapper.clone(),
        )
        .await;

        let id = redemption_aggregate_id("partial-unwrap");
        let symbol = Symbol::new("TEST").unwrap();
        let token = transfer
            .vault_lookup
            .vault_token_for_symbol(&symbol)
            .await
            .unwrap();

        // Orchestrator-driven partial withdrawal: the receipt yields `actual` <
        // `requested`, which the aggregate collapses into the persisted amount.
        transfer
            .withdraw_from_raindex(&id, &symbol, quantity, token, requested)
            .await
            .unwrap();
        transfer
            .redemption_store
            .send(&id, EquityRedemptionCommand::UnwrapTokens)
            .await
            .unwrap();

        let (unwrap_token, wrapped_amount, raindex_withdraw_block) =
            match transfer.redemption_store.load(&id).await.unwrap().unwrap() {
                EquityRedemption::UnwrapPending {
                    token,
                    wrapped_amount,
                    raindex_withdraw_block,
                    ..
                } => (token, wrapped_amount, raindex_withdraw_block),
                other => panic!("expected UnwrapPending, got: {other:?}"),
            };

        assert_eq!(
            wrapped_amount, actual,
            "orchestrator-driven confirm must collapse the persisted amount to the \
             partial receipt amount"
        );

        // The unwrap submits the collapsed amount the orchestrator loaded, not the
        // original request. (Asserted before confirm, which consumes the record.)
        step::submit_token_unwrap(
            transfer.wrapper.as_ref(),
            unwrap_token,
            wrapped_amount,
            raindex_withdraw_block,
        )
        .await
        .unwrap();

        assert_eq!(
            wrapper.submitted_unwrap_amount(),
            Some(actual),
            "the orchestrator must unwrap the actual partial-withdraw amount, not the request"
        );
    }

    /// RPC lag while waiting to unwrap surfaces as a retryable
    /// `RedemptionError::Step(Wrapper(Evm(NodeBehindRequiredBlock)))`, so apalis
    /// retries it. Documents the intended retryable shape for unwrap waits (the
    /// unwrap path mirrors the mint path's treatment of the same wrapper wait,
    /// unlike the send path which maps its tokenizer wait to `NodeSync`).
    #[tokio::test]
    async fn unwrap_wait_for_block_lag_surfaces_as_retryable_step_error() {
        let tokenizer: Arc<dyn Tokenizer> = Arc::new(MockTokenizer::new());
        let transfer = create_equity_transfer(
            tokenizer,
            Arc::new(MockRaindex::new()),
            Arc::new(MockWrapper::failing_wait_for_block()),
        )
        .await;

        let id = redemption_aggregate_id("unwrap-wait-lag");
        let symbol = Symbol::new("TEST").unwrap();
        let quantity = FractionalShares::new(float!(50));
        let token = transfer
            .vault_lookup
            .vault_token_for_symbol(&symbol)
            .await
            .unwrap();
        let amount = quantity.to_u256_18_decimals().unwrap();

        transfer
            .withdraw_from_raindex(&id, &symbol, quantity, token, amount)
            .await
            .unwrap();

        let error = transfer.unwrap_and_send(&id).await.unwrap_err();

        assert!(
            matches!(
                error,
                RedemptionError::Step(step::EquityVaultStepError::Wrapper(WrapperError::Evm(
                    EvmError::NodeBehindRequiredBlock { .. }
                )))
            ),
            "unwrap wait-for-block lag must surface as a retryable Step error, got: {error:?}"
        );
    }

    /// The full redemption flow broadcasts the Alpaca transfer exactly once
    /// (submit/confirm split: SubmitSend records the broadcast, ConfirmSend
    /// finalizes without re-sending).
    #[tokio::test]
    async fn send_redemption_broadcasts_the_transfer_exactly_once() {
        let tokenizer = Arc::new(
            MockTokenizer::new()
                .with_detection_outcome(MockDetectionOutcome::Detected)
                .with_completion_outcome(MockCompletionOutcome::Completed),
        );
        let transfer = create_equity_transfer(
            tokenizer.clone(),
            Arc::new(MockRaindex::new()),
            Arc::new(MockWrapper::new()),
        )
        .await;

        let id = redemption_aggregate_id("send-once");
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
            "expected Completed, got: {entity:?}"
        );
        assert_eq!(
            tokenizer.redemption_send_count(),
            1,
            "the redemption transfer must be broadcast exactly once across the full flow"
        );
    }

    /// Resume from `SendSubmitted` finalizes the persisted redemption tx without
    /// re-broadcasting -- the crash-window the submit/confirm split closes.
    #[tokio::test]
    async fn resume_from_send_submitted_finalizes_without_resending() {
        let tokenizer = Arc::new(
            MockTokenizer::new()
                .with_detection_outcome(MockDetectionOutcome::Detected)
                .with_completion_outcome(MockCompletionOutcome::Completed),
        );
        let transfer = create_equity_transfer(
            tokenizer.clone(),
            Arc::new(MockRaindex::new()),
            Arc::new(MockWrapper::new()),
        )
        .await;

        let id = redemption_aggregate_id("resume-send-submitted");
        let symbol = Symbol::new("TEST").unwrap();
        let quantity = FractionalShares::new(float!(50));
        let token = transfer
            .vault_lookup
            .vault_token_for_symbol(&symbol)
            .await
            .unwrap();
        let amount = quantity.to_u256_18_decimals().unwrap();

        // Drive to `SendPending` through the orchestrator.
        transfer
            .withdraw_from_raindex(&id, &symbol, quantity, token, amount)
            .await
            .unwrap();
        transfer
            .redemption_store
            .send(&id, EquityRedemptionCommand::UnwrapTokens)
            .await
            .unwrap();
        let (unwrap_token, wrapped_amount, raindex_withdraw_block) =
            match transfer.redemption_store.load(&id).await.unwrap().unwrap() {
                EquityRedemption::UnwrapPending {
                    token,
                    wrapped_amount,
                    raindex_withdraw_block,
                    ..
                } => (token, wrapped_amount, raindex_withdraw_block),
                other => panic!("expected UnwrapPending, got: {other:?}"),
            };
        transfer
            .submit_and_confirm_unwrap(
                &id,
                &symbol,
                unwrap_token,
                wrapped_amount,
                raindex_withdraw_block,
            )
            .await
            .unwrap();
        transfer
            .redemption_store
            .send(&id, EquityRedemptionCommand::PrepareSend)
            .await
            .unwrap();

        // Seed `SendSubmitted`: the transfer was broadcast and recorded, then a
        // crash before the bookkeeping finalization. (SubmitSend is pure -- no
        // real send happens in the test, so the send counter stays at zero.)
        transfer
            .redemption_store
            .send(
                &id,
                EquityRedemptionCommand::SubmitSend {
                    redemption_wallet: Address::random(),
                    redemption_tx: TxHash::random(),
                },
            )
            .await
            .unwrap();
        let seeded = transfer.redemption_store.load(&id).await.unwrap().unwrap();
        assert!(
            matches!(seeded, EquityRedemption::SendSubmitted { .. }),
            "test setup must seed SendSubmitted, got: {seeded:?}"
        );

        // Resume takes the confirm-only arm. Re-entering the send path would
        // re-send `SubmitSend` from a non-SendPending state (rejected) and
        // re-broadcast, so reaching Completed with zero sends proves the transfer
        // was finalized, never re-broadcast.
        transfer.resume_redemption(&id).await.unwrap();

        let entity = transfer.redemption_store.load(&id).await.unwrap().unwrap();
        assert!(
            matches!(entity, EquityRedemption::Completed { .. }),
            "resume from SendSubmitted must finalize to Completed, got: {entity:?}"
        );
        assert_eq!(
            tokenizer.redemption_send_count(),
            0,
            "resume from SendSubmitted must NOT re-broadcast the irreversible transfer"
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

    /// Re-running the job entry point with an id whose aggregate already
    /// exists must resume from the persisted state (the crash-recovery path)
    /// rather than re-running the vault withdrawal from scratch.
    #[tokio::test]
    async fn resume_equity_to_hedging_resumes_existing_aggregate() {
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
        // RecordMintRequested -> MintAccepted -> RecordTokensReceived ->
        // TokensReceived -> WrapSubmitted -> TokensWrapped
        transfer
            .mint_store
            .send(
                &id,
                TokenizedEquityMintCommand::RecordMintRequested {
                    issuer_request_id: id.clone(),
                    symbol: Symbol::new("AAPL").unwrap(),
                    quantity: float!(10),
                    wallet: transfer.wallet,
                    tokenization_request_id: TokenizationRequestId("tok-1".to_string()),
                },
            )
            .await
            .unwrap();

        // RecordTokensReceived advances to TokensReceived
        transfer
            .mint_store
            .send(
                &id,
                TokenizedEquityMintCommand::RecordTokensReceived {
                    tx_hash: Some(TxHash::ZERO),
                    token_symbol: Some("tAAPL".to_string()),
                    fees: None,
                },
            )
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
        // RecordMintRequested -> MintAccepted -> RecordTokensReceived ->
        // TokensReceived -> WrapSubmitted
        transfer
            .mint_store
            .send(
                &id,
                TokenizedEquityMintCommand::RecordMintRequested {
                    issuer_request_id: id.clone(),
                    symbol: Symbol::new("AAPL").unwrap(),
                    quantity: float!(10),
                    wallet: transfer.wallet,
                    tokenization_request_id: TokenizationRequestId("tok-1".to_string()),
                },
            )
            .await
            .unwrap();

        transfer
            .mint_store
            .send(
                &id,
                TokenizedEquityMintCommand::RecordTokensReceived {
                    tx_hash: Some(TxHash::ZERO),
                    token_symbol: Some("tAAPL".to_string()),
                    fees: None,
                },
            )
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
                TokenizedEquityMintCommand::RecordMintRequested {
                    issuer_request_id: id.clone(),
                    symbol: Symbol::new("AAPL").unwrap(),
                    quantity: float!(10),
                    wallet: transfer.wallet,
                    tokenization_request_id: TokenizationRequestId("tok-1".to_string()),
                },
            )
            .await
            .unwrap();

        transfer
            .mint_store
            .send(
                &id,
                TokenizedEquityMintCommand::RecordTokensReceived {
                    tx_hash: Some(TxHash::ZERO),
                    token_symbol: Some("tAAPL".to_string()),
                    fees: None,
                },
            )
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
                TokenizedEquityMintCommand::RecordMintRequested {
                    issuer_request_id: id.clone(),
                    symbol: Symbol::new("AAPL").unwrap(),
                    quantity: float!(10),
                    wallet: transfer.wallet,
                    tokenization_request_id: TokenizationRequestId("tok-1".to_string()),
                },
            )
            .await
            .unwrap();

        transfer
            .mint_store
            .send(
                &id,
                TokenizedEquityMintCommand::RecordTokensReceived {
                    tx_hash: Some(TxHash::ZERO),
                    token_symbol: Some("tAAPL".to_string()),
                    fees: None,
                },
            )
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
                TokenizedEquityMintCommand::RecordMintRequested {
                    issuer_request_id: id.clone(),
                    symbol: Symbol::new("AAPL").unwrap(),
                    quantity: float!(10),
                    wallet: transfer.wallet,
                    tokenization_request_id: TokenizationRequestId("tok-1".to_string()),
                },
            )
            .await
            .unwrap();

        transfer
            .mint_store
            .send(
                &id,
                TokenizedEquityMintCommand::RecordTokensReceived {
                    tx_hash: Some(TxHash::ZERO),
                    token_symbol: Some("tAAPL".to_string()),
                    fees: None,
                },
            )
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

        // Advance to WrapSubmitted: RecordMintRequested ->
        // RecordTokensReceived -> SubmitWrap
        transfer
            .mint_store
            .send(
                &id,
                TokenizedEquityMintCommand::RecordMintRequested {
                    issuer_request_id: id.clone(),
                    symbol: Symbol::new("AAPL").unwrap(),
                    quantity: float!(10),
                    wallet: transfer.wallet,
                    tokenization_request_id: TokenizationRequestId("tok-1".to_string()),
                },
            )
            .await
            .unwrap();

        transfer
            .mint_store
            .send(
                &id,
                TokenizedEquityMintCommand::RecordTokensReceived {
                    tx_hash: Some(TxHash::ZERO),
                    token_symbol: Some("tAAPL".to_string()),
                    fees: None,
                },
            )
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
