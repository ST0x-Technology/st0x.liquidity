//! Aggregate modeling the lifecycle of minting tokenized
//! equities from underlying Alpaca shares.
//!
//! Tracks the workflow from requesting a mint through
//! Alpaca's tokenization API to receiving onchain tokens.
//!
//! # State Flow
//!
//! ```text
//! MintRequested -> MintAccepted -> TokensReceived -> DepositedIntoRaindex -> Completed
//!       |               |               |
//!       v               v               v
//!     Failed          Failed          Failed
//! ```
//!
//! - `MintRequested` can be rejected via `RejectMint`
//! - `MintAccepted` can fail via `FailAcceptance`
//! - `Completed` and `Failed` are terminal states
//!
//! # Alpaca API Integration
//!
//! The mint process integrates with Alpaca's tokenization API:
//!
//! 1. **Request**: System initiates mint request with symbol,
//!    quantity, and destination wallet
//! 2. **Acceptance**: Alpaca responds with `issuer_request_id` and
//!    `tokenization_request_id`
//! 3. **Transfer**: Alpaca executes onchain transfer, system detects
//!    transaction
//! 4. **Vault Deposit**: Tokens deposited from wallet to Raindex vault
//! 5. **Completion**: System verifies receipt and finalizes mint
//!
//! # Error Handling
//!
//! The aggregate enforces strict state transitions:
//!
//! - Commands that don't match current state return appropriate errors
//! - Terminal states (Completed, Failed) reject all state-changing commands
//! - All state transitions are captured as events for complete audit trail

use alloy::primitives::{Address, TxHash, U256};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use tracing::warn;

use st0x_event_sorcery::{DomainEvent, EventSourced, Table};
use st0x_execution::{FractionalShares, Symbol};

use crate::rebalancing::equity::EquityTransferServices;
use crate::tokenization::TokenizationRequestStatus;

/// Alpaca issuer request identifier returned when a tokenization request is accepted.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct IssuerRequestId(pub(crate) String);

impl IssuerRequestId {
    pub(crate) fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }
}

impl std::fmt::Display for IssuerRequestId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for IssuerRequestId {
    type Err = std::convert::Infallible;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Ok(Self(value.to_string()))
    }
}

/// Alpaca tokenization request identifier used to track the mint operation through their API.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct TokenizationRequestId(pub(crate) String);

impl std::fmt::Display for TokenizationRequestId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Onchain receipt identifier (U256) for the token transfer transaction.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ReceiptId(pub(crate) U256);

/// HTTP status code from API responses.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct HttpStatusCode(pub(crate) u16);

/// Errors that can occur during tokenized equity mint operations.
///
/// These errors enforce state machine constraints and prevent
/// invalid transitions.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, thiserror::Error)]
pub(crate) enum TokenizedEquityMintError {
    /// Attempted to request mint when already in progress
    #[error("Mint already in progress")]
    AlreadyInProgress,
    /// Attempted to deposit to vault before tokens were received
    #[error("Cannot deposit to vault: tokens not received")]
    TokensNotReceived,
    /// Attempted to modify a completed mint operation
    #[error("Already completed")]
    AlreadyCompleted,
    /// Attempted to modify a failed mint operation
    #[error("Already failed")]
    AlreadyFailed,
    /// Tokenizer API failed to submit the mint request
    #[error("Mint request failed")]
    RequestFailed,
    /// Completed mint response missing tx_hash
    #[error("Missing tx_hash in completed mint response")]
    MissingTxHash,
    /// Decimal overflow when scaling quantity to 18 decimals
    #[error("Decimal overflow when scaling {value} to 18 decimals")]
    DecimalScalingOverflow { value: Decimal },
    /// U256 conversion failed for a scaled decimal value
    #[error("Failed to convert scaled decimal {scaled_value} to U256")]
    U256ConversionFailed { scaled_value: String },
    /// Negative quantity is invalid for minting
    #[error("Negative quantity: {value}")]
    NegativeQuantity { value: Decimal },
    /// Vault lookup failed for the given symbol
    #[error("Vault lookup failed for {0}")]
    VaultLookupFailed(Symbol),
    /// Vault deposit transaction failed
    #[error("Vault deposit failed")]
    VaultDepositFailed,
}

/// Commands for the TokenizedEquityMint aggregate.
#[derive(Debug, Clone)]
pub(crate) enum TokenizedEquityMintCommand {
    /// Request tokenization from Alpaca and poll until tokens arrive or failure.
    ///
    /// Flow: MintRequested -> MintAccepted -> TokensReceived (success)
    ///                     or MintRejected (immediate failure)
    ///                     or MintAcceptanceFailed (failure after acceptance)
    Mint {
        issuer_request_id: IssuerRequestId,
        symbol: Symbol,
        quantity: Decimal,
        wallet: Address,
    },
    /// Deposit received tokens into Raindex vault.
    ///
    /// Flow: DepositedIntoRaindex -> Completed (success)
    ///    or RaindexDepositFailed (failure, terminal)
    Deposit,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum TokenizedEquityMintEvent {
    MintRequested {
        symbol: Symbol,
        quantity: Decimal,
        wallet: Address,
        requested_at: DateTime<Utc>,
    },
    /// Alpaca rejected the mint request before acceptance.
    /// Shares remain in offchain available - no funds were moved.
    MintRejected {
        reason: String,
        rejected_at: DateTime<Utc>,
    },

    MintAccepted {
        issuer_request_id: IssuerRequestId,
        tokenization_request_id: TokenizationRequestId,
        accepted_at: DateTime<Utc>,
    },
    /// Mint failed after acceptance but before tokens were received.
    /// Shares were moved to inflight, can be safely restored to offchain available.
    MintAcceptanceFailed {
        reason: String,
        failed_at: DateTime<Utc>,
    },

    TokensReceived {
        tx_hash: TxHash,
        receipt_id: ReceiptId,
        shares_minted: U256,
        received_at: DateTime<Utc>,
    },

    /// Tokens deposited from wallet to Raindex vault.
    VaultDeposited {
        vault_deposit_tx_hash: TxHash,
        deposited_at: DateTime<Utc>,
    },
    /// Vault deposit failed after tokens were received.
    /// Tokens remain in wallet, can be retried or manually recovered.
    RaindexDepositFailed {
        symbol: Symbol,
        quantity: Decimal,
        failed_tx_hash: Option<TxHash>,
        failed_at: DateTime<Utc>,
    },

    MintCompleted {
        completed_at: DateTime<Utc>,
    },
}

impl DomainEvent for TokenizedEquityMintEvent {
    fn event_type(&self) -> String {
        match self {
            Self::MintRequested { .. } => "TokenizedEquityMintEvent::MintRequested".to_string(),
            Self::MintRejected { .. } => "TokenizedEquityMintEvent::MintRejected".to_string(),
            Self::MintAccepted { .. } => "TokenizedEquityMintEvent::MintAccepted".to_string(),
            Self::MintAcceptanceFailed { .. } => {
                "TokenizedEquityMintEvent::MintAcceptanceFailed".to_string()
            }
            Self::TokensReceived { .. } => "TokenizedEquityMintEvent::TokensReceived".to_string(),
            Self::VaultDeposited { .. } => "TokenizedEquityMintEvent::VaultDeposited".to_string(),
            Self::RaindexDepositFailed { .. } => {
                "TokenizedEquityMintEvent::RaindexDepositFailed".to_string()
            }
            Self::MintCompleted { .. } => "TokenizedEquityMintEvent::MintCompleted".to_string(),
        }
    }

    fn event_version(&self) -> String {
        "1.0".to_string()
    }
}

/// Tokenized equity mint aggregate state machine.
///
/// Uses the typestate pattern via enum variants to make invalid
/// states unrepresentable. Each variant contains exactly the data
/// valid for that state.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum TokenizedEquityMint {
    /// Mint request initiated with symbol, quantity, and destination wallet
    MintRequested {
        symbol: Symbol,
        quantity: Decimal,
        wallet: Address,
        requested_at: DateTime<Utc>,
    },

    /// Alpaca API accepted the mint request and returned tracking identifiers
    MintAccepted {
        symbol: Symbol,
        quantity: Decimal,
        wallet: Address,
        issuer_request_id: IssuerRequestId,
        tokenization_request_id: TokenizationRequestId,
        requested_at: DateTime<Utc>,
        accepted_at: DateTime<Utc>,
    },

    /// Onchain token transfer detected with transaction details
    TokensReceived {
        symbol: Symbol,
        quantity: Decimal,
        wallet: Address,
        issuer_request_id: IssuerRequestId,
        tokenization_request_id: TokenizationRequestId,
        tx_hash: TxHash,
        receipt_id: ReceiptId,
        shares_minted: U256,
        requested_at: DateTime<Utc>,
        accepted_at: DateTime<Utc>,
        received_at: DateTime<Utc>,
    },

    /// Tokens deposited from wallet to Raindex vault
    DepositedIntoRaindex {
        symbol: Symbol,
        quantity: Decimal,
        wallet: Address,
        issuer_request_id: IssuerRequestId,
        tokenization_request_id: TokenizationRequestId,
        tx_hash: TxHash,
        receipt_id: ReceiptId,
        shares_minted: U256,
        vault_deposit_tx_hash: TxHash,
        requested_at: DateTime<Utc>,
        accepted_at: DateTime<Utc>,
        received_at: DateTime<Utc>,
        deposited_at: DateTime<Utc>,
    },

    /// Mint operation successfully completed (terminal state)
    Completed {
        symbol: Symbol,
        quantity: Decimal,
        /// Alpaca cross-system identifiers for auditing
        issuer_request_id: IssuerRequestId,
        tokenization_request_id: TokenizationRequestId,
        /// Token receipt transaction from Alpaca
        token_tx_hash: TxHash,
        /// Vault deposit transaction to Raindex
        vault_deposit_tx_hash: TxHash,
        completed_at: DateTime<Utc>,
    },

    /// Mint operation failed (terminal state)
    Failed {
        symbol: Symbol,
        quantity: Decimal,
        reason: String,
        requested_at: DateTime<Utc>,
        failed_at: DateTime<Utc>,
    },
}

/// Our tokenized equity tokens use 18 decimals.
const TOKENIZED_EQUITY_DECIMALS: u8 = 18;

fn decimal_to_u256_18_decimals(value: Decimal) -> Result<U256, TokenizedEquityMintError> {
    if value.is_sign_negative() {
        return Err(TokenizedEquityMintError::NegativeQuantity { value });
    }

    let scale_factor = Decimal::from(10u64.pow(18));
    let scaled = value
        .checked_mul(scale_factor)
        .ok_or(TokenizedEquityMintError::DecimalScalingOverflow { value })?;
    let truncated = scaled.trunc();

    let repr = truncated.to_string();
    let Ok(amount) = U256::from_str_radix(&repr, 10) else {
        return Err(TokenizedEquityMintError::U256ConversionFailed { scaled_value: repr });
    };

    Ok(amount)
}

#[async_trait]
impl EventSourced for TokenizedEquityMint {
    type Id = IssuerRequestId;
    type Event = TokenizedEquityMintEvent;
    type Command = TokenizedEquityMintCommand;
    type Error = TokenizedEquityMintError;
    type Services = EquityTransferServices;

    const AGGREGATE_TYPE: &'static str = "TokenizedEquityMint";
    const PROJECTION: Option<Table> = None;
    const SCHEMA_VERSION: u64 = 1;

    fn originate(event: &Self::Event) -> Option<Self> {
        use TokenizedEquityMintEvent::*;
        match event {
            MintRequested {
                symbol,
                quantity,
                wallet,
                requested_at,
            } => Some(Self::MintRequested {
                symbol: symbol.clone(),
                quantity: *quantity,
                wallet: *wallet,
                requested_at: *requested_at,
            }),
            _ => None,
        }
    }

    fn evolve(entity: &Self, event: &Self::Event) -> Result<Option<Self>, Self::Error> {
        use TokenizedEquityMintEvent::*;
        Ok(match event {
            MintRequested { .. } => None,
            MintRejected {
                reason,
                rejected_at,
            } => entity.try_apply_rejected(reason, *rejected_at),
            MintAccepted {
                issuer_request_id,
                tokenization_request_id,
                accepted_at,
            } => {
                entity.try_apply_accepted(issuer_request_id, tokenization_request_id, *accepted_at)
            }
            MintAcceptanceFailed { reason, failed_at } => {
                entity.try_apply_acceptance_failed(reason, *failed_at)
            }
            TokensReceived {
                tx_hash,
                receipt_id,
                shares_minted,
                received_at,
            } => {
                entity.try_apply_tokens_received(*tx_hash, receipt_id, *shares_minted, *received_at)
            }
            VaultDeposited {
                vault_deposit_tx_hash,
                deposited_at,
            } => entity.try_apply_vault_deposited(*vault_deposit_tx_hash, *deposited_at),
            RaindexDepositFailed { failed_at, .. } => {
                entity.try_apply_vault_deposit_failed(*failed_at)
            }
            MintCompleted { completed_at } => entity.try_apply_completed(*completed_at),
        })
    }

    async fn initialize(
        command: Self::Command,
        services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        use TokenizedEquityMintEvent::*;
        match command {
            TokenizedEquityMintCommand::Mint {
                issuer_request_id,
                symbol,
                quantity,
                wallet,
            } => {
                let now = Utc::now();
                let mint_requested = MintRequested {
                    symbol: symbol.clone(),
                    quantity,
                    wallet,
                    requested_at: now,
                };

                let alpaca_request = match services
                    .tokenizer
                    .request_mint(
                        symbol,
                        FractionalShares::new(quantity),
                        wallet,
                        issuer_request_id.clone(),
                    )
                    .await
                {
                    Ok(req) => req,
                    Err(error) => {
                        warn!(%error, "Mint request failed");
                        return Err(TokenizedEquityMintError::RequestFailed);
                    }
                };

                if matches!(alpaca_request.status, TokenizationRequestStatus::Rejected) {
                    return Ok(vec![
                        mint_requested,
                        MintRejected {
                            reason: "Rejected by Alpaca".to_string(),
                            rejected_at: now,
                        },
                    ]);
                }

                let mint_accepted = MintAccepted {
                    issuer_request_id,
                    tokenization_request_id: alpaca_request.id.clone(),
                    accepted_at: now,
                };

                let completed = match services
                    .tokenizer
                    .poll_mint_until_complete(&alpaca_request.id)
                    .await
                {
                    Ok(req) => req,
                    Err(error) => {
                        warn!(%error, "Polling failed");
                        return Ok(vec![
                            mint_requested,
                            mint_accepted,
                            MintAcceptanceFailed {
                                reason: format!("Polling failed: {error}"),
                                failed_at: Utc::now(),
                            },
                        ]);
                    }
                };

                match completed.status {
                    TokenizationRequestStatus::Completed => {
                        let tx_hash = completed
                            .tx_hash
                            .ok_or(TokenizedEquityMintError::MissingTxHash)?;
                        let shares_minted = decimal_to_u256_18_decimals(quantity)?;

                        Ok(vec![
                            mint_requested,
                            mint_accepted,
                            TokensReceived {
                                tx_hash,
                                receipt_id: ReceiptId(U256::ZERO),
                                shares_minted,
                                received_at: Utc::now(),
                            },
                        ])
                    }
                    TokenizationRequestStatus::Rejected => Ok(vec![
                        mint_requested,
                        mint_accepted,
                        MintAcceptanceFailed {
                            reason: "Rejected by Alpaca after acceptance".to_string(),
                            failed_at: Utc::now(),
                        },
                    ]),
                    TokenizationRequestStatus::Pending => Ok(vec![
                        mint_requested,
                        mint_accepted,
                        MintAcceptanceFailed {
                            reason: "Unexpected Pending status after polling".to_string(),
                            failed_at: Utc::now(),
                        },
                    ]),
                }
            }

            TokenizedEquityMintCommand::Deposit => Err(TokenizedEquityMintError::TokensNotReceived),
        }
    }

    async fn transition(
        &self,
        command: Self::Command,
        services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        use TokenizedEquityMintEvent::*;
        match command {
            TokenizedEquityMintCommand::Mint { .. } => {
                Err(TokenizedEquityMintError::AlreadyInProgress)
            }

            TokenizedEquityMintCommand::Deposit => match self {
                Self::MintRequested { .. } | Self::MintAccepted { .. } => {
                    Err(TokenizedEquityMintError::TokensNotReceived)
                }

                Self::TokensReceived {
                    symbol,
                    quantity,
                    shares_minted,
                    ..
                } => {
                    let (token, vault_id) = match services.raindex.lookup_vault_info(symbol).await {
                        Ok(info) => info,
                        Err(error) => {
                            warn!(%error, %symbol, "Vault lookup failed");
                            return Ok(vec![RaindexDepositFailed {
                                symbol: symbol.clone(),
                                quantity: *quantity,
                                failed_tx_hash: None,
                                failed_at: Utc::now(),
                            }]);
                        }
                    };

                    match services
                        .raindex
                        .deposit(token, vault_id, *shares_minted, TOKENIZED_EQUITY_DECIMALS)
                        .await
                    {
                        Ok(vault_deposit_tx_hash) => {
                            let now = Utc::now();
                            Ok(vec![
                                VaultDeposited {
                                    vault_deposit_tx_hash,
                                    deposited_at: now,
                                },
                                MintCompleted { completed_at: now },
                            ])
                        }
                        Err(error) => {
                            warn!(%error, "Vault deposit failed");
                            Ok(vec![RaindexDepositFailed {
                                symbol: symbol.clone(),
                                quantity: *quantity,
                                failed_tx_hash: None,
                                failed_at: Utc::now(),
                            }])
                        }
                    }
                }

                Self::DepositedIntoRaindex { .. } | Self::Completed { .. } => {
                    Err(TokenizedEquityMintError::AlreadyCompleted)
                }
                Self::Failed { .. } => Err(TokenizedEquityMintError::AlreadyFailed),
            },
        }
    }
}

impl TokenizedEquityMint {
    fn try_apply_accepted(
        &self,
        issuer_request_id: &IssuerRequestId,
        tokenization_request_id: &TokenizationRequestId,
        accepted_at: DateTime<Utc>,
    ) -> Option<Self> {
        let Self::MintRequested {
            symbol,
            quantity,
            wallet,
            requested_at,
        } = self
        else {
            return None;
        };

        Some(Self::MintAccepted {
            symbol: symbol.clone(),
            quantity: *quantity,
            wallet: *wallet,
            issuer_request_id: issuer_request_id.clone(),
            tokenization_request_id: tokenization_request_id.clone(),
            requested_at: *requested_at,
            accepted_at,
        })
    }

    fn try_apply_tokens_received(
        &self,
        tx_hash: TxHash,
        receipt_id: &ReceiptId,
        shares_minted: U256,
        received_at: DateTime<Utc>,
    ) -> Option<Self> {
        let Self::MintAccepted {
            symbol,
            quantity,
            wallet,
            issuer_request_id,
            tokenization_request_id,
            requested_at,
            accepted_at,
        } = self
        else {
            return None;
        };

        Some(Self::TokensReceived {
            symbol: symbol.clone(),
            quantity: *quantity,
            wallet: *wallet,
            issuer_request_id: issuer_request_id.clone(),
            tokenization_request_id: tokenization_request_id.clone(),
            tx_hash,
            receipt_id: receipt_id.clone(),
            shares_minted,
            requested_at: *requested_at,
            accepted_at: *accepted_at,
            received_at,
        })
    }

    fn try_apply_vault_deposited(
        &self,
        vault_deposit_tx_hash: TxHash,
        deposited_at: DateTime<Utc>,
    ) -> Option<Self> {
        let Self::TokensReceived {
            symbol,
            quantity,
            wallet,
            issuer_request_id,
            tokenization_request_id,
            tx_hash,
            receipt_id,
            shares_minted,
            requested_at,
            accepted_at,
            received_at,
        } = self
        else {
            return None;
        };

        Some(Self::DepositedIntoRaindex {
            symbol: symbol.clone(),
            quantity: *quantity,
            wallet: *wallet,
            issuer_request_id: issuer_request_id.clone(),
            tokenization_request_id: tokenization_request_id.clone(),
            tx_hash: *tx_hash,
            receipt_id: receipt_id.clone(),
            shares_minted: *shares_minted,
            vault_deposit_tx_hash,
            requested_at: *requested_at,
            accepted_at: *accepted_at,
            received_at: *received_at,
            deposited_at,
        })
    }

    fn try_apply_vault_deposit_failed(&self, failed_at: DateTime<Utc>) -> Option<Self> {
        let Self::TokensReceived {
            symbol,
            quantity,
            requested_at,
            ..
        } = self
        else {
            return None;
        };

        Some(Self::Failed {
            symbol: symbol.clone(),
            quantity: *quantity,
            reason: "Raindex vault deposit failed".to_string(),
            requested_at: *requested_at,
            failed_at,
        })
    }

    fn try_apply_completed(&self, completed_at: DateTime<Utc>) -> Option<Self> {
        let Self::DepositedIntoRaindex {
            symbol,
            quantity,
            issuer_request_id,
            tokenization_request_id,
            tx_hash,
            vault_deposit_tx_hash,
            ..
        } = self
        else {
            return None;
        };

        Some(Self::Completed {
            symbol: symbol.clone(),
            quantity: *quantity,
            issuer_request_id: issuer_request_id.clone(),
            tokenization_request_id: tokenization_request_id.clone(),
            token_tx_hash: *tx_hash,
            vault_deposit_tx_hash: *vault_deposit_tx_hash,
            completed_at,
        })
    }

    fn try_apply_rejected(&self, reason: &str, rejected_at: DateTime<Utc>) -> Option<Self> {
        let Self::MintRequested {
            symbol,
            quantity,
            requested_at,
            ..
        } = self
        else {
            return None;
        };

        Some(Self::Failed {
            symbol: symbol.clone(),
            quantity: *quantity,
            reason: reason.to_string(),
            requested_at: *requested_at,
            failed_at: rejected_at,
        })
    }

    fn try_apply_acceptance_failed(&self, reason: &str, failed_at: DateTime<Utc>) -> Option<Self> {
        let Self::MintAccepted {
            symbol,
            quantity,
            requested_at,
            ..
        } = self
        else {
            return None;
        };

        Some(Self::Failed {
            symbol: symbol.clone(),
            quantity: *quantity,
            reason: reason.to_string(),
            requested_at: *requested_at,
            failed_at,
        })
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{B256, U256};
    use async_trait::async_trait;
    use rust_decimal_macros::dec;
    use std::sync::Arc;

    use st0x_event_sorcery::{LifecycleError, TestHarness};

    use super::*;
    use crate::onchain::mock::MockRaindex;
    use crate::onchain::raindex::{Raindex, RaindexError, RaindexVaultId};

    #[test]
    fn test_evolve_accepted_rejects_wrong_state() {
        let completed = TokenizedEquityMint::Completed {
            symbol: Symbol::new("AAPL").unwrap(),
            quantity: dec!(100.5),
            issuer_request_id: IssuerRequestId("ISS123".to_string()),
            tokenization_request_id: TokenizationRequestId("TOK456".to_string()),
            token_tx_hash: TxHash::random(),
            vault_deposit_tx_hash: TxHash::random(),
            completed_at: Utc::now(),
        };

        let event = TokenizedEquityMintEvent::MintAccepted {
            issuer_request_id: IssuerRequestId("ISS999".to_string()),
            tokenization_request_id: TokenizationRequestId("TOK999".to_string()),
            accepted_at: Utc::now(),
        };

        let result = TokenizedEquityMint::evolve(&completed, &event).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_evolve_tokens_received_rejects_wrong_state() {
        let requested = TokenizedEquityMint::MintRequested {
            symbol: Symbol::new("AAPL").unwrap(),
            quantity: dec!(100.5),
            wallet: Address::random(),
            requested_at: Utc::now(),
        };

        let event = TokenizedEquityMintEvent::TokensReceived {
            tx_hash: TxHash::random(),
            receipt_id: ReceiptId(U256::from(789)),
            shares_minted: U256::from(100_500_000_000_000_000_000_u128),
            received_at: Utc::now(),
        };

        let result = TokenizedEquityMint::evolve(&requested, &event).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_evolve_completed_rejects_wrong_state() {
        let accepted = TokenizedEquityMint::MintAccepted {
            symbol: Symbol::new("AAPL").unwrap(),
            quantity: dec!(100.5),
            wallet: Address::random(),
            issuer_request_id: IssuerRequestId("ISS123".to_string()),
            tokenization_request_id: TokenizationRequestId("TOK456".to_string()),
            requested_at: Utc::now(),
            accepted_at: Utc::now(),
        };

        let event = TokenizedEquityMintEvent::MintCompleted {
            completed_at: Utc::now(),
        };

        let result = TokenizedEquityMint::evolve(&accepted, &event).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_evolve_rejected_rejects_non_requested_states() {
        let accepted = TokenizedEquityMint::MintAccepted {
            symbol: Symbol::new("AAPL").unwrap(),
            quantity: dec!(100.5),
            wallet: Address::random(),
            issuer_request_id: IssuerRequestId("ISS123".to_string()),
            tokenization_request_id: TokenizationRequestId("TOK456".to_string()),
            requested_at: Utc::now(),
            accepted_at: Utc::now(),
        };

        let event = TokenizedEquityMintEvent::MintRejected {
            reason: "Should not apply".to_string(),
            rejected_at: Utc::now(),
        };

        let result = TokenizedEquityMint::evolve(&accepted, &event).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_evolve_acceptance_failed_rejects_non_accepted_states() {
        let requested = TokenizedEquityMint::MintRequested {
            symbol: Symbol::new("AAPL").unwrap(),
            quantity: dec!(100.5),
            wallet: Address::random(),
            requested_at: Utc::now(),
        };

        let event = TokenizedEquityMintEvent::MintAcceptanceFailed {
            reason: "Should not apply".to_string(),
            failed_at: Utc::now(),
        };

        let result = TokenizedEquityMint::evolve(&requested, &event).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_evolve_rejects_mint_requested_on_existing_state() {
        let requested = TokenizedEquityMint::MintRequested {
            symbol: Symbol::new("AAPL").unwrap(),
            quantity: dec!(100.5),
            wallet: Address::random(),
            requested_at: Utc::now(),
        };

        let event = TokenizedEquityMintEvent::MintRequested {
            symbol: Symbol::new("GOOG").unwrap(),
            quantity: dec!(50.0),
            wallet: Address::random(),
            requested_at: Utc::now(),
        };

        let result = TokenizedEquityMint::evolve(&requested, &event).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_apply_vault_deposited_rejects_wrong_state() {
        let accepted = TokenizedEquityMint::MintAccepted {
            symbol: Symbol::new("AAPL").unwrap(),
            quantity: dec!(100.5),
            wallet: Address::random(),
            issuer_request_id: IssuerRequestId("ISS123".to_string()),
            tokenization_request_id: TokenizationRequestId("TOK456".to_string()),
            requested_at: Utc::now(),
            accepted_at: Utc::now(),
        };

        let event = TokenizedEquityMintEvent::VaultDeposited {
            vault_deposit_tx_hash: TxHash::random(),
            deposited_at: Utc::now(),
        };

        let result = TokenizedEquityMint::evolve(&accepted, &event).unwrap();
        assert_eq!(result, None);
    }

    struct FailingLookupRaindex;

    #[async_trait]
    impl Raindex for FailingLookupRaindex {
        async fn lookup_vault_id(&self, _token: Address) -> Result<RaindexVaultId, RaindexError> {
            Err(RaindexError::ZeroAmount)
        }

        async fn lookup_vault_info(
            &self,
            _symbol: &Symbol,
        ) -> Result<(Address, RaindexVaultId), RaindexError> {
            Err(RaindexError::ZeroAmount)
        }

        async fn deposit(
            &self,
            _token: Address,
            _vault_id: RaindexVaultId,
            _amount: U256,
            _decimals: u8,
        ) -> Result<TxHash, RaindexError> {
            unreachable!("deposit should not be called when lookup fails")
        }

        async fn withdraw(
            &self,
            _token: Address,
            _vault_id: RaindexVaultId,
            _target_amount: U256,
            _decimals: u8,
        ) -> Result<TxHash, RaindexError> {
            unreachable!()
        }
    }

    struct FailingDepositRaindex;

    #[async_trait]
    impl Raindex for FailingDepositRaindex {
        async fn lookup_vault_id(&self, _token: Address) -> Result<RaindexVaultId, RaindexError> {
            Ok(RaindexVaultId(B256::ZERO))
        }

        async fn lookup_vault_info(
            &self,
            _symbol: &Symbol,
        ) -> Result<(Address, RaindexVaultId), RaindexError> {
            Ok((Address::ZERO, RaindexVaultId(B256::ZERO)))
        }

        async fn deposit(
            &self,
            _token: Address,
            _vault_id: RaindexVaultId,
            _amount: U256,
            _decimals: u8,
        ) -> Result<TxHash, RaindexError> {
            Err(RaindexError::ZeroAmount)
        }

        async fn withdraw(
            &self,
            _token: Address,
            _vault_id: RaindexVaultId,
            _target_amount: U256,
            _decimals: u8,
        ) -> Result<TxHash, RaindexError> {
            unreachable!()
        }
    }

    use crate::tokenization::mock::{MockMintPollOutcome, MockMintRequestOutcome, MockTokenizer};

    fn mint_services(tokenizer: MockTokenizer) -> EquityTransferServices {
        EquityTransferServices {
            tokenizer: Arc::new(tokenizer),
            raindex: Arc::new(MockRaindex::new()),
        }
    }

    fn mint_command() -> TokenizedEquityMintCommand {
        TokenizedEquityMintCommand::Mint {
            issuer_request_id: IssuerRequestId::new("ISS001"),
            symbol: Symbol::new("AAPL").unwrap(),
            quantity: dec!(10),
            wallet: Address::ZERO,
        }
    }

    #[tokio::test]
    async fn initialize_mint_happy_path_emits_requested_accepted_tokens_received() {
        let events = TestHarness::<TokenizedEquityMint>::with(mint_services(MockTokenizer::new()))
            .given_no_previous_events()
            .when(mint_command())
            .await
            .events();

        assert_eq!(events.len(), 3);
        assert!(
            matches!(&events[0], TokenizedEquityMintEvent::MintRequested { symbol, .. } if symbol == &Symbol::new("AAPL").unwrap()),
            "Expected MintRequested, got: {:?}",
            events[0]
        );
        assert!(
            matches!(&events[1], TokenizedEquityMintEvent::MintAccepted { .. }),
            "Expected MintAccepted, got: {:?}",
            events[1]
        );
        assert!(
            matches!(&events[2], TokenizedEquityMintEvent::TokensReceived { shares_minted, .. } if *shares_minted == U256::from(10_000_000_000_000_000_000_u128)),
            "Expected TokensReceived, got: {:?}",
            events[2]
        );
    }

    #[tokio::test]
    async fn initialize_mint_immediate_rejection_emits_requested_and_rejected() {
        let tokenizer =
            MockTokenizer::new().with_mint_request_outcome(MockMintRequestOutcome::Rejected);
        let events = TestHarness::<TokenizedEquityMint>::with(mint_services(tokenizer))
            .given_no_previous_events()
            .when(mint_command())
            .await
            .events();

        assert_eq!(events.len(), 2);
        assert!(
            matches!(&events[0], TokenizedEquityMintEvent::MintRequested { .. }),
            "Expected MintRequested, got: {:?}",
            events[0]
        );
        assert!(
            matches!(&events[1], TokenizedEquityMintEvent::MintRejected { reason, .. } if reason == "Rejected by Alpaca"),
            "Expected MintRejected, got: {:?}",
            events[1]
        );
    }

    #[tokio::test]
    async fn initialize_mint_api_error_returns_request_failed() {
        let tokenizer =
            MockTokenizer::new().with_mint_request_outcome(MockMintRequestOutcome::ApiError);
        let error = TestHarness::<TokenizedEquityMint>::with(mint_services(tokenizer))
            .given_no_previous_events()
            .when(mint_command())
            .await
            .then_expect_error();

        assert!(
            matches!(
                error,
                LifecycleError::Apply(TokenizedEquityMintError::RequestFailed)
            ),
            "Expected RequestFailed, got: {error:?}",
        );
    }

    #[tokio::test]
    async fn initialize_mint_poll_failure_emits_acceptance_failed() {
        let tokenizer = MockTokenizer::new().with_mint_poll_outcome(MockMintPollOutcome::PollError);
        let events = TestHarness::<TokenizedEquityMint>::with(mint_services(tokenizer))
            .given_no_previous_events()
            .when(mint_command())
            .await
            .events();

        assert_eq!(events.len(), 3);
        assert!(
            matches!(&events[0], TokenizedEquityMintEvent::MintRequested { .. }),
            "Expected MintRequested, got: {:?}",
            events[0]
        );
        assert!(
            matches!(&events[1], TokenizedEquityMintEvent::MintAccepted { .. }),
            "Expected MintAccepted, got: {:?}",
            events[1]
        );
        assert!(
            matches!(&events[2], TokenizedEquityMintEvent::MintAcceptanceFailed { reason, .. } if reason.contains("Polling failed")),
            "Expected MintAcceptanceFailed, got: {:?}",
            events[2]
        );
    }

    #[tokio::test]
    async fn initialize_mint_post_acceptance_rejection_emits_acceptance_failed() {
        let tokenizer = MockTokenizer::new().with_mint_poll_outcome(MockMintPollOutcome::Rejected);
        let events = TestHarness::<TokenizedEquityMint>::with(mint_services(tokenizer))
            .given_no_previous_events()
            .when(mint_command())
            .await
            .events();

        assert_eq!(events.len(), 3);
        assert!(
            matches!(&events[0], TokenizedEquityMintEvent::MintRequested { .. }),
            "Expected MintRequested, got: {:?}",
            events[0]
        );
        assert!(
            matches!(&events[1], TokenizedEquityMintEvent::MintAccepted { .. }),
            "Expected MintAccepted, got: {:?}",
            events[1]
        );
        assert!(
            matches!(
                &events[2],
                TokenizedEquityMintEvent::MintAcceptanceFailed { reason, .. }
                    if reason == "Rejected by Alpaca after acceptance"
            ),
            "Expected MintAcceptanceFailed with rejection reason, got: {:?}",
            events[2]
        );
    }

    fn services_with_failing_lookup() -> EquityTransferServices {
        EquityTransferServices {
            tokenizer: Arc::new(crate::tokenization::mock::MockTokenizer::new()),
            raindex: Arc::new(FailingLookupRaindex),
        }
    }

    fn services_with_failing_deposit() -> EquityTransferServices {
        EquityTransferServices {
            tokenizer: Arc::new(crate::tokenization::mock::MockTokenizer::new()),
            raindex: Arc::new(FailingDepositRaindex),
        }
    }

    fn tokens_received_events() -> Vec<TokenizedEquityMintEvent> {
        vec![
            TokenizedEquityMintEvent::MintRequested {
                symbol: Symbol::new("AAPL").unwrap(),
                quantity: dec!(10),
                wallet: Address::ZERO,
                requested_at: Utc::now(),
            },
            TokenizedEquityMintEvent::MintAccepted {
                issuer_request_id: IssuerRequestId("ISS001".to_string()),
                tokenization_request_id: TokenizationRequestId("TOK001".to_string()),
                accepted_at: Utc::now(),
            },
            TokenizedEquityMintEvent::TokensReceived {
                tx_hash: TxHash::random(),
                receipt_id: ReceiptId(U256::from(1)),
                shares_minted: U256::from(10_000_000_000_000_000_000_u128),
                received_at: Utc::now(),
            },
        ]
    }

    #[tokio::test]
    async fn deposit_emits_raindex_deposit_failed_on_vault_lookup_failure() {
        let events = TestHarness::<TokenizedEquityMint>::with(services_with_failing_lookup())
            .given(tokens_received_events())
            .when(TokenizedEquityMintCommand::Deposit)
            .await
            .events();

        assert_eq!(events.len(), 1);
        assert!(
            matches!(
                &events[0],
                TokenizedEquityMintEvent::RaindexDepositFailed {
                    symbol,
                    quantity,
                    failed_tx_hash: None,
                    ..
                } if symbol == &Symbol::new("AAPL").unwrap() && *quantity == dec!(10)
            ),
            "Expected RaindexDepositFailed, got: {:?}",
            events[0]
        );
    }

    #[tokio::test]
    async fn deposit_emits_raindex_deposit_failed_on_deposit_failure() {
        let events = TestHarness::<TokenizedEquityMint>::with(services_with_failing_deposit())
            .given(tokens_received_events())
            .when(TokenizedEquityMintCommand::Deposit)
            .await
            .events();

        assert_eq!(events.len(), 1);
        assert!(
            matches!(
                &events[0],
                TokenizedEquityMintEvent::RaindexDepositFailed {
                    symbol,
                    quantity,
                    failed_tx_hash: None,
                    ..
                } if symbol == &Symbol::new("AAPL").unwrap() && *quantity == dec!(10)
            ),
            "Expected RaindexDepositFailed, got: {:?}",
            events[0]
        );
    }

    #[tokio::test]
    async fn deposit_happy_path_emits_vault_deposited_and_completed() {
        let events = TestHarness::<TokenizedEquityMint>::with(mint_services(MockTokenizer::new()))
            .given(tokens_received_events())
            .when(TokenizedEquityMintCommand::Deposit)
            .await
            .events();

        assert_eq!(events.len(), 2);
        assert!(
            matches!(&events[0], TokenizedEquityMintEvent::VaultDeposited { .. }),
            "Expected VaultDeposited, got: {:?}",
            events[0]
        );
        assert!(
            matches!(&events[1], TokenizedEquityMintEvent::MintCompleted { .. }),
            "Expected MintCompleted, got: {:?}",
            events[1]
        );
    }

    #[tokio::test]
    async fn mint_pending_status_emits_acceptance_failed() {
        let tokenizer = MockTokenizer::new().with_mint_poll_outcome(MockMintPollOutcome::Pending);
        let events = TestHarness::<TokenizedEquityMint>::with(mint_services(tokenizer))
            .given_no_previous_events()
            .when(mint_command())
            .await
            .events();

        assert_eq!(events.len(), 3);
        assert!(
            matches!(&events[2], TokenizedEquityMintEvent::MintAcceptanceFailed { reason, .. }
                if reason.contains("Pending")),
            "Expected MintAcceptanceFailed with Pending reason, got: {:?}",
            events[2]
        );
    }

    #[test]
    fn decimal_to_u256_18_decimals_rejects_negative() {
        let error = decimal_to_u256_18_decimals(dec!(-5)).unwrap_err();
        assert!(
            matches!(error, TokenizedEquityMintError::NegativeQuantity { .. }),
            "Expected NegativeQuantity, got: {error:?}"
        );
    }

    #[test]
    fn decimal_to_u256_18_decimals_converts_correctly() {
        let result = decimal_to_u256_18_decimals(dec!(3)).unwrap();
        assert_eq!(result, U256::from(3_000_000_000_000_000_000_u128));
    }

    #[test]
    fn decimal_to_u256_18_decimals_zero_returns_zero() {
        let result = decimal_to_u256_18_decimals(dec!(0)).unwrap();
        assert_eq!(result, U256::ZERO);
    }

    #[tokio::test]
    async fn mint_uses_command_issuer_request_id() {
        let events = TestHarness::<TokenizedEquityMint>::with(mint_services(MockTokenizer::new()))
            .given_no_previous_events()
            .when(mint_command())
            .await
            .events();

        assert!(
            matches!(
                &events[1],
                TokenizedEquityMintEvent::MintAccepted { issuer_request_id, .. }
                    if issuer_request_id.0 == "ISS001"
            ),
            "Expected issuer_request_id from command, got: {:?}",
            events[1]
        );
    }
}
