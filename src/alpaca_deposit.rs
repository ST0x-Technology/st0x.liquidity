//! The record of a USDC deposit to Alpaca sent through the capital
//! `alpaca-deposit` route, keyed by the operation id the client sends with
//! the request. Like `CctpBurnOperation` (ADR 0023), the signed transfer is
//! persisted before it is broadcast, so a rerun with the same id reports the
//! recorded transfer, or sends its same bytes again, instead of signing a
//! second one, and startup restores the nonce of a transfer whose fate is not
//! proven yet.

use alloy::primitives::{Address, TxHash, U256};
use alloy::providers::{Provider, RootProvider};
use alloy::transports::TransportError;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::SqlitePool;
use std::fmt;
use std::str::FromStr;
use std::sync::Arc;
use tracing::{error, info, warn};
use uuid::Uuid;

use st0x_event_sorcery::{DomainEvent, EventSourced, Nil, Store};
use st0x_evm::{PreparedTransaction, Wallet};

use crate::alerts::AlertKind;
use crate::cctp_burn::{ids_whose_latest_event_is, scrubbed};

/// The bot's Ethereum wallet, which signs every deposit transfer.
pub(crate) type DepositWallet = Arc<dyn Wallet<Provider = RootProvider>>;

/// The note the wallet logs a deposit transfer under, the one the bot's own
/// deposit leg signs its transfer with.
pub(crate) const DEPOSIT_TRANSFER_NOTE: &str = "USDC deposit to Alpaca";

/// The client's id for one deposit: every request carrying it means that
/// deposit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub(crate) struct AlpacaDepositOperationId(pub(crate) Uuid);

impl fmt::Display for AlpacaDepositOperationId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl FromStr for AlpacaDepositOperationId {
    type Err = uuid::Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        value.parse().map(Self)
    }
}

/// A deposit's state as the route reports it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum AlpacaDepositStatus {
    Pending,
    Confirmed,
    Reverted,
    Superseded,
}

/// What the chain proved about the signed transfer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum AlpacaDepositOutcome {
    /// Nothing proven yet: the transfer may still mine.
    Pending,
    /// Mined and succeeded, at Ethereum's required confirmations.
    Confirmed { confirmed_at: DateTime<Utc> },
    /// Mined and reverted, at Ethereum's required confirmations: it moved no
    /// USDC.
    Reverted { reverted_at: DateTime<Utc> },
    /// The signer's nonce at a block with Ethereum's required confirmations
    /// is past the transfer's nonce while the node shows no receipt for the
    /// transfer: another tx took its nonce, so it can never mine and moved
    /// no USDC.
    Superseded { superseded_at: DateTime<Utc> },
}

impl AlpacaDepositOutcome {
    pub(crate) const fn status(self) -> AlpacaDepositStatus {
        match self {
            Self::Pending => AlpacaDepositStatus::Pending,
            Self::Confirmed { .. } => AlpacaDepositStatus::Confirmed,
            Self::Reverted { .. } => AlpacaDepositStatus::Reverted,
            Self::Superseded { .. } => AlpacaDepositStatus::Superseded,
        }
    }
}

/// The signed transfer a deposit operation holds, and what it pays.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct DepositTransfer {
    /// The sent amount in USDC base units (6 decimals).
    pub(crate) amount: U256,
    /// Alpaca's USDC deposit address on Ethereum the transfer pays.
    pub(crate) deposit_address: Address,
    pub(crate) prepared: PreparedTransaction,
}

impl DepositTransfer {
    /// The signed transfer's own hash, which the bot broadcasts and restores.
    pub(crate) fn transfer_tx(&self) -> TxHash {
        self.prepared.tx_hash()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct AlpacaDepositOperation {
    pub(crate) transfer: DepositTransfer,
    pub(crate) prepared_at: DateTime<Utc>,
    pub(crate) outcome: AlpacaDepositOutcome,
}

impl AlpacaDepositOperation {
    /// Records `attempted` from `Pending`. The same outcome again is a no op,
    /// so a retry that reads the same chain state records nothing new; any
    /// other recorded outcome is a conflict.
    fn record_outcome(
        &self,
        attempted: AlpacaDepositOutcome,
    ) -> Result<Vec<AlpacaDepositOperationEvent>, AlpacaDepositOperationError> {
        let event = match (self.outcome, attempted) {
            (AlpacaDepositOutcome::Pending, AlpacaDepositOutcome::Confirmed { confirmed_at }) => {
                AlpacaDepositOperationEvent::Confirmed { confirmed_at }
            }
            (AlpacaDepositOutcome::Pending, AlpacaDepositOutcome::Reverted { reverted_at }) => {
                AlpacaDepositOperationEvent::Reverted { reverted_at }
            }
            (AlpacaDepositOutcome::Pending, AlpacaDepositOutcome::Superseded { superseded_at }) => {
                AlpacaDepositOperationEvent::Superseded { superseded_at }
            }
            (AlpacaDepositOutcome::Confirmed { .. }, AlpacaDepositOutcome::Confirmed { .. })
            | (AlpacaDepositOutcome::Reverted { .. }, AlpacaDepositOutcome::Reverted { .. })
            | (AlpacaDepositOutcome::Superseded { .. }, AlpacaDepositOutcome::Superseded { .. }) => {
                return Ok(Vec::new());
            }
            (recorded, attempted) => {
                return Err(AlpacaDepositOperationError::OutcomeConflict {
                    recorded,
                    attempted,
                });
            }
        };
        Ok(vec![event])
    }
}

#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum AlpacaDepositOperationError {
    #[error("the operation already holds a signed deposit transfer")]
    AlreadyPrepared,
    #[error("the operation holds no signed deposit transfer")]
    NotPrepared,
    #[error("the deposit's recorded outcome is {recorded:?}, not {attempted:?}")]
    OutcomeConflict {
        recorded: AlpacaDepositOutcome,
        attempted: AlpacaDepositOutcome,
    },
}

#[derive(Debug, Clone)]
pub(crate) enum AlpacaDepositOperationCommand {
    Prepare { transfer: DepositTransfer },
    RecordConfirmed,
    RecordReverted,
    RecordSuperseded,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum AlpacaDepositOperationEvent {
    Prepared {
        amount: U256,
        deposit_address: Address,
        prepared: PreparedTransaction,
        prepared_at: DateTime<Utc>,
    },
    Confirmed {
        confirmed_at: DateTime<Utc>,
    },
    Reverted {
        reverted_at: DateTime<Utc>,
    },
    Superseded {
        superseded_at: DateTime<Utc>,
    },
}

const PREPARED_EVENT_TYPE: &str = "AlpacaDepositOperationEvent::Prepared";

impl DomainEvent for AlpacaDepositOperationEvent {
    fn event_type(&self) -> String {
        match self {
            Self::Prepared { .. } => PREPARED_EVENT_TYPE.to_owned(),
            Self::Confirmed { .. } => "AlpacaDepositOperationEvent::Confirmed".to_owned(),
            Self::Reverted { .. } => "AlpacaDepositOperationEvent::Reverted".to_owned(),
            Self::Superseded { .. } => "AlpacaDepositOperationEvent::Superseded".to_owned(),
        }
    }

    fn event_version(&self) -> String {
        "1.0".to_owned()
    }
}

#[async_trait]
impl EventSourced for AlpacaDepositOperation {
    type Id = AlpacaDepositOperationId;
    type Event = AlpacaDepositOperationEvent;
    type Command = AlpacaDepositOperationCommand;
    type Error = AlpacaDepositOperationError;
    type Services = ();
    type Materialized = Nil;

    const AGGREGATE_TYPE: &'static str = "AlpacaDepositOperation";
    const PROJECTION: Nil = Nil;
    const SCHEMA_VERSION: u64 = 1;

    fn originate(event: &Self::Event) -> Option<Self> {
        match event {
            AlpacaDepositOperationEvent::Prepared {
                amount,
                deposit_address,
                prepared,
                prepared_at,
            } => Some(Self {
                transfer: DepositTransfer {
                    amount: *amount,
                    deposit_address: *deposit_address,
                    prepared: prepared.clone(),
                },
                prepared_at: *prepared_at,
                outcome: AlpacaDepositOutcome::Pending,
            }),
            AlpacaDepositOperationEvent::Confirmed { .. }
            | AlpacaDepositOperationEvent::Reverted { .. }
            | AlpacaDepositOperationEvent::Superseded { .. } => None,
        }
    }

    fn evolve(entity: &Self, event: &Self::Event) -> Result<Option<Self>, Self::Error> {
        let outcome = match event {
            AlpacaDepositOperationEvent::Prepared { .. } => return Ok(None),
            AlpacaDepositOperationEvent::Confirmed { confirmed_at } => {
                AlpacaDepositOutcome::Confirmed {
                    confirmed_at: *confirmed_at,
                }
            }
            AlpacaDepositOperationEvent::Reverted { reverted_at } => {
                AlpacaDepositOutcome::Reverted {
                    reverted_at: *reverted_at,
                }
            }
            AlpacaDepositOperationEvent::Superseded { superseded_at } => {
                AlpacaDepositOutcome::Superseded {
                    superseded_at: *superseded_at,
                }
            }
        };
        Ok(Some(Self {
            outcome,
            ..entity.clone()
        }))
    }

    async fn initialize(
        command: Self::Command,
        _services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        match command {
            AlpacaDepositOperationCommand::Prepare {
                transfer:
                    DepositTransfer {
                        amount,
                        deposit_address,
                        prepared,
                    },
            } => Ok(vec![AlpacaDepositOperationEvent::Prepared {
                amount,
                deposit_address,
                prepared,
                prepared_at: Utc::now(),
            }]),
            AlpacaDepositOperationCommand::RecordConfirmed
            | AlpacaDepositOperationCommand::RecordReverted
            | AlpacaDepositOperationCommand::RecordSuperseded => {
                Err(AlpacaDepositOperationError::NotPrepared)
            }
        }
    }

    async fn transition(
        &self,
        command: Self::Command,
        _services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        let now = Utc::now();
        match command {
            AlpacaDepositOperationCommand::Prepare { .. } => {
                Err(AlpacaDepositOperationError::AlreadyPrepared)
            }
            AlpacaDepositOperationCommand::RecordConfirmed => {
                self.record_outcome(AlpacaDepositOutcome::Confirmed { confirmed_at: now })
            }
            AlpacaDepositOperationCommand::RecordReverted => {
                self.record_outcome(AlpacaDepositOutcome::Reverted { reverted_at: now })
            }
            AlpacaDepositOperationCommand::RecordSuperseded => {
                self.record_outcome(AlpacaDepositOutcome::Superseded { superseded_at: now })
            }
        }
    }
}

/// The operations whose latest event leaves a signed transfer with no proven
/// outcome (`Prepared`), so startup can reserve their nonces before any other
/// send takes them. Unparseable ids are returned apart, for the caller to
/// page.
pub(crate) async fn pending_alpaca_deposit_ids(
    pool: &SqlitePool,
) -> Result<(Vec<AlpacaDepositOperationId>, Vec<String>), sqlx::Error> {
    // Bound to the constants the aggregate writes, so a rename can never
    // silently empty the startup restore.
    ids_whose_latest_event_is(
        pool,
        AlpacaDepositOperation::AGGREGATE_TYPE,
        PREPARED_EVENT_TYPE,
    )
    .await
}

/// What the chain proves about a recorded deposit transfer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DepositFate {
    Confirmed,
    Reverted,
    Superseded,
}

/// Reads what the chain proves about `prepared` at `required_confirmations`.
/// Its own canonical receipt decides once it is that deep. With no receipt at
/// all, a nonce of its signer past the transfer's, read at the newest block
/// with the required confirmations, proves another tx took that nonce, so
/// the transfer can never mine. `None` while nothing is proven: unmined with
/// its nonce still open, shallow, or bytes whose signer cannot be recovered.
///
/// The nonce is read before the receipt: a tx at the transfer's nonce is
/// mined at or before the block the nonce was read at, so if that tx is the
/// transfer, the receipt read after it shows the transfer.
pub(crate) async fn deposit_fate(
    provider: &RootProvider,
    prepared: &PreparedTransaction,
    required_confirmations: u64,
) -> Result<Option<DepositFate>, TransportError> {
    let nonce_taken = match prepared.signer() {
        Some(signer) => confirmed_nonce(provider, signer, required_confirmations)
            .await?
            .is_some_and(|confirmed| confirmed > prepared.nonce()),
        None => false,
    };
    let mined = st0x_evm::mined_tx(provider, prepared.tx_hash()).await?;
    Ok(match mined {
        Some(mined) if mined.confirmations >= required_confirmations => Some(if mined.succeeded {
            DepositFate::Confirmed
        } else {
            DepositFate::Reverted
        }),
        Some(_) => None,
        None if nonce_taken => Some(DepositFate::Superseded),
        None => None,
    })
}

/// The number of txs `signer` has mined at the newest block that has
/// `required_confirmations` (the inclusion block counts as the first), or
/// `None` while the chain is shorter than that.
async fn confirmed_nonce(
    provider: &RootProvider,
    signer: Address,
    required_confirmations: u64,
) -> Result<Option<u64>, TransportError> {
    let head = provider.get_block_number().await?;
    let Some(block) = (head + 1).checked_sub(required_confirmations.max(1)) else {
        return Ok(None);
    };
    Ok(Some(
        provider.get_transaction_count(signer).number(block).await?,
    ))
}

/// Records `fate` on the operation and returns the status it settles. A
/// failed write is logged: the chain still decides, so the next read records
/// it. A superseded transfer alerts: it moved no USDC, and the tx that took
/// its nonce is not the bot's.
async fn record_deposit_fate(
    store: &Store<AlpacaDepositOperation>,
    id: &AlpacaDepositOperationId,
    transfer: &DepositTransfer,
    fate: DepositFate,
) -> AlpacaDepositStatus {
    let transfer_tx = transfer.transfer_tx();
    let (command, status) = match fate {
        DepositFate::Confirmed => (
            AlpacaDepositOperationCommand::RecordConfirmed,
            AlpacaDepositStatus::Confirmed,
        ),
        DepositFate::Reverted => (
            AlpacaDepositOperationCommand::RecordReverted,
            AlpacaDepositStatus::Reverted,
        ),
        DepositFate::Superseded => (
            AlpacaDepositOperationCommand::RecordSuperseded,
            AlpacaDepositStatus::Superseded,
        ),
    };
    if let Err(error) = store.send(id, command).await {
        error!(operation_id = %id, %transfer_tx, ?fate, ?error, "Could not record the Alpaca deposit transfer's outcome; the next read of the chain records it");
    }
    if fate == DepositFate::Superseded {
        error!(target: "operational_alert", alert = true, kind = AlertKind::CapitalAlpacaDepositSuperseded.as_str(), operation_id = %id, %transfer_tx, nonce = transfer.prepared.nonce(), deposit_address = %transfer.deposit_address, amount_raw = %transfer.amount, "An operator Alpaca deposit transfer can never mine: another tx from the wallet took its nonce, so this transfer moved no USDC. Check that tx onchain: only a copy of the deposit sent by hand would have paid Alpaca");
    }
    status
}

/// The deposit's status: a recorded outcome stands, and a pending one is
/// settled from the chain ([`settle_pending_deposit`]).
pub(crate) async fn deposit_status(
    store: &Store<AlpacaDepositOperation>,
    wallet: &DepositWallet,
    id: &AlpacaDepositOperationId,
    operation: &AlpacaDepositOperation,
    required_confirmations: u64,
) -> AlpacaDepositStatus {
    if operation.outcome != AlpacaDepositOutcome::Pending {
        return operation.outcome.status();
    }
    settle_pending_deposit(
        store,
        wallet,
        id,
        &operation.transfer,
        required_confirmations,
    )
    .await
}

/// Reads the chain for a deposit recorded pending, and records the outcome
/// its receipt or its signer's nonce now decides. A superseded transfer
/// signed by `wallet` releases its nonce there, since none of its bytes can
/// mine any more. `Pending` while nothing is proven, or the read fails.
pub(crate) async fn settle_pending_deposit(
    store: &Store<AlpacaDepositOperation>,
    wallet: &DepositWallet,
    id: &AlpacaDepositOperationId,
    transfer: &DepositTransfer,
    required_confirmations: u64,
) -> AlpacaDepositStatus {
    let transfer_tx = transfer.transfer_tx();
    match deposit_fate(
        wallet.provider(),
        &transfer.prepared,
        required_confirmations,
    )
    .await
    {
        Ok(Some(fate)) => {
            let status = record_deposit_fate(store, id, transfer, fate).await;
            if fate == DepositFate::Superseded && transfer.prepared.signed_by(wallet.address()) {
                wallet.release_superseded(transfer_tx).await;
            }
            status
        }
        Ok(None) => AlpacaDepositStatus::Pending,
        Err(error) => {
            warn!(operation_id = %id, %transfer_tx, error = %scrubbed(&error), "Could not read the Alpaca deposit transfer's fate; reporting it pending");
            AlpacaDepositStatus::Pending
        }
    }
}

/// What the startup restore of pending deposits did.
#[derive(Debug, Default)]
pub(crate) struct RestoredAlpacaDeposits {
    /// Deposits whose nonce was reserved again; each is also rebroadcast
    /// unless the node shows it mined.
    pub(crate) restored: usize,
    /// Pending deposits whose fate the chain now decides, recorded instead of
    /// restored.
    pub(crate) settled: usize,
    /// The restored deposits, still unproven: until each is proven, the bot
    /// must not spend the Ethereum wallet's USDC.
    pub(crate) pending: Vec<(AlpacaDepositOperationId, DepositTransfer)>,
    /// The pending deposits could not all be listed or loaded, so one may be
    /// unproven without being restored.
    pub(crate) unreadable: bool,
    /// A restored deposit is not mined after its rebroadcast, or one signed
    /// by another wallet was skipped: startup skips the Ethereum approvals and
    /// revokes, which could take its nonce or wait behind it.
    pub(crate) unmined: bool,
}

/// Reserves the nonce of every pending deposit signed by `wallet` and
/// rebroadcasts its exact bytes, so no other send from the wallet takes the
/// nonce or waits behind a deposit no node holds after a restart. A pending
/// deposit the chain now decides is recorded instead. One the node shows
/// mined (a shallow receipt) keeps its reservation but is not sent again, and
/// one signed by another wallet (a rotated key) is not restored: that key's
/// nonces and USDC are not this wallet's. Never fails startup: a deposit that
/// cannot be listed, loaded or rebroadcast pages. Mirrors
/// `restore_pending_cctp_burns`.
pub(crate) async fn restore_pending_alpaca_deposits(
    pool: &SqlitePool,
    store: &Store<AlpacaDepositOperation>,
    wallet: &DepositWallet,
    required_confirmations: Option<u64>,
) -> RestoredAlpacaDeposits {
    let mut outcome = RestoredAlpacaDeposits::default();
    let (ids, unparseable) = match pending_alpaca_deposit_ids(pool).await {
        Ok(found) => found,
        Err(error) => {
            error!(target: "operational_alert", alert = true, kind = AlertKind::CapitalAlpacaDepositsUnlisted.as_str(), ?error, "Could not list the pending capital Alpaca deposits at startup; their nonces are not reserved, so the USDC driver stays paused until a restart and startup skips Ethereum token approvals and allowance revokes");
            outcome.unreadable = true;
            return outcome;
        }
    };
    if !unparseable.is_empty() {
        error!(target: "operational_alert", alert = true, kind = AlertKind::CapitalAlpacaDepositIdsUnparseable.as_str(), ?unparseable, "Pending capital Alpaca deposits with unparseable operation ids were not restored at startup, so the USDC driver stays paused until a restart and startup skips Ethereum token approvals and allowance revokes");
        outcome.unreadable = true;
    }

    for id in ids {
        let transfer = match store.load(&id).await {
            Ok(Some(operation)) if operation.outcome == AlpacaDepositOutcome::Pending => {
                operation.transfer
            }
            Ok(state) => {
                warn!(operation_id = %id, ?state, "Capital Alpaca deposit is no longer pending at startup");
                continue;
            }
            Err(error) => {
                error!(target: "operational_alert", alert = true, kind = AlertKind::CapitalAlpacaDepositUnloadable.as_str(), operation_id = %id, ?error, "Could not load a pending capital Alpaca deposit at startup; its nonce is not reserved, so the USDC driver stays paused until a restart and startup skips Ethereum token approvals and allowance revokes");
                outcome.unreadable = true;
                continue;
            }
        };
        let transfer_tx = transfer.transfer_tx();
        let nonce = transfer.prepared.nonce();

        if let Some(required) = required_confirmations
            && settle_pending_deposit(store, wallet, &id, &transfer, required).await
                != AlpacaDepositStatus::Pending
        {
            outcome.settled += 1;
            continue;
        }

        if !transfer.prepared.signed_by(wallet.address()) {
            error!(target: "operational_alert", alert = true, kind = AlertKind::CapitalAlpacaDepositSignedByAnotherWallet.as_str(), operation_id = %id, %transfer_tx, nonce, signer = ?transfer.prepared.signer(), wallet = %wallet.address(), "A pending capital Alpaca deposit was signed by another wallet (was the key rotated?); not restoring it into this wallet's nonces, so startup skips Ethereum token approvals and allowance revokes");
            outcome.unmined = true;
            continue;
        }
        // Like the siblings, the nonce is reserved even for a transfer the
        // node shows mined: a shallow one can still be reorged out, and the
        // hold keeps a fresh send from taking its nonce then.
        wallet.restore_prepared(&transfer.prepared).await;
        info!(operation_id = %id, %transfer_tx, nonce, "Reserved the nonce of a pending capital Alpaca deposit");
        outcome.restored += 1;

        let mined_before = st0x_evm::mined_tx(wallet.provider(), transfer_tx).await;
        if !matches!(mined_before, Ok(Some(_))) {
            if let Err(error) = wallet
                .broadcast_prepared(&transfer.prepared, DEPOSIT_TRANSFER_NOTE)
                .await
            {
                error!(target: "operational_alert", alert = true, kind = AlertKind::CapitalAlpacaDepositRebroadcastFailed.as_str(), operation_id = %id, %transfer_tx, nonce, error = %scrubbed(&error), "Could not rebroadcast a pending capital Alpaca deposit at startup; its nonce stays reserved, the USDC driver stays paused, and the bot keeps sending the same bytes until its fate is proven");
                outcome.unmined = true;
            } else if !matches!(
                st0x_evm::mined_tx(wallet.provider(), transfer_tx).await,
                Ok(Some(_))
            ) {
                warn!(operation_id = %id, %transfer_tx, nonce, "Restored capital Alpaca deposit is not mined yet at startup");
                outcome.unmined = true;
            }
        }
        outcome.pending.push((id, transfer));
    }

    outcome
}

#[cfg(test)]
mod tests {
    use st0x_event_sorcery::{LifecycleError, TestHarness, test_store};

    use super::*;
    use crate::test_utils::setup_test_db;

    fn prepared_event() -> AlpacaDepositOperationEvent {
        AlpacaDepositOperationEvent::Prepared {
            amount: U256::from(5_000_000_u64),
            deposit_address: Address::repeat_byte(0x11),
            prepared: PreparedTransaction::for_test(TxHash::repeat_byte(0x22), 7),
            prepared_at: Utc::now(),
        }
    }

    fn prepare_command() -> AlpacaDepositOperationCommand {
        AlpacaDepositOperationCommand::Prepare {
            transfer: DepositTransfer {
                amount: U256::from(5_000_000_u64),
                deposit_address: Address::repeat_byte(0x11),
                prepared: PreparedTransaction::for_test(TxHash::repeat_byte(0x33), 8),
            },
        }
    }

    /// A second signed transfer for one operation id is refused, so one id
    /// can never broadcast two deposits.
    #[tokio::test]
    async fn a_second_prepare_for_the_same_operation_is_refused() {
        let error = TestHarness::<AlpacaDepositOperation>::with(())
            .given(vec![prepared_event()])
            .when(prepare_command())
            .await
            .then_expect_error();

        assert!(matches!(
            error,
            LifecycleError::Apply(AlpacaDepositOperationError::AlreadyPrepared)
        ));
    }

    /// Reading the same chain state twice records the outcome once; a
    /// different outcome after it is a conflict, never a silent overwrite.
    #[tokio::test]
    async fn an_outcome_is_recorded_once_and_never_overwritten() {
        let superseded = AlpacaDepositOperationEvent::Superseded {
            superseded_at: Utc::now(),
        };
        TestHarness::<AlpacaDepositOperation>::with(())
            .given(vec![prepared_event(), superseded.clone()])
            .when(AlpacaDepositOperationCommand::RecordSuperseded)
            .await
            .then_expect_events(&[]);

        let error = TestHarness::<AlpacaDepositOperation>::with(())
            .given(vec![prepared_event(), superseded])
            .when(AlpacaDepositOperationCommand::RecordConfirmed)
            .await
            .then_expect_error();
        assert!(matches!(
            error,
            LifecycleError::Apply(AlpacaDepositOperationError::OutcomeConflict {
                recorded: AlpacaDepositOutcome::Superseded { .. },
                attempted: AlpacaDepositOutcome::Confirmed { .. },
            })
        ));
    }

    /// Startup restores only the deposits still pending.
    #[tokio::test]
    async fn only_pending_deposits_are_listed_for_the_startup_restore() {
        let pool = setup_test_db().await;
        let store = test_store::<AlpacaDepositOperation>(pool.clone(), ());
        let pending = AlpacaDepositOperationId(Uuid::new_v4());
        let superseded = AlpacaDepositOperationId(Uuid::new_v4());
        for id in [pending, superseded] {
            store.send(&id, prepare_command()).await.unwrap();
        }
        store
            .send(&superseded, AlpacaDepositOperationCommand::RecordSuperseded)
            .await
            .unwrap();

        let (ids, unparseable) = pending_alpaca_deposit_ids(&pool).await.unwrap();

        assert_eq!(ids, vec![pending]);
        assert!(unparseable.is_empty());
    }
}
