//! The record of a CCTP burn sent through the capital `cctp-bridge` route,
//! keyed by the operation id the client sends with the request. The signed
//! burn is persisted before it is broadcast, so a retried request with the
//! same id adopts the recorded burn instead of burning again (ADR 0023).

use alloy::eips::eip2718::EIP7702_TX_TYPE_ID;
use alloy::primitives::{Address, TxHash, U256};
use alloy::providers::RootProvider;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures_util::FutureExt as _;
use itertools::{Either, Itertools};
use serde::{Deserialize, Serialize};
use sqlx::SqlitePool;
use std::collections::BTreeSet;
use std::fmt;
use std::str::FromStr;
use std::sync::Arc;
use tracing::{error, info, warn};
use uuid::Uuid;

use st0x_bridge::BridgeDirection;
use st0x_bridge::cctp::{CctpBridge, CctpCtx, CctpError};
use st0x_config::{ChainRegistry, Ctx, OnchainWalletCtx};
use st0x_event_sorcery::{DomainEvent, EventSourced, Nil, Store};
use st0x_evm::{Chain, MinedTx, PreparedTransaction, Wallet};

/// The CCTP bridge over the bot's own Ethereum and Base wallets, as the
/// capital `cctp-bridge` route and the startup burn restore use it.
pub(crate) type BotCctpBridge =
    CctpBridge<Arc<dyn Wallet<Provider = RootProvider>>, Arc<dyn Wallet<Provider = RootProvider>>>;

/// Builds [`BotCctpBridge`] from the bot's wallets and its corridor. The CLI
/// passes the production constants; the bot takes its own configured
/// overrides, like the conductor's bridge, so a test deployment's burn reaches
/// the same contracts as its rebalances.
pub(crate) fn bot_cctp_bridge(
    ctx: &Ctx,
    wallets: &OnchainWalletCtx,
) -> Result<BotCctpBridge, CctpError> {
    CctpBridge::try_from_ctx(CctpCtx {
        corridor: ctx.rebalancing.cctp_corridor,
        ethereum_wallet: Arc::clone(wallets.ethereum_wallet()),
        base_wallet: Arc::clone(wallets.base_wallet()),
        #[cfg(feature = "test-support")]
        circle_api_base: ctx.rebalancing.circle_api_base.clone(),
        #[cfg(feature = "test-support")]
        token_messenger: ctx.rebalancing.token_messenger,
        #[cfg(feature = "test-support")]
        message_transmitter: ctx.rebalancing.message_transmitter,
    })
}

/// The burn's chain, kebab cased on the wire (`ethereum`, `base`), matching
/// the CLI's `--source-chain` and `--from` values; the mint lands on the
/// other one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum CctpSourceChain {
    Ethereum,
    Base,
}

impl CctpSourceChain {
    pub(crate) const fn bridge_direction(self) -> BridgeDirection {
        match self {
            Self::Ethereum => BridgeDirection::EthereumToBase,
            Self::Base => BridgeDirection::BaseToEthereum,
        }
    }

    pub(crate) const fn chain(self) -> Chain {
        match self {
            Self::Ethereum => Chain::Ethereum,
            Self::Base => Chain::Base,
        }
    }

    pub(crate) const fn destination_chain(self) -> Chain {
        match self {
            Self::Ethereum => Chain::Base,
            Self::Base => Chain::Ethereum,
        }
    }
}

/// A burn's state as the routes report it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CctpBurnStatus {
    Pending,
    Confirmed,
    Reverted,
    Superseded,
    Replaced,
}

/// The client's id for one burn: every request carrying it means that burn.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub(crate) struct CctpBurnOperationId(pub(crate) Uuid);

impl fmt::Display for CctpBurnOperationId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl FromStr for CctpBurnOperationId {
    type Err = uuid::Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        value.parse().map(Self)
    }
}

/// The amount a request asked to burn, as it asked: an exact amount, or the
/// source wallet's whole balance read under the driver pause.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum RequestedBurn {
    Exact { amount: U256 },
    All,
}

/// What the chain proved about the signed burn.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum CctpBurnOutcome {
    /// Nothing proven yet: the burn may still mine.
    Pending,
    /// Mined and succeeded, at the source chain's required confirmations.
    Confirmed { confirmed_at: DateTime<Utc> },
    /// Mined and reverted, at the source chain's required confirmations: it
    /// burned nothing.
    Reverted { reverted_at: DateTime<Utc> },
    /// Another tx from the source wallet is mined at the burn's nonce, at the
    /// required confirmations, so the burn can never mine.
    Superseded {
        superseding_tx: TxHash,
        superseded_at: DateTime<Utc>,
    },
    /// A fee bumped copy of the burn (same signer, nonce, contract and
    /// calldata) is mined at the burn's nonce, succeeded and emitted
    /// `MessageSent`, at the required confirmations: that copy did the burn.
    Replaced {
        replacement_tx: TxHash,
        replaced_at: DateTime<Utc>,
    },
}

impl CctpBurnOutcome {
    pub(crate) const fn status(self) -> CctpBurnStatus {
        match self {
            Self::Pending => CctpBurnStatus::Pending,
            Self::Confirmed { .. } => CctpBurnStatus::Confirmed,
            Self::Reverted { .. } => CctpBurnStatus::Reverted,
            Self::Superseded { .. } => CctpBurnStatus::Superseded,
            Self::Replaced { .. } => CctpBurnStatus::Replaced,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct CctpBurnOperation {
    pub(crate) source: CctpSourceChain,
    pub(crate) requested: RequestedBurn,
    /// The burned amount in USDC base units (6 decimals).
    pub(crate) amount: U256,
    pub(crate) recipient: Address,
    pub(crate) prepared: PreparedTransaction,
    pub(crate) prepared_at: DateTime<Utc>,
    pub(crate) outcome: CctpBurnOutcome,
}

impl CctpBurnOperation {
    /// The signed burn's own hash, which the bot broadcasts and restores.
    pub(crate) fn burn_tx(&self) -> TxHash {
        self.prepared.tx_hash()
    }

    /// The tx that burned: the replacement that took the signed burn's
    /// nonce, if one was recorded, else the signed burn.
    pub(crate) fn effective_burn_tx(&self) -> TxHash {
        match self.outcome {
            CctpBurnOutcome::Replaced { replacement_tx, .. } => replacement_tx,
            CctpBurnOutcome::Pending
            | CctpBurnOutcome::Confirmed { .. }
            | CctpBurnOutcome::Reverted { .. }
            | CctpBurnOutcome::Superseded { .. } => self.burn_tx(),
        }
    }
}

#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum CctpBurnOperationError {
    #[error("the operation already holds a signed burn")]
    AlreadyPrepared,
    #[error("the operation holds no signed burn")]
    NotPrepared,
    #[error("the burn's recorded outcome is {recorded:?}, not {attempted:?}")]
    OutcomeConflict {
        recorded: CctpBurnOutcome,
        attempted: CctpBurnOutcome,
    },
}

#[derive(Debug, Clone)]
pub(crate) enum CctpBurnOperationCommand {
    Prepare {
        source: CctpSourceChain,
        requested: RequestedBurn,
        amount: U256,
        recipient: Address,
        prepared: PreparedTransaction,
    },
    RecordConfirmed,
    RecordReverted,
    RecordSuperseded {
        superseding_tx: TxHash,
    },
    RecordReplaced {
        replacement_tx: TxHash,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum CctpBurnOperationEvent {
    Prepared {
        source: CctpSourceChain,
        requested: RequestedBurn,
        amount: U256,
        recipient: Address,
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
        superseding_tx: TxHash,
        superseded_at: DateTime<Utc>,
    },
    Replaced {
        replacement_tx: TxHash,
        replaced_at: DateTime<Utc>,
    },
}

const PREPARED_EVENT_TYPE: &str = "CctpBurnOperationEvent::Prepared";

impl DomainEvent for CctpBurnOperationEvent {
    fn event_type(&self) -> String {
        match self {
            Self::Prepared { .. } => PREPARED_EVENT_TYPE.to_owned(),
            Self::Confirmed { .. } => "CctpBurnOperationEvent::Confirmed".to_owned(),
            Self::Reverted { .. } => "CctpBurnOperationEvent::Reverted".to_owned(),
            Self::Superseded { .. } => "CctpBurnOperationEvent::Superseded".to_owned(),
            Self::Replaced { .. } => "CctpBurnOperationEvent::Replaced".to_owned(),
        }
    }

    fn event_version(&self) -> String {
        "1.0".to_owned()
    }
}

impl CctpBurnOperation {
    /// Records `attempted` from `Pending`. The same outcome again is a no op,
    /// so a retry that reads the same receipt records nothing new; any other
    /// recorded outcome is a conflict.
    fn record_outcome(
        &self,
        attempted: CctpBurnOutcome,
    ) -> Result<Vec<CctpBurnOperationEvent>, CctpBurnOperationError> {
        let event = match (self.outcome, attempted) {
            (CctpBurnOutcome::Pending, CctpBurnOutcome::Confirmed { confirmed_at }) => {
                CctpBurnOperationEvent::Confirmed { confirmed_at }
            }
            (CctpBurnOutcome::Pending, CctpBurnOutcome::Reverted { reverted_at }) => {
                CctpBurnOperationEvent::Reverted { reverted_at }
            }
            (
                CctpBurnOutcome::Pending,
                CctpBurnOutcome::Superseded {
                    superseding_tx,
                    superseded_at,
                },
            ) => CctpBurnOperationEvent::Superseded {
                superseding_tx,
                superseded_at,
            },
            (
                CctpBurnOutcome::Pending,
                CctpBurnOutcome::Replaced {
                    replacement_tx,
                    replaced_at,
                },
            ) => CctpBurnOperationEvent::Replaced {
                replacement_tx,
                replaced_at,
            },
            (CctpBurnOutcome::Confirmed { .. }, CctpBurnOutcome::Confirmed { .. })
            | (CctpBurnOutcome::Reverted { .. }, CctpBurnOutcome::Reverted { .. }) => {
                return Ok(Vec::new());
            }
            (
                CctpBurnOutcome::Superseded {
                    superseding_tx: recorded,
                    ..
                },
                CctpBurnOutcome::Superseded { superseding_tx, .. },
            ) if recorded == superseding_tx => return Ok(Vec::new()),
            (
                CctpBurnOutcome::Replaced {
                    replacement_tx: recorded,
                    ..
                },
                CctpBurnOutcome::Replaced { replacement_tx, .. },
            ) if recorded == replacement_tx => return Ok(Vec::new()),
            (recorded, attempted) => {
                return Err(CctpBurnOperationError::OutcomeConflict {
                    recorded,
                    attempted,
                });
            }
        };
        Ok(vec![event])
    }
}

#[async_trait]
impl EventSourced for CctpBurnOperation {
    type Id = CctpBurnOperationId;
    type Event = CctpBurnOperationEvent;
    type Command = CctpBurnOperationCommand;
    type Error = CctpBurnOperationError;
    type Services = ();
    type Materialized = Nil;

    const AGGREGATE_TYPE: &'static str = "CctpBurnOperation";
    const PROJECTION: Nil = Nil;
    const SCHEMA_VERSION: u64 = 1;

    fn originate(event: &Self::Event) -> Option<Self> {
        match event {
            CctpBurnOperationEvent::Prepared {
                source,
                requested,
                amount,
                recipient,
                prepared,
                prepared_at,
            } => Some(Self {
                source: *source,
                requested: *requested,
                amount: *amount,
                recipient: *recipient,
                prepared: prepared.clone(),
                prepared_at: *prepared_at,
                outcome: CctpBurnOutcome::Pending,
            }),
            CctpBurnOperationEvent::Confirmed { .. }
            | CctpBurnOperationEvent::Reverted { .. }
            | CctpBurnOperationEvent::Superseded { .. }
            | CctpBurnOperationEvent::Replaced { .. } => None,
        }
    }

    fn evolve(entity: &Self, event: &Self::Event) -> Result<Option<Self>, Self::Error> {
        let outcome = match event {
            CctpBurnOperationEvent::Prepared { .. } => return Ok(None),
            CctpBurnOperationEvent::Confirmed { confirmed_at } => CctpBurnOutcome::Confirmed {
                confirmed_at: *confirmed_at,
            },
            CctpBurnOperationEvent::Reverted { reverted_at } => CctpBurnOutcome::Reverted {
                reverted_at: *reverted_at,
            },
            CctpBurnOperationEvent::Superseded {
                superseding_tx,
                superseded_at,
            } => CctpBurnOutcome::Superseded {
                superseding_tx: *superseding_tx,
                superseded_at: *superseded_at,
            },
            CctpBurnOperationEvent::Replaced {
                replacement_tx,
                replaced_at,
            } => CctpBurnOutcome::Replaced {
                replacement_tx: *replacement_tx,
                replaced_at: *replaced_at,
            },
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
            CctpBurnOperationCommand::Prepare {
                source,
                requested,
                amount,
                recipient,
                prepared,
            } => Ok(vec![CctpBurnOperationEvent::Prepared {
                source,
                requested,
                amount,
                recipient,
                prepared,
                prepared_at: Utc::now(),
            }]),
            CctpBurnOperationCommand::RecordConfirmed
            | CctpBurnOperationCommand::RecordReverted
            | CctpBurnOperationCommand::RecordSuperseded { .. }
            | CctpBurnOperationCommand::RecordReplaced { .. } => {
                Err(CctpBurnOperationError::NotPrepared)
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
            CctpBurnOperationCommand::Prepare { .. } => {
                Err(CctpBurnOperationError::AlreadyPrepared)
            }
            CctpBurnOperationCommand::RecordConfirmed => {
                self.record_outcome(CctpBurnOutcome::Confirmed { confirmed_at: now })
            }
            CctpBurnOperationCommand::RecordReverted => {
                self.record_outcome(CctpBurnOutcome::Reverted { reverted_at: now })
            }
            CctpBurnOperationCommand::RecordSuperseded { superseding_tx } => {
                self.record_outcome(CctpBurnOutcome::Superseded {
                    superseding_tx,
                    superseded_at: now,
                })
            }
            CctpBurnOperationCommand::RecordReplaced { replacement_tx } => {
                self.record_outcome(CctpBurnOutcome::Replaced {
                    replacement_tx,
                    replaced_at: now,
                })
            }
        }
    }
}

/// The operations whose latest event leaves a signed burn with no proven
/// outcome (`Prepared`), so startup can reserve those burns' nonces before any
/// other send takes them. Unparseable ids are returned apart, for the caller
/// to page.
pub(crate) async fn pending_cctp_burn_ids(
    pool: &SqlitePool,
) -> Result<(Vec<CctpBurnOperationId>, Vec<String>), sqlx::Error> {
    // Bound to the constants the aggregate writes, so a rename can never
    // silently empty the startup restore.
    let rows: Vec<String> = sqlx::query_scalar(
        "WITH latest AS ( \
             SELECT aggregate_id, MAX(sequence) AS max_seq \
             FROM events \
             WHERE aggregate_type = ?1 \
             GROUP BY aggregate_id \
         ) \
         SELECT latest.aggregate_id \
         FROM events last_ev \
         INNER JOIN latest \
             ON last_ev.aggregate_id = latest.aggregate_id \
            AND last_ev.sequence = latest.max_seq \
         WHERE last_ev.aggregate_type = ?1 \
           AND last_ev.event_type = ?2 \
         ORDER BY latest.aggregate_id",
    )
    .bind(CctpBurnOperation::AGGREGATE_TYPE)
    .bind(PREPARED_EVENT_TYPE)
    .fetch_all(pool)
    .await?;

    Ok(rows.into_iter().partition_map(|raw| {
        raw.parse::<CctpBurnOperationId>()
            .map_or_else(|_| Either::Right(raw), Either::Left)
    }))
}

/// What a burn's own receipt proves once it has the source chain's required
/// confirmations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BurnReceiptFate {
    Confirmed,
    Reverted,
}

/// Reads the burn's own canonical receipt on its source chain: `None` while it
/// is unmined or shallower than `required_confirmations`, since a drop report,
/// a timeout or a shallow receipt proves nothing. A successful receipt counts
/// as a burn only with the CCTP `MessageSent` event, the rule `confirm_burn`
/// applies: without it Circle has nothing to attest, so the burn is not
/// recorded confirmed but paged, and stays pending for the operator.
pub(crate) async fn burn_receipt_fate(
    bridge: &BotCctpBridge,
    source: CctpSourceChain,
    burn_tx: TxHash,
    required_confirmations: u64,
) -> Result<Option<BurnReceiptFate>, CctpError> {
    let direction = source.bridge_direction();
    let Some(mined) = bridge
        .source_mined_tx(direction, burn_tx)
        .boxed()
        .await?
        .filter(|mined| mined.confirmations >= required_confirmations)
    else {
        return Ok(None);
    };
    if !mined.succeeded {
        return Ok(Some(BurnReceiptFate::Reverted));
    }
    match bridge
        .source_emitted_message_sent(direction, burn_tx)
        .boxed()
        .await?
    {
        Some(true) => Ok(Some(BurnReceiptFate::Confirmed)),
        Some(false) => {
            error!(target: "operational_alert", alert = true, %burn_tx, ?source, "CCTP burn mined and succeeded but emitted no MessageSent, so Circle has nothing to attest; check the configured TokenMessenger before completing or retrying it");
            Ok(None)
        }
        // The node that answered this read has no receipt yet, though the
        // previous read found one: it lags, and proves nothing.
        None => Ok(None),
    }
}

/// Records `fate` on the operation. A failed write is logged: the burn's
/// receipt still decides, so the next read records it.
pub(crate) async fn record_burn_fate(
    store: &Store<CctpBurnOperation>,
    id: &CctpBurnOperationId,
    burn_tx: TxHash,
    fate: BurnReceiptFate,
) -> CctpBurnStatus {
    let (command, status) = match fate {
        BurnReceiptFate::Confirmed => (
            CctpBurnOperationCommand::RecordConfirmed,
            CctpBurnStatus::Confirmed,
        ),
        BurnReceiptFate::Reverted => (
            CctpBurnOperationCommand::RecordReverted,
            CctpBurnStatus::Reverted,
        ),
    };
    if let Err(error) = store.send(id, command).await {
        error!(operation_id = %id, %burn_tx, ?fate, ?error, "Could not record the CCTP burn's outcome; the next read of its receipt records it");
    }
    status
}

/// The burn's status after reading its receipt: a recorded outcome stands,
/// and a pending burn whose receipt now decides records that outcome.
pub(crate) async fn settle_burn_from_receipt(
    store: &Store<CctpBurnOperation>,
    bridge: &BotCctpBridge,
    id: &CctpBurnOperationId,
    operation: &CctpBurnOperation,
    required_confirmations: u64,
) -> CctpBurnStatus {
    if operation.outcome != CctpBurnOutcome::Pending {
        return operation.outcome.status();
    }
    let burn_tx = operation.burn_tx();
    match burn_receipt_fate(bridge, operation.source, burn_tx, required_confirmations).await {
        Ok(Some(fate)) => record_burn_fate(store, id, burn_tx, fate).await,
        Ok(None) => CctpBurnStatus::Pending,
        Err(error) => {
            warn!(operation_id = %id, %burn_tx, ?error, "Could not read the CCTP burn's receipt; reporting it pending");
            CctpBurnStatus::Pending
        }
    }
}

/// Why a tx the operator named does not prove a signed burn can never mine.
#[derive(Debug, thiserror::Error)]
pub(crate) enum BurnNotSuperseded {
    #[error("burn {tx} is not a readable signed tx; its signer is unknown")]
    UnreadableBurn { tx: TxHash },
    /// Nonces are per sender, so only a tx from the burn's signer can take
    /// its nonce, and the check reads the configured wallet's txs only.
    #[error(
        "burn {tx} was signed by {signer}, not the bot wallet {bot_wallet} (was the key \
         rotated?): only a tx from {signer} at its nonce can supersede it"
    )]
    BurnSignedByAnotherWallet {
        tx: TxHash,
        signer: Address,
        bot_wallet: Address,
    },
    #[error(
        "burn {tx} itself is mined, with {confirmations} of the {required} required \
         confirmations, so no other tx took its nonce; a cctp-bridge rerun with its operation \
         id reports its outcome"
    )]
    BurnMined {
        tx: TxHash,
        confirmations: u64,
        required: u64,
    },
    #[error("superseding tx {tx} is the burn itself")]
    SupersedingTxIsTheBurn { tx: TxHash },
    #[error(
        "superseding tx {superseding} is not mined (unknown hash, or still pending); retry \
         once it is mined"
    )]
    SupersedingTxNotMined { superseding: TxHash },
    #[error("superseding tx {superseding} was sent by {from}, not the bot wallet {bot_wallet}")]
    SupersedingTxFromAnotherSender {
        superseding: TxHash,
        from: Address,
        bot_wallet: Address,
    },
    #[error(
        "superseding tx {superseding} is at nonce {superseding_nonce}, not the burn's nonce \
         {nonce}"
    )]
    SupersedingTxAtAnotherNonce {
        superseding: TxHash,
        superseding_nonce: u64,
        nonce: u64,
    },
    #[error(
        "superseding tx {superseding} has {confirmations} of the {required} required \
         confirmations; retry once it has them"
    )]
    SupersedingTxUnconfirmed {
        superseding: TxHash,
        confirmations: u64,
        required: u64,
    },
    /// A successful tx at the nonce moved something unless it is a plain
    /// cancel; only a copy of the burn itself is adopted as its replacement.
    #[error(
        "superseding tx {superseding} succeeded but is neither a plain cancel (a 0 value \
         transfer to the bot wallet {bot_wallet} with no calldata and no logs) nor a copy of \
         the burn (its calldata sent to the TokenMessenger, emitting MessageSent), so what it \
         did is unknown"
    )]
    SupersedingTxNotAPlainCancel {
        superseding: TxHash,
        bot_wallet: Address,
    },
    #[error("could not read tx {tx} on chain; retry")]
    Read {
        tx: TxHash,
        #[source]
        source: Box<CctpError>,
    },
}

/// How a mined tx took a signed burn's nonce.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NonceTakenBy {
    /// A plain cancel, or any reverted tx: nothing was burned.
    Cancel,
    /// A fee bumped copy of the burn: it burned in the signed burn's place.
    Replacement,
}

/// Checks that `superseding` took the nonce of `prepared`, an operation's
/// signed burn, so the burn can never mine. Mirrors
/// `verify_withdrawal_superseded`, and like the vault withdrawal's
/// `verify_withdrawal_replacement` it adopts a copy of the signed tx that
/// went out in its place.
///
/// The burn must be signed by `bot_wallet` and not be mined itself.
/// `superseding` must be a different tx from `bot_wallet` at the burn's nonce
/// with `required_confirmations`. A reverted one burned nothing. A successful
/// one is a [`NonceTakenBy::Cancel`] only as a plain cancel, or a
/// [`NonceTakenBy::Replacement`] only when it sent the burn's exact calldata to
/// the burn's `TokenMessengerV2` with no value and emitted `MessageSent`: a fee
/// bumped copy of the burn, which burned the same amount toward the same
/// recipient. Anything else is refused, since it may have moved funds. Only a
/// tx the node shows mined in the canonical chain counts, so a node that lags
/// refuses rather than settles.
pub(crate) async fn verify_burn_superseded(
    bridge: &BotCctpBridge,
    source: CctpSourceChain,
    prepared: &PreparedTransaction,
    superseding: TxHash,
    bot_wallet: Address,
    required_confirmations: u64,
) -> Result<NonceTakenBy, BurnNotSuperseded> {
    let tx = prepared.tx_hash();
    let nonce = prepared.nonce();
    let direction = source.bridge_direction();

    let signer = prepared
        .signer()
        .ok_or(BurnNotSuperseded::UnreadableBurn { tx })?;
    if signer != bot_wallet {
        return Err(BurnNotSuperseded::BurnSignedByAnotherWallet {
            tx,
            signer,
            bot_wallet,
        });
    }
    if let Some(burn) = read_source_tx(bridge, direction, tx).await? {
        return Err(BurnNotSuperseded::BurnMined {
            tx,
            confirmations: burn.confirmations,
            required: required_confirmations,
        });
    }
    if superseding == tx {
        return Err(BurnNotSuperseded::SupersedingTxIsTheBurn { tx });
    }

    let Some(MinedTx {
        from,
        to,
        nonce: superseding_nonce,
        value,
        input,
        tx_type,
        emitted_logs,
        succeeded,
        confirmations,
    }) = read_source_tx(bridge, direction, superseding).await?
    else {
        return Err(BurnNotSuperseded::SupersedingTxNotMined { superseding });
    };
    if from != bot_wallet {
        return Err(BurnNotSuperseded::SupersedingTxFromAnotherSender {
            superseding,
            from,
            bot_wallet,
        });
    }
    if superseding_nonce != nonce {
        return Err(BurnNotSuperseded::SupersedingTxAtAnotherNonce {
            superseding,
            superseding_nonce,
            nonce,
        });
    }
    if confirmations < required_confirmations {
        return Err(BurnNotSuperseded::SupersedingTxUnconfirmed {
            superseding,
            confirmations,
            required: required_confirmations,
        });
    }
    if !succeeded {
        return Ok(NonceTakenBy::Cancel);
    }

    let plain_cancel = to == Some(bot_wallet)
        && value.is_zero()
        && input.is_empty()
        && tx_type != EIP7702_TX_TYPE_ID
        && !emitted_logs;
    if plain_cancel {
        return Ok(NonceTakenBy::Cancel);
    }
    // A copy calls the same contract the signed burn calls, with its exact
    // calldata, so it burned the same amount toward the same recipient. Its
    // tx type does not matter: the `MessageSent` event proves the burn.
    let copies_the_burn =
        to == prepared.to() && value.is_zero() && prepared.input().as_ref() == Some(&input);
    let burned = copies_the_burn
        && bridge
            .source_emitted_message_sent(direction, superseding)
            .boxed()
            .await
            .map_err(|source| BurnNotSuperseded::Read {
                tx: superseding,
                source: Box::new(source),
            })?
            == Some(true);
    if burned {
        return Ok(NonceTakenBy::Replacement);
    }
    Err(BurnNotSuperseded::SupersedingTxNotAPlainCancel {
        superseding,
        bot_wallet,
    })
}

async fn read_source_tx(
    bridge: &BotCctpBridge,
    direction: BridgeDirection,
    tx: TxHash,
) -> Result<Option<MinedTx>, BurnNotSuperseded> {
    bridge
        .source_mined_tx(direction, tx)
        .boxed()
        .await
        .map_err(|source| BurnNotSuperseded::Read {
            tx,
            source: Box::new(source),
        })
}

/// Whether the burn's signed bytes were signed by the wallet that would
/// restore or rebroadcast them. Nonce bookkeeping is per wallet address, so a
/// burn signed by another key (a rotated wallet) must never enter it.
pub(crate) fn signed_by_source_wallet(
    bridge: &BotCctpBridge,
    operation: &CctpBurnOperation,
) -> bool {
    operation.prepared.signer() == Some(bridge.source_signer(operation.source.bridge_direction()))
}

/// What the startup restore of pending burns did.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct RestoredCctpBurns {
    /// Burns whose nonce was reserved again; each is also rebroadcast unless
    /// it is mined at the required confirmations without `MessageSent`.
    pub(crate) restored: usize,
    /// Pending burns whose receipt now decides, recorded instead of restored.
    pub(crate) settled: usize,
    /// Chains with a restored burn not mined yet, or whose burns could not be
    /// listed or loaded: startup skips their approvals and revokes, which
    /// could take a burn's nonce or wait behind it.
    pub(crate) unmined_chains: BTreeSet<Chain>,
}

/// Reserves the nonce of every pending burn signed by the source wallet and
/// rebroadcasts its exact bytes, so no other send from that wallet takes the
/// nonce or waits behind a burn no node holds after a restart. A pending burn
/// whose receipt now decides is recorded instead. One already mined at the
/// required confirmations without `MessageSent` keeps its reservation but is
/// not sent again, and one signed by another wallet (a rotated key) is not
/// restored and gates its chain. Never fails startup: a burn that cannot be
/// listed, loaded or rebroadcast pages, and a rerun of `cctp-bridge` with its
/// operation id rebroadcasts it again. Mirrors
/// `restore_prepared_deposit_sends`.
pub(crate) async fn restore_pending_cctp_burns(
    pool: &SqlitePool,
    store: &Store<CctpBurnOperation>,
    bridge: &BotCctpBridge,
    chains: &ChainRegistry,
) -> RestoredCctpBurns {
    let mut outcome = RestoredCctpBurns::default();
    let both_chains = [Chain::Ethereum, Chain::Base];
    let (ids, unparseable) = match pending_cctp_burn_ids(pool).await {
        Ok(found) => found,
        Err(error) => {
            error!(target: "operational_alert", alert = true, ?error, "Could not list the pending capital CCTP burns at startup; their nonces are not reserved, so startup skips Ethereum and Base token approvals and allowance revokes");
            outcome.unmined_chains.extend(both_chains);
            return outcome;
        }
    };
    if !unparseable.is_empty() {
        error!(target: "operational_alert", alert = true, ?unparseable, "Pending capital CCTP burns with unparseable operation ids were not restored at startup, so startup skips Ethereum and Base token approvals and allowance revokes");
        outcome.unmined_chains.extend(both_chains);
    }

    for id in ids {
        let operation = match store.load(&id).await {
            Ok(Some(operation)) if operation.outcome == CctpBurnOutcome::Pending => operation,
            Ok(state) => {
                warn!(operation_id = %id, ?state, "Capital CCTP burn is no longer pending at startup");
                continue;
            }
            Err(error) => {
                error!(target: "operational_alert", alert = true, operation_id = %id, ?error, "Could not load a pending capital CCTP burn at startup; its nonce is not reserved, so startup skips Ethereum and Base token approvals and allowance revokes");
                outcome.unmined_chains.extend(both_chains);
                continue;
            }
        };
        let source = operation.source;
        let burn_tx = operation.burn_tx();
        let nonce = operation.prepared.nonce();

        let required = chains.required_confirmations(source.chain());
        if let Some(required) = required
            && settle_burn_from_receipt(store, bridge, &id, &operation, required).await
                != CctpBurnStatus::Pending
        {
            outcome.settled += 1;
            continue;
        }

        let direction = source.bridge_direction();
        if !signed_by_source_wallet(bridge, &operation) {
            error!(target: "operational_alert", alert = true, operation_id = %id, %burn_tx, nonce, signer = ?operation.prepared.signer(), wallet = %bridge.source_signer(direction), "A pending capital CCTP burn was signed by another wallet (was the key rotated?); not restoring it into this wallet's nonces, so startup skips that chain's token approvals and allowance revokes");
            outcome.unmined_chains.insert(source.chain());
            continue;
        }
        // Like the siblings, the nonce is reserved even for a burn the node
        // shows mined: a shallow one can still be reorged out, and the hold
        // keeps a fresh send from taking its nonce then.
        bridge
            .restore_prepared_burn(direction, &operation.prepared)
            .boxed()
            .await;
        info!(operation_id = %id, %burn_tx, nonce, ?source, "Reserved the nonce of a pending capital CCTP burn");
        outcome.restored += 1;

        // A burn mined at the required confirmations that is still pending
        // succeeded without `MessageSent` (paged by the receipt read above):
        // sending its bytes again cannot change anything.
        if let Ok(Some(mined)) = bridge.source_mined_tx(direction, burn_tx).boxed().await
            && required.is_some_and(|required| mined.confirmations >= required)
        {
            continue;
        }
        if let Err(error) = bridge
            .broadcast_prepared_burn(direction, &operation.prepared)
            .boxed()
            .await
        {
            error!(target: "operational_alert", alert = true, operation_id = %id, %burn_tx, nonce, ?error, "Could not rebroadcast a pending capital CCTP burn at startup; its nonce stays reserved, so startup skips that chain's token approvals and allowance revokes, and a cctp-bridge rerun with its operation id broadcasts it again");
            outcome.unmined_chains.insert(source.chain());
            continue;
        }
        match bridge.source_mined_tx(direction, burn_tx).boxed().await {
            Ok(Some(_)) => {}
            Ok(None) => {
                warn!(operation_id = %id, %burn_tx, nonce, "Restored capital CCTP burn is not mined yet at startup");
                outcome.unmined_chains.insert(source.chain());
            }
            Err(error) => {
                warn!(operation_id = %id, %burn_tx, nonce, ?error, "Could not read the receipt of a restored capital CCTP burn at startup; treating it as not mined");
                outcome.unmined_chains.insert(source.chain());
            }
        }
    }

    outcome
}

#[cfg(test)]
mod tests {
    use st0x_event_sorcery::{LifecycleError, TestHarness, test_store};

    use super::*;
    use crate::test_utils::setup_test_db;

    fn prepared_event() -> CctpBurnOperationEvent {
        CctpBurnOperationEvent::Prepared {
            source: CctpSourceChain::Base,
            requested: RequestedBurn::All,
            amount: U256::from(5_000_000_u64),
            recipient: Address::repeat_byte(0x11),
            prepared: PreparedTransaction::for_test(TxHash::repeat_byte(0x22), 7),
            prepared_at: Utc::now(),
        }
    }

    fn prepare_command() -> CctpBurnOperationCommand {
        CctpBurnOperationCommand::Prepare {
            source: CctpSourceChain::Base,
            requested: RequestedBurn::All,
            amount: U256::from(5_000_000_u64),
            recipient: Address::repeat_byte(0x11),
            prepared: PreparedTransaction::for_test(TxHash::repeat_byte(0x33), 8),
        }
    }

    /// A second signed burn for one operation id is refused, so one id can
    /// never broadcast two burns.
    #[tokio::test]
    async fn a_second_prepare_for_the_same_operation_is_refused() {
        let error = TestHarness::<CctpBurnOperation>::with(())
            .given(vec![prepared_event()])
            .when(prepare_command())
            .await
            .then_expect_error();

        assert!(matches!(
            error,
            LifecycleError::Apply(CctpBurnOperationError::AlreadyPrepared)
        ));
    }

    /// An outcome can only be recorded for a signed burn.
    #[tokio::test]
    async fn an_outcome_without_a_signed_burn_is_refused() {
        let error = TestHarness::<CctpBurnOperation>::with(())
            .given_no_previous_events()
            .when(CctpBurnOperationCommand::RecordConfirmed)
            .await
            .then_expect_error();

        assert!(matches!(
            error,
            LifecycleError::Apply(CctpBurnOperationError::NotPrepared)
        ));
    }

    /// Reading the same receipt twice records the outcome once; a different
    /// outcome after it is a conflict, never a silent overwrite.
    #[tokio::test]
    async fn an_outcome_is_recorded_once_and_never_overwritten() {
        let confirmed = CctpBurnOperationEvent::Confirmed {
            confirmed_at: Utc::now(),
        };
        TestHarness::<CctpBurnOperation>::with(())
            .given(vec![prepared_event(), confirmed.clone()])
            .when(CctpBurnOperationCommand::RecordConfirmed)
            .await
            .then_expect_events(&[]);

        let error = TestHarness::<CctpBurnOperation>::with(())
            .given(vec![prepared_event(), confirmed])
            .when(CctpBurnOperationCommand::RecordReverted)
            .await
            .then_expect_error();
        assert!(matches!(
            error,
            LifecycleError::Apply(CctpBurnOperationError::OutcomeConflict {
                recorded: CctpBurnOutcome::Confirmed { .. },
                attempted: CctpBurnOutcome::Reverted { .. },
            })
        ));

        let superseded = CctpBurnOperationEvent::Superseded {
            superseding_tx: TxHash::repeat_byte(0x44),
            superseded_at: Utc::now(),
        };
        let error = TestHarness::<CctpBurnOperation>::with(())
            .given(vec![prepared_event(), superseded])
            .when(CctpBurnOperationCommand::RecordSuperseded {
                superseding_tx: TxHash::repeat_byte(0x55),
            })
            .await
            .then_expect_error();
        assert!(matches!(
            error,
            LifecycleError::Apply(CctpBurnOperationError::OutcomeConflict { .. })
        ));
    }

    /// Startup restores only the burns still pending: a confirmed one has
    /// nothing left to rebroadcast.
    #[tokio::test]
    async fn only_pending_burns_are_listed_for_the_startup_restore() {
        let pool = setup_test_db().await;
        let store = test_store::<CctpBurnOperation>(pool.clone(), ());
        let pending = CctpBurnOperationId(Uuid::new_v4());
        let confirmed = CctpBurnOperationId(Uuid::new_v4());
        for id in [pending, confirmed] {
            store.send(&id, prepare_command()).await.unwrap();
        }
        store
            .send(&confirmed, CctpBurnOperationCommand::RecordConfirmed)
            .await
            .unwrap();

        let (ids, unparseable) = pending_cctp_burn_ids(&pool).await.unwrap();

        assert_eq!(ids, vec![pending]);
        assert!(unparseable.is_empty());
    }
}
