//! Routes each cash transfer to the service of the corridor it runs on.
//!
//! Every served corridor has its own [`CrossVenueCashTransfer`] on its own
//! chain's orderbook, vault, wallet and gas check. [`UsdcCorridorTransfers`]
//! implements the job, recovery and startup entry points once and hands each
//! call to the service of the transfer's recorded corridor, so the apalis
//! queues and the operator routes stay shared.
//!
//! [`CrossVenueCashTransfer`]: super::CrossVenueCashTransfer

use alloy::primitives::TxHash;
use async_trait::async_trait;
use sqlx::SqlitePool;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use tracing::error;

use st0x_bridge::BridgeDirection;
use st0x_bridge::cctp::AttestationResponse;
use st0x_bridge::corridor::UsdcCorridor;
use st0x_event_sorcery::Store;
use st0x_evm::PreparedTransaction;
use st0x_finance::Usdc;

use super::manager::RecoveredCctpMint;
use super::{
    CctpMintRecoveryError, DepositSendNotSuperseded, RecheckUsdcDeposit, RecoverCctpMint,
    RestorePreparedDepositSends, RestoredDepositSends, ResumeAlpacaToBase, ResumeBaseToAlpaca,
    UsdcRecheckError, UsdcTransferError, refuse_unserved_corridor,
};
use crate::rebalancing::equity::RecheckOutcome;
use crate::usdc_rebalance::{UsdcRebalance, UsdcRebalanceId};

const CCTP_CORRIDOR_NOT_SERVED: CctpMintRecoveryError = CctpMintRecoveryError::CorridorNotServed {
    corridor: UsdcCorridor::BASE_CCTP,
};

/// Every entry point one corridor's cash transfer service offers.
pub(crate) trait CorridorTransfer:
    ResumeBaseToAlpaca
    + ResumeAlpacaToBase
    + RecheckUsdcDeposit
    + RestorePreparedDepositSends
    + RecoverCctpMint
{
}

impl<Transfer> CorridorTransfer for Transfer where
    Transfer: ResumeBaseToAlpaca
        + ResumeAlpacaToBase
        + RecheckUsdcDeposit
        + RestorePreparedDepositSends
        + RecoverCctpMint
{
}

/// The cash transfer services of every served corridor, keyed by corridor.
pub(crate) struct UsdcCorridorTransfers {
    by_corridor: BTreeMap<UsdcCorridor, Arc<dyn CorridorTransfer>>,
    store: Arc<Store<UsdcRebalance>>,
}

impl UsdcCorridorTransfers {
    pub(crate) fn new(
        by_corridor: BTreeMap<UsdcCorridor, Arc<dyn CorridorTransfer>>,
        store: Arc<Store<UsdcRebalance>>,
    ) -> Self {
        Self { by_corridor, store }
    }

    /// The service of `id`'s recorded corridor, or of `requested` for a
    /// transfer not recorded yet. A corridor no service carries is refused
    /// exactly as a service refuses one it does not serve.
    async fn route(
        &self,
        id: &UsdcRebalanceId,
        requested: UsdcCorridor,
    ) -> Result<&dyn CorridorTransfer, UsdcTransferError> {
        let state = self.store.load(id).await?;
        let corridor = state.as_ref().map_or(requested, UsdcRebalance::corridor);

        if let Some(service) = self.by_corridor.get(&corridor) {
            return Ok(service.as_ref());
        }

        let served = self.by_corridor.keys().copied().collect::<BTreeSet<_>>();
        refuse_unserved_corridor(id, requested, &served, state.as_ref())?;

        Err(UsdcTransferError::CorridorNotServed {
            id: id.clone(),
            requested: corridor,
            served,
        })
    }

    /// The Base via CCTP service, the one corridor whose bridge carries a
    /// CCTP burn.
    fn cctp_service(&self) -> Option<&dyn CorridorTransfer> {
        self.by_corridor
            .get(&UsdcCorridor::BASE_CCTP)
            .map(AsRef::as_ref)
    }
}

#[async_trait]
impl ResumeBaseToAlpaca for UsdcCorridorTransfers {
    async fn resume_base_to_alpaca(
        &self,
        id: &UsdcRebalanceId,
        amount: Usdc,
        corridor: UsdcCorridor,
    ) -> Result<(), UsdcTransferError> {
        self.route(id, corridor)
            .await?
            .resume_base_to_alpaca(id, amount, corridor)
            .await
    }
}

#[async_trait]
impl ResumeAlpacaToBase for UsdcCorridorTransfers {
    async fn resume_alpaca_to_base(
        &self,
        id: &UsdcRebalanceId,
        amount: Usdc,
        corridor: UsdcCorridor,
    ) -> Result<(), UsdcTransferError> {
        self.route(id, corridor)
            .await?
            .resume_alpaca_to_base(id, amount, corridor)
            .await
    }
}

#[async_trait]
impl RecheckUsdcDeposit for UsdcCorridorTransfers {
    async fn recheck_deposit(
        &self,
        id: &UsdcRebalanceId,
        operator_deposit_tx: Option<TxHash>,
    ) -> Result<RecheckOutcome, UsdcRecheckError> {
        let recorded = self
            .store
            .load(id)
            .await
            .map_err(|error| Box::new(UsdcTransferError::from(error)))?
            .ok_or_else(|| UsdcRecheckError::NotFound(id.clone()))?;

        self.route(id, recorded.corridor())
            .await
            .map_err(Box::new)?
            .recheck_deposit(id, operator_deposit_tx)
            .await
    }

    async fn verify_deposit_send_superseded(
        &self,
        id: &UsdcRebalanceId,
        corridor: UsdcCorridor,
        prepared: &PreparedTransaction,
        superseding_tx: Option<TxHash>,
    ) -> Result<(), DepositSendNotSuperseded> {
        let service = self
            .by_corridor
            .get(&corridor)
            .ok_or(DepositSendNotSuperseded::CorridorNotServed { corridor })?;

        service
            .verify_deposit_send_superseded(id, corridor, prepared, superseding_tx)
            .await
    }
}

/// Runs once, on one service: the signed deposit sends all belong to the
/// shared Ethereum wallet, and each must be restored exactly once or its
/// nonce would be rebroadcast twice.
#[async_trait]
impl RestorePreparedDepositSends for UsdcCorridorTransfers {
    async fn restore_prepared_deposit_sends(&self, pool: &SqlitePool) -> RestoredDepositSends {
        let Some(service) = self.by_corridor.values().next() else {
            error!(
                target: "rebalance",
                "No cash transfer service is built, so no signed Alpaca deposit send is restored"
            );
            return RestoredDepositSends::default();
        };

        service.restore_prepared_deposit_sends(pool).await
    }
}

#[async_trait]
impl RecoverCctpMint for UsdcCorridorTransfers {
    async fn fetch_recovery_attestation(
        &self,
        direction: BridgeDirection,
        burn_tx: TxHash,
    ) -> Result<AttestationResponse, CctpMintRecoveryError> {
        self.cctp_service()
            .ok_or(CCTP_CORRIDOR_NOT_SERVED)?
            .fetch_recovery_attestation(direction, burn_tx)
            .await
    }

    async fn submit_recovered_cctp_mint(
        &self,
        direction: BridgeDirection,
        burn_tx: TxHash,
        attestation: AttestationResponse,
    ) -> Result<RecoveredCctpMint, CctpMintRecoveryError> {
        self.cctp_service()
            .ok_or(CCTP_CORRIDOR_NOT_SERVED)?
            .submit_recovered_cctp_mint(direction, burn_tx, attestation)
            .await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use uuid::Uuid;

    use st0x_bridge::corridor::HopKind;
    use st0x_event_sorcery::test_store;
    use st0x_evm::Chain;
    use st0x_float_macro::float;

    use super::*;
    use crate::test_utils::setup_test_db;
    use crate::usdc_rebalance::{RebalanceDirection, UsdcRebalanceCommand};

    const ROBINHOOD_CCTP: UsdcCorridor = UsdcCorridor::HubRouted {
        chain: Chain::Robinhood,
        hop: HopKind::Cctp,
    };
    const ROBINHOOD_RELAY: UsdcCorridor = UsdcCorridor::HubRouted {
        chain: Chain::Robinhood,
        hop: HopKind::Relay,
    };

    /// Records which transfers reached it and how many restores ran.
    #[derive(Default)]
    struct StubTransfer {
        resumed: Mutex<Vec<UsdcRebalanceId>>,
        restores: Mutex<usize>,
    }

    #[async_trait]
    impl ResumeBaseToAlpaca for StubTransfer {
        async fn resume_base_to_alpaca(
            &self,
            id: &UsdcRebalanceId,
            _amount: Usdc,
            _corridor: UsdcCorridor,
        ) -> Result<(), UsdcTransferError> {
            self.resumed.lock().unwrap().push(id.clone());
            Ok(())
        }
    }

    #[async_trait]
    impl ResumeAlpacaToBase for StubTransfer {
        async fn resume_alpaca_to_base(
            &self,
            id: &UsdcRebalanceId,
            _amount: Usdc,
            _corridor: UsdcCorridor,
        ) -> Result<(), UsdcTransferError> {
            self.resumed.lock().unwrap().push(id.clone());
            Ok(())
        }
    }

    #[async_trait]
    impl RecheckUsdcDeposit for StubTransfer {
        async fn recheck_deposit(
            &self,
            _id: &UsdcRebalanceId,
            _operator_deposit_tx: Option<TxHash>,
        ) -> Result<RecheckOutcome, UsdcRecheckError> {
            unimplemented!("StubTransfer: recheck not used")
        }

        async fn verify_deposit_send_superseded(
            &self,
            _id: &UsdcRebalanceId,
            _corridor: UsdcCorridor,
            _prepared: &PreparedTransaction,
            _superseding_tx: Option<TxHash>,
        ) -> Result<(), DepositSendNotSuperseded> {
            unimplemented!("StubTransfer: reconcile not used")
        }
    }

    #[async_trait]
    impl RestorePreparedDepositSends for StubTransfer {
        async fn restore_prepared_deposit_sends(&self, _pool: &SqlitePool) -> RestoredDepositSends {
            *self.restores.lock().unwrap() += 1;
            RestoredDepositSends::default()
        }
    }

    #[async_trait]
    impl RecoverCctpMint for StubTransfer {
        async fn fetch_recovery_attestation(
            &self,
            _direction: BridgeDirection,
            _burn_tx: TxHash,
        ) -> Result<AttestationResponse, CctpMintRecoveryError> {
            unimplemented!("StubTransfer: CCTP recovery not used")
        }

        async fn submit_recovered_cctp_mint(
            &self,
            _direction: BridgeDirection,
            _burn_tx: TxHash,
            _attestation: AttestationResponse,
        ) -> Result<RecoveredCctpMint, CctpMintRecoveryError> {
            unimplemented!("StubTransfer: CCTP recovery not used")
        }
    }

    struct TwoCorridors {
        transfers: UsdcCorridorTransfers,
        base: Arc<StubTransfer>,
        robinhood: Arc<StubTransfer>,
        store: Arc<Store<UsdcRebalance>>,
        pool: SqlitePool,
    }

    /// Base via CCTP and Robinhood via CCTP, each on its own stub service.
    async fn two_corridors() -> TwoCorridors {
        let pool = setup_test_db().await;
        let store = Arc::new(test_store(pool.clone(), ()));
        let base = Arc::new(StubTransfer::default());
        let robinhood = Arc::new(StubTransfer::default());
        let by_corridor = BTreeMap::from([
            (
                UsdcCorridor::BASE_CCTP,
                Arc::clone(&base) as Arc<dyn CorridorTransfer>,
            ),
            (
                ROBINHOOD_CCTP,
                Arc::clone(&robinhood) as Arc<dyn CorridorTransfer>,
            ),
        ]);

        TwoCorridors {
            transfers: UsdcCorridorTransfers::new(by_corridor, Arc::clone(&store)),
            base,
            robinhood,
            store,
            pool,
        }
    }

    async fn record_on(store: &Store<UsdcRebalance>, corridor: UsdcCorridor) -> UsdcRebalanceId {
        let id = UsdcRebalanceId(Uuid::new_v4());
        store
            .send(
                &id,
                UsdcRebalanceCommand::BeginWithdrawal {
                    direction: RebalanceDirection::BaseToAlpaca,
                    corridor,
                    amount: Usdc::new(float!(1)),
                    from_block: 0,
                },
            )
            .await
            .unwrap();
        id
    }

    /// A job reaches the service of its transfer's recorded corridor, or of
    /// its own corridor when nothing is recorded yet; a corridor no service
    /// carries is refused as a service refuses one it does not serve.
    #[tokio::test]
    async fn corridor_transfers_route_each_job_to_its_service() {
        let TwoCorridors {
            transfers,
            base,
            robinhood,
            store,
            ..
        } = two_corridors().await;
        let amount = Usdc::new(float!(1));

        let fresh_base = UsdcRebalanceId(Uuid::new_v4());
        transfers
            .resume_base_to_alpaca(&fresh_base, amount, UsdcCorridor::BASE_CCTP)
            .await
            .unwrap();
        let fresh_robinhood = UsdcRebalanceId(Uuid::new_v4());
        transfers
            .resume_alpaca_to_base(&fresh_robinhood, amount, ROBINHOOD_CCTP)
            .await
            .unwrap();
        let recorded_robinhood = record_on(&store, ROBINHOOD_CCTP).await;
        transfers
            .resume_base_to_alpaca(&recorded_robinhood, amount, UsdcCorridor::BASE_CCTP)
            .await
            .unwrap();

        assert_eq!(*base.resumed.lock().unwrap(), vec![fresh_base]);
        assert_eq!(
            *robinhood.resumed.lock().unwrap(),
            vec![fresh_robinhood, recorded_robinhood]
        );

        let recorded_unserved = record_on(&store, ROBINHOOD_RELAY).await;
        let error = transfers
            .resume_base_to_alpaca(&recorded_unserved, amount, ROBINHOOD_RELAY)
            .await
            .unwrap_err();
        assert!(
            matches!(
                error,
                UsdcTransferError::CorridorMismatch {
                    recorded: ROBINHOOD_RELAY,
                    holds_guard: true,
                    ref served,
                    ..
                } if *served == BTreeSet::from([UsdcCorridor::BASE_CCTP, ROBINHOOD_CCTP])
            ),
            "got {error:?}"
        );

        let error = transfers
            .resume_alpaca_to_base(&UsdcRebalanceId(Uuid::new_v4()), amount, ROBINHOOD_RELAY)
            .await
            .unwrap_err();
        assert!(
            matches!(
                error,
                UsdcTransferError::CorridorNotServed {
                    requested: ROBINHOOD_RELAY,
                    ..
                }
            ),
            "got {error:?}"
        );
    }

    /// The signed deposit sends belong to the one Ethereum wallet, so the
    /// startup restore runs once however many corridors are served.
    #[tokio::test]
    async fn deposit_sends_are_restored_once_across_corridors() {
        let TwoCorridors {
            transfers,
            base,
            robinhood,
            pool,
            ..
        } = two_corridors().await;

        transfers.restore_prepared_deposit_sends(&pool).await;

        let restores = *base.restores.lock().unwrap() + *robinhood.restores.lock().unwrap();
        assert_eq!(restores, 1);
    }
}
