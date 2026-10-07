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
use tracing::{error, warn};

use st0x_bridge::BridgeDirection;
use st0x_bridge::cctp::AttestationResponse;
use st0x_bridge::corridor::UsdcCorridor;
use st0x_event_sorcery::Store;
use st0x_evm::{Chain, PreparedTransaction};
use st0x_finance::Usdc;

use super::manager::RecoveredCctpMint;
use super::{
    CctpMintRecoveryError, DepositSendNotSuperseded, RecheckUsdcDeposit, RecoverCctpMint,
    RestorePreparedDepositSends, RestoredDepositSends, ResumeAlpacaToBase, ResumeBaseToAlpaca,
    UsdcRecheckError, UsdcTransferError, unserved_corridor,
};
use crate::rebalancing::equity::RecheckOutcome;
use crate::usdc_rebalance::{
    RebalanceDirection, UsdcRebalance, UsdcRebalanceId, prepared_swap_ids,
};

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
/// At least one corridor is served, so the startup restore always has a
/// service to run on.
pub(crate) struct UsdcCorridorTransfers {
    by_corridor: BTreeMap<UsdcCorridor, Arc<dyn CorridorTransfer>>,
    /// Runs the work that touches only the shared Ethereum wallet.
    hub: Arc<dyn CorridorTransfer>,
    store: Arc<Store<UsdcRebalance>>,
}

impl UsdcCorridorTransfers {
    /// `None` when no corridor is served.
    pub(crate) fn new(
        by_corridor: BTreeMap<UsdcCorridor, Arc<dyn CorridorTransfer>>,
        store: Arc<Store<UsdcRebalance>>,
    ) -> Option<Self> {
        let hub = Arc::clone(by_corridor.values().next()?);

        Some(Self {
            by_corridor,
            hub,
            store,
        })
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
        Err(unserved_corridor(id, requested, &served, state.as_ref()))
    }

    /// Pages the chain-signed Relay pairs persisted on a corridor no service
    /// carries, whose nonces nothing restores, and returns how many there are
    /// on each chain. A listing or load failure is paged by the hub's restore.
    async fn page_unserved_chain_signed_swaps(&self, pool: &SqlitePool) -> BTreeMap<Chain, usize> {
        let ids = match prepared_swap_ids(pool).await {
            Ok((ids, _unparseable)) => ids,
            Err(error) => {
                warn!(target: "rebalance", ?error, "Could not list signed Relay pairs to check their corridors at startup");
                return BTreeMap::new();
            }
        };

        let mut unserved: BTreeMap<UsdcCorridor, Vec<UsdcRebalanceId>> = BTreeMap::new();
        for id in ids {
            match self.store.load(&id).await {
                Ok(Some(state))
                    if state.direction() == RebalanceDirection::BaseToAlpaca
                        && !self.by_corridor.contains_key(&state.corridor()) =>
                {
                    unserved.entry(state.corridor()).or_default().push(id);
                }
                Ok(_) => {}
                Err(error) => {
                    warn!(target: "rebalance", %id, ?error, "Could not load a transfer with a signed Relay pair to check its corridor at startup");
                }
            }
        }

        unserved
            .into_iter()
            .map(|(corridor, ids)| {
                let chain = corridor.chain();
                error!(target: "operational_alert", alert = true, %corridor, %chain, ?ids, "Signed Relay pairs on a corridor this build does not serve were not restored at startup; their nonces are not reserved, so startup skips {chain}'s approvals and revokes until a build serving {corridor} restores them");
                (chain, ids.len())
            })
            .fold(BTreeMap::new(), |mut by_chain, (chain, count)| {
                *by_chain.entry(chain).or_default() += count;
                by_chain
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

    /// Runs on the hub service whatever the transfer's corridor: the check
    /// reads only the shared Ethereum wallet, so a transfer on a corridor this
    /// build no longer serves can still be reconciled.
    async fn verify_deposit_send_superseded(
        &self,
        id: &UsdcRebalanceId,
        prepared: &PreparedTransaction,
        superseding_tx: Option<TxHash>,
    ) -> Result<(), DepositSendNotSuperseded> {
        self.hub
            .verify_deposit_send_superseded(id, prepared, superseding_tx)
            .await
    }
}

#[async_trait]
impl RestorePreparedDepositSends for UsdcCorridorTransfers {
    /// Runs once, on one service: the signed deposit sends and the Relay
    /// pairs signed on the Ethereum wallet all belong to it, and each must be
    /// restored exactly once or its nonce would be rebroadcast twice.
    async fn restore_prepared_deposit_sends(&self, pool: &SqlitePool) -> RestoredDepositSends {
        self.hub.restore_prepared_deposit_sends(pool).await
    }

    /// Runs on every service: each restores the Relay pairs signed on its own
    /// corridor chain, one chain per service. A pair on a corridor no service
    /// carries is paged and counted unmined on its chain.
    async fn restore_chain_signed_swaps(
        &self,
        pool: &SqlitePool,
    ) -> BTreeMap<Chain, RestoredDepositSends> {
        let mut by_chain = BTreeMap::new();
        for service in self.by_corridor.values() {
            by_chain.extend(service.restore_chain_signed_swaps(pool).await);
        }

        for (chain, unrestored) in self.page_unserved_chain_signed_swaps(pool).await {
            let restored: &mut RestoredDepositSends = by_chain.entry(chain).or_default();
            restored.unmined += unrestored;
        }

        by_chain
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

    use st0x_bridge::cctp::CctpError;
    use st0x_bridge::corridor::HopKind;
    use st0x_event_sorcery::test_store;
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

    /// Records which transfers reached it and how many restores and CCTP
    /// recoveries ran.
    #[derive(Default)]
    struct StubTransfer {
        resumed: Mutex<Vec<UsdcRebalanceId>>,
        restores: Mutex<usize>,
        cctp_recoveries: Mutex<usize>,
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
            id: &UsdcRebalanceId,
            _operator_deposit_tx: Option<TxHash>,
        ) -> Result<RecheckOutcome, UsdcRecheckError> {
            self.resumed.lock().unwrap().push(id.clone());
            Ok(RecheckOutcome::LeftUnchanged)
        }

        async fn verify_deposit_send_superseded(
            &self,
            id: &UsdcRebalanceId,
            _prepared: &PreparedTransaction,
            _superseding_tx: Option<TxHash>,
        ) -> Result<(), DepositSendNotSuperseded> {
            self.resumed.lock().unwrap().push(id.clone());
            Ok(())
        }
    }

    #[async_trait]
    impl RestorePreparedDepositSends for StubTransfer {
        async fn restore_prepared_deposit_sends(&self, _pool: &SqlitePool) -> RestoredDepositSends {
            *self.restores.lock().unwrap() += 1;
            RestoredDepositSends::default()
        }

        async fn restore_chain_signed_swaps(
            &self,
            _pool: &SqlitePool,
        ) -> BTreeMap<Chain, RestoredDepositSends> {
            BTreeMap::new()
        }
    }

    #[async_trait]
    impl RecoverCctpMint for StubTransfer {
        async fn fetch_recovery_attestation(
            &self,
            _direction: BridgeDirection,
            burn_tx: TxHash,
        ) -> Result<AttestationResponse, CctpMintRecoveryError> {
            *self.cctp_recoveries.lock().unwrap() += 1;
            Err(CctpMintRecoveryError::Attestation {
                burn_tx,
                source: CctpError::TxNotMined { tx_hash: burn_tx },
            })
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
            transfers: UsdcCorridorTransfers::new(by_corridor, Arc::clone(&store)).unwrap(),
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

    /// A pair signed on a corridor chain's wallet and persisted on a
    /// corridor the config no longer serves has no service to restore it:
    /// startup pages its id and counts it unmined on that chain.
    #[tokio::test]
    #[tracing_test::traced_test]
    async fn chain_signed_pair_on_an_unserved_corridor_pages_at_startup() {
        let TwoCorridors {
            transfers,
            store,
            pool,
            ..
        } = two_corridors().await;
        let id = UsdcRebalanceId(Uuid::new_v4());
        crate::usdc_rebalance::record_swap_pair_for_test(
            &store,
            &id,
            RebalanceDirection::BaseToAlpaca,
            ROBINHOOD_RELAY,
            None,
            PreparedTransaction::for_test(TxHash::repeat_byte(0xc2), 61),
        )
        .await;

        let by_chain = transfers.restore_chain_signed_swaps(&pool).await;

        assert_eq!(
            by_chain.get(&Chain::Robinhood),
            Some(&RestoredDepositSends {
                restored: 0,
                unmined: 1,
            })
        );
        assert!(logs_contain("operational_alert"));
        assert!(logs_contain(&id.to_string()));
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

    /// An operator recheck reaches the service of the transfer's recorded
    /// corridor; an unknown transfer is not found.
    #[tokio::test]
    async fn recheck_routes_to_the_recorded_corridors_service() {
        let TwoCorridors {
            transfers,
            base,
            robinhood,
            store,
            ..
        } = two_corridors().await;
        let id = record_on(&store, ROBINHOOD_CCTP).await;

        transfers.recheck_deposit(&id, None).await.unwrap();

        assert!(base.resumed.lock().unwrap().is_empty());
        assert_eq!(*robinhood.resumed.lock().unwrap(), vec![id]);

        let unknown = UsdcRebalanceId(Uuid::new_v4());
        let error = transfers.recheck_deposit(&unknown, None).await.unwrap_err();
        assert!(
            matches!(error, UsdcRecheckError::NotFound(ref missing) if *missing == unknown),
            "got {error:?}"
        );
    }

    /// The superseded-send check reads only the shared Ethereum wallet, so it
    /// reaches one service however the transfer's corridor is recorded,
    /// served or not.
    #[tokio::test]
    async fn superseded_send_check_reaches_a_service_whatever_the_corridor() {
        let TwoCorridors {
            transfers,
            base,
            robinhood,
            store,
            ..
        } = two_corridors().await;
        let unserved = record_on(&store, ROBINHOOD_RELAY).await;
        let prepared = PreparedTransaction::for_test(TxHash::repeat_byte(0x66), 7);

        transfers
            .verify_deposit_send_superseded(&unserved, &prepared, None)
            .await
            .unwrap();

        let checked = [
            base.resumed.lock().unwrap(),
            robinhood.resumed.lock().unwrap(),
        ]
        .iter()
        .map(|resumed| resumed.len())
        .sum::<usize>();
        assert_eq!(checked, 1);
    }

    /// CCTP mint recovery runs on the Base via CCTP service, and is refused
    /// by name when no service carries that corridor.
    #[tokio::test]
    async fn cctp_mint_recovery_routes_to_the_base_cctp_service() {
        let TwoCorridors {
            transfers,
            base,
            robinhood,
            store,
            ..
        } = two_corridors().await;
        let burn_tx = TxHash::repeat_byte(0x77);

        transfers
            .fetch_recovery_attestation(BridgeDirection::BaseToEthereum, burn_tx)
            .await
            .unwrap_err();

        assert_eq!(*base.cctp_recoveries.lock().unwrap(), 1);
        assert_eq!(*robinhood.cctp_recoveries.lock().unwrap(), 0);

        let robinhood_only = UsdcCorridorTransfers::new(
            BTreeMap::from([(ROBINHOOD_CCTP, robinhood as Arc<dyn CorridorTransfer>)]),
            store,
        )
        .unwrap();
        let error = robinhood_only
            .fetch_recovery_attestation(BridgeDirection::BaseToEthereum, burn_tx)
            .await
            .unwrap_err();
        assert!(
            matches!(
                error,
                CctpMintRecoveryError::CorridorNotServed {
                    corridor: UsdcCorridor::BASE_CCTP
                }
            ),
            "got {error:?}"
        );
    }

    /// No dispatcher exists without a served corridor, so the startup
    /// restore can never silently skip the signed deposit sends.
    #[tokio::test]
    async fn no_dispatcher_without_a_served_corridor() {
        let store = Arc::new(test_store(setup_test_db().await, ()));

        assert!(UsdcCorridorTransfers::new(BTreeMap::new(), store).is_none());
    }
}
