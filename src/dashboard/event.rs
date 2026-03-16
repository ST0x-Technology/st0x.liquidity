//! Reactor that broadcasts aggregate events to WebSocket dashboard
//! clients as [`Statement`] notifications.
//!
//! Each domain event produces a [`Statement`] with the appropriate
//! [`Concern`] variant, notifying connected clients that something
//! changed. Clients use the concern type to know what to
//! refetch/invalidate.

use async_trait::async_trait;
use tokio::sync::broadcast;
use tracing::warn;

use st0x_dto::{Concern, Statement};
use st0x_event_sorcery::{DomainEvent, EntityList, EventSourced, Never, Reactor, deps};

use crate::equity_redemption::EquityRedemption;
use crate::tokenized_equity_mint::TokenizedEquityMint;
use crate::usdc_rebalance::UsdcRebalance;

deps!(
    Broadcaster,
    [TokenizedEquityMint, EquityRedemption, UsdcRebalance,]
);

/// Reactor that broadcasts [`Statement`] notifications to connected
/// WebSocket clients when domain events occur.
///
/// Implements [`Reactor`] with exhaustive handling for all
/// broadcast-eligible aggregate types.
pub(crate) struct Broadcaster {
    sender: broadcast::Sender<Statement>,
}

impl Broadcaster {
    pub(crate) fn new(sender: broadcast::Sender<Statement>) -> Self {
        Self { sender }
    }

    fn notify<Entity: EventSourced>(&self, id: &Entity::Id) {
        let statement = Statement {
            id: id.to_string(),
            statement: Concern::Transfer,
        };

        if let Err(error) = self.sender.send(statement) {
            warn!("Failed to broadcast statement (no receivers): {error}");
        }
    }

    fn broadcast_transfer(&self, transfer: st0x_dto::TransferOperation) {
        if let Err(error) = self.sender.send(ServerMessage::Transfer(transfer)) {
            warn!("Failed to broadcast transfer update (no receivers): {error}");
        }
    }
}

#[async_trait]
impl Reactor for Broadcaster {
    type Error = Never;

    async fn react(
        &self,
        event: <Self::Dependencies as EntityList>::Event,
    ) -> Result<(), Self::Error> {
        event
            .on(|id, _event| async move {
                self.notify::<TokenizedEquityMint>(&id);
            })
            .on(|id, _event| async move {
                self.notify::<EquityRedemption>(&id);
            })
            .on(|id, _event| async move {
                self.notify::<UsdcRebalance>(&id);
            })
            .exhaustive()
            .await;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use st0x_event_sorcery::ReactorHarness;
    use st0x_execution::Symbol;

    use super::*;
    use crate::equity_redemption::{EquityRedemptionEvent, RedemptionAggregateId};
    use crate::test_utils::setup_test_db;
    use crate::tokenized_equity_mint::{IssuerRequestId, TokenizedEquityMintEvent};
    use crate::usdc_rebalance::{UsdcRebalanceEvent, UsdcRebalanceId};

    fn test_broadcaster() -> (Broadcaster, broadcast::Receiver<Statement>) {
        let (sender, receiver) = broadcast::channel(16);
        let broadcaster = Broadcaster::new(sender);
        (broadcaster, receiver)
    }

    fn make_mint_requested(symbol: &str) -> TokenizedEquityMintEvent {
        use alloy::primitives::Address;
        use rain_math_float::Float;

        TokenizedEquityMintEvent::MintRequested {
            symbol: Symbol::new(symbol).unwrap(),
            quantity: Float::parse("100".to_string()).unwrap(),
            wallet: Address::ZERO,
            requested_at: chrono::Utc::now(),
        }
    }

    fn make_redemption_completed() -> EquityRedemptionEvent {
        EquityRedemptionEvent::Completed {
            completed_at: chrono::Utc::now(),
        }
    }

    fn make_usdc_withdrawal_confirmed() -> UsdcRebalanceEvent {
        UsdcRebalanceEvent::WithdrawalConfirmed {
            confirmed_at: chrono::Utc::now(),
        }
    }

    #[tokio::test]
    async fn broadcaster_sends_transfer_statement() {
        let (broadcaster, mut receiver) = test_broadcaster();

        let id = IssuerRequestId::new("mint-123".to_string());
        broadcaster.notify::<TokenizedEquityMint>(&id);

        let msg = receiver.recv().await.expect("should receive message");

        assert_eq!(msg.id, "mint-123");
        assert!(
            matches!(msg.statement, Concern::Transfer),
            "expected Transfer concern, got {:?}",
            msg.statement
        );
    }

    #[tokio::test]
    async fn broadcaster_handles_no_receivers() {
        let (sender, _) = broadcast::channel::<Statement>(16);
        let broadcaster = Broadcaster::new(sender);

        let id = IssuerRequestId::new("mint-456".to_string());
        broadcaster.notify::<TokenizedEquityMint>(&id);
    }

    #[tokio::test]
    async fn reactor_broadcasts_mint_event() {
        let (broadcaster, mut receiver) = test_broadcaster();
        let harness = ReactorHarness::new(broadcaster);

        let id = IssuerRequestId::new("mint-multi".to_string());

        harness
            .receive::<TokenizedEquityMint>(id, make_mint_requested("NVDA"))
            .await
            .unwrap();

        let msg = receiver.recv().await.expect("should receive message");

        assert_eq!(msg.id, "mint-multi");
        assert!(matches!(msg.statement, Concern::Transfer));
    }

    #[tokio::test]
    async fn reactor_broadcasts_equity_redemption() {
        let (broadcaster, mut receiver) = test_broadcaster();
        let harness = ReactorHarness::new(broadcaster);

        let id = RedemptionAggregateId::new("redemption-123".to_string());

        harness
            .receive::<EquityRedemption>(id, make_redemption_completed())
            .await
            .unwrap();

        let msg = receiver.recv().await.expect("should receive message");

        assert_eq!(msg.id, "redemption-123");
        assert!(matches!(msg.statement, Concern::Transfer));
    }

    #[tokio::test]
    async fn reactor_broadcasts_usdc_rebalance() {
        let (broadcaster, mut receiver) = test_broadcaster();
        let harness = ReactorHarness::new(broadcaster);

        let id = UsdcRebalanceId(Uuid::new_v4());
        let expected_id = id.to_string();

        harness
            .receive::<UsdcRebalance>(id, make_usdc_withdrawal_confirmed())
            .await
            .unwrap();

        let msg = receiver.recv().await.expect("should receive message");

        assert_eq!(msg.id, expected_id);
        assert!(matches!(msg.statement, Concern::Transfer));
    }

    #[tokio::test]
    async fn multiple_subscribers_receive_same_statement() {
        let (sender, mut receiver1) = broadcast::channel(16);
        let mut receiver2 = sender.subscribe();
        let broadcaster = Broadcaster::new(sender);

        let id = IssuerRequestId::new("multi-sub".to_string());
        broadcaster.notify::<TokenizedEquityMint>(&id);

        let msg1 = receiver1
            .recv()
            .await
            .expect("receiver1 should get message");
        let msg2 = receiver2
            .recv()
            .await
            .expect("receiver2 should get message");

        for (idx, msg) in [msg1, msg2].into_iter().enumerate() {
            assert_eq!(msg.id, "multi-sub", "receiver {} got wrong id", idx + 1);
        }
    }

    #[tokio::test]
    async fn no_broadcast_without_events() {
        let (sender, mut receiver) = broadcast::channel(16);
        let _broadcaster = Broadcaster::new(sender);

        let result =
            tokio::time::timeout(std::time::Duration::from_millis(10), receiver.recv()).await;

        assert!(result.is_err(), "should timeout with no messages");
    }
}
