//! Reactor that broadcasts aggregate events to WebSocket dashboard clients.

use async_trait::async_trait;
use chrono::Utc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::broadcast;
use tracing::warn;

use st0x_dto::{EventStoreEntry, ServerMessage};
use st0x_event_sorcery::{DomainEvent, EntityList, EventSourced, Never, Reactor, deps};

use crate::equity_redemption::EquityRedemption;
use crate::tokenized_equity_mint::TokenizedEquityMint;
use crate::usdc_rebalance::UsdcRebalance;

deps!(
    EventBroadcaster,
    [TokenizedEquityMint, EquityRedemption, UsdcRebalance,]
);

/// Reactor that broadcasts events to connected WebSocket clients.
///
/// Implements [`Reactor`] with exhaustive handling for all
/// broadcast-eligible aggregate types.
pub(crate) struct EventBroadcaster {
    sender: broadcast::Sender<ServerMessage>,
    sequence: AtomicU64,
}

impl EventBroadcaster {
    pub(crate) fn new(sender: broadcast::Sender<ServerMessage>) -> Self {
        Self {
            sender,
            sequence: AtomicU64::new(0),
        }
    }

    fn broadcast_event<Entity: EventSourced>(&self, id: &Entity::Id, event: &Entity::Event) {
        let entry = EventStoreEntry {
            aggregate_type: Entity::AGGREGATE_TYPE.to_string(),
            aggregate_id: id.to_string(),
            sequence: self.sequence.fetch_add(1, Ordering::Relaxed),
            event_type: event.event_type(),
            timestamp: Utc::now(),
        };

        let msg = ServerMessage::Event(entry);

        if let Err(error) = self.sender.send(msg) {
            warn!("Failed to broadcast event (no receivers): {error}");
        }
    }
}

#[async_trait]
impl Reactor for EventBroadcaster {
    type Error = Never;

    async fn react(
        &self,
        event: <Self::Dependencies as EntityList>::Event,
    ) -> Result<(), Self::Error> {
        event
            .on(|id, event| async move {
                self.broadcast_event::<TokenizedEquityMint>(&id, &event);
            })
            .on(|id, event| async move {
                self.broadcast_event::<EquityRedemption>(&id, &event);
            })
            .on(|id, event| async move {
                self.broadcast_event::<UsdcRebalance>(&id, &event);
            })
            .exhaustive()
            .await;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::Address;
    use st0x_execution::Symbol;
    use uuid::Uuid;

    use st0x_event_sorcery::ReactorHarness;

    use super::*;
    use crate::equity_redemption::{EquityRedemptionEvent, RedemptionAggregateId};
    use crate::tokenized_equity_mint::{IssuerRequestId, TokenizedEquityMintEvent};
    use crate::usdc_rebalance::{UsdcRebalanceEvent, UsdcRebalanceId};

    fn make_mint_requested(symbol: &str, quantity: u64) -> TokenizedEquityMintEvent {
        TokenizedEquityMintEvent::MintRequested {
            symbol: Symbol::new(symbol).unwrap(),
            quantity: quantity.into(),
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
    async fn event_broadcaster_sends_to_channel() {
        let (sender, mut receiver) = broadcast::channel(16);
        let broadcaster = EventBroadcaster::new(sender);

        let id = IssuerRequestId::new("mint-123".to_string());

        broadcaster.broadcast_event::<TokenizedEquityMint>(&id, &make_mint_requested("TSLA", 50));

        let msg = receiver.recv().await.expect("should receive message");

        match msg {
            ServerMessage::Event(entry) => {
                assert_eq!(entry.aggregate_type, "TokenizedEquityMint");
                assert_eq!(entry.aggregate_id, "mint-123");
                assert_eq!(entry.event_type, "TokenizedEquityMintEvent::MintRequested");
            }
            ServerMessage::Initial(_) => panic!("expected Event message"),
        }
    }

    #[tokio::test]
    async fn event_broadcaster_handles_no_receivers() {
        let (sender, _) = broadcast::channel::<ServerMessage>(16);
        let broadcaster = EventBroadcaster::new(sender);

        let id = IssuerRequestId::new("mint-456".to_string());

        broadcaster.broadcast_event::<TokenizedEquityMint>(&id, &make_mint_requested("GOOG", 10));
    }

    #[tokio::test]
    async fn reactor_receive_broadcasts_mint_event() {
        let (sender, mut receiver) = broadcast::channel(16);
        let harness = ReactorHarness::new(EventBroadcaster::new(sender));

        let id = IssuerRequestId::new("mint-multi".to_string());

        harness
            .receive::<TokenizedEquityMint>(id, make_mint_requested("NVDA", 25))
            .await
            .unwrap();

        let msg = receiver.recv().await.expect("should receive message");

        match msg {
            ServerMessage::Event(entry) => {
                assert_eq!(entry.event_type, "TokenizedEquityMintEvent::MintRequested");
            }
            ServerMessage::Initial(_) => panic!("expected Event message"),
        }
    }

    #[tokio::test]
    async fn reactor_receive_works_for_equity_redemption() {
        let (sender, mut receiver) = broadcast::channel(16);
        let harness = ReactorHarness::new(EventBroadcaster::new(sender));

        let id = RedemptionAggregateId::new("redemption-123".to_string());

        harness
            .receive::<EquityRedemption>(id, make_redemption_completed())
            .await
            .unwrap();

        let msg = receiver.recv().await.expect("should receive message");

        match msg {
            ServerMessage::Event(entry) => {
                assert_eq!(entry.aggregate_type, "EquityRedemption");
                assert_eq!(entry.aggregate_id, "redemption-123");
                assert_eq!(entry.event_type, "EquityRedemptionEvent::Completed");
            }
            ServerMessage::Initial(_) => panic!("expected Event message"),
        }
    }

    #[tokio::test]
    async fn reactor_receive_works_for_usdc_rebalance() {
        let (sender, mut receiver) = broadcast::channel(16);
        let harness = ReactorHarness::new(EventBroadcaster::new(sender));

        let id = UsdcRebalanceId(Uuid::new_v4());

        harness
            .receive::<UsdcRebalance>(id.clone(), make_usdc_withdrawal_confirmed())
            .await
            .unwrap();

        let msg = receiver.recv().await.expect("should receive message");

        match msg {
            ServerMessage::Event(entry) => {
                assert_eq!(entry.aggregate_type, "UsdcRebalance");
                assert_eq!(entry.aggregate_id, id.to_string());
                assert_eq!(entry.event_type, "UsdcRebalanceEvent::WithdrawalConfirmed");
            }
            ServerMessage::Initial(_) => panic!("expected Event message"),
        }
    }

    #[tokio::test]
    async fn multiple_subscribers_receive_same_event() {
        let (sender, mut receiver1) = broadcast::channel(16);
        let mut receiver2 = sender.subscribe();
        let mut receiver3 = sender.subscribe();
        let broadcaster = EventBroadcaster::new(sender);

        let id = IssuerRequestId::new("multi-sub".to_string());

        broadcaster.broadcast_event::<TokenizedEquityMint>(&id, &make_mint_requested("MSFT", 100));

        let msg1 = receiver1
            .recv()
            .await
            .expect("receiver1 should get message");
        let msg2 = receiver2
            .recv()
            .await
            .expect("receiver2 should get message");
        let msg3 = receiver3
            .recv()
            .await
            .expect("receiver3 should get message");

        for (i, msg) in [msg1, msg2, msg3].into_iter().enumerate() {
            match msg {
                ServerMessage::Event(entry) => {
                    assert_eq!(
                        entry.aggregate_id,
                        "multi-sub",
                        "receiver {} got wrong aggregate_id",
                        i + 1
                    );
                }
                ServerMessage::Initial(_) => panic!("receiver {} expected Event message", i + 1),
            }
        }
    }

    #[tokio::test]
    async fn broadcast_empty_does_nothing() {
        let (sender, mut receiver) = broadcast::channel(16);
        let _broadcaster = EventBroadcaster::new(sender);

        let result =
            tokio::time::timeout(std::time::Duration::from_millis(10), receiver.recv()).await;

        assert!(result.is_err(), "should timeout with no messages");
    }

    #[test]
    fn event_store_entry_serializes_correctly() {
        let (sender, _) = broadcast::channel(16);
        let broadcaster = EventBroadcaster::new(sender);

        let id = IssuerRequestId::new("serialize-test".to_string());

        broadcaster.broadcast_event::<TokenizedEquityMint>(&id, &make_mint_requested("GOOG", 10));

        // Verify the entry via JSON (can't get the msg since receiver was dropped,
        // but we can test the entry construction directly)
        let entry = EventStoreEntry {
            aggregate_type: "TokenizedEquityMint".to_string(),
            aggregate_id: "serialize-test".to_string(),
            sequence: 42,
            event_type: "TokenizedEquityMintEvent::MintRequested".to_string(),
            timestamp: Utc::now(),
        };
        let json = serde_json::to_string(&entry).expect("serialization should succeed");

        assert!(json.contains("\"aggregate_type\":\"TokenizedEquityMint\""));
        assert!(json.contains("\"aggregate_id\":\"serialize-test\""));
        assert!(json.contains("\"sequence\":42"));
        assert!(json.contains("\"event_type\":\"TokenizedEquityMintEvent::MintRequested\""));
        assert!(json.contains("\"timestamp\""));
    }
}
