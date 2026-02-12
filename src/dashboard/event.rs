//! CQRS query processor that broadcasts aggregate events to WebSocket
//! dashboard clients.

use async_trait::async_trait;
use chrono::Utc;
use cqrs_es::{Aggregate, DomainEvent, EventEnvelope, Query};
use tokio::sync::broadcast;
use tracing::warn;

use st0x_dto::{EventStoreEntry, ServerMessage};

use crate::equity_redemption::EquityRedemption;
use crate::lifecycle::Lifecycle;
use crate::tokenized_equity_mint::TokenizedEquityMint;
use crate::usdc_rebalance::UsdcRebalance;

fn event_store_entry_from_envelope<A: Aggregate>(envelope: &EventEnvelope<A>) -> EventStoreEntry
where
    A::Event: DomainEvent,
{
    EventStoreEntry {
        aggregate_type: A::aggregate_type(),
        aggregate_id: envelope.aggregate_id.clone(),
        sequence: envelope.sequence as u64,
        event_type: envelope.payload.event_type(),
        timestamp: Utc::now(),
    }
}

/// A CQRS Query that broadcasts events to connected WebSocket clients.
///
/// This is a generic broadcaster that can be used with any aggregate type.
/// It implements `Query<A>` for specific aggregate types to integrate with
/// the CQRS framework.
pub(crate) struct EventBroadcaster {
    sender: broadcast::Sender<ServerMessage>,
}

impl EventBroadcaster {
    pub(crate) fn new(sender: broadcast::Sender<ServerMessage>) -> Self {
        Self { sender }
    }

    fn broadcast_events<A: Aggregate>(&self, events: &[EventEnvelope<A>])
    where
        A::Event: DomainEvent,
    {
        for envelope in events {
            let entry = event_store_entry_from_envelope(envelope);
            let msg = ServerMessage::Event(entry);

            if let Err(e) = self.sender.send(msg) {
                warn!("Failed to broadcast event (no receivers): {e}");
            }
        }
    }
}

#[async_trait]
impl Query<Lifecycle<TokenizedEquityMint>> for EventBroadcaster {
    async fn dispatch(
        &self,
        _aggregate_id: &str,
        events: &[EventEnvelope<Lifecycle<TokenizedEquityMint>>],
    ) {
        self.broadcast_events(events);
    }
}

#[async_trait]
impl Query<Lifecycle<EquityRedemption>> for EventBroadcaster {
    async fn dispatch(
        &self,
        _aggregate_id: &str,
        events: &[EventEnvelope<Lifecycle<EquityRedemption>>],
    ) {
        self.broadcast_events(events);
    }
}

#[async_trait]
impl Query<Lifecycle<UsdcRebalance>> for EventBroadcaster {
    async fn dispatch(
        &self,
        _aggregate_id: &str,
        events: &[EventEnvelope<Lifecycle<UsdcRebalance>>],
    ) {
        self.broadcast_events(events);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::equity_redemption::EquityRedemptionEvent;
    use crate::tokenized_equity_mint::TokenizedEquityMintEvent;
    use crate::usdc_rebalance::UsdcRebalanceEvent;
    use alloy::primitives::Address;
    use cqrs_es::Query;
    use st0x_execution::Symbol;
    use std::collections::HashMap;

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

    #[test]
    fn event_store_entry_from_envelope_extracts_fields() {
        let envelope: EventEnvelope<Lifecycle<TokenizedEquityMint>> = EventEnvelope {
            aggregate_id: "test-aggregate-123".to_string(),
            sequence: 5,
            payload: make_mint_requested("AAPL", 100),
            metadata: HashMap::new(),
        };

        let entry = event_store_entry_from_envelope(&envelope);

        assert_eq!(entry.aggregate_type, "TokenizedEquityMint");
        assert_eq!(entry.aggregate_id, "test-aggregate-123");
        assert_eq!(entry.sequence, 5);
        assert_eq!(entry.event_type, "TokenizedEquityMintEvent::MintRequested");
    }

    #[tokio::test]
    async fn event_broadcaster_sends_to_channel() {
        let (sender, mut receiver) = broadcast::channel(16);
        let broadcaster = EventBroadcaster::new(sender);

        let envelope: EventEnvelope<Lifecycle<TokenizedEquityMint>> = EventEnvelope {
            aggregate_id: "mint-123".to_string(),
            sequence: 1,
            payload: make_mint_requested("TSLA", 50),
            metadata: HashMap::new(),
        };

        broadcaster.broadcast_events(&[envelope]);

        let msg = receiver.recv().await.expect("should receive message");

        match msg {
            ServerMessage::Event(entry) => {
                assert_eq!(entry.aggregate_type, "TokenizedEquityMint");
                assert_eq!(entry.aggregate_id, "mint-123");
                assert_eq!(entry.sequence, 1);
                assert_eq!(entry.event_type, "TokenizedEquityMintEvent::MintRequested");
            }
            ServerMessage::Initial(_) => panic!("expected Event message"),
        }
    }

    #[tokio::test]
    async fn event_broadcaster_handles_no_receivers() {
        let (sender, _) = broadcast::channel::<ServerMessage>(16);
        let broadcaster = EventBroadcaster::new(sender);

        let envelope: EventEnvelope<Lifecycle<TokenizedEquityMint>> = EventEnvelope {
            aggregate_id: "mint-456".to_string(),
            sequence: 1,
            payload: make_mint_requested("GOOG", 10),
            metadata: HashMap::new(),
        };

        broadcaster.broadcast_events(&[envelope]);
    }

    #[tokio::test]
    async fn query_dispatch_broadcasts_multiple_events() {
        let (sender, mut receiver) = broadcast::channel(16);
        let broadcaster = EventBroadcaster::new(sender);

        let events: Vec<EventEnvelope<Lifecycle<TokenizedEquityMint>>> = vec![
            EventEnvelope {
                aggregate_id: "mint-multi".to_string(),
                sequence: 1,
                payload: make_mint_requested("NVDA", 25),
                metadata: HashMap::new(),
            },
            EventEnvelope {
                aggregate_id: "mint-multi".to_string(),
                sequence: 2,
                payload: TokenizedEquityMintEvent::MintCompleted {
                    completed_at: chrono::Utc::now(),
                },
                metadata: HashMap::new(),
            },
        ];

        Query::<Lifecycle<TokenizedEquityMint>>::dispatch(
            &broadcaster,
            "mint-multi",
            &events,
        )
        .await;

        let msg1 = receiver.recv().await.expect("should receive first message");
        let msg2 = receiver
            .recv()
            .await
            .expect("should receive second message");

        match msg1 {
            ServerMessage::Event(entry) => {
                assert_eq!(entry.sequence, 1);
                assert_eq!(entry.event_type, "TokenizedEquityMintEvent::MintRequested");
            }
            ServerMessage::Initial(_) => panic!("expected Event message"),
        }

        match msg2 {
            ServerMessage::Event(entry) => {
                assert_eq!(entry.sequence, 2);
                assert_eq!(entry.event_type, "TokenizedEquityMintEvent::MintCompleted");
            }
            ServerMessage::Initial(_) => panic!("expected Event message"),
        }
    }

    #[tokio::test]
    async fn query_dispatch_works_for_equity_redemption() {
        let (sender, mut receiver) = broadcast::channel(16);
        let broadcaster = EventBroadcaster::new(sender);

        let events: Vec<EventEnvelope<Lifecycle<EquityRedemption>>> = vec![EventEnvelope {
            aggregate_id: "redemption-123".to_string(),
            sequence: 1,
            payload: make_redemption_completed(),
            metadata: HashMap::new(),
        }];

        Query::<Lifecycle<EquityRedemption>>::dispatch(
            &broadcaster,
            "redemption-123",
            &events,
        )
        .await;

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
    async fn query_dispatch_works_for_usdc_rebalance() {
        let (sender, mut receiver) = broadcast::channel(16);
        let broadcaster = EventBroadcaster::new(sender);

        let events: Vec<EventEnvelope<Lifecycle<UsdcRebalance>>> = vec![EventEnvelope {
            aggregate_id: "usdc-456".to_string(),
            sequence: 1,
            payload: make_usdc_withdrawal_confirmed(),
            metadata: HashMap::new(),
        }];

        Query::<Lifecycle<UsdcRebalance>>::dispatch(&broadcaster, "usdc-456", &events).await;

        let msg = receiver.recv().await.expect("should receive message");

        match msg {
            ServerMessage::Event(entry) => {
                assert_eq!(entry.aggregate_type, "UsdcRebalance");
                assert_eq!(entry.aggregate_id, "usdc-456");
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

        let envelope: EventEnvelope<Lifecycle<TokenizedEquityMint>> = EventEnvelope {
            aggregate_id: "multi-sub".to_string(),
            sequence: 1,
            payload: make_mint_requested("MSFT", 100),
            metadata: HashMap::new(),
        };

        broadcaster.broadcast_events(&[envelope]);

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
    async fn broadcast_empty_events_does_nothing() {
        let (sender, mut receiver) = broadcast::channel(16);
        let broadcaster = EventBroadcaster::new(sender);

        let events: Vec<EventEnvelope<Lifecycle<TokenizedEquityMint>>> = vec![];

        broadcaster.broadcast_events(&events);

        let result =
            tokio::time::timeout(std::time::Duration::from_millis(10), receiver.recv()).await;

        assert!(result.is_err(), "should timeout with no messages");
    }

    #[test]
    fn event_store_entry_serializes_correctly() {
        let envelope: EventEnvelope<Lifecycle<TokenizedEquityMint>> = EventEnvelope {
            aggregate_id: "serialize-test".to_string(),
            sequence: 42,
            payload: make_mint_requested("GOOG", 10),
            metadata: HashMap::new(),
        };

        let entry = event_store_entry_from_envelope(&envelope);
        let json = serde_json::to_string(&entry).expect("serialization should succeed");

        assert!(json.contains("\"aggregate_type\":\"TokenizedEquityMint\""));
        assert!(json.contains("\"aggregate_id\":\"serialize-test\""));
        assert!(json.contains("\"sequence\":42"));
        assert!(json.contains("\"event_type\":\"TokenizedEquityMintEvent::MintRequested\""));
        assert!(json.contains("\"timestamp\""));
    }
}
