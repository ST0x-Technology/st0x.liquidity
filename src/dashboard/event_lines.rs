//! The `liq_trade`, `liq_transfer` and `liq_event` log lines: one structured
//! line per terminal trade, per transfer status change, and per committed
//! trade or transfer event, which the log pipeline ships as rows.
//!
//! The dashboard [`Broadcaster`](super::event::Broadcaster) writes them from
//! `Reactor::react_committed`. The store calls that once per commit and never
//! when it replays events, so a restart does not write old lines again. Each
//! line has an `event_id`, `<aggregate type>:<aggregate id>:<sequence>`, that
//! names the committed event behind it. One commit can write a `liq_event`
//! and a `liq_trade` or `liq_transfer` line with the same `event_id`, so a
//! consumer drops a duplicate by target and `event_id` together.
//!
//! Every line has the same fields every time. A value that is not known, such
//! as the price of a failed trade, is an empty string.

use chrono::{SecondsFormat, Utc};
use rain_math_float::Float;
use serde_json::Value;
use std::collections::HashMap;
use std::fmt::{self, Display};
use tokio::sync::Mutex;
use tracing::{info, warn};

use st0x_dto::{
    Direction, EquityMintStatus, EquityRedemptionStatus, Trade, TradeOutcome, TradingVenue,
    TransferOperation, UsdcBridgeDirection, UsdcBridgeStatus,
};
use st0x_event_sorcery::{Committed, DomainEvent, EventSourced};
use st0x_finance::{Symbol, Usd};

use super::equity_price::EquityPriceStore;
use super::transfer_loader::TransferKind;

/// Names one committed event: `<aggregate type>:<aggregate id>:<sequence>`.
///
/// The event store assigns the sequence once, at commit, so the same event
/// has the same id after a restart.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EventId {
    aggregate_type: &'static str,
    aggregate_id: String,
    sequence: usize,
}

impl EventId {
    pub(crate) fn of<Entity: EventSourced>(id: &Entity::Id, committed: Committed) -> Self {
        Self {
            aggregate_type: Entity::AGGREGATE_TYPE,
            aggregate_id: id.to_string(),
            sequence: committed.sequence,
        }
    }
}

impl Display for EventId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self {
            aggregate_type,
            aggregate_id,
            sequence,
        } = self;
        write!(formatter, "{aggregate_type}:{aggregate_id}:{sequence}")
    }
}

/// What a committed event belongs to: a trade at a venue, or a transfer of a
/// kind. The board's detail view finds a row's events by these.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EventParent {
    Trade(TradingVenue),
    Transfer(TransferKind),
}

/// Writes the `liq_event` line for one committed trade or transfer event.
///
/// `step` is the event's variant name and `payload` is the variant's fields as
/// a JSON string, the same shape the trade and transfer event endpoints
/// return, except that signed material is redacted.
pub(crate) fn log_event<Entity: EventSourced>(
    parent: EventParent,
    event: &Entity::Event,
    event_id: &EventId,
) {
    let event_type = event.event_type();
    let step = event_step(&event_type);

    let mut payload = match serde_json::to_value(event) {
        Ok(value) => variant_fields(step, value),
        Err(error) => {
            warn!(
                target: "dashboard",
                %event_id,
                %error,
                "Failed to serialize a committed event; its liq_event line is not written",
            );
            return;
        }
    };
    redact_signing_material(&mut payload);

    let (parent, venue, kind) = match parent {
        EventParent::Trade(venue) => ("trade", venue.to_string(), String::new()),
        EventParent::Transfer(kind) => ("transfer", String::new(), kind.to_string()),
    };

    info!(
        target: "liq_event",
        %event_id,
        parent,
        venue,
        kind,
        id = event_id.aggregate_id,
        sequence = event_id.sequence,
        step,
        payload = %payload,
        "Committed event",
    );
}

/// The variant name in an event type such as `OnChainTradeEvent::Filled`.
pub(crate) fn event_step(event_type: &str) -> &str {
    event_type.rsplit("::").next().unwrap_or(event_type)
}

/// The fields of the `step` variant of an event serialized as
/// `{"<step>": {...}}`, or the whole value when it has another shape.
pub(crate) fn variant_fields(step: &str, event: Value) -> Value {
    match event {
        Value::Object(mut variants) => variants.remove(step).unwrap_or(Value::Object(variants)),
        other => other,
    }
}

/// Payload keys that hold signed material: a mint authorization `signature`
/// and the `raw` bytes of a signed transaction. Events persist them before
/// delivery or broadcast, and a log line must not copy them.
const REDACTED_KEYS: [&str; 2] = ["raw", "signature"];

/// Replaces the value of every [`REDACTED_KEYS`] key, at any depth, with
/// `"redacted"`.
fn redact_signing_material(value: &mut Value) {
    match value {
        Value::Object(fields) => {
            for (key, field) in fields.iter_mut() {
                if REDACTED_KEYS.contains(&key.as_str()) {
                    *field = Value::String("redacted".to_string());
                } else {
                    redact_signing_material(field);
                }
            }
        }
        Value::Array(items) => items.iter_mut().for_each(redact_signing_material),
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
    }
}

/// Writes the `liq_trade` line for a trade at its terminal status.
///
/// `occurred_at` is when the trade filled or ended, which can be well before
/// the line when the bot catches up on onchain fills. `usd` is
/// `shares * price`, empty when the trade has no price.
pub(crate) fn log_trade(trade: &Trade, event_id: &EventId) {
    let Trade {
        id,
        occurred_at,
        venue,
        direction,
        symbol,
        shares,
        price,
        outcome,
    } = trade;

    let (status, error) = match outcome {
        TradeOutcome::Filled => ("filled", ""),
        TradeOutcome::Failed { error, .. } => ("failed", error.as_str()),
        TradeOutcome::Cancelled { .. } => ("cancelled", ""),
    };

    let usd = (*price)
        .and_then(|price| usd_value(shares.inner().inner(), price.inner(), event_id))
        .map(|usd| usd.to_string())
        .unwrap_or_default();

    info!(
        target: "liq_trade",
        %event_id,
        id,
        occurred_at = occurred_at.to_rfc3339_opts(SecondsFormat::AutoSi, true),
        %venue,
        direction = direction_name(*direction),
        %symbol,
        %shares,
        status,
        error,
        price = price.map(|price| price.to_string()).unwrap_or_default(),
        usd,
        "Trade reached a terminal status",
    );
}

/// Writes the `liq_transfer` line when a transfer's status changes.
///
/// Remembers the last status it wrote per transfer and skips an event that
/// leaves the status as it was. It keeps a transfer after a terminal status
/// too: an event can follow one without changing it, for example a late
/// `MintAuthorizationDelivered` after a completed mint. The memory grows by
/// one short entry per transfer this process sees, and it is in-process:
/// after a restart, the first event of a transfer writes its status again,
/// under a new `event_id`.
pub(crate) struct TransferLines {
    equity_prices: EquityPriceStore,
    statuses: Mutex<HashMap<String, &'static str>>,
}

impl TransferLines {
    pub(crate) fn new(equity_prices: EquityPriceStore) -> Self {
        Self {
            equity_prices,
            statuses: Mutex::new(HashMap::new()),
        }
    }

    /// `usd` values an equity transfer at its symbol's mark when the event
    /// commits, and a USDC bridge at its amount. It is empty for an equity
    /// transfer whose symbol has no live mark.
    pub(crate) async fn log(&self, transfer: &TransferOperation, event_id: &EventId) {
        let line = TransferLine::of(transfer);
        let key = format!("{}:{}", event_id.aggregate_type, event_id.aggregate_id);

        {
            let mut statuses = self.statuses.lock().await;
            if statuses.get(&key) == Some(&line.status) {
                return;
            }

            statuses.insert(key, line.status);
        }

        let usd = match line.value {
            TransferValue::Shares { symbol, quantity } => self
                .equity_prices
                .mark(symbol, Utc::now())
                .await
                .and_then(|mark| usd_value(quantity, mark.price, event_id)),
            TransferValue::Usdc(amount) => Some(Usd::new(amount)),
        };

        info!(
            target: "liq_transfer",
            %event_id,
            kind = %line.kind,
            id = line.id,
            symbol = line.symbol,
            direction = line.direction,
            amount = line.amount,
            status = line.status,
            started_at = transfer
                .started_at()
                .to_rfc3339_opts(SecondsFormat::AutoSi, true),
            usd = usd.map(|usd| usd.to_string()).unwrap_or_default(),
            "Transfer status changed",
        );
    }
}

/// The fields of a `liq_transfer` line that come from the transfer itself.
struct TransferLine<'transfer> {
    kind: TransferKind,
    id: String,
    symbol: String,
    direction: &'static str,
    amount: String,
    status: &'static str,
    value: TransferValue<'transfer>,
}

/// The kind of a transfer, as its `liq_event` and `liq_transfer` lines name it.
pub(crate) const fn transfer_kind(transfer: &TransferOperation) -> TransferKind {
    match transfer {
        TransferOperation::EquityMint(_) => TransferKind::EquityMint,
        TransferOperation::EquityRedemption(_) => TransferKind::EquityRedemption,
        TransferOperation::UsdcBridge(_) => TransferKind::UsdcBridge,
    }
}

/// What a transfer moves, for its USD value.
enum TransferValue<'transfer> {
    Shares {
        symbol: &'transfer Symbol,
        quantity: Float,
    },
    Usdc(Float),
}

impl<'transfer> TransferLine<'transfer> {
    fn of(transfer: &'transfer TransferOperation) -> Self {
        match transfer {
            TransferOperation::EquityMint(operation) => Self {
                kind: transfer_kind(transfer),
                id: operation.id.to_string(),
                symbol: operation.symbol.to_string(),
                direction: "",
                amount: operation.quantity.to_string(),
                status: mint_status(&operation.status),
                value: TransferValue::Shares {
                    symbol: &operation.symbol,
                    quantity: operation.quantity.inner(),
                },
            },
            TransferOperation::EquityRedemption(operation) => Self {
                kind: transfer_kind(transfer),
                id: operation.id.to_string(),
                symbol: operation.symbol.to_string(),
                direction: "",
                amount: operation.quantity.to_string(),
                status: redemption_status(&operation.status),
                value: TransferValue::Shares {
                    symbol: &operation.symbol,
                    quantity: operation.quantity.inner(),
                },
            },
            TransferOperation::UsdcBridge(operation) => Self {
                kind: transfer_kind(transfer),
                id: operation.id.to_string(),
                symbol: String::new(),
                direction: match operation.direction {
                    UsdcBridgeDirection::AlpacaToBase => "alpaca_to_base",
                    UsdcBridgeDirection::BaseToAlpaca => "base_to_alpaca",
                },
                amount: operation.amount.to_string(),
                status: usdc_status(&operation.status),
                value: TransferValue::Usdc(operation.amount.inner()),
            },
        }
    }
}

/// `quantity * price` as USD. Logs and returns `None` when the product does
/// not fit a float.
fn usd_value(quantity: Float, price: Float, event_id: &EventId) -> Option<Usd> {
    match quantity * price {
        Ok(product) => Some(Usd::new(product)),
        Err(error) => {
            warn!(
                target: "dashboard",
                %event_id,
                ?error,
                "Failed to compute the USD value of a log line; its usd field is empty",
            );
            None
        }
    }
}

const fn direction_name(direction: Direction) -> &'static str {
    match direction {
        Direction::Buy => "buy",
        Direction::Sell => "sell",
    }
}

const fn mint_status(status: &EquityMintStatus) -> &'static str {
    match status {
        EquityMintStatus::Minting => "minting",
        EquityMintStatus::Wrapping => "wrapping",
        EquityMintStatus::Depositing => "depositing",
        EquityMintStatus::Completed { .. } => "completed",
        EquityMintStatus::Failed { .. } => "failed",
        EquityMintStatus::Reconciled { .. } => "reconciled",
    }
}

const fn redemption_status(status: &EquityRedemptionStatus) -> &'static str {
    match status {
        EquityRedemptionStatus::Withdrawing => "withdrawing",
        EquityRedemptionStatus::Unwrapping => "unwrapping",
        EquityRedemptionStatus::Sending => "sending",
        EquityRedemptionStatus::PendingConfirmation => "pending_confirmation",
        EquityRedemptionStatus::Completed { .. } => "completed",
        EquityRedemptionStatus::Failed { .. } => "failed",
        EquityRedemptionStatus::Reconciled { .. } => "reconciled",
    }
}

const fn usdc_status(status: &UsdcBridgeStatus) -> &'static str {
    match status {
        UsdcBridgeStatus::Converting => "converting",
        UsdcBridgeStatus::Withdrawing => "withdrawing",
        UsdcBridgeStatus::Bridging => "bridging",
        UsdcBridgeStatus::Depositing => "depositing",
        UsdcBridgeStatus::Completed { .. } => "completed",
        UsdcBridgeStatus::Failed { .. } => "failed",
        UsdcBridgeStatus::Reconciled { .. } => "reconciled",
    }
}

/// Captures the `liq_*` lines a test writes, as their target and fields.
#[cfg(test)]
pub(crate) mod test_support {
    use std::collections::BTreeMap;
    use std::fmt::Debug;
    use std::sync::{Arc, Mutex};
    use tracing::field::{Field, Visit};
    use tracing::subscriber::DefaultGuard;
    use tracing::{Event, Subscriber};
    use tracing_subscriber::layer::{Context, Layer, SubscriberExt};

    /// One line: its target and every field, `message` included, as text.
    pub(crate) type Line = (String, BTreeMap<String, String>);

    #[derive(Clone, Default)]
    pub(crate) struct CapturedLines(Arc<Mutex<Vec<Line>>>);

    impl CapturedLines {
        /// Captures on this thread while the guard lives. Tokio tests run on
        /// one thread, so this sees the reactor's lines.
        pub(crate) fn install() -> (Self, DefaultGuard) {
            let captured = Self::default();
            let subscriber = tracing_subscriber::registry().with(captured.clone());
            (captured, tracing::subscriber::set_default(subscriber))
        }

        /// The lines captured so far, oldest first, and forgets them.
        pub(crate) fn take(&self) -> Vec<Line> {
            std::mem::take(&mut *self.0.lock().unwrap())
        }
    }

    impl<S: Subscriber> Layer<S> for CapturedLines {
        fn on_event(&self, event: &Event<'_>, _context: Context<'_, S>) {
            let target = event.metadata().target();
            if !target.starts_with("liq_") {
                return;
            }

            let mut fields = FieldMap::default();
            event.record(&mut fields);
            let FieldMap(fields) = fields;
            self.0.lock().unwrap().push((target.to_string(), fields));
        }
    }

    #[derive(Default)]
    struct FieldMap(BTreeMap<String, String>);

    impl Visit for FieldMap {
        fn record_str(&mut self, field: &Field, value: &str) {
            self.0.insert(field.name().to_string(), value.to_string());
        }

        fn record_u64(&mut self, field: &Field, value: u64) {
            self.0.insert(field.name().to_string(), value.to_string());
        }

        fn record_debug(&mut self, field: &Field, value: &dyn Debug) {
            self.0
                .insert(field.name().to_string(), format!("{value:?}"));
        }
    }

    /// A line's fields from `(name, value)` pairs, for exact comparisons.
    pub(crate) fn fields<const COUNT: usize>(
        pairs: [(&str, &str); COUNT],
    ) -> BTreeMap<String, String> {
        pairs
            .into_iter()
            .map(|(name, value)| (name.to_string(), value.to_string()))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{B256, Bytes, TxHash};
    use chrono::{TimeZone, Utc};
    use std::collections::BTreeMap;

    use st0x_dto::{EquityMintOperation, UsdcBridgeOperation};
    use st0x_evm::PreparedTransaction;
    use st0x_finance::{FractionalShares, Id, Positive, Usdc};
    use st0x_float_macro::float;

    use super::test_support::{CapturedLines, fields};
    use super::*;
    use crate::equity_redemption::{EquityRedemption, EquityRedemptionEvent};
    use crate::offchain::order::{OffchainOrder, OffchainOrderId};
    use crate::onchain_trade::{OnChainTrade, OnChainTradeEvent};
    use crate::tokenized_equity_mint::{TokenizedEquityMint, TokenizedEquityMintEvent};
    use crate::usdc_rebalance::{UsdcRebalance, UsdcRebalanceEvent};

    fn event_id(aggregate_type: &'static str, sequence: usize) -> EventId {
        EventId {
            aggregate_type,
            aggregate_id: "agg-1".to_string(),
            sequence,
        }
    }

    fn trade(price: Option<Usd>, outcome: TradeOutcome) -> Trade {
        Trade {
            id: "trade-1".to_string(),
            occurred_at: Utc.timestamp_opt(1_700_000_000, 0).unwrap(),
            venue: TradingVenue::Raindex,
            direction: Direction::Buy,
            symbol: Symbol::new("AAPL").unwrap(),
            shares: Positive::new(FractionalShares::new(float!(2))).unwrap(),
            price,
            outcome,
        }
    }

    fn usdc_bridge(status: UsdcBridgeStatus) -> TransferOperation {
        TransferOperation::UsdcBridge(UsdcBridgeOperation {
            id: Id::new("bridge-1").unwrap(),
            direction: UsdcBridgeDirection::BaseToAlpaca,
            amount: Usdc::new(float!(250.5)),
            status,
            started_at: Utc.timestamp_opt(1_700_000_000, 0).unwrap(),
            updated_at: Utc.timestamp_opt(1_700_000_060, 0).unwrap(),
        })
    }

    fn equity_mint(status: EquityMintStatus) -> TransferOperation {
        TransferOperation::EquityMint(EquityMintOperation {
            id: Id::new("mint-1").unwrap(),
            symbol: Symbol::new("AAPL").unwrap(),
            quantity: FractionalShares::new(float!(3)),
            status,
            started_at: Utc.timestamp_opt(1_700_000_000, 0).unwrap(),
            updated_at: Utc.timestamp_opt(1_700_000_060, 0).unwrap(),
        })
    }

    #[test]
    fn event_id_names_aggregate_type_id_and_sequence() {
        let id = OffchainOrderId::new();

        let event_id = EventId::of::<OffchainOrder>(&id, Committed::new(7));

        assert_eq!(event_id.to_string(), format!("OffchainOrder:{id}:7"));
    }

    #[test]
    fn filled_trade_line_has_price_and_usd() {
        let (captured, _guard) = CapturedLines::install();

        log_trade(
            &trade(Some(Usd::new(float!(150.5))), TradeOutcome::Filled),
            &event_id("OnChainTrade", 1),
        );

        assert_eq!(
            captured.take(),
            vec![(
                "liq_trade".to_string(),
                fields([
                    ("message", "Trade reached a terminal status"),
                    ("event_id", "OnChainTrade:agg-1:1"),
                    ("id", "trade-1"),
                    ("occurred_at", "2023-11-14T22:13:20Z"),
                    ("venue", "raindex"),
                    ("direction", "buy"),
                    ("symbol", "AAPL"),
                    ("shares", "2"),
                    ("status", "filled"),
                    ("error", ""),
                    ("price", "150.5"),
                    ("usd", "301"),
                ]),
            )]
        );
    }

    #[test]
    fn failed_trade_line_has_the_error_and_no_price() {
        let (captured, _guard) = CapturedLines::install();

        log_trade(
            &trade(
                None,
                TradeOutcome::Failed {
                    error: "broker unavailable".to_string(),
                    accepted_shares: None,
                    filled_shares: None,
                    remaining_shares: None,
                    excess_shares: None,
                },
            ),
            &event_id("OffchainOrder", 3),
        );

        assert_eq!(
            captured.take(),
            vec![(
                "liq_trade".to_string(),
                fields([
                    ("message", "Trade reached a terminal status"),
                    ("event_id", "OffchainOrder:agg-1:3"),
                    ("id", "trade-1"),
                    ("occurred_at", "2023-11-14T22:13:20Z"),
                    ("venue", "raindex"),
                    ("direction", "buy"),
                    ("symbol", "AAPL"),
                    ("shares", "2"),
                    ("status", "failed"),
                    ("error", "broker unavailable"),
                    ("price", ""),
                    ("usd", ""),
                ]),
            )]
        );
    }

    #[test]
    fn cancelled_trade_line_has_no_error_or_price() {
        let (captured, _guard) = CapturedLines::install();

        log_trade(
            &trade(
                None,
                TradeOutcome::Cancelled {
                    accepted_shares: None,
                    filled_shares: None,
                    remaining_shares: None,
                    excess_shares: None,
                },
            ),
            &event_id("OffchainOrder", 4),
        );

        let [(target, line)] = captured.take().try_into().unwrap();
        assert_eq!(target, "liq_trade");
        assert_eq!(line["status"], "cancelled");
        assert_eq!(line["error"], "");
        assert_eq!(line["price"], "");
        assert_eq!(line["usd"], "");
    }

    fn usdc_line(event_id: &str, status: &str) -> (String, BTreeMap<String, String>) {
        (
            "liq_transfer".to_string(),
            fields([
                ("message", "Transfer status changed"),
                ("event_id", event_id),
                ("kind", "usdc_bridge"),
                ("id", "bridge-1"),
                ("symbol", ""),
                ("direction", "base_to_alpaca"),
                ("amount", "250.5"),
                ("status", status),
                ("started_at", "2023-11-14T22:13:20Z"),
                ("usd", "250.5"),
            ]),
        )
    }

    #[tokio::test]
    async fn transfer_line_is_written_only_when_the_status_changes() {
        let (captured, _guard) = CapturedLines::install();
        let lines = TransferLines::new(EquityPriceStore::new([]));
        let completed = UsdcBridgeStatus::Completed {
            completed_at: Utc.timestamp_opt(1_700_000_120, 0).unwrap(),
        };

        for (sequence, status) in [
            (1, UsdcBridgeStatus::Converting),
            (2, UsdcBridgeStatus::Converting),
            (3, UsdcBridgeStatus::Bridging),
            (4, completed.clone()),
            (5, completed),
        ] {
            lines
                .log(&usdc_bridge(status), &event_id("UsdcRebalance", sequence))
                .await;
        }

        assert_eq!(
            captured.take(),
            vec![
                usdc_line("UsdcRebalance:agg-1:1", "converting"),
                usdc_line("UsdcRebalance:agg-1:3", "bridging"),
                usdc_line("UsdcRebalance:agg-1:4", "completed"),
            ]
        );
    }

    #[tokio::test]
    async fn reconciling_a_failed_transfer_is_a_status_change() {
        let (captured, _guard) = CapturedLines::install();
        let lines = TransferLines::new(EquityPriceStore::new([]));
        let failed = UsdcBridgeStatus::Failed {
            failed_at: Utc.timestamp_opt(1_700_000_120, 0).unwrap(),
            post_burn: true,
        };

        for (sequence, status) in [
            (1, failed.clone()),
            (2, failed),
            (
                3,
                UsdcBridgeStatus::Reconciled {
                    reconciled_at: Utc.timestamp_opt(1_700_000_180, 0).unwrap(),
                    failure_reason: None,
                    reconcile_reason: "operator confirmed".to_string(),
                },
            ),
        ] {
            lines
                .log(&usdc_bridge(status), &event_id("UsdcRebalance", sequence))
                .await;
        }

        assert_eq!(
            captured.take(),
            vec![
                usdc_line("UsdcRebalance:agg-1:1", "failed"),
                usdc_line("UsdcRebalance:agg-1:3", "reconciled"),
            ]
        );
    }

    #[tokio::test]
    async fn equity_transfer_usd_uses_the_live_mark() {
        let (captured, _guard) = CapturedLines::install();
        let lines = TransferLines::new(EquityPriceStore::with_live_mark(
            Symbol::new("AAPL").unwrap(),
            float!(100.25),
        ));

        lines
            .log(
                &equity_mint(EquityMintStatus::Minting),
                &event_id("TokenizedEquityMint", 1),
            )
            .await;

        assert_eq!(
            captured.take(),
            vec![(
                "liq_transfer".to_string(),
                fields([
                    ("message", "Transfer status changed"),
                    ("event_id", "TokenizedEquityMint:agg-1:1"),
                    ("kind", "equity_mint"),
                    ("id", "mint-1"),
                    ("symbol", "AAPL"),
                    ("direction", ""),
                    ("amount", "3"),
                    ("status", "minting"),
                    ("started_at", "2023-11-14T22:13:20Z"),
                    ("usd", "300.75"),
                ]),
            )]
        );
    }

    #[tokio::test]
    async fn equity_transfer_usd_is_empty_without_a_mark() {
        let (captured, _guard) = CapturedLines::install();
        let lines = TransferLines::new(EquityPriceStore::new([]));

        lines
            .log(
                &equity_mint(EquityMintStatus::Wrapping),
                &event_id("TokenizedEquityMint", 2),
            )
            .await;

        let [(_target, line)] = captured.take().try_into().unwrap();
        assert_eq!(line["status"], "wrapping");
        assert_eq!(line["usd"], "");
    }

    #[test]
    fn trade_event_line_carries_step_and_variant_payload() {
        let (captured, _guard) = CapturedLines::install();

        log_event::<OnChainTrade>(
            EventParent::Trade(TradingVenue::Bebop),
            &OnChainTradeEvent::Acknowledged {
                acknowledged_at: Utc.timestamp_opt(1_700_000_000, 0).unwrap(),
            },
            &event_id("OnChainTrade", 4),
        );

        assert_eq!(
            captured.take(),
            vec![(
                "liq_event".to_string(),
                fields([
                    ("message", "Committed event"),
                    ("event_id", "OnChainTrade:agg-1:4"),
                    ("parent", "trade"),
                    ("venue", "bebop"),
                    ("kind", ""),
                    ("id", "agg-1"),
                    ("sequence", "4"),
                    ("step", "Acknowledged"),
                    ("payload", r#"{"acknowledged_at":"2023-11-14T22:13:20Z"}"#),
                ]),
            )]
        );
    }

    #[test]
    fn transfer_event_line_names_the_kind() {
        let (captured, _guard) = CapturedLines::install();

        log_event::<UsdcRebalance>(
            EventParent::Transfer(TransferKind::UsdcBridge),
            &UsdcRebalanceEvent::ConversionFailed {
                reason: "venue rejected".to_string(),
                failed_at: Utc.timestamp_opt(1_700_000_000, 0).unwrap(),
            },
            &event_id("UsdcRebalance", 2),
        );

        let [(target, line)] = captured.take().try_into().unwrap();
        assert_eq!(target, "liq_event");
        assert_eq!(line["parent"], "transfer");
        assert_eq!(line["venue"], "");
        assert_eq!(line["kind"], "usdc_bridge");
        assert_eq!(line["step"], "ConversionFailed");
    }

    #[test]
    fn event_line_redacts_signatures_and_signed_transactions() {
        let mut payload = serde_json::json!({
            "nonce": "0x01",
            "signature": "0xabcd",
            "prepared_send": {
                "prepared": {"tx_hash": "0x02", "nonce": 7, "raw": "0xf86c"},
                "redemption_wallet": "0x03",
            },
            "replacements": [{"raw": "0xf86d"}],
        });

        redact_signing_material(&mut payload);

        assert_eq!(
            payload,
            serde_json::json!({
                "nonce": "0x01",
                "signature": "redacted",
                "prepared_send": {
                    "prepared": {"tx_hash": "0x02", "nonce": 7, "raw": "redacted"},
                    "redemption_wallet": "0x03",
                },
                "replacements": [{"raw": "redacted"}],
            })
        );
    }

    #[test]
    fn variant_fields_unwraps_the_step_variant() {
        assert_eq!(
            variant_fields("Filled", serde_json::json!({"Filled": {"amount": "2"}})),
            serde_json::json!({"amount": "2"})
        );
        assert_eq!(
            variant_fields("Filled", serde_json::json!("Acknowledged")),
            serde_json::json!("Acknowledged")
        );
        assert_eq!(event_step("OnChainTradeEvent::Filled"), "Filled");
    }

    #[test]
    fn signed_material_in_real_events_never_reaches_the_line() {
        let (captured, _guard) = CapturedLines::install();
        let at = Utc.timestamp_opt(1_700_000_000, 0).unwrap();

        log_event::<TokenizedEquityMint>(
            EventParent::Transfer(TransferKind::EquityMint),
            &TokenizedEquityMintEvent::MintAuthorizationSigned {
                nonce: B256::repeat_byte(0x01),
                signature: Bytes::from(vec![0xab, 0xcd]),
                signed_at: at,
            },
            &event_id("TokenizedEquityMint", 3),
        );
        log_event::<EquityRedemption>(
            EventParent::Transfer(TransferKind::EquityRedemption),
            &EquityRedemptionEvent::SendReplaced {
                replacement: PreparedTransaction::for_test(TxHash::repeat_byte(0x11), 7),
                replaced_at: at,
            },
            &event_id("EquityRedemption", 4),
        );

        let payloads: Vec<Value> = captured
            .take()
            .into_iter()
            .map(|(_target, line)| serde_json::from_str(&line["payload"]).unwrap())
            .collect();
        assert_eq!(
            payloads,
            vec![
                serde_json::json!({
                    "nonce": B256::repeat_byte(0x01),
                    "signature": "redacted",
                    "signed_at": "2023-11-14T22:13:20Z",
                }),
                serde_json::json!({
                    "replacement": {
                        "tx_hash": TxHash::repeat_byte(0x11),
                        "nonce": 7,
                        "raw": "redacted",
                    },
                    "replaced_at": "2023-11-14T22:13:20Z",
                }),
            ]
        );
    }
}
