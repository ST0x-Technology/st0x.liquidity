//! Cutoff-block computation for the `OrderFillMonitor` WebSocket
//! subscription.
//!
//! After (re)subscribing to `ClearV3` / `TakeOrderV3` streams, the
//! monitor needs a single block number that bounds "events the new
//! subscription will deliver" so that any earlier blocks can be
//! covered by a backfill job.
//!
//! The cutoff is the block number of the first event observed on
//! either stream within a 5-second window. If no event arrives in
//! that window, the cutoff falls back to the provider's current head
//! block. The first event consumed by this helper is included in the
//! backfill range — the caller does not also feed it to the live
//! listen loop.

use alloy::providers::Provider;
use alloy::sol_types;
use futures_util::{Stream, StreamExt};
use std::time::Duration;
use tracing::{debug, info};

use crate::bindings::IOrderBookV6::{ClearV3, TakeOrderV3};

/// Determines the block number at which the WS subscription starts.
///
/// Waits up to 5 seconds for the first event on either stream. If an
/// event arrives, its block number is the cutoff. If no events
/// arrive within the timeout, falls back to the provider's current
/// block number.
pub(crate) async fn get_cutoff_block<ClearEvents, TakeOrderEvents, P>(
    clear_stream: &mut ClearEvents,
    take_stream: &mut TakeOrderEvents,
    provider: &P,
) -> anyhow::Result<u64>
where
    ClearEvents: Stream<Item = Result<(ClearV3, alloy::rpc::types::Log), sol_types::Error>> + Unpin,
    TakeOrderEvents:
        Stream<Item = Result<(TakeOrderV3, alloy::rpc::types::Log), sol_types::Error>> + Unpin,
    P: Provider + Clone,
{
    info!("Waiting for first WebSocket event to determine cutoff block...");

    let timeout = tokio::time::sleep(Duration::from_secs(5));
    tokio::pin!(timeout);

    loop {
        let block_number =
            await_next_block(clear_stream, take_stream, &mut timeout, provider).await?;

        if let Some(block) = block_number {
            info!("First event at block {block}, using as cutoff");
            return Ok(block);
        }

        debug!("Event missing block number, waiting for next event");
    }
}

async fn await_next_block<ClearEvents, TakeOrderEvents, P>(
    clear_stream: &mut ClearEvents,
    take_stream: &mut TakeOrderEvents,
    timeout: &mut std::pin::Pin<&mut tokio::time::Sleep>,
    provider: &P,
) -> anyhow::Result<Option<u64>>
where
    ClearEvents: Stream<Item = Result<(ClearV3, alloy::rpc::types::Log), sol_types::Error>> + Unpin,
    TakeOrderEvents:
        Stream<Item = Result<(TakeOrderV3, alloy::rpc::types::Log), sol_types::Error>> + Unpin,
    P: Provider + Clone,
{
    tokio::select! {
        Some(result) = clear_stream.next() => {
            Ok(result?.1.block_number)
        }
        Some(result) = take_stream.next() => {
            Ok(result?.1.block_number)
        }
        () = &mut *timeout => {
            let current_block = provider.get_block_number().await?;
            info!("No events within timeout, using current block {current_block} as cutoff");
            Ok(Some(current_block))
        }
    }
}

#[cfg(test)]
mod tests {
    use alloy::providers::{ProviderBuilder, mock::Asserter};
    use futures_util::stream;

    use super::*;

    #[tokio::test]
    async fn cutoff_falls_back_to_current_block_when_no_events() {
        // Both streams immediately return `None` from `.next()`, so
        // the corresponding `Some(result)` arms in the `select!` never
        // match and the 5s timeout fires, returning the provider's
        // current block.
        let asserter = Asserter::new();
        asserter.push_success(&serde_json::Value::from(12345_u64));
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        let mut clear_stream =
            stream::empty::<Result<(ClearV3, alloy::rpc::types::Log), sol_types::Error>>();
        let mut take_stream =
            stream::empty::<Result<(TakeOrderV3, alloy::rpc::types::Log), sol_types::Error>>();

        let cutoff = get_cutoff_block(&mut clear_stream, &mut take_stream, &provider)
            .await
            .unwrap();

        assert_eq!(cutoff, 12345);
    }
}
