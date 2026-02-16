//! Cross-venue asset transfer abstraction.
//!
//! Provides the [`CrossVenueTransfer`] trait that models directional
//! asset movement between the market-making venue (Raindex) and the
//! hedging venue (Alpaca). Venue markers ([`MarketMakingVenue`],
//! [`HedgingVenue`]) encode transfer direction in the type system.
//!
//! [`EquityTransferServices`] bundles the service traits needed by
//! both equity transfer aggregates (`TokenizedEquityMint` and
//! `EquityRedemption`).

use async_trait::async_trait;
use std::sync::Arc;

use crate::onchain::raindex::Raindex;
use crate::tokenization::Tokenizer;

/// Raindex orderbook -- where market-making inventory lives.
pub(crate) struct MarketMakingVenue;

/// Alpaca brokerage -- where hedging positions and share custody live.
pub(crate) struct HedgingVenue;

/// Transfers an asset from one venue to another.
///
/// `Source` and `Destination` are venue marker types that encode the
/// transfer direction at the type level, preventing accidental misuse.
#[async_trait]
pub(crate) trait CrossVenueTransfer<Source, Destination>: Send + Sync {
    type Asset;
    type Error;

    async fn transfer(&self, asset: Self::Asset) -> Result<(), Self::Error>;
}

/// Services shared by both equity transfer aggregates.
///
/// Both `TokenizedEquityMint` (hedging -> market-making) and
/// `EquityRedemption` (market-making -> hedging) need Raindex for
/// vault operations and Tokenizer for Alpaca API interactions.
#[derive(Clone)]
pub(crate) struct EquityTransferServices {
    pub(crate) raindex: Arc<dyn Raindex>,
    pub(crate) tokenizer: Arc<dyn Tokenizer>,
}
