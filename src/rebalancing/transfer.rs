//! Cross-venue asset transfer abstraction.
//!
//! [`CrossVenueTransfer`] defines the interface for moving assets between
//! trading venues. Implementations encapsulate the full multi-step lifecycle
//! (e.g. mint + deposit, or withdraw + redeem). The inventory layer calls
//! this without knowing the steps involved.

use async_trait::async_trait;

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
