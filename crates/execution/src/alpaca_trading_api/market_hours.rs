use apca::api::v2::clock;
use apca::{Client, RequestError};

#[derive(Debug, thiserror::Error)]
pub enum MarketHoursError {
    #[error("Alpaca API request failed")]
    ApiRequest(#[from] RequestError<clock::GetError>),
}

pub(super) async fn is_market_open(client: &Client) -> Result<bool, MarketHoursError> {
    let clock_data = client.issue::<clock::Get>(&()).await?;
    Ok(clock_data.open)
}
