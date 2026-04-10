//! Inventory lookups for Alpaca Trading API.

use rain_math_float::Float;
use serde::Deserialize;
use st0x_float_macro::float;
use tracing::error;

use super::AlpacaTradingApiError;
use super::auth::AlpacaTradingApiClient;
use crate::{
    EquityPosition, FractionalShares, Inventory, Symbol, deserialize_float_from_number_or_string,
    deserialize_option_float_from_number_or_string,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct AccountFunds {
    pub(super) cash_balance_cents: i64,
    pub(super) margin_safe_buying_power_cents: i64,
}

#[derive(Debug, Deserialize)]
struct PositionResponse {
    symbol: String,
    #[serde(
        rename = "qty",
        deserialize_with = "deserialize_float_from_number_or_string"
    )]
    quantity: Float,
    #[serde(
        default,
        deserialize_with = "deserialize_option_float_from_number_or_string"
    )]
    market_value: Option<Float>,
}

#[derive(Debug, Deserialize)]
pub(super) struct AccountResponse {
    #[serde(deserialize_with = "deserialize_float_from_number_or_string")]
    pub(super) cash: Float,
    #[serde(
        default,
        deserialize_with = "deserialize_option_float_from_number_or_string"
    )]
    pub(super) non_marginable_buying_power: Option<Float>,
}

pub(super) async fn fetch_inventory(
    client: &AlpacaTradingApiClient,
) -> Result<Inventory, AlpacaTradingApiError> {
    let positions = list_positions(client).await?;
    let account_funds = get_account_funds(client).await?;

    Ok(Inventory {
        positions: positions
            .into_iter()
            .map(|position| {
                let symbol = Symbol::new(&position.symbol).inspect_err(|_| {
                    error!(
                        symbol = %position.symbol,
                        position = ?position,
                        "Invalid symbol in Alpaca Trading API position"
                    );
                })?;

                Ok(EquityPosition {
                    symbol,
                    quantity: FractionalShares::new(position.quantity),
                    market_value: position.market_value,
                })
            })
            .collect::<Result<Vec<_>, AlpacaTradingApiError>>()?,
        cash_balance_cents: account_funds.cash_balance_cents,
    })
}

pub(super) async fn get_account_funds(
    client: &AlpacaTradingApiClient,
) -> Result<AccountFunds, AlpacaTradingApiError> {
    let account = get_account_details(client).await?;
    let cash_balance_cents = to_cash_value_cents(account.cash)?;
    let margin_safe_buying_power_cents = account
        .non_marginable_buying_power
        .map(to_cash_value_cents)
        .transpose()?
        .map_or(cash_balance_cents, |available| {
            available.min(cash_balance_cents)
        });

    Ok(AccountFunds {
        cash_balance_cents,
        margin_safe_buying_power_cents,
    })
}

pub(super) async fn get_account_details(
    client: &AlpacaTradingApiClient,
) -> Result<AccountResponse, AlpacaTradingApiError> {
    client
        .http_client()
        .get(format!("{}/v2/account", client.base_url()))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await
        .map_err(Into::into)
}

async fn list_positions(
    client: &AlpacaTradingApiClient,
) -> Result<Vec<PositionResponse>, AlpacaTradingApiError> {
    client
        .http_client()
        .get(format!("{}/v2/positions", client.base_url()))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await
        .map_err(Into::into)
}

pub(super) fn to_cash_value_cents(cash: Float) -> Result<i64, AlpacaTradingApiError> {
    let hundred = float!(100);
    let cents = (cash * hundred)?;
    let fractional = cents.frac()?;

    if !fractional.is_zero()? {
        return Err(AlpacaTradingApiError::FractionalCents(cash));
    }

    let integer_cents = cents.integer()?;
    let formatted = integer_cents.format_with_scientific(false)?;
    formatted
        .parse()
        .map_err(|_| AlpacaTradingApiError::CashBalanceConversion(cash))
}
