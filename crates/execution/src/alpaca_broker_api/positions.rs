//! Liquidity inventory mapping over the shared Alpaca Broker API.

use rain_math_float::Float;
use st0x_alpaca::broker::Position;
use st0x_finance::{HasZero, Usd, Usdc};
use st0x_float_macro::float;
use tracing::{debug, trace};

use super::AlpacaBrokerApiError;
use super::client::AlpacaBrokerApiClient;
use crate::{EquityPosition, FractionalShares, Inventory};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct AccountFunds {
    pub(super) balance: i64,
    pub(super) buying_power: i64,
    pub(super) withdrawable: Option<i64>,
}

pub(super) async fn fetch_inventory(
    client: &AlpacaBrokerApiClient,
) -> Result<Inventory, AlpacaBrokerApiError> {
    let positions = st0x_alpaca::broker::list_positions(client.broker()).await?;
    let account_funds = get_account_funds(client).await?;
    let alpaca_usdc = alpaca_usdc_balance(&positions)?;

    let broker_positions = positions
        .into_iter()
        .filter(|position| {
            if position.is_non_equity() {
                trace!(
                    target: "broker",
                    symbol = %position.symbol,
                    asset_class = ?position.asset_class,
                    exchange = ?position.exchange,
                    quantity = ?position.quantity,
                    "Skipping non-equity Alpaca position from equity inventory"
                );
                return false;
            }

            true
        })
        .map(|position| {
            Ok(EquityPosition {
                symbol: position.symbol,
                quantity: FractionalShares::new(decimal_to_float(position.quantity)?),
                market_value: position.market_value.map(Usd::inner),
            })
        })
        .collect::<Result<Vec<_>, AlpacaBrokerApiError>>()?;

    debug!(target: "broker", count = broker_positions.len(), "Fetched broker positions");

    Ok(Inventory {
        positions: broker_positions,
        alpaca_usdc: Some(alpaca_usdc),
        usd_balance_cents: account_funds.balance,
        cash_buying_power_cents: Some(account_funds.buying_power),
        cash_withdrawable_cents: account_funds.withdrawable,
    })
}

fn alpaca_usdc_balance(positions: &[Position]) -> Result<Usdc, AlpacaBrokerApiError> {
    let Some(position) = positions
        .iter()
        .find(|position| position.symbol.as_str() == "USDCUSD")
    else {
        return Ok(Usdc::ZERO);
    };

    let total_quantity = position
        .total_quantity
        .ok_or(AlpacaBrokerApiError::MissingPositionQuantity)?;

    Ok(Usdc::new(decimal_to_float(total_quantity)?))
}

pub(super) async fn get_account_funds(
    client: &AlpacaBrokerApiClient,
) -> Result<AccountFunds, AlpacaBrokerApiError> {
    let account = st0x_alpaca::broker::get_account_details(client.broker()).await?;
    let balance = to_cash_value_cents(account.cash.inner())?;
    let withdrawable = account
        .cash_withdrawable
        .map(|cash| to_cash_value_cents(cash.inner()))
        .transpose()?;

    Ok(AccountFunds {
        balance,
        buying_power: balance,
        withdrawable,
    })
}

fn decimal_to_float(value: impl std::fmt::Display) -> Result<Float, AlpacaBrokerApiError> {
    Float::parse(value.to_string()).map_err(Into::into)
}

fn to_cash_value_cents(cash: Float) -> Result<i64, AlpacaBrokerApiError> {
    let hundred = float!(100);
    let cents = (cash * hundred).map_err(AlpacaBrokerApiError::FloatConversion)?;
    let frac = cents
        .frac()
        .map_err(AlpacaBrokerApiError::FloatConversion)?;

    if !frac
        .is_zero()
        .map_err(AlpacaBrokerApiError::FloatConversion)?
    {
        return Err(AlpacaBrokerApiError::FractionalCents(cash));
    }

    let integer_cents = cents
        .integer()
        .map_err(AlpacaBrokerApiError::FloatConversion)?;
    let formatted = integer_cents
        .format_with_scientific(false)
        .map_err(AlpacaBrokerApiError::FloatConversion)?;

    formatted
        .parse()
        .map_err(|_| AlpacaBrokerApiError::UsdBalanceConversion(cash))
}
