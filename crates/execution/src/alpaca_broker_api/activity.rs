//! Liquidity-owned account-activity query presets over the shared Alpaca API.

use chrono::{DateTime, Utc};

use super::AlpacaBrokerApiError;
use super::auth::AlpacaBrokerApiCtx;
use super::client::AlpacaBrokerApiClient;

pub use st0x_alpaca::broker::AccountActivity;

const PNL_ACTIVITY_TYPES: &[&str] = &[
    "FEE", "PTC", "INT", "DIV", "DIVCGL", "DIVCGS", "DIVNRA", "DIVROC", "DIVTXEX", "CGD", "CIL",
    "CSD",
];

#[derive(Debug, Clone)]
pub struct AccountActivitiesQuery {
    pub activity_types: Vec<String>,
    pub after: Option<DateTime<Utc>>,
    pub until: Option<DateTime<Utc>>,
}

impl AccountActivitiesQuery {
    pub fn pnl(after: Option<DateTime<Utc>>, until: Option<DateTime<Utc>>) -> Self {
        Self {
            activity_types: PNL_ACTIVITY_TYPES
                .iter()
                .map(std::string::ToString::to_string)
                .collect(),
            after,
            until,
        }
    }

    fn as_shared(&self) -> st0x_alpaca::broker::AccountActivitiesQuery {
        st0x_alpaca::broker::AccountActivitiesQuery {
            activity_types: self.activity_types.clone(),
            after: self.after,
            until: self.until,
        }
    }
}

impl AlpacaBrokerApiCtx {
    pub async fn fetch_account_activities(
        &self,
        query: &AccountActivitiesQuery,
    ) -> Result<Vec<AccountActivity>, AlpacaBrokerApiError> {
        let client = AlpacaBrokerApiClient::new(self)?;
        st0x_alpaca::broker::get_account_activities(client.broker(), &query.as_shared())
            .await
            .map_err(Into::into)
    }
}
