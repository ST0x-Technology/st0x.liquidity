//! Alpaca Broker account activity retrieval for reporting-only cost and revenue ingestion.
//!
//! This module fetches immutable broker activity rows for the configured account. It does not
//! place orders, mutate account state, or participate in hot-path execution.

use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::AlpacaBrokerApiError;
use super::auth::AlpacaBrokerApiCtx;
use super::client::AlpacaBrokerApiClient;

// Broker API docs list `activity_types` as the server-side filter and enumerate supported
// activity codes on the specific-type endpoint:
// https://docs.alpaca.markets/us/reference/getaccountactivities
// https://docs.alpaca.markets/us/reference/getaccountactivitiesbytype
// The PnL report fetches only rows that can affect fees, credits, interest, dividends, or
// capital-gain distributions.
const PNL_ACTIVITY_TYPES: &[&str] = &[
    "FEE", "PTC", "INT", "DIV", "DIVCGL", "DIVCGS", "DIVNRA", "DIVROC", "DIVTXEX", "CGD", "CIL",
    "CSD",
];

const ACCOUNT_ACTIVITIES_PAGE_SIZE: usize = 100;

#[cfg(not(test))]
const MAX_ACCOUNT_ACTIVITIES_PAGES: usize = 1000;
#[cfg(test)]
const MAX_ACCOUNT_ACTIVITIES_PAGES: usize = 3;

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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct AccountActivity {
    pub id: String,
    pub activity_type: String,
    #[serde(default)]
    pub activity_sub_type: Option<String>,
    #[serde(default)]
    pub date: Option<NaiveDate>,
    #[serde(default)]
    pub created_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub net_amount: Option<String>,
    #[serde(default)]
    pub symbol: Option<String>,
    #[serde(default)]
    pub qty: Option<String>,
    #[serde(default)]
    pub per_share_amount: Option<String>,
    #[serde(default)]
    pub price: Option<String>,
    #[serde(default)]
    pub side: Option<String>,
    #[serde(default)]
    pub order_id: Option<Uuid>,
    #[serde(default)]
    pub transaction_time: Option<DateTime<Utc>>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub group_id: Option<String>,
    #[serde(default)]
    pub currency: Option<String>,
}

pub(super) async fn get_account_activities(
    client: &AlpacaBrokerApiClient,
    query: &AccountActivitiesQuery,
) -> Result<Vec<AccountActivity>, AlpacaBrokerApiError> {
    let mut rows = Vec::new();
    let mut page_token: Option<String> = None;

    for _ in 0..MAX_ACCOUNT_ACTIVITIES_PAGES {
        let page = fetch_account_activities_page(client, query, page_token.as_deref()).await?;
        if page.is_empty() {
            return Ok(rows);
        }

        // Alpaca documents `page_token` as the ID of the last item from the current page; with
        // `direction=asc`, the next page begins immediately after that activity.
        let last_id = page.last().map(|row| row.id.clone());
        let page_len = page.len();
        rows.extend(page);

        if page_len < ACCOUNT_ACTIVITIES_PAGE_SIZE {
            return Ok(rows);
        }

        if page_token == last_id {
            return Err(AlpacaBrokerApiError::AccountActivitiesPaginationInvariantViolation);
        }

        page_token = last_id;
    }

    Err(AlpacaBrokerApiError::AccountActivitiesPageLimitExceeded {
        pages: MAX_ACCOUNT_ACTIVITIES_PAGES,
    })
}

impl AlpacaBrokerApiCtx {
    pub async fn fetch_account_activities(
        &self,
        query: &AccountActivitiesQuery,
    ) -> Result<Vec<AccountActivity>, AlpacaBrokerApiError> {
        let client = AlpacaBrokerApiClient::new(self)?;
        get_account_activities(&client, query).await
    }
}

async fn fetch_account_activities_page(
    client: &AlpacaBrokerApiClient,
    query: &AccountActivitiesQuery,
    page_token: Option<&str>,
) -> Result<Vec<AccountActivity>, AlpacaBrokerApiError> {
    let base = format!("{}/v1/accounts/activities", client.base_url());
    let mut url = reqwest::Url::parse(&base).map_err(|error| {
        AlpacaBrokerApiError::InvalidAccountActivitiesUrl {
            url: base.clone(),
            source: error,
        }
    })?;

    {
        let mut pairs = url.query_pairs_mut();
        pairs.append_pair("account_id", &client.account_id().to_string());
        pairs.append_pair("direction", "asc");
        pairs.append_pair("page_size", &ACCOUNT_ACTIVITIES_PAGE_SIZE.to_string());

        if !query.activity_types.is_empty() {
            pairs.append_pair("activity_types", &query.activity_types.join(","));
        }

        if let Some(after) = query.after {
            pairs.append_pair("after", &after.to_rfc3339());
        }

        if let Some(until) = query.until {
            pairs.append_pair("until", &until.to_rfc3339());
        }

        if let Some(page_token) = page_token {
            pairs.append_pair("page_token", page_token);
        }
    }

    client.get(url.as_str()).await
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;
    use httpmock::prelude::*;
    use uuid::uuid;

    use super::*;
    use crate::alpaca_broker_api::TimeInForce;
    use crate::alpaca_broker_api::auth::{
        AlpacaAccountId, AlpacaBrokerApiCtx, AlpacaBrokerApiMode,
    };

    const TEST_ACCOUNT_ID: AlpacaAccountId =
        AlpacaAccountId::new(uuid!("904837e3-3b76-47ec-b432-046db621571b"));

    fn create_test_ctx(server: &MockServer) -> AlpacaBrokerApiCtx {
        AlpacaBrokerApiCtx {
            api_key: "test_key_id".to_string(),
            api_secret: "test_secret_key".to_string(),
            account_id: TEST_ACCOUNT_ID,
            mode: Some(AlpacaBrokerApiMode::Mock(server.base_url())),
            asset_cache_ttl: std::time::Duration::from_secs(3600),
            time_in_force: TimeInForce::Day,
            counter_trade_slippage_bps: crate::DEFAULT_ALPACA_COUNTER_TRADE_SLIPPAGE_BPS,
        }
    }

    #[test]
    fn pnl_activity_query_requests_capital_gain_distributions() {
        let query = AccountActivitiesQuery::pnl(None, None);

        assert!(
            query
                .activity_types
                .iter()
                .any(|activity_type| activity_type == "CGD")
        );
    }

    #[test]
    fn pnl_activity_query_uses_broker_supported_activity_types() {
        let query = AccountActivitiesQuery::pnl(None, None);

        assert_eq!(
            query.activity_types,
            vec![
                "FEE", "PTC", "INT", "DIV", "DIVCGL", "DIVCGS", "DIVNRA", "DIVROC", "DIVTXEX",
                "CGD", "CIL", "CSD",
            ]
        );
    }

    #[tokio::test]
    async fn fetches_account_activities_with_pagination() {
        let server = MockServer::start();
        let second_page = server.mock(|when, then| {
            when.method(GET)
                .path("/v1/accounts/activities")
                .query_param("account_id", TEST_ACCOUNT_ID.to_string())
                .query_param("page_token", "20260600000000000099::fee");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(serde_json::json!([
                    {
                        "activity_type": "DIV",
                        "id": "20260604000000000::div",
                        "date": "2026-06-04",
                        "net_amount": "1.25",
                        "symbol": "SGOV"
                    }
                ]));
        });
        let first_page = server.mock(|when, then| {
            when.method(GET)
                .path("/v1/accounts/activities")
                .query_param("account_id", TEST_ACCOUNT_ID.to_string())
                .query_param("activity_types", "FEE,DIV")
                .query_param("direction", "asc")
                .query_param("page_size", ACCOUNT_ACTIVITIES_PAGE_SIZE.to_string());
            then.status(200)
                .header("content-type", "application/json")
                .json_body(serde_json::json!(
                    (0..ACCOUNT_ACTIVITIES_PAGE_SIZE)
                        .map(|idx| serde_json::json!({
                            "activity_type": "FEE",
                            "id": format!("202606{:014}::fee", idx),
                            "date": "2026-06-03",
                            "net_amount": "-0.01"
                        }))
                        .collect::<Vec<_>>()
                ));
        });

        let client = AlpacaBrokerApiClient::new(&create_test_ctx(&server)).unwrap();
        let rows = get_account_activities(
            &client,
            &AccountActivitiesQuery {
                activity_types: vec!["FEE".to_string(), "DIV".to_string()],
                after: None,
                until: None,
            },
        )
        .await
        .unwrap();

        first_page.assert();
        second_page.assert();
        assert_eq!(rows.len(), ACCOUNT_ACTIVITIES_PAGE_SIZE + 1);
        assert_eq!(rows.last().unwrap().activity_type, "DIV");
        assert_eq!(rows.last().unwrap().symbol.as_deref(), Some("SGOV"));
    }

    #[tokio::test]
    async fn sends_after_and_until_filters() {
        let server = MockServer::start();
        let mock = server.mock(|when, then| {
            when.method(GET)
                .path("/v1/accounts/activities")
                .query_param("after", "2026-06-03T00:00:00+00:00")
                .query_param("until", "2026-06-04T00:00:00+00:00");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(serde_json::json!([]));
        });

        let client = AlpacaBrokerApiClient::new(&create_test_ctx(&server)).unwrap();
        get_account_activities(
            &client,
            &AccountActivitiesQuery {
                activity_types: Vec::new(),
                after: Utc.with_ymd_and_hms(2026, 6, 3, 0, 0, 0).single(),
                until: Utc.with_ymd_and_hms(2026, 6, 4, 0, 0, 0).single(),
            },
        )
        .await
        .unwrap();

        mock.assert();
    }

    #[tokio::test]
    async fn rejects_repeated_account_activities_page_token() {
        let server = MockServer::start();
        let second_page = server.mock(|when, then| {
            when.method(GET)
                .path("/v1/accounts/activities")
                .query_param("account_id", TEST_ACCOUNT_ID.to_string())
                .query_param("page_token", "repeated-token");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(full_page("repeated-token"));
        });
        let first_page = server.mock(|when, then| {
            when.method(GET)
                .path("/v1/accounts/activities")
                .query_param("account_id", TEST_ACCOUNT_ID.to_string())
                .query_param("page_size", ACCOUNT_ACTIVITIES_PAGE_SIZE.to_string());
            then.status(200)
                .header("content-type", "application/json")
                .json_body(full_page("repeated-token"));
        });

        let client = AlpacaBrokerApiClient::new(&create_test_ctx(&server)).unwrap();
        let error = get_account_activities(
            &client,
            &AccountActivitiesQuery {
                activity_types: Vec::new(),
                after: None,
                until: None,
            },
        )
        .await
        .unwrap_err();

        first_page.assert();
        second_page.assert();
        assert!(matches!(
            error,
            AlpacaBrokerApiError::AccountActivitiesPaginationInvariantViolation
        ));
    }

    #[tokio::test]
    async fn rejects_account_activities_after_page_limit() {
        let server = MockServer::start();
        let mut mocks = Vec::new();
        for page_index in 0..MAX_ACCOUNT_ACTIVITIES_PAGES {
            let page_token = page_index
                .checked_sub(1)
                .map(|previous| format!("page-{previous}-last"));
            let last_id = format!("page-{page_index}-last");
            let mock = server.mock(|when, then| {
                let when = when
                    .method(GET)
                    .path("/v1/accounts/activities")
                    .query_param("account_id", TEST_ACCOUNT_ID.to_string())
                    .query_param("page_size", ACCOUNT_ACTIVITIES_PAGE_SIZE.to_string());
                if let Some(page_token) = &page_token {
                    when.query_param("page_token", page_token);
                } else {
                    when.query_param_missing("page_token");
                }
                then.status(200)
                    .header("content-type", "application/json")
                    .json_body(full_page(&last_id));
            });
            mocks.push(mock);
        }

        let client = AlpacaBrokerApiClient::new(&create_test_ctx(&server)).unwrap();
        let error = get_account_activities(
            &client,
            &AccountActivitiesQuery {
                activity_types: Vec::new(),
                after: None,
                until: None,
            },
        )
        .await
        .unwrap_err();

        for mock in mocks {
            mock.assert();
        }
        assert!(matches!(
            error,
            AlpacaBrokerApiError::AccountActivitiesPageLimitExceeded {
                pages: MAX_ACCOUNT_ACTIVITIES_PAGES
            }
        ));
    }

    fn full_page(last_id: &str) -> serde_json::Value {
        serde_json::json!(
            (0..ACCOUNT_ACTIVITIES_PAGE_SIZE)
                .map(|idx| {
                    let id = if idx + 1 == ACCOUNT_ACTIVITIES_PAGE_SIZE {
                        last_id.to_owned()
                    } else {
                        format!("{last_id}-{idx}")
                    };
                    serde_json::json!({
                        "activity_type": "FEE",
                        "id": id,
                        "date": "2026-06-03",
                        "net_amount": "-0.01"
                    })
                })
                .collect::<Vec<_>>()
        )
    }
}
