//! Schedule polling and one-shot position-scan wakeups.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use chrono::Utc;
use reqwest::{Client, StatusCode};
use serde::Deserialize;
use task_supervisor::{SupervisedTask, TaskResult};
use thiserror::Error;
use tracing::{info, warn};

use st0x_config::{PricingAuth, PricingCtx};
use st0x_pricing_types::trading_state::TradingState;

use super::{TradingScheduleError, TradingScheduleStore};
use crate::conductor::job::QueuePushError;
use crate::position_check::{CheckPositions, CheckPositionsJobQueue};
use crate::pricing_identity::{METADATA_IDENTITY_URL, PricingIdentityError, fetch_identity_from};

#[derive(Clone)]
pub(crate) struct TradingScheduleMonitor {
    pub(crate) store: Arc<TradingScheduleStore>,
    pub(crate) pricing: PricingCtx,
    pub(crate) queue: CheckPositionsJobQueue,
}

#[derive(Debug, Error)]
enum MonitorError {
    #[error(transparent)]
    Identity(#[from] PricingIdentityError),
    #[error(transparent)]
    Request(#[from] reqwest::Error),
    #[error("trading schedule answered unexpected HTTP status {0}")]
    UnexpectedStatus(StatusCode),
    #[error("invalid pricing HTTP endpoint")]
    Endpoint,
    #[error(transparent)]
    Schedule(#[from] TradingScheduleError),
    #[error(transparent)]
    Queue(#[from] QueuePushError),
    #[error("trading schedule request exceeded its deadline")]
    Timeout(#[from] tokio::time::error::Elapsed),
    #[error("metadata token has invalid audience or expiration")]
    TokenClaims,
    #[error("trading schedule duration cannot be represented")]
    Duration(#[from] std::num::TryFromIntError),
}

struct Credential {
    value: String,
    expires_at: Option<i64>,
}

#[derive(Deserialize)]
struct IdentityClaims {
    aud: String,
    exp: i64,
}

fn token_expiration(
    token: &str,
    audience: &str,
    now: i64,
    margin: i64,
) -> Result<i64, MonitorError> {
    let parts: Vec<_> = token.split('.').collect();
    let [_, payload, _] = parts.as_slice() else {
        return Err(MonitorError::TokenClaims);
    };
    let payload = URL_SAFE_NO_PAD
        .decode(payload)
        .map_err(|_| MonitorError::TokenClaims)?;
    let claims: IdentityClaims =
        serde_json::from_slice(&payload).map_err(|_| MonitorError::TokenClaims)?;
    if claims.aud != audience || claims.exp <= now.saturating_add(margin) {
        return Err(MonitorError::TokenClaims);
    }
    Ok(claims.exp)
}

impl TradingScheduleMonitor {
    async fn poll(
        &self,
        client: &Client,
        credential: &mut Option<Credential>,
    ) -> Result<(), MonitorError> {
        let mut url = self.pricing.ws_url.clone();
        let scheme = if url.scheme() == "wss" {
            "https"
        } else {
            "http"
        };
        url.set_scheme(scheme)
            .map_err(|()| MonitorError::Endpoint)?;
        url.set_path("/trading-state");
        for scope in &self.store.config.scopes {
            let result = tokio::time::timeout(
                Duration::from_secs(self.store.config.request_timeout_secs.get()),
                self.fetch(client, url.clone(), &scope.id, credential),
            )
            .await;
            match result {
                Ok(Ok(response)) => match self.store.accept(scope, response, Utc::now()).await {
                    Ok(()) => {}
                    Err(error) => {
                        warn!(scope = %scope.id, %error, "Rejected trading schedule; retaining safety latch");
                    }
                },
                Ok(Err(error)) => {
                    warn!(scope = %scope.id, %error, "Trading schedule unavailable; retaining safety latch");
                }
                Err(error) => {
                    warn!(scope = %scope.id, %error, "Trading schedule request timed out; retaining safety latch");
                }
            }
        }
        Ok(())
    }

    async fn fetch(
        &self,
        client: &Client,
        mut url: url::Url,
        scope: &str,
        credential: &mut Option<Credential>,
    ) -> Result<TradingState, MonitorError> {
        url.query_pairs_mut().append_pair("scope", scope);
        let margin = i64::try_from(
            self.store.config.request_timeout_secs.get()
                + self.store.config.evidence_clock_skew_secs.get(),
        )?;
        if credential.as_ref().is_some_and(|credential| {
            credential
                .expires_at
                .is_some_and(|expiry| expiry <= Utc::now().timestamp().saturating_add(margin))
        }) {
            *credential = None;
        }
        let token = match &mut *credential {
            Some(token) => token,
            empty @ None => empty.insert(self.create_credential(margin).await?),
        };
        let mut response = client
            .get(url.clone())
            .bearer_auth(&token.value)
            .send()
            .await?;
        if response.status() == StatusCode::UNAUTHORIZED {
            *credential = None;
            let token = credential.insert(self.create_credential(margin).await?);
            response = client.get(url).bearer_auth(&token.value).send().await?;
        }
        let response = response.error_for_status()?;
        if !response.status().is_success() {
            return Err(MonitorError::UnexpectedStatus(response.status()));
        }
        Ok(response.json().await?)
    }

    async fn create_credential(&self, margin: i64) -> Result<Credential, MonitorError> {
        match &self.pricing.auth {
            PricingAuth::ApiKey(key) => Ok(Credential {
                value: key.bearer_value().to_owned(),
                expires_at: None,
            }),
            PricingAuth::GcpIdToken { audience } => {
                let value = fetch_identity_from(
                    METADATA_IDENTITY_URL,
                    audience,
                    Duration::from_secs(self.store.config.request_timeout_secs.get()),
                )
                .await?;
                let expires_at =
                    token_expiration(&value, audience, Utc::now().timestamp(), margin)?;
                Ok(Credential {
                    value,
                    expires_at: Some(expires_at),
                })
            }
        }
    }

    async fn polling(&self) -> Result<(), MonitorError> {
        let client = Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_secs(
                self.store.config.request_timeout_secs.get(),
            ))
            .build()?;
        let mut ticks = tokio::time::interval(Duration::from_secs(
            self.store.config.poll_interval_secs.get(),
        ));
        ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut credential = None;
        loop {
            ticks.tick().await;
            self.poll(&client, &mut credential).await?;
        }
    }

    async fn deadlines(&self) -> Result<(), MonitorError> {
        let mut queue = self.queue.clone();
        let mut emitted = HashSet::new();
        loop {
            let now = Utc::now();
            self.wake_due(&mut queue, &mut emitted, now).await?;
            let delay = self
                .store
                .next_boundary(now)?
                .and_then(|boundary| (boundary - now).to_std().ok())
                .unwrap_or_else(|| Duration::from_secs(self.store.config.poll_interval_secs.get()))
                .min(Duration::from_secs(
                    self.store.config.poll_interval_secs.get(),
                ));
            tokio::select! {
                () = tokio::time::sleep(delay) => {},
                () = self.store.changed.notified() => {},
            }
        }
    }

    async fn wake_due(
        &self,
        queue: &mut CheckPositionsJobQueue,
        emitted: &mut HashSet<String>,
        now: chrono::DateTime<Utc>,
    ) -> Result<(), MonitorError> {
        if self.store.enabled() {
            for identity in self.store.due_wakeups(now).await? {
                if emitted.insert(identity.clone()) {
                    if let Err(error) = queue
                        .push_idempotent(&identity, CheckPositions { one_shot: true })
                        .await
                    {
                        emitted.remove(&identity);
                        return Err(error.into());
                    }
                    info!("Trading schedule boundary triggered a one-shot position scan");
                }
            }
        }
        Ok(())
    }
}

impl SupervisedTask for TradingScheduleMonitor {
    async fn run(&mut self) -> TaskResult {
        tokio::try_join!(self.polling(), self.deadlines())?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[tokio::test]
    async fn repeated_boundary_delivery_enqueues_only_one_one_shot_scan() {
        let (pool, apalis_pool) = crate::test_utils::setup_test_pools().await;
        let config = super::super::tests::config();
        let store = Arc::new(
            TradingScheduleStore::load(config.clone(), pool)
                .await
                .unwrap(),
        );
        store
            .accept(
                &config.scopes[0],
                super::super::tests::response(2000, 4000),
                chrono::DateTime::from_timestamp_millis(1000).unwrap(),
            )
            .await
            .unwrap();
        let queue = CheckPositionsJobQueue::new(&apalis_pool);
        let pricing =
            PricingCtx::new(url::Url::parse("ws://localhost/ws").unwrap(), "test".into()).unwrap();
        let monitor = TradingScheduleMonitor {
            store,
            pricing,
            queue: queue.clone(),
        };
        let mut emitted = HashSet::new();
        let mut queue = queue;
        sqlx_apalis::query("ALTER TABLE Jobs RENAME TO UnavailableJobs")
            .execute(&apalis_pool)
            .await
            .unwrap();
        let error = monitor
            .wake_due(
                &mut queue,
                &mut emitted,
                chrono::DateTime::from_timestamp_millis(2000).unwrap(),
            )
            .await
            .unwrap_err();
        assert!(matches!(error, MonitorError::Queue(_)));
        assert!(emitted.is_empty());
        sqlx_apalis::query("ALTER TABLE UnavailableJobs RENAME TO Jobs")
            .execute(&apalis_pool)
            .await
            .unwrap();
        for _ in 0..2 {
            monitor
                .wake_due(
                    &mut queue,
                    &mut emitted,
                    chrono::DateTime::from_timestamp_millis(2000).unwrap(),
                )
                .await
                .unwrap();
            emitted.clear();
        }
        let jobs: i64 = sqlx_apalis::query_scalar("SELECT COUNT(*) FROM Jobs WHERE job_type = ?")
            .bind(std::any::type_name::<CheckPositions>())
            .fetch_one(&apalis_pool)
            .await
            .unwrap();
        assert_eq!(jobs, 1);
    }

    #[tokio::test]
    async fn observation_mode_never_enqueues_a_deadline_scan() {
        let (pool, apalis_pool) = crate::test_utils::setup_test_pools().await;
        let mut config = super::super::tests::config();
        config.mode = st0x_config::TradingScheduleMode::Observe;
        let store = Arc::new(
            TradingScheduleStore::load(config.clone(), pool)
                .await
                .unwrap(),
        );
        store
            .accept(
                &config.scopes[0],
                super::super::tests::response(2000, 4000),
                chrono::DateTime::from_timestamp_millis(1000).unwrap(),
            )
            .await
            .unwrap();
        let queue = CheckPositionsJobQueue::new(&apalis_pool);
        let pricing =
            PricingCtx::new(url::Url::parse("ws://localhost/ws").unwrap(), "test".into()).unwrap();
        let monitor = TradingScheduleMonitor {
            store,
            pricing,
            queue: queue.clone(),
        };
        monitor
            .wake_due(
                &mut queue.clone(),
                &mut HashSet::new(),
                chrono::DateTime::from_timestamp_millis(4000).unwrap(),
            )
            .await
            .unwrap();
        let jobs: i64 = sqlx_apalis::query_scalar("SELECT COUNT(*) FROM Jobs WHERE job_type = ?")
            .bind(std::any::type_name::<CheckPositions>())
            .fetch_one(&apalis_pool)
            .await
            .unwrap();
        assert_eq!(jobs, 0);
    }

    #[tokio::test]
    async fn unauthorized_retries_once_and_forbidden_does_not_retry() {
        for (status, expected_calls) in [(401, 2), (403, 1)] {
            let (pool, apalis_pool) = crate::test_utils::setup_test_pools().await;
            let server = httpmock::MockServer::start_async().await;
            let rejection = server
                .mock_async(|when, then| {
                    when.path("/trading-state")
                        .query_param("scope", "regular")
                        .header("authorization", "Bearer test");
                    then.status(status);
                })
                .await;
            let monitor = TradingScheduleMonitor {
                store: Arc::new(
                    TradingScheduleStore::load(super::super::tests::config(), pool)
                        .await
                        .unwrap(),
                ),
                pricing: PricingCtx::new(
                    url::Url::parse("ws://localhost/ws").unwrap(),
                    "test".into(),
                )
                .unwrap(),
                queue: CheckPositionsJobQueue::new(&apalis_pool),
            };
            let error = monitor
                .fetch(
                    &Client::new(),
                    url::Url::parse(&server.url("/trading-state")).unwrap(),
                    "regular",
                    &mut None,
                )
                .await
                .unwrap_err();
            let MonitorError::Request(error) = error else {
                panic!("expected HTTP rejection, got {error:?}");
            };
            assert_eq!(error.status(), Some(StatusCode::from_u16(status).unwrap()));
            rejection.assert_calls_async(expected_calls).await;
        }
    }

    #[test]
    fn token_claims_require_expected_audience_and_refresh_margin() {
        let token = format!(
            "header.{}.signature",
            URL_SAFE_NO_PAD.encode(
                serde_json::to_vec(&json!({"aud": "https://pricing.example", "exp": 100})).unwrap()
            )
        );
        assert_eq!(
            token_expiration(&token, "https://pricing.example", 90, 5).unwrap(),
            100
        );
        assert!(matches!(
            token_expiration(&token, "https://other.example", 90, 5).unwrap_err(),
            MonitorError::TokenClaims
        ));
        assert!(matches!(
            token_expiration(&token, "https://pricing.example", 95, 5).unwrap_err(),
            MonitorError::TokenClaims
        ));
    }

    #[tokio::test]
    async fn rejected_cached_credential_is_replaced_and_redirects_are_not_followed() {
        let (pool, apalis_pool) = crate::test_utils::setup_test_pools().await;
        let server = httpmock::MockServer::start_async().await;
        let stale = server
            .mock_async(|when, then| {
                when.path("/trading-state")
                    .header("authorization", "Bearer stale");
                then.status(401);
            })
            .await;
        let fresh = server
            .mock_async(|when, then| {
                when.path("/trading-state")
                    .header("authorization", "Bearer test");
                then.status(200)
                    .json_body_obj(&super::super::tests::response(2000, 4000));
            })
            .await;
        let redirect = server
            .mock_async(|when, then| {
                when.path("/redirect");
                then.status(302)
                    .header("location", server.url("/secret-target"))
                    .json_body_obj(&super::super::tests::response(2000, 4000));
            })
            .await;
        let target = server
            .mock_async(|when, then| {
                when.path("/secret-target");
                then.status(200);
            })
            .await;
        let monitor = TradingScheduleMonitor {
            store: Arc::new(
                TradingScheduleStore::load(super::super::tests::config(), pool)
                    .await
                    .unwrap(),
            ),
            pricing: PricingCtx::new(url::Url::parse("ws://localhost/ws").unwrap(), "test".into())
                .unwrap(),
            queue: CheckPositionsJobQueue::new(&apalis_pool),
        };
        let client = Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .unwrap();
        let mut credential = Some(Credential {
            value: "stale".into(),
            expires_at: None,
        });
        monitor
            .fetch(
                &client,
                url::Url::parse(&server.url("/trading-state")).unwrap(),
                "regular",
                &mut credential,
            )
            .await
            .unwrap();
        stale.assert_async().await;
        fresh.assert_async().await;
        let error = monitor
            .fetch(
                &client,
                url::Url::parse(&server.url("/redirect")).unwrap(),
                "regular",
                &mut credential,
            )
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            MonitorError::UnexpectedStatus(StatusCode::FOUND)
        ));
        redirect.assert_async().await;
        target.assert_calls_async(0).await;
    }
}
