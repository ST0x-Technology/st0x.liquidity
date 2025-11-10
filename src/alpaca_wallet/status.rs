use backon::{ExponentialBuilder, Retryable};
use std::time::Duration;
use tokio::time::{Instant, sleep};
use tracing::info;

use super::client::{AlpacaWalletClient, AlpacaWalletError};
use super::transfer::{Transfer, TransferId, TransferStatus, get_transfer_status};

pub(super) struct PollingConfig {
    pub(super) interval: Duration,
    pub(super) timeout: Duration,
    pub(super) max_retries: usize,
}

impl Default for PollingConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(10),
            timeout: Duration::from_secs(30 * 60),
            max_retries: 10,
        }
    }
}

pub(super) async fn poll_transfer_status(
    client: &AlpacaWalletClient,
    transfer_id: &TransferId,
    config: &PollingConfig,
) -> Result<Transfer, AlpacaWalletError> {
    let start = Instant::now();
    let mut retries = 0;
    let mut last_status = None;

    loop {
        if start.elapsed() >= config.timeout {
            return Err(AlpacaWalletError::TransferTimeout {
                transfer_id: transfer_id.to_string(),
                elapsed: start.elapsed(),
            });
        }

        let transfer = match get_transfer_status(client, transfer_id).await {
            Ok(transfer) => {
                retries = 0;
                transfer
            }
            Err(AlpacaWalletError::ApiError { status, .. }) if status.is_server_error() => {
                retries += 1;
                if retries > config.max_retries {
                    return Err(AlpacaWalletError::MaxRetriesExceeded {
                        transfer_id: transfer_id.to_string(),
                        retries,
                    });
                }

                let backoff = Duration::from_secs(2u64.pow(retries - 1));
                info!(
                    "Server error polling transfer {}, retry {}/{} after {:?}",
                    transfer_id, retries, config.max_retries, backoff
                );
                sleep(backoff).await;
                continue;
            }
            Err(e) => return Err(e),
        };

        if let Some(prev_status) = last_status {
            if is_status_regression(prev_status, transfer.status) {
                return Err(AlpacaWalletError::InvalidStatusTransition {
                    transfer_id: transfer_id.to_string(),
                    from: format!("{:?}", prev_status),
                    to: format!("{:?}", transfer.status),
                });
            }

            if prev_status != transfer.status {
                info!(
                    "Transfer {} status: {:?} -> {:?}",
                    transfer_id, prev_status, transfer.status
                );
            }
        } else {
            info!(
                "Transfer {} initial status: {:?}",
                transfer_id, transfer.status
            );
        }

        match transfer.status {
            TransferStatus::Complete => {
                info!("Transfer {} completed successfully", transfer_id);
                return Ok(transfer);
            }
            TransferStatus::Failed => {
                info!("Transfer {} failed", transfer_id);
                return Ok(transfer);
            }
            TransferStatus::Pending | TransferStatus::Processing => {
                last_status = Some(transfer.status);
                sleep(config.interval).await;
            }
        }
    }
}

fn is_status_regression(from: TransferStatus, to: TransferStatus) -> bool {
    matches!(
        (from, to),
        (TransferStatus::Processing, TransferStatus::Pending)
            | (TransferStatus::Complete, _)
            | (TransferStatus::Failed, _)
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use httpmock::prelude::*;
    use serde_json::json;
    use uuid::Uuid;

    fn create_account_mock<'a>(server: &'a MockServer, account_id: &str) -> httpmock::Mock<'a> {
        server.mock(|when, then| {
            when.method(GET).path("/v2/account");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": account_id,
                    "account_number": "PA1234567890",
                    "status": "ACTIVE",
                    "currency": "USD",
                    "buying_power": "100000.00",
                    "regt_buying_power": "100000.00",
                    "daytrading_buying_power": "400000.00",
                    "non_marginable_buying_power": "100000.00",
                    "cash": "100000.00",
                    "accrued_fees": "0",
                    "pending_transfer_out": "0",
                    "pending_transfer_in": "0",
                    "portfolio_value": "100000.00",
                    "pattern_day_trader": false,
                    "trading_blocked": false,
                    "transfers_blocked": false,
                    "account_blocked": false,
                    "created_at": "2020-01-01T00:00:00Z",
                    "trade_suspended_by_user": false,
                    "multiplier": "4",
                    "shorting_enabled": true,
                    "equity": "100000.00",
                    "last_equity": "100000.00",
                    "long_market_value": "0",
                    "short_market_value": "0",
                    "initial_margin": "0",
                    "maintenance_margin": "0",
                    "last_maintenance_margin": "0",
                    "sma": "0",
                    "daytrade_count": 0
                }));
        })
    }

    #[tokio::test]
    async fn test_poll_transfer_processing_to_complete() {
        let server = MockServer::start();
        let expected_account_id = "904837e3-3b76-47ec-b432-046db621571b";
        let account_mock = create_account_mock(&server, expected_account_id);

        let transfer_id = Uuid::new_v4();

        let status_mock = server.mock(|when, then| {
            when.method(GET)
                .path(format!(
                    "/v1/accounts/{}/wallets/transfers",
                    expected_account_id
                ))
                .query_param("transfer_id", transfer_id.to_string());
            then.status(200)
                .header("content-type", "application/json")
                .json_body_obj(&json!([{
                    "id": transfer_id,
                    "relationship": "OUTGOING",
                    "amount": "100.0",
                    "asset": "USDC",
                    "from_address": null,
                    "to_address": "0x1234567890abcdef1234567890abcdef12345678",
                    "status": "PROCESSING",
                    "tx_hash": "0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890",
                    "created_at": "2024-01-01T00:00:00Z",
                    "network_fee_amount": "0.5"
                }]));
        });

        let complete_mock = server.mock(|when, then| {
            when.method(GET)
                .path(format!(
                    "/v1/accounts/{}/wallets/transfers",
                    expected_account_id
                ))
                .query_param("transfer_id", transfer_id.to_string());
            then.status(200)
                .header("content-type", "application/json")
                .json_body_obj(&json!([{
                    "id": transfer_id,
                    "relationship": "OUTGOING",
                    "amount": "100.0",
                    "asset": "USDC",
                    "from_address": null,
                    "to_address": "0x1234567890abcdef1234567890abcdef12345678",
                    "status": "COMPLETE",
                    "tx_hash": "0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890",
                    "created_at": "2024-01-01T00:00:00Z",
                    "network_fee_amount": "0.5"
                }]));
        });

        let client = AlpacaWalletClient::new_with_base_url(
            server.base_url(),
            "test_key_id".to_string(),
            "test_secret_key".to_string(),
        )
        .await
        .unwrap();

        let config = PollingConfig {
            interval: Duration::from_millis(100),
            timeout: Duration::from_secs(5),
            max_retries: 3,
        };

        let result = poll_transfer_status(&client, &TransferId::from(transfer_id), &config)
            .await
            .unwrap();

        assert_eq!(result.status, TransferStatus::Complete);

        account_mock.assert();
        status_mock.assert();
        complete_mock.assert();
    }

    #[tokio::test]
    async fn test_poll_transfer_failed() {
        let server = MockServer::start();
        let expected_account_id = "904837e3-3b76-47ec-b432-046db621571b";
        let account_mock = create_account_mock(&server, expected_account_id);

        let transfer_id = Uuid::new_v4();

        let status_mock = server.mock(|when, then| {
            when.method(GET)
                .path(format!(
                    "/v1/accounts/{}/wallets/transfers",
                    expected_account_id
                ))
                .query_param("transfer_id", transfer_id.to_string());
            then.status(200)
                .header("content-type", "application/json")
                .json_body_obj(&json!([{
                    "id": transfer_id,
                    "relationship": "OUTGOING",
                    "amount": "100.0",
                    "asset": "USDC",
                    "from_address": null,
                    "to_address": "0x1234567890abcdef1234567890abcdef12345678",
                    "status": "FAILED",
                    "tx_hash": null,
                    "created_at": "2024-01-01T00:00:00Z",
                    "network_fee_amount": null
                }]));
        });

        let client = AlpacaWalletClient::new_with_base_url(
            server.base_url(),
            "test_key_id".to_string(),
            "test_secret_key".to_string(),
        )
        .await
        .unwrap();

        let config = PollingConfig {
            interval: Duration::from_millis(100),
            timeout: Duration::from_secs(5),
            max_retries: 3,
        };

        let result = poll_transfer_status(&client, &TransferId::from(transfer_id), &config)
            .await
            .unwrap();

        assert_eq!(result.status, TransferStatus::Failed);

        account_mock.assert();
        status_mock.assert();
    }

    #[tokio::test]
    async fn test_poll_transfer_timeout() {
        let server = MockServer::start();
        let expected_account_id = "904837e3-3b76-47ec-b432-046db621571b";
        let account_mock = create_account_mock(&server, expected_account_id);

        let transfer_id = Uuid::new_v4();

        let status_mock = server.mock(|when, then| {
            when.method(GET)
                .path(format!(
                    "/v1/accounts/{}/wallets/transfers",
                    expected_account_id
                ))
                .query_param("transfer_id", transfer_id.to_string());
            then.status(200)
                .header("content-type", "application/json")
                .json_body_obj(&json!([{
                    "id": transfer_id,
                    "relationship": "OUTGOING",
                    "amount": "100.0",
                    "asset": "USDC",
                    "from_address": null,
                    "to_address": "0x1234567890abcdef1234567890abcdef12345678",
                    "status": "PENDING",
                    "tx_hash": null,
                    "created_at": "2024-01-01T00:00:00Z",
                    "network_fee_amount": "0.5"
                }]));
        });

        let client = AlpacaWalletClient::new_with_base_url(
            server.base_url(),
            "test_key_id".to_string(),
            "test_secret_key".to_string(),
        )
        .await
        .unwrap();

        let config = PollingConfig {
            interval: Duration::from_millis(100),
            timeout: Duration::from_millis(500),
            max_retries: 3,
        };

        let result = poll_transfer_status(&client, &TransferId::from(transfer_id), &config).await;

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            AlpacaWalletError::TransferTimeout { .. }
        ));

        account_mock.assert();
        assert!(status_mock.hits() >= 2);
    }

    #[tokio::test]
    async fn test_poll_transfer_retry_on_5xx() {
        let server = MockServer::start();
        let expected_account_id = "904837e3-3b76-47ec-b432-046db621571b";
        let account_mock = create_account_mock(&server, expected_account_id);

        let transfer_id = Uuid::new_v4();

        let error_mock = server.mock(|when, then| {
            when.method(GET)
                .path(format!(
                    "/v1/accounts/{}/wallets/transfers",
                    expected_account_id
                ))
                .query_param("transfer_id", transfer_id.to_string());
            then.status(503).body("Service Unavailable");
        });

        let success_mock = server.mock(|when, then| {
            when.method(GET)
                .path(format!(
                    "/v1/accounts/{}/wallets/transfers",
                    expected_account_id
                ))
                .query_param("transfer_id", transfer_id.to_string());
            then.status(200)
                .header("content-type", "application/json")
                .json_body_obj(&json!([{
                    "id": transfer_id,
                    "relationship": "OUTGOING",
                    "amount": "100.0",
                    "asset": "USDC",
                    "from_address": null,
                    "to_address": "0x1234567890abcdef1234567890abcdef12345678",
                    "status": "COMPLETE",
                    "tx_hash": "0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890",
                    "created_at": "2024-01-01T00:00:00Z",
                    "network_fee_amount": "0.5"
                }]));
        });

        let client = AlpacaWalletClient::new_with_base_url(
            server.base_url(),
            "test_key_id".to_string(),
            "test_secret_key".to_string(),
        )
        .await
        .unwrap();

        let config = PollingConfig {
            interval: Duration::from_millis(100),
            timeout: Duration::from_secs(10),
            max_retries: 3,
        };

        let result = poll_transfer_status(&client, &TransferId::from(transfer_id), &config)
            .await
            .unwrap();

        assert_eq!(result.status, TransferStatus::Complete);

        account_mock.assert();
        assert!(error_mock.hits() >= 1);
        success_mock.assert();
    }

    #[tokio::test]
    async fn test_poll_transfer_status_regression() {
        let server = MockServer::start();
        let expected_account_id = "904837e3-3b76-47ec-b432-046db621571b";
        let account_mock = create_account_mock(&server, expected_account_id);

        let transfer_id = Uuid::new_v4();

        let processing_mock = server.mock(|when, then| {
            when.method(GET)
                .path(format!(
                    "/v1/accounts/{}/wallets/transfers",
                    expected_account_id
                ))
                .query_param("transfer_id", transfer_id.to_string());
            then.status(200)
                .header("content-type", "application/json")
                .json_body_obj(&json!([{
                    "id": transfer_id,
                    "relationship": "OUTGOING",
                    "amount": "100.0",
                    "asset": "USDC",
                    "from_address": null,
                    "to_address": "0x1234567890abcdef1234567890abcdef12345678",
                    "status": "PROCESSING",
                    "tx_hash": "0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890",
                    "created_at": "2024-01-01T00:00:00Z",
                    "network_fee_amount": "0.5"
                }]));
        });

        let pending_mock = server.mock(|when, then| {
            when.method(GET)
                .path(format!(
                    "/v1/accounts/{}/wallets/transfers",
                    expected_account_id
                ))
                .query_param("transfer_id", transfer_id.to_string());
            then.status(200)
                .header("content-type", "application/json")
                .json_body_obj(&json!([{
                    "id": transfer_id,
                    "relationship": "OUTGOING",
                    "amount": "100.0",
                    "asset": "USDC",
                    "from_address": null,
                    "to_address": "0x1234567890abcdef1234567890abcdef12345678",
                    "status": "PENDING",
                    "tx_hash": null,
                    "created_at": "2024-01-01T00:00:00Z",
                    "network_fee_amount": "0.5"
                }]));
        });

        let client = AlpacaWalletClient::new_with_base_url(
            server.base_url(),
            "test_key_id".to_string(),
            "test_secret_key".to_string(),
        )
        .await
        .unwrap();

        let config = PollingConfig {
            interval: Duration::from_millis(100),
            timeout: Duration::from_secs(5),
            max_retries: 3,
        };

        let result = poll_transfer_status(&client, &TransferId::from(transfer_id), &config).await;

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            AlpacaWalletError::InvalidStatusTransition { .. }
        ));

        account_mock.assert();
        processing_mock.assert();
        pending_mock.assert();
    }
}
