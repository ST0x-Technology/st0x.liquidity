use rust_decimal::Decimal;
use serde::{Deserialize, Deserializer, Serialize};
use st0x_exact_decimal::ExactDecimal;

/// Deserialize orderId from Schwab API as int64 and convert to string for database compatibility.
///
/// NOTE: Schwab API spec defines orderId as int64, but our database schema stores it as TEXT.
/// This conversion bridges the API format to our storage format. We may want to change the
/// database schema to INTEGER before production deployment.
fn deserialize_order_id<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: Deserializer<'de>,
{
    let opt_value = Option::<u64>::deserialize(deserializer)?;
    Ok(opt_value.map(|order_id| order_id.to_string()))
}

/// Order status enum matching Schwab API states
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum OrderStatus {
    Queued,
    Working,
    Filled,
    Canceled,
    Rejected,
    PendingActivation,
    PendingReview,
    Accepted,
    AwaitingParentOrder,
    AwaitingCondition,
    AwaitingManualReview,
    AwaitingStopCondition,
    Expired,
    New,
    AwaitingReleaseTime,
    PendingReplace,
    Replaced,
}

/// Order status response from Schwab API
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct OrderStatusResponse {
    #[serde(default, deserialize_with = "deserialize_order_id")]
    pub order_id: Option<String>,
    pub status: Option<OrderStatus>,
    #[serde(default, with = "rust_decimal::serde::float_option")]
    pub filled_quantity: Option<Decimal>,
    #[serde(default, with = "rust_decimal::serde::float_option")]
    pub remaining_quantity: Option<Decimal>,
    pub entered_time: Option<String>,
    pub close_time: Option<String>,
    #[serde(rename = "orderActivityCollection")]
    pub order_activity_collection: Option<Vec<OrderActivity>>,
}

/// Order activity from Schwab API orderActivityCollection
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct OrderActivity {
    pub activity_type: Option<String>,
    pub execution_legs: Option<Vec<ExecutionLeg>>,
}

/// Execution leg details from Schwab API
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ExecutionLeg {
    #[serde(with = "rust_decimal::serde::float")]
    pub quantity: Decimal,
    #[serde(with = "rust_decimal::serde::float")]
    pub price: Decimal,
}

impl OrderStatusResponse {
    /// Weighted average fill price from execution legs.
    ///
    /// Internal arithmetic uses `Decimal`; the result is converted to
    /// `ExactDecimal` at the boundary.
    pub(crate) fn price(&self) -> Option<ExactDecimal> {
        let activities = self.order_activity_collection.as_ref()?;

        let (total_value, total_quantity) = activities
            .iter()
            .filter_map(|activity| activity.execution_legs.as_ref())
            .flat_map(|legs| legs.iter())
            .fold(
                (Decimal::ZERO, Decimal::ZERO),
                |(acc_value, acc_qty), leg| {
                    (acc_value + leg.price * leg.quantity, acc_qty + leg.quantity)
                },
            );

        if total_quantity > Decimal::ZERO {
            let price_decimal = total_value / total_quantity;
            ExactDecimal::parse(&price_decimal.to_string()).ok()
        } else {
            None
        }
    }

    /// Check if order is completely filled
    pub(crate) const fn is_filled(&self) -> bool {
        matches!(self.status, Some(OrderStatus::Filled))
    }

    /// Check if order is still pending/working
    #[cfg(test)]
    pub(crate) const fn is_pending(&self) -> bool {
        matches!(
            self.status,
            Some(
                OrderStatus::Queued
                    | OrderStatus::Working
                    | OrderStatus::PendingActivation
                    | OrderStatus::PendingReview
                    | OrderStatus::Accepted
                    | OrderStatus::AwaitingParentOrder
                    | OrderStatus::AwaitingCondition
                    | OrderStatus::AwaitingManualReview
                    | OrderStatus::AwaitingStopCondition
                    | OrderStatus::New
                    | OrderStatus::AwaitingReleaseTime
                    | OrderStatus::PendingReplace
            )
        )
    }

    /// Check if order was canceled or rejected
    pub(crate) const fn is_terminal_failure(&self) -> bool {
        matches!(
            self.status,
            Some(OrderStatus::Canceled | OrderStatus::Rejected | OrderStatus::Expired)
        )
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal_macros::dec;

    use super::*;

    #[test]
    fn test_order_status_serialization() {
        let status = OrderStatus::Filled;
        let json = serde_json::to_string(&status).unwrap();
        assert_eq!(json, "\"FILLED\"");

        let deserialized: OrderStatus = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized, status);
    }

    #[test]
    fn test_order_status_response_deserialization() {
        // Test parsing a typical API response with orderActivityCollection
        let json_response = r#"{
            "orderId": 1004055538123,
            "status": "FILLED",
            "filledQuantity": 100.0,
            "remainingQuantity": 0.0,
            "enteredTime": "2023-10-15T10:25:00Z",
            "closeTime": "2023-10-15T10:30:00Z",
            "orderActivityCollection": [{
                "activityType": "EXECUTION",
                "executionLegs": [{
                    "executionId": "EXEC123",
                    "quantity": 100.0,
                    "price": 150.25,
                    "time": "2023-10-15T10:30:00Z"
                }]
            }]
        }"#;

        let response: OrderStatusResponse = serde_json::from_str(json_response).unwrap();

        assert_eq!(response.order_id, Some("1004055538123".to_string()));
        assert_eq!(response.status, Some(OrderStatus::Filled));
        assert_eq!(response.filled_quantity.unwrap(), dec!(100.0));
        assert_eq!(response.remaining_quantity.unwrap(), dec!(0.0));
        assert_eq!(
            response.order_activity_collection.as_ref().unwrap().len(),
            1
        );
    }

    #[test]
    fn test_calculate_weighted_average_price_single_leg() {
        let response = OrderStatusResponse {
            order_id: Some("1004055538123".to_string()),
            status: Some(OrderStatus::Filled),
            filled_quantity: Some(dec!(100.0)),
            remaining_quantity: Some(dec!(0.0)),
            entered_time: Some("2023-10-15T10:25:00Z".to_string()),
            close_time: Some("2023-10-15T10:30:00Z".to_string()),
            order_activity_collection: Some(vec![OrderActivity {
                activity_type: Some("EXECUTION".to_string()),
                execution_legs: Some(vec![ExecutionLeg {
                    quantity: dec!(100.0),
                    price: dec!(150.25),
                }]),
            }]),
        };

        assert_eq!(
            response.price(),
            Some(ExactDecimal::parse("150.25").unwrap())
        );
    }

    #[test]
    fn test_calculate_weighted_average_price_multiple_legs() {
        let response = OrderStatusResponse {
            order_id: Some("1004055538123".to_string()),
            status: Some(OrderStatus::Filled),
            filled_quantity: Some(dec!(200.0)),
            remaining_quantity: Some(dec!(0.0)),
            entered_time: Some("2023-10-15T10:25:00Z".to_string()),
            close_time: Some("2023-10-15T10:30:10Z".to_string()),
            order_activity_collection: Some(vec![OrderActivity {
                activity_type: Some("EXECUTION".to_string()),
                execution_legs: Some(vec![
                    ExecutionLeg {
                        quantity: dec!(100.0),
                        price: dec!(150.00),
                    },
                    ExecutionLeg {
                        quantity: dec!(100.0),
                        price: dec!(151.00),
                    },
                ]),
            }]),
        };

        assert_eq!(
            response.price(),
            Some(ExactDecimal::parse("150.5").unwrap())
        );
    }

    #[test]
    fn test_calculate_weighted_average_price_weighted() {
        let response = OrderStatusResponse {
            order_id: Some("1004055538123".to_string()),
            status: Some(OrderStatus::Filled),
            filled_quantity: Some(dec!(300.0)),
            remaining_quantity: Some(dec!(0.0)),
            entered_time: Some("2023-10-15T10:25:00Z".to_string()),
            close_time: Some("2023-10-15T10:30:10Z".to_string()),
            order_activity_collection: Some(vec![OrderActivity {
                activity_type: Some("EXECUTION".to_string()),
                execution_legs: Some(vec![
                    ExecutionLeg {
                        quantity: dec!(200.0),
                        price: dec!(150.00),
                    },
                    ExecutionLeg {
                        quantity: dec!(100.0),
                        price: dec!(153.00),
                    },
                ]),
            }]),
        };

        assert_eq!(response.price(), Some(ExactDecimal::parse("151").unwrap()));
    }

    #[test]
    fn test_calculate_weighted_average_price_empty_legs() {
        let response = OrderStatusResponse {
            order_id: Some("1004055538123".to_string()),
            status: Some(OrderStatus::Working),
            filled_quantity: Some(dec!(0.0)),
            remaining_quantity: Some(dec!(100.0)),
            entered_time: Some("2023-10-15T10:25:00Z".to_string()),
            close_time: None,
            order_activity_collection: Some(vec![]),
        };

        assert_eq!(response.price(), None);
    }

    #[test]
    fn test_price_preserves_sub_cent_precision() {
        let response = OrderStatusResponse {
            order_id: Some("1004055538123".to_string()),
            status: Some(OrderStatus::Filled),
            filled_quantity: Some(dec!(100.0)),
            remaining_quantity: Some(dec!(0.0)),
            entered_time: Some("2023-10-15T10:25:00Z".to_string()),
            close_time: Some("2023-10-15T10:30:00Z".to_string()),
            order_activity_collection: Some(vec![OrderActivity {
                activity_type: Some("EXECUTION".to_string()),
                execution_legs: Some(vec![ExecutionLeg {
                    quantity: dec!(100.0),
                    price: dec!(150.254),
                }]),
            }]),
        };

        assert_eq!(
            response.price(),
            Some(ExactDecimal::parse("150.254").unwrap())
        );
    }

    #[test]
    fn test_is_filled() {
        let mut response = OrderStatusResponse {
            order_id: Some("1004055538123".to_string()),
            status: Some(OrderStatus::Filled),
            filled_quantity: Some(dec!(100.0)),
            remaining_quantity: Some(dec!(0.0)),
            entered_time: Some("2023-10-15T10:25:00Z".to_string()),
            close_time: Some("2023-10-15T10:30:00Z".to_string()),
            order_activity_collection: Some(vec![]),
        };

        assert!(response.is_filled());

        response.status = Some(OrderStatus::Working);
        assert!(!response.is_filled());
    }

    #[test]
    fn test_is_pending() {
        let pending_states = [
            OrderStatus::Queued,
            OrderStatus::Working,
            OrderStatus::PendingActivation,
            OrderStatus::PendingReview,
            OrderStatus::Accepted,
            OrderStatus::AwaitingParentOrder,
            OrderStatus::AwaitingCondition,
            OrderStatus::AwaitingManualReview,
            OrderStatus::AwaitingStopCondition,
            OrderStatus::New,
            OrderStatus::AwaitingReleaseTime,
            OrderStatus::PendingReplace,
        ];

        for status in pending_states {
            let response = OrderStatusResponse {
                order_id: Some("1004055538123".to_string()),
                status: Some(status),
                filled_quantity: Some(dec!(0.0)),
                remaining_quantity: Some(dec!(100.0)),
                entered_time: Some("2023-10-15T10:25:00Z".to_string()),
                close_time: None,
                order_activity_collection: Some(vec![]),
            };
            assert!(response.is_pending(), "Status {status:?} should be pending");
        }

        let non_pending_states = [
            OrderStatus::Filled,
            OrderStatus::Canceled,
            OrderStatus::Rejected,
            OrderStatus::Expired,
            OrderStatus::Replaced,
        ];

        for status in non_pending_states {
            let response = OrderStatusResponse {
                order_id: Some("1004055538123".to_string()),
                status: Some(status),
                filled_quantity: Some(dec!(100.0)),
                remaining_quantity: Some(dec!(0.0)),
                entered_time: Some("2023-10-15T10:25:00Z".to_string()),
                close_time: Some("2023-10-15T10:30:00Z".to_string()),
                order_activity_collection: Some(vec![]),
            };
            assert!(
                !response.is_pending(),
                "Status {status:?} should not be pending"
            );
        }
    }

    #[test]
    fn test_is_terminal_failure() {
        let failure_states = [
            OrderStatus::Canceled,
            OrderStatus::Rejected,
            OrderStatus::Expired,
        ];

        for status in failure_states {
            let response = OrderStatusResponse {
                order_id: Some("1004055538123".to_string()),
                status: Some(status),
                filled_quantity: Some(dec!(0.0)),
                remaining_quantity: Some(dec!(100.0)),
                entered_time: Some("2023-10-15T10:25:00Z".to_string()),
                close_time: Some("2023-10-15T10:30:00Z".to_string()),
                order_activity_collection: Some(vec![]),
            };
            assert!(
                response.is_terminal_failure(),
                "Status {status:?} should be terminal failure"
            );
        }

        let non_failure_states = [
            OrderStatus::Filled,
            OrderStatus::Working,
            OrderStatus::Queued,
            OrderStatus::New,
        ];

        for status in non_failure_states {
            let response = OrderStatusResponse {
                order_id: Some("1004055538123".to_string()),
                status: Some(status),
                filled_quantity: Some(dec!(0.0)),
                remaining_quantity: Some(dec!(100.0)),
                entered_time: Some("2023-10-15T10:25:00Z".to_string()),
                close_time: None,
                order_activity_collection: Some(vec![]),
            };
            assert!(
                !response.is_terminal_failure(),
                "Status {status:?} should not be terminal failure"
            );
        }
    }

    #[test]
    fn test_complex_api_response_parsing() {
        let json_response = r#"
        {
            "orderId": 1004055538999,
            "status": "FILLED",
            "filledQuantity": 200.0,
            "remainingQuantity": 0.0,
            "enteredTime": "2023-10-15T10:25:00Z",
            "closeTime": "2023-10-15T10:30:05Z",
            "orderActivityCollection": [{
                "activityType": "EXECUTION",
                "executionLegs": [
                    {
                        "executionId": "EXEC001",
                        "quantity": 150.0,
                        "price": 100.25,
                        "time": "2023-10-15T10:30:00Z"
                    },
                    {
                        "executionId": "EXEC002",
                        "quantity": 50.0,
                        "price": 100.75,
                        "time": "2023-10-15T10:30:05Z"
                    }
                ]
            }]
        }
        "#;

        let response: OrderStatusResponse = serde_json::from_str(json_response).unwrap();

        assert_eq!(response.order_id, Some("1004055538999".to_string()));
        assert_eq!(response.status, Some(OrderStatus::Filled));
        assert_eq!(response.filled_quantity.unwrap(), dec!(200.0));
        assert_eq!(response.remaining_quantity.unwrap(), dec!(0.0));
        assert_eq!(
            response.order_activity_collection.as_ref().unwrap().len(),
            1
        );

        // Test weighted average: (150 * 100.25 + 50 * 100.75) / 200 = (15037.5 + 5037.5) / 200 = 100.375
        assert_eq!(
            response.price(),
            Some(ExactDecimal::parse("100.375").unwrap())
        );
    }

    #[test]
    fn test_edge_case_zero_quantity_legs() {
        let response = OrderStatusResponse {
            order_id: Some("1004055538123".to_string()),
            status: Some(OrderStatus::Working),
            filled_quantity: Some(dec!(0.0)),
            remaining_quantity: Some(dec!(100.0)),
            entered_time: Some("2023-10-15T10:25:00Z".to_string()),
            close_time: None,
            order_activity_collection: Some(vec![OrderActivity {
                activity_type: Some("EXECUTION".to_string()),
                execution_legs: Some(vec![ExecutionLeg {
                    quantity: dec!(0.0),
                    price: dec!(150.25),
                }]),
            }]),
        };

        assert_eq!(response.price(), None);
    }

    #[test]
    fn test_actual_schwab_api_response_filled_order() {
        // This is the actual response format returned by Schwab API for a filled GME order
        let actual_response = r#"{
            "session":"NORMAL",
            "duration":"DAY",
            "orderType":"MARKET",
            "complexOrderStrategyType":"NONE",
            "quantity":1.0,
            "filledQuantity":1.0,
            "remainingQuantity":0.0,
            "requestedDestination":"AUTO",
            "destinationLinkName":"HRTF",
            "orderLegCollection":[{
                "orderLegType":"EQUITY",
                "legId":1,
                "instrument":{
                    "assetType":"EQUITY",
                    "cusip":"36467W109",
                    "symbol":"GME",
                    "instrumentId":4430271
                },
                "instruction":"BUY",
                "positionEffect":"OPENING",
                "quantity":1.0
            }],
            "orderStrategyType":"SINGLE",
            "orderId":1004055538153,
            "cancelable":false,
            "editable":false,
            "status":"FILLED",
            "enteredTime":"2025-08-29T17:15:17+0000",
            "closeTime":"2025-08-29T17:15:18+0000",
            "tag":"TA_nickmagliocchetticom1751890824",
            "accountNumber":49359741,
            "orderActivityCollection":[{
                "activityType":"EXECUTION",
                "activityId":102102029816,
                "executionType":"FILL",
                "quantity":1.0,
                "orderRemainingQuantity":0.0,
                "executionLegs":[{
                    "legId":1,
                    "quantity":1.0,
                    "mismarkedQuantity":0.0,
                    "price":22.7299,
                    "time":"2025-08-29T17:15:18+0000",
                    "instrumentId":4430271
                }]
            }]
        }"#;

        // This should parse successfully now
        let parsed: OrderStatusResponse =
            serde_json::from_str(actual_response).expect("Should parse actual Schwab API response");

        // Verify the parsed values
        assert_eq!(parsed.order_id, Some("1004055538153".to_string()));
        assert_eq!(parsed.status, Some(OrderStatus::Filled));
        assert_eq!(parsed.filled_quantity.unwrap(), dec!(1.0));
        assert_eq!(parsed.remaining_quantity.unwrap(), dec!(0.0));
        assert_eq!(
            parsed.entered_time,
            Some("2025-08-29T17:15:17+0000".to_string())
        );
        assert_eq!(
            parsed.close_time,
            Some("2025-08-29T17:15:18+0000".to_string())
        );

        // Verify price extraction from orderActivityCollection
        assert_eq!(
            parsed.price(),
            Some(ExactDecimal::parse("22.7299").unwrap())
        );
    }

    #[test]
    fn test_order_id_as_number() {
        // Test that we can handle orderId as a number (actual Schwab format)
        let response_json = r#"{
            "orderId": 1004055538153,
            "status": "FILLED",
            "filledQuantity": 1.0,
            "remainingQuantity": 0.0
        }"#;

        let parsed: OrderStatusResponse =
            serde_json::from_str(response_json).expect("Should parse orderId as number");

        assert_eq!(parsed.order_id, Some("1004055538153".to_string()));
        assert_eq!(parsed.status, Some(OrderStatus::Filled));
    }

    #[test]
    fn test_order_id_as_string_should_fail() {
        // Test that we reject orderId as string (not the actual API format)
        let response_json = r#"{
            "orderId": "ORDER123",
            "status": "WORKING",
            "filledQuantity": 0.0,
            "remainingQuantity": 100.0
        }"#;

        let result = serde_json::from_str::<OrderStatusResponse>(response_json);
        assert!(result.is_err(), "Should reject string orderId format");
    }

    #[test]
    fn test_order_id_missing() {
        // Test that we can handle missing orderId field
        let response_json = r#"{
            "status": "QUEUED",
            "filledQuantity": 0.0,
            "remainingQuantity": 100.0
        }"#;

        let parsed: OrderStatusResponse =
            serde_json::from_str(response_json).expect("Should parse response without orderId");

        assert_eq!(parsed.order_id, None);
        assert_eq!(parsed.status, Some(OrderStatus::Queued));
    }

    #[test]
    fn test_missing_optional_fields() {
        let minimal_response = r#"{
            "status": "QUEUED"
        }"#;

        let parsed: OrderStatusResponse =
            serde_json::from_str(minimal_response).expect("Should parse minimal response");

        assert_eq!(parsed.order_id, None);
        assert_eq!(parsed.status, Some(OrderStatus::Queued));
        assert_eq!(parsed.filled_quantity, None);
        assert_eq!(parsed.remaining_quantity, None);
        assert_eq!(parsed.entered_time, None);
        assert_eq!(parsed.close_time, None);
        assert_eq!(parsed.order_activity_collection, None);
    }

    #[test]
    fn test_missing_status_field_none() {
        let no_status_response = r#"{
            "orderId": 1004055538123,
            "filledQuantity": 0.0,
            "remainingQuantity": 100.0
        }"#;

        let parsed: OrderStatusResponse =
            serde_json::from_str(no_status_response).expect("Should parse response without status");

        assert_eq!(parsed.order_id, Some("1004055538123".to_string()));
        assert_eq!(parsed.status, None);
        assert_eq!(parsed.filled_quantity, Some(dec!(0.0)));
        assert_eq!(parsed.remaining_quantity, Some(dec!(100.0)));
    }

    #[test]
    fn test_price_calculation_from_order_activity_collection() {
        let response_json = r#"{
            "status": "FILLED",
            "filledQuantity": 2.0,
            "remainingQuantity": 0.0,
            "orderActivityCollection": [{
                "activityType": "EXECUTION",
                "executionLegs": [{
                    "quantity": 1.0,
                    "price": 100.50
                }, {
                    "quantity": 1.0,
                    "price": 101.50
                }]
            }]
        }"#;

        let parsed: OrderStatusResponse = serde_json::from_str(response_json)
            .expect("Should parse response with orderActivityCollection");

        assert_eq!(parsed.status, Some(OrderStatus::Filled));
        assert_eq!(parsed.filled_quantity.unwrap(), dec!(2.0));
        assert_eq!(parsed.remaining_quantity.unwrap(), dec!(0.0));

        assert_eq!(parsed.price(), Some(ExactDecimal::parse("101").unwrap()));
    }
}
