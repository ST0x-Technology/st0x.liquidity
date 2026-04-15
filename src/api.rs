//! HTTP API endpoints for health checks.

use chrono::{DateTime, Utc};
use rocket::serde::json::Json;
use rocket::serde::{Deserialize, Serialize};
use rocket::{Route, get, routes};

#[derive(Serialize, Deserialize)]
struct HealthResponse {
    status: String,
    timestamp: DateTime<Utc>,
}

#[get("/health")]
fn health() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "healthy".to_string(),
        timestamp: Utc::now(),
    })
}

pub(crate) fn routes() -> Vec<Route> {
    routes![health]
}

#[cfg(test)]
mod tests {
    use rocket::http::Status;
    use rocket::local::asynchronous::Client;

    use super::*;

    #[test]
    fn test_num_of_routes() {
        let routes_list = routes();
        assert_eq!(routes_list.len(), 1);
    }

    #[tokio::test]
    async fn test_health_endpoint() {
        let rocket = rocket::build().mount("/", routes![health]);
        let client = Client::tracked(rocket)
            .await
            .expect("valid rocket instance");

        let response = client.get("/health").dispatch().await;
        assert_eq!(response.status(), Status::Ok);

        let body = response.into_string().await.expect("response body");
        let health_response: HealthResponse =
            serde_json::from_str(&body).expect("valid JSON response");

        assert_eq!(health_response.status, "healthy");
        assert!(health_response.timestamp <= chrono::Utc::now());
    }
}
