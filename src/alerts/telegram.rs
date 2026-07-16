//! Telegram-backed [`Notifier`] implementation.
//!
//! Delivers alert messages by POSTing to the Telegram Bot API `sendMessage`
//! endpoint. The notifier is held behind the [`Notifier`] trait so the gas
//! monitor can be exercised against a capturing mock in tests.

use async_trait::async_trait;
use serde_json::json;

use super::{Notifier, NotifierError};

/// Telegram Bot API client that posts alerts to a fixed chat.
pub(crate) struct TelegramNotifier {
    http_client: reqwest::Client,
    /// Pre-built `sendMessage` URL embedding the bot token.
    send_message_url: String,
    chat_id: i64,
    /// Forum topic to post into, or `None` for the chat's default topic.
    message_thread_id: Option<i64>,
}

impl std::fmt::Debug for TelegramNotifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The send-message URL embeds the bot token, so it is redacted.
        f.debug_struct("TelegramNotifier")
            .field("send_message_url", &"[REDACTED]")
            .field("chat_id", &self.chat_id)
            .field("message_thread_id", &self.message_thread_id)
            .finish_non_exhaustive()
    }
}

impl TelegramNotifier {
    pub(crate) fn new(
        bot_token: &str,
        chat_id: i64,
        message_thread_id: Option<i64>,
    ) -> Result<Self, NotifierError> {
        Self::with_base_url(
            "https://api.telegram.org",
            bot_token,
            chat_id,
            message_thread_id,
        )
    }

    /// Construction with an overridable API base URL. Production callers use
    /// [`TelegramNotifier::new`]; tests point `base_url` at an `httpmock`
    /// server so no real Telegram request is ever made.
    fn with_base_url(
        base_url: &str,
        bot_token: &str,
        chat_id: i64,
        message_thread_id: Option<i64>,
    ) -> Result<Self, NotifierError> {
        let http_client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .build()
            .map_err(NotifierError::ClientBuild)?;

        Ok(Self {
            http_client,
            send_message_url: format!("{base_url}/bot{bot_token}/sendMessage"),
            chat_id,
            message_thread_id,
        })
    }
}

#[async_trait]
impl Notifier for TelegramNotifier {
    async fn notify(&self, message: &str) -> Result<(), NotifierError> {
        let mut body = json!({
            "chat_id": self.chat_id,
            "text": message,
        });

        // Only forum (topic-enabled) supergroups accept message_thread_id;
        // omit it entirely otherwise so the field never reaches a plain chat.
        if let Some(thread_id) = self.message_thread_id {
            body["message_thread_id"] = json!(thread_id);
        }

        let response = self
            .http_client
            .post(&self.send_message_url)
            .json(&body)
            .send()
            .await
            .map_err(NotifierError::Request)?;

        let status = response.status();
        if status.is_success() {
            return Ok(());
        }

        let body = response.text().await.map_err(NotifierError::Request)?;
        Err(NotifierError::ApiError { status, body })
    }
}

#[cfg(test)]
mod tests {
    use httpmock::Method::POST;
    use httpmock::MockServer;
    use reqwest::StatusCode;
    use serde_json::{Value, json};

    use super::*;

    #[tokio::test]
    async fn notify_posts_chat_id_and_text() {
        let server = MockServer::start_async().await;

        let mock = server
            .mock_async(|when, then| {
                when.method(POST)
                    .path("/bot123:abc/sendMessage")
                    .json_body(json!({
                        "chat_id": -1_001_234_567_890_i64,
                        "text": "balance low",
                    }));
                then.status(200)
                    .header("content-type", "application/json")
                    .json_body(json!({ "ok": true }));
            })
            .await;

        let notifier = TelegramNotifier::with_base_url(
            &server.base_url(),
            "123:abc",
            -1_001_234_567_890,
            None,
        )
        .unwrap();

        notifier.notify("balance low").await.unwrap();

        mock.assert_async().await;
    }

    #[tokio::test]
    async fn notify_includes_message_thread_id_for_forum_topic() {
        let server = MockServer::start_async().await;

        let mock = server
            .mock_async(|when, then| {
                when.method(POST)
                    .path("/bot123:abc/sendMessage")
                    .json_body(json!({
                        "chat_id": -1_001_234_567_890_i64,
                        "text": "balance low",
                        "message_thread_id": 42_i64,
                    }));
                then.status(200)
                    .header("content-type", "application/json")
                    .json_body(json!({ "ok": true }));
            })
            .await;

        let notifier = TelegramNotifier::with_base_url(
            &server.base_url(),
            "123:abc",
            -1_001_234_567_890,
            Some(42),
        )
        .unwrap();

        notifier.notify("balance low").await.unwrap();

        mock.assert_async().await;
    }

    #[tokio::test]
    async fn notify_surfaces_api_error_status() {
        let server = MockServer::start_async().await;

        let mock = server
            .mock_async(|when, then| {
                when.method(POST).path("/bot123:abc/sendMessage");
                then.status(400)
                    .header("content-type", "application/json")
                    .json_body(json!({
                        "ok": false,
                        "error_code": 400,
                        "description": "Bad Request: chat not found",
                    }));
            })
            .await;

        let notifier =
            TelegramNotifier::with_base_url(&server.base_url(), "123:abc", 42, None).unwrap();

        let error = notifier.notify("hello").await.unwrap_err();

        let NotifierError::ApiError { status, body } = error else {
            panic!("expected ApiError, got: {error}");
        };

        assert_eq!(status, StatusCode::BAD_REQUEST);
        let parsed: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(parsed["description"], json!("Bad Request: chat not found"));

        mock.assert_async().await;
    }
}
