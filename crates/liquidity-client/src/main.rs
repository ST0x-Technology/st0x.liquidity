//! Binary entrypoint: resolves the target environment, builds the auth-backed
//! transport, dispatches the parsed command, and maps failures to exit codes.

mod auth;
mod cli;
mod output;
mod target;
mod transport;
mod wire;

use clap::Parser;
use std::process::ExitCode;

use crate::auth::{AuthError, StaticToken, TokenSource};
use crate::cli::{Cli, Command, Debug, PortfolioSnapshot, Position, Read};
use crate::output::OutputError;
use crate::target::Auth;
use crate::transport::{Client, TransportError, encode_segment};

#[tokio::main]
async fn main() -> ExitCode {
    match execute(Cli::parse()).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(Failure::Setup(error)) => {
            eprintln!("error: {error:#}");
            ExitCode::from(2)
        }
        Err(Failure::Api { error, logging_url }) => {
            eprintln!("error: {error}");
            if let Some(url) = logging_url {
                eprintln!("\nT0 Cloud Logging: {url}");
            }
            ExitCode::from(error.exit_code())
        }
    }
}

enum Failure {
    Setup(anyhow::Error),
    Api {
        error: ApiError,
        logging_url: Option<String>,
    },
}

/// Aggregates the feature errors at the CLI boundary for display and exit
/// codes. Auth and access-denied failures exit 77; everything else exits 1.
#[derive(Debug)]
enum ApiError {
    Transport(TransportError),
    Output(OutputError),
    Auth(AuthError),
}

impl ApiError {
    fn exit_code(&self) -> u8 {
        match self {
            Self::Auth(_)
            | Self::Transport(
                TransportError::Unauthorized(_)
                | TransportError::Forbidden(_)
                | TransportError::Auth(_),
            ) => 77,
            _ => 1,
        }
    }
}

impl From<TransportError> for ApiError {
    fn from(error: TransportError) -> Self {
        Self::Transport(error)
    }
}

impl From<OutputError> for ApiError {
    fn from(error: OutputError) -> Self {
        Self::Output(error)
    }
}

impl From<AuthError> for ApiError {
    fn from(error: AuthError) -> Self {
        Self::Auth(error)
    }
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Transport(error) => write!(formatter, "{error}"),
            Self::Output(error) => write!(formatter, "{error}"),
            Self::Auth(error) => write!(formatter, "{error}"),
        }
    }
}

impl std::error::Error for ApiError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Transport(error) => Some(error),
            Self::Output(error) => Some(error),
            Self::Auth(error) => Some(error),
        }
    }
}

async fn execute(cli: Cli) -> Result<(), Failure> {
    let target = target::resolve(cli.env).map_err(Failure::Setup)?;
    let logging_url = target.logging_url;
    let Auth::OauthDesktop {
        client_id,
        client_secret,
    } = target.auth;
    let token = auth::desktop_id_token(
        cli.env.cache_slug(),
        &client_id,
        &client_secret,
        target.request_timeout,
        target.connect_timeout,
    )
    .await
    .map_err(|error| Failure::Api {
        error: error.into(),
        logging_url: logging_url.clone(),
    })?;
    let client = Client::new(
        target.base_url,
        StaticToken(token.clone()),
        StaticToken(token),
        target.request_timeout,
        target.connect_timeout,
    )
    .map_err(Failure::Setup)?;
    dispatch(&client, cli.command)
        .await
        .map_err(|error| Failure::Api { error, logging_url })
}

async fn dispatch<A: TokenSource + Sync>(
    client: &Client<A>,
    command: Command,
) -> Result<(), ApiError> {
    let value = match command {
        Command::Read(Read::Resource(args)) => {
            client.get(args.resource.path(), &args.params).await?
        }
        Command::Read(Read::TradeEvents(args)) => {
            let path = format!(
                "/trades/{}/{}/events",
                encode_segment(&args.venue),
                encode_segment(&args.aggregate_id)
            );
            client.get(&path, &args.params).await?
        }
        Command::Read(Read::TransferEvents(args)) => {
            let path = format!(
                "/transfers/{}/{}/events",
                encode_segment(&args.kind),
                encode_segment(&args.aggregate_id)
            );
            client.get(&path, &args.params).await?
        }
        Command::Debug(Debug::Resume) => client.post("/transfers/resume").await?,
        Command::Debug(Debug::Recheck { kind, id }) => {
            let kind = encode_segment(&kind);
            let id = encode_segment(&id);
            client
                .post(&format!("/transfers/recheck/{kind}/{id}"))
                .await?
        }
        Command::Debug(Debug::ResumeUsdc { direction, id }) => {
            let direction = direction.segment();
            let id = encode_segment(&id);
            client
                .post(&format!("/transfers/usdc/resume/{direction}/{id}"))
                .await?
        }
        Command::Debug(Debug::ReconcileUsdc { id, reason }) => {
            let id = encode_segment(&id);
            client
                .post_json(
                    &format!("/transfers/usdc/{id}/reconcile"),
                    &wire::ReconcileUsdcRequest { reason },
                )
                .await?
        }
        Command::Debug(Debug::ReconcileEquity { kind, id, reason }) => {
            let kind = kind.segment();
            let id = encode_segment(&id);
            client
                .post_json(
                    &format!("/transfers/{kind}/{id}/reconcile"),
                    &wire::ReconcileEquityRequest { reason },
                )
                .await?
        }
        Command::Debug(Debug::ClearPendingBurn { id, reason }) => {
            let id = encode_segment(&id);
            client
                .post_json(
                    &format!("/transfers/usdc/{id}/clear-pending-burn"),
                    &wire::ClearPendingBurnRequest { reason },
                )
                .await?
        }
        Command::Debug(Debug::FailUsdcTransfer { id, reason }) => {
            let id = encode_segment(&id);
            client
                .post_json(
                    &format!("/transfers/usdc/{id}/fail"),
                    &wire::FailUsdcTransferRequest { reason },
                )
                .await?
        }
        Command::Debug(Debug::Position(Position::Set(args))) => {
            let symbol = encode_segment(&args.symbol);
            client
                .post_json(
                    &format!("/positions/{symbol}/set"),
                    &wire::SetPositionRequest {
                        target_net: args.target_net,
                        price_usdc: args.price_usdc,
                        reason: args.reason,
                    },
                )
                .await?
        }
        Command::Debug(Debug::Position(Position::ReleaseHedge(args))) => {
            let symbol = encode_segment(&args.symbol);
            client
                .post_json(
                    &format!("/positions/{symbol}/release-hedge"),
                    &wire::ReleaseHedgeRequest {
                        order_id: args.order_id,
                        reason: args.reason,
                    },
                )
                .await?
        }
        Command::Debug(Debug::PortfolioSnapshot(PortfolioSnapshot::SetMark(args))) => {
            client
                .post_json(
                    "/portfolio-snapshot/marks",
                    &wire::SetEquityMarkRequest {
                        day: args.day,
                        symbol: args.symbol,
                        usd_mark: args.usd_mark,
                        observed_at: args.observed_at,
                        source: args.source,
                        reason: args.reason,
                    },
                )
                .await?
        }
        Command::Debug(Debug::ProcessTx { tx_hash }) => {
            let tx_hash = encode_segment(&tx_hash);
            client
                .post(&format!("/transactions/{tx_hash}/process"))
                .await?
        }
    };
    output::print(&value).map_err(ApiError::from)
}

#[cfg(test)]
mod tests {
    //! Tests for command dispatch and CLI-boundary error classification.
    use std::io::{Read as _, Write as _};
    use std::net::TcpListener;
    use std::sync::mpsc::{Receiver, channel};
    use std::time::Duration;

    use super::{ApiError, dispatch};
    use crate::auth::{AuthError, StaticToken};
    use crate::cli::{
        Command, Debug, EquityTransferKind, PortfolioSnapshot, Position, Read, ReadResource,
        ReleaseHedgeArgs, ResourceArgs, SetMarkArgs, SetPositionArgs, TradeEventsArgs,
        TransferEventsArgs, UsdcDirection,
    };
    use crate::output::OutputError;
    use crate::transport::{Client, TransportError};
    use crate::wire::ReconcileUsdcReason;

    /// Accepts one connection, captures the raw request bytes, and replies with
    /// an empty JSON object.
    fn capture_server() -> std::io::Result<(u16, Receiver<String>)> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let port = listener.local_addr()?.port();
        let (sender, receiver) = channel();
        std::thread::spawn(move || {
            if let Ok((mut stream, _)) = listener.accept() {
                let mut request = Vec::new();
                let mut buffer = [0u8; 4096];
                while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                    match stream.read(&mut buffer) {
                        Ok(0) | Err(_) => break,
                        Ok(count) => request.extend_from_slice(&buffer[..count]),
                    }
                }
                let expected = header_end(&request) + content_length(&request);
                while request.len() < expected {
                    match stream.read(&mut buffer) {
                        Ok(0) | Err(_) => break,
                        Ok(count) => request.extend_from_slice(&buffer[..count]),
                    }
                }
                let _ = sender.send(String::from_utf8_lossy(&request).into_owned());
                let response = "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}";
                let _ = stream.write_all(response.as_bytes());
            }
        });
        Ok((port, receiver))
    }

    fn build_client(port: u16) -> Result<Client<StaticToken>, Box<dyn std::error::Error>> {
        let base = url::Url::parse(&format!("http://127.0.0.1:{port}/"))?;
        Ok(Client::new(
            base,
            StaticToken("read".to_owned()),
            StaticToken("write".to_owned()),
            Duration::from_secs(5),
            Duration::from_secs(5),
        )?)
    }

    /// The HTTP request line (method, target, version) of a captured request.
    fn request_line(request: &str) -> &str {
        request.lines().next().unwrap_or_default()
    }

    /// Byte offset just past the header terminator, or the buffer length when
    /// the headers never completed.
    fn header_end(request: &[u8]) -> usize {
        request
            .windows(4)
            .position(|window| window == b"\r\n\r\n")
            .map_or(request.len(), |at| at + 4)
    }

    /// Declared body length, zero when absent (GETs and bodiless POSTs).
    fn content_length(request: &[u8]) -> usize {
        String::from_utf8_lossy(request)
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse().ok())
                    .flatten()
            })
            .unwrap_or(0)
    }

    /// The JSON body of a captured request, parsed.
    fn request_body(request: &str) -> serde_json::Value {
        let (_, body) = request.split_once("\r\n\r\n").unwrap_or_default();
        serde_json::from_str(body).expect("request body must be JSON")
    }

    /// Dispatches one command against the capture server and returns the raw
    /// request text so a test can assert its method and route.
    async fn request_for(command: Command) -> Result<String, Box<dyn std::error::Error>> {
        let (port, requests) = capture_server()?;
        let client = build_client(port)?;
        dispatch(&client, command).await?;
        Ok(requests.recv()?)
    }

    #[tokio::test]
    async fn resource_read_gets_the_read_path() -> Result<(), Box<dyn std::error::Error>> {
        let request = request_for(Command::Read(Read::Resource(ResourceArgs {
            resource: ReadResource::Health,
            params: vec![],
        })))
        .await?;
        assert_eq!(
            request_line(&request),
            "GET /liquidity-read/health HTTP/1.1"
        );
        Ok(())
    }

    #[tokio::test]
    async fn trade_events_get_the_trade_path() -> Result<(), Box<dyn std::error::Error>> {
        let request = request_for(Command::Read(Read::TradeEvents(TradeEventsArgs {
            venue: "raindex".to_owned(),
            aggregate_id: "abc".to_owned(),
            params: vec![],
        })))
        .await?;
        assert_eq!(
            request_line(&request),
            "GET /liquidity-read/trades/raindex/abc/events HTTP/1.1"
        );
        Ok(())
    }

    #[tokio::test]
    async fn transfer_events_get_the_transfer_path() -> Result<(), Box<dyn std::error::Error>> {
        let request = request_for(Command::Read(Read::TransferEvents(TransferEventsArgs {
            kind: "mint".to_owned(),
            aggregate_id: "abc".to_owned(),
            params: vec![],
        })))
        .await?;
        assert_eq!(
            request_line(&request),
            "GET /liquidity-read/transfers/mint/abc/events HTTP/1.1"
        );
        Ok(())
    }

    #[tokio::test]
    async fn resume_posts_the_write_path() -> Result<(), Box<dyn std::error::Error>> {
        let request = request_for(Command::Debug(Debug::Resume)).await?;
        assert_eq!(
            request_line(&request),
            "POST /liquidity-write/transfers/resume HTTP/1.1"
        );
        Ok(())
    }

    #[tokio::test]
    async fn recheck_posts_the_write_path() -> Result<(), Box<dyn std::error::Error>> {
        let request = request_for(Command::Debug(Debug::Recheck {
            kind: "mint".to_owned(),
            id: "abc".to_owned(),
        }))
        .await?;
        assert_eq!(
            request_line(&request),
            "POST /liquidity-write/transfers/recheck/mint/abc HTTP/1.1"
        );
        Ok(())
    }

    #[tokio::test]
    async fn resume_usdc_posts_the_direction_segment() -> Result<(), Box<dyn std::error::Error>> {
        let request = request_for(Command::Debug(Debug::ResumeUsdc {
            direction: UsdcDirection::BaseToAlpaca,
            id: "r/1".to_owned(),
        }))
        .await?;
        assert_eq!(
            request_line(&request),
            "POST /liquidity-write/transfers/usdc/resume/base_to_alpaca/r%2F1 HTTP/1.1"
        );
        Ok(())
    }

    #[tokio::test]
    async fn reconcile_usdc_posts_the_kebab_case_reason() -> Result<(), Box<dyn std::error::Error>>
    {
        let request = request_for(Command::Debug(Debug::ReconcileUsdc {
            id: "abc".to_owned(),
            reason: ReconcileUsdcReason::DepositCreditedOffline,
        }))
        .await?;
        assert_eq!(
            request_line(&request),
            "POST /liquidity-write/transfers/usdc/abc/reconcile HTTP/1.1"
        );
        assert_eq!(
            request_body(&request),
            serde_json::json!({ "reason": "deposit-credited-offline" })
        );
        Ok(())
    }

    #[tokio::test]
    async fn reconcile_equity_posts_the_kind_segment_and_reason()
    -> Result<(), Box<dyn std::error::Error>> {
        let request = request_for(Command::Debug(Debug::ReconcileEquity {
            kind: EquityTransferKind::Redemption,
            id: "abc".to_owned(),
            reason: "settled by hand".to_owned(),
        }))
        .await?;
        assert_eq!(
            request_line(&request),
            "POST /liquidity-write/transfers/equity_redemption/abc/reconcile HTTP/1.1"
        );
        assert_eq!(
            request_body(&request),
            serde_json::json!({ "reason": "settled by hand" })
        );
        Ok(())
    }

    #[tokio::test]
    async fn clear_pending_burn_posts_the_reason() -> Result<(), Box<dyn std::error::Error>> {
        let request = request_for(Command::Debug(Debug::ClearPendingBurn {
            id: "abc".to_owned(),
            reason: "burn never landed".to_owned(),
        }))
        .await?;
        assert_eq!(
            request_line(&request),
            "POST /liquidity-write/transfers/usdc/abc/clear-pending-burn HTTP/1.1"
        );
        assert_eq!(
            request_body(&request),
            serde_json::json!({ "reason": "burn never landed" })
        );
        Ok(())
    }

    #[tokio::test]
    async fn fail_usdc_transfer_posts_the_reason() -> Result<(), Box<dyn std::error::Error>> {
        let request = request_for(Command::Debug(Debug::FailUsdcTransfer {
            id: "abc".to_owned(),
            reason: "pre-burn crash, burn not attempted".to_owned(),
        }))
        .await?;
        assert_eq!(
            request_line(&request),
            "POST /liquidity-write/transfers/usdc/abc/fail HTTP/1.1"
        );
        assert_eq!(
            request_body(&request),
            serde_json::json!({ "reason": "pre-burn crash, burn not attempted" })
        );
        Ok(())
    }

    #[tokio::test]
    async fn position_set_omits_an_absent_price() -> Result<(), Box<dyn std::error::Error>> {
        let request = request_for(Command::Debug(Debug::Position(Position::Set(
            SetPositionArgs {
                symbol: "AAPL".to_owned(),
                target_net: "-1.5".to_owned(),
                price_usdc: None,
                reason: "manual correction".to_owned(),
            },
        ))))
        .await?;
        assert_eq!(
            request_line(&request),
            "POST /liquidity-write/positions/AAPL/set HTTP/1.1"
        );
        assert_eq!(
            request_body(&request),
            serde_json::json!({ "target_net": "-1.5", "reason": "manual correction" })
        );
        Ok(())
    }

    #[tokio::test]
    async fn position_set_sends_the_price_when_given() -> Result<(), Box<dyn std::error::Error>> {
        let request = request_for(Command::Debug(Debug::Position(Position::Set(
            SetPositionArgs {
                symbol: "AAPL".to_owned(),
                target_net: "2".to_owned(),
                price_usdc: Some("150.25".to_owned()),
                reason: "manual correction".to_owned(),
            },
        ))))
        .await?;
        assert_eq!(
            request_body(&request),
            serde_json::json!({
                "target_net": "2",
                "price_usdc": "150.25",
                "reason": "manual correction"
            })
        );
        Ok(())
    }

    #[tokio::test]
    async fn release_hedge_posts_the_order_id() -> Result<(), Box<dyn std::error::Error>> {
        let request = request_for(Command::Debug(Debug::Position(Position::ReleaseHedge(
            ReleaseHedgeArgs {
                symbol: "AAPL".to_owned(),
                order_id: "ord-1".to_owned(),
                reason: "broker cancelled".to_owned(),
            },
        ))))
        .await?;
        assert_eq!(
            request_line(&request),
            "POST /liquidity-write/positions/AAPL/release-hedge HTTP/1.1"
        );
        assert_eq!(
            request_body(&request),
            serde_json::json!({ "order_id": "ord-1", "reason": "broker cancelled" })
        );
        Ok(())
    }

    #[tokio::test]
    async fn set_mark_posts_every_field() -> Result<(), Box<dyn std::error::Error>> {
        let request = request_for(Command::Debug(Debug::PortfolioSnapshot(
            PortfolioSnapshot::SetMark(SetMarkArgs {
                day: "2026-09-01".to_owned(),
                symbol: "AAPL".to_owned(),
                usd_mark: "150.25".to_owned(),
                observed_at: "2026-09-01T20:00:00Z".to_owned(),
                source: "nasdaq".to_owned(),
                reason: "stale mark".to_owned(),
            }),
        )))
        .await?;
        assert_eq!(
            request_line(&request),
            "POST /liquidity-write/portfolio-snapshot/marks HTTP/1.1"
        );
        assert_eq!(
            request_body(&request),
            serde_json::json!({
                "day": "2026-09-01",
                "symbol": "AAPL",
                "usd_mark": "150.25",
                "observed_at": "2026-09-01T20:00:00Z",
                "source": "nasdaq",
                "reason": "stale mark"
            })
        );
        Ok(())
    }

    #[tokio::test]
    async fn process_tx_posts_the_hash_segment() -> Result<(), Box<dyn std::error::Error>> {
        let request = request_for(Command::Debug(Debug::ProcessTx {
            tx_hash: "0xabc".to_owned(),
        }))
        .await?;
        assert_eq!(
            request_line(&request),
            "POST /liquidity-write/transactions/0xabc/process HTTP/1.1"
        );
        Ok(())
    }

    #[test]
    fn exit_code_is_77_for_auth_and_access_denied() {
        assert_eq!(
            ApiError::Auth(AuthError::Flow("x".to_owned())).exit_code(),
            77
        );
        assert_eq!(
            ApiError::Transport(TransportError::Unauthorized("x".to_owned())).exit_code(),
            77
        );
        assert_eq!(
            ApiError::Transport(TransportError::Forbidden("x".to_owned())).exit_code(),
            77
        );
        assert_eq!(
            ApiError::Transport(TransportError::Auth(AuthError::Flow("x".to_owned()))).exit_code(),
            77
        );
    }

    #[test]
    fn exit_code_is_1_for_other_failures() {
        assert_eq!(
            ApiError::Transport(TransportError::Decode {
                source: serde_json::from_str::<serde_json::Value>("x").unwrap_err(),
                content_type: String::new(),
                body_prefix: String::new(),
            })
            .exit_code(),
            1
        );
        assert_eq!(
            ApiError::Output(OutputError::Write(std::io::Error::other("x"))).exit_code(),
            1
        );
    }
}
