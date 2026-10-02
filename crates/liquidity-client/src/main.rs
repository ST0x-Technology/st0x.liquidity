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
use crate::cli::{
    Capital, Cctp, Cli, Command, Debug, HedgedChain, PortfolioSnapshot, Position, Read,
    RebuildableView, RecheckTransferType, UsdcDirection, VaultArgs, View,
};
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

/// A single query parameter when `value` is given, none otherwise, so an
/// omitted flag sends no query and the bot applies its default.
fn optional_query(key: &str, value: Option<String>) -> Vec<(String, String)> {
    value
        .map(|value| (key.to_owned(), value))
        .into_iter()
        .collect()
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
        Command::Debug(debug) => send_debug(client, debug).await?,
        Command::Capital(capital) => dispatch_capital(client, capital).await?,
    };
    output::print(&value).map_err(ApiError::from)
}

/// Sends one operator write through the write prefix and returns the bot's
/// response for `dispatch` to print.
async fn send_debug<A: TokenSource + Sync>(
    client: &Client<A>,
    debug: Debug,
) -> Result<serde_json::Value, TransportError> {
    let value = match debug {
        Debug::Resume => client.post("/transfers/resume", &[]).await?,
        Debug::Recheck {
            kind,
            id,
            deposit_tx,
        } => {
            let kind = match kind {
                RecheckTransferType::Mint => "equity_mint",
                RecheckTransferType::Redemption => "equity_redemption",
                RecheckTransferType::Usdc => "usdc_bridge",
            };
            let id = encode_segment(&id);
            let params = optional_query("deposit_tx", deposit_tx);
            client
                .post(&format!("/transfers/recheck/{kind}/{id}"), &params)
                .await?
        }
        Debug::ResumeUsdc { direction, id } => {
            let direction = match direction {
                UsdcDirection::AlpacaToBase => "alpaca_to_base",
                UsdcDirection::BaseToAlpaca => "base_to_alpaca",
            };
            let id = encode_segment(&id);
            client
                .post(&format!("/transfers/usdc/resume/{direction}/{id}"), &[])
                .await?
        }
        Debug::ReconcileUsdc {
            id,
            reason,
            superseding_tx,
        } => {
            let id = encode_segment(&id);
            client
                .post_json(
                    &format!("/transfers/usdc/{id}/reconcile"),
                    &wire::ReconcileUsdcRequest {
                        reason,
                        superseding_tx,
                    },
                )
                .await?
        }
        Debug::ReconcileEquity {
            kind,
            id,
            reason,
            superseding_tx,
        } => {
            let kind = kind.route_segment();
            let id = encode_segment(&id);
            client
                .post_json(
                    &format!("/transfers/{kind}/{id}/reconcile"),
                    &wire::ReconcileEquityRequest {
                        reason,
                        superseding_tx,
                    },
                )
                .await?
        }
        Debug::AdoptWithdrawal {
            id,
            replacement_tx,
            reason,
        } => {
            let id = encode_segment(&id);
            client
                .post_json(
                    &format!("/transfers/equity_redemption/{id}/adopt-withdrawal"),
                    &wire::AdoptWithdrawalRequest {
                        reason,
                        replacement_tx,
                    },
                )
                .await?
        }
        Debug::ClearPendingBurn { id, reason } => {
            let id = encode_segment(&id);
            client
                .post_json(
                    &format!("/transfers/usdc/{id}/clear-pending-burn"),
                    &wire::ClearPendingBurnRequest { reason },
                )
                .await?
        }
        Debug::FailUsdcTransfer { id, reason } => {
            let id = encode_segment(&id);
            client
                .post_json(
                    &format!("/transfers/usdc/{id}/fail"),
                    &wire::FailUsdcTransferRequest { reason },
                )
                .await?
        }
        Debug::FailEquityTransfer { kind, id, reason } => {
            let kind = kind.route_segment();
            let id = encode_segment(&id);
            client
                .post_json(
                    &format!("/transfers/fail/{kind}/{id}"),
                    &wire::FailEquityTransferRequest { reason },
                )
                .await?
        }
        Debug::Position(Position::Set(args)) => {
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
        Debug::Position(Position::ReleaseHedge(args)) => {
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
        Debug::PortfolioSnapshot(PortfolioSnapshot::SetMark(args)) => {
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
        Debug::ProcessTx { tx_hash, chain } => {
            let tx_hash = encode_segment(&tx_hash);
            let params = optional_query("chain", chain.map(|chain| chain.wire_name().to_owned()));
            client
                .post(&format!("/transactions/{tx_hash}/process"), &params)
                .await?
        }
        Debug::View(View::Rebuild(args)) => {
            let view = match args.view {
                RebuildableView::Position => "position",
                RebuildableView::OffchainOrder => "offchain-order",
                RebuildableView::VaultRegistry => "vault-registry",
                RebuildableView::RebalanceTiming => "rebalance-timing",
                RebuildableView::EquityTiming => "equity-timing",
                RebuildableView::LifecycleFailure => "lifecycle-failure",
                RebuildableView::PortfolioSnapshot => "portfolio-snapshot",
            };
            client
                .post_json(
                    &format!("/views/{view}/rebuild"),
                    &wire::RebuildViewRequest {
                        id: args.id,
                        all: args.all,
                    },
                )
                .await?
        }
        Debug::Cctp(Cctp::CompleteMint {
            burn_tx,
            source_chain,
        }) => {
            client
                .post_json(
                    "/cctp/complete-mint",
                    &wire::CompleteCctpMintRequest {
                        burn_tx,
                        source_chain: source_chain.wire_name(),
                    },
                )
                .await?
        }
    };
    Ok(value)
}

/// Sends one `capital` verb through the write prefix and returns the bot's
/// response for `dispatch` to print, like `send_debug` for the debug verbs.
async fn dispatch_capital<A: TokenSource + Sync>(
    client: &Client<A>,
    capital: Capital,
) -> Result<serde_json::Value, TransportError> {
    match capital {
        Capital::TransferUsdc {
            direction,
            amount,
            chain,
        } => {
            client
                .post_json(
                    "/capital/transfer-usdc",
                    &wire::TransferUsdcRequest {
                        direction,
                        amount,
                        chain: chain.map(HedgedChain::wire_name),
                    },
                )
                .await
        }
        Capital::VaultDeposit(args) => {
            client
                .post_json("/capital/vault-deposit", &vault_request(args))
                .await
        }
        Capital::VaultWithdraw(args) => {
            client
                .post_json("/capital/vault-withdraw", &vault_request(args))
                .await
        }
        Capital::VaultWithdrawUsdc { amount, network } => {
            client
                .post_json(
                    "/capital/vault-withdraw-usdc",
                    &wire::VaultWithdrawUsdcRequest {
                        chain: network.wire_name(),
                        amount,
                    },
                )
                .await
        }
        Capital::CctpBridge {
            amount,
            all,
            from,
            operation_id,
        } => {
            let operation_id = operation_id.unwrap_or_else(uuid::Uuid::new_v4);
            // Printed before the request, so it survives a timeout or an
            // interrupted run: the id is what makes the rerun safe.
            eprintln!(
                "operation id {operation_id}: after a failure or a timeout, rerun with \
                 --operation-id {operation_id} to report this burn instead of burning again \
                 (only against a bot that records operation ids, see below)"
            );
            let answer = client
                .post_json(
                    "/capital/cctp-bridge",
                    &wire::CctpBridgeRequest {
                        operation_id,
                        from: from.wire_name(),
                        amount,
                        all,
                    },
                )
                .await?;
            if !records_operation(&answer, operation_id) {
                eprintln!(
                    "WARNING: the bot did not answer with operation id {operation_id} and a \
                     status, so it predates operation ids and did not record this burn. A \
                     rerun, even with --operation-id, burns again: do not rerun, finish this \
                     burn with debug cctp complete-mint"
                );
            }
            Ok(answer)
        }
        Capital::CctpBurnSupersede {
            operation_id,
            superseding_tx,
        } => {
            client
                .post_json(
                    "/capital/cctp-burn-supersede",
                    &wire::CctpBurnSupersedeRequest {
                        operation_id,
                        superseding_tx,
                    },
                )
                .await
        }
        Capital::ResetAllowance { network } => {
            client
                .post_json(
                    "/capital/reset-allowance",
                    &wire::ResetAllowanceRequest {
                        chain: network.wire_name(),
                    },
                )
                .await
        }
    }
}

/// Whether a `cctp-bridge` answer comes from a bot that records operation
/// ids: it echoes this run's id and a status. An older bot ignores the id and
/// burns on every call, so a rerun with the same id is only safe when this
/// holds.
fn records_operation(answer: &serde_json::Value, operation_id: uuid::Uuid) -> bool {
    let echoed = answer
        .get("operationId")
        .and_then(serde_json::Value::as_str)
        == Some(operation_id.to_string().as_str());
    echoed
        && answer
            .get("status")
            .is_some_and(serde_json::Value::is_string)
}

/// The body `vault-deposit` and `vault-withdraw` share.
fn vault_request(args: VaultArgs) -> wire::VaultTransferRequest {
    wire::VaultTransferRequest {
        chain: args.network.wire_name(),
        token: args.token,
        vault_id: args.vault_id,
        amount: args.amount,
    }
}

#[cfg(test)]
mod tests {
    //! Tests for command dispatch and CLI-boundary error classification.
    use std::io::{Read as _, Write as _};
    use std::net::TcpListener;
    use std::sync::mpsc::{Receiver, channel};
    use std::time::Duration;

    use super::{ApiError, dispatch, records_operation};
    use crate::auth::{AuthError, StaticToken};
    use crate::cli::{
        Capital, Cctp, CctpSourceChain, Command, Debug, EquityTransferKind, HedgedChain,
        PortfolioSnapshot, Position, Read, ReadResource, RebuildViewArgs, RebuildableView,
        RecheckTransferType, ReleaseHedgeArgs, ResourceArgs, SetMarkArgs, SetPositionArgs,
        TradeEventsArgs, TransferEventsArgs, UsdcDirection, VaultArgs, View,
    };
    use crate::output::OutputError;
    use crate::transport::{Client, TransportError};
    use crate::wire::{ReconcileUsdcReason, TransferUsdcDirection};

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
    async fn recheck_maps_each_kind_to_its_write_path() -> Result<(), Box<dyn std::error::Error>> {
        for (kind, segment) in [
            (RecheckTransferType::Mint, "equity_mint"),
            (RecheckTransferType::Redemption, "equity_redemption"),
            (RecheckTransferType::Usdc, "usdc_bridge"),
        ] {
            let request = request_for(Command::Debug(Debug::Recheck {
                kind,
                id: "abc".to_owned(),
                deposit_tx: None,
            }))
            .await?;
            assert_eq!(
                request_line(&request),
                format!("POST /liquidity-write/transfers/recheck/{segment}/abc HTTP/1.1")
            );
        }
        Ok(())
    }

    /// A USDC recheck sends the operator's deposit tx as the `deposit_tx`
    /// query parameter the bot reads.
    #[tokio::test]
    async fn recheck_sends_the_deposit_tx_as_its_query_parameter()
    -> Result<(), Box<dyn std::error::Error>> {
        let request = request_for(Command::Debug(Debug::Recheck {
            kind: RecheckTransferType::Usdc,
            id: "abc".to_owned(),
            deposit_tx: Some("0xdeposit".to_owned()),
        }))
        .await?;
        assert_eq!(
            request_line(&request),
            "POST /liquidity-write/transfers/recheck/usdc_bridge/abc?deposit_tx=0xdeposit HTTP/1.1"
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
            superseding_tx: None,
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

    /// A superseding tx travels as the camelCase `supersedingTx` the bot's
    /// reconcile body reads.
    #[tokio::test]
    async fn reconcile_usdc_sends_the_superseding_tx_when_given()
    -> Result<(), Box<dyn std::error::Error>> {
        let request = request_for(Command::Debug(Debug::ReconcileUsdc {
            id: "abc".to_owned(),
            reason: ReconcileUsdcReason::FundsMovedManually,
            superseding_tx: Some("0xcancel".to_owned()),
        }))
        .await?;
        assert_eq!(
            request_body(&request),
            serde_json::json!({
                "reason": "funds-moved-manually",
                "supersedingTx": "0xcancel",
            })
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
            superseding_tx: None,
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

    /// A redemption's superseding tx travels as the camelCase `supersedingTx`
    /// the bot's equity reconcile body reads.
    #[tokio::test]
    async fn reconcile_equity_sends_the_superseding_tx_when_given()
    -> Result<(), Box<dyn std::error::Error>> {
        let request = request_for(Command::Debug(Debug::ReconcileEquity {
            kind: EquityTransferKind::Redemption,
            id: "abc".to_owned(),
            reason: "cancelled at its nonce".to_owned(),
            superseding_tx: Some("0xcancel".to_owned()),
        }))
        .await?;
        assert_eq!(
            request_body(&request),
            serde_json::json!({
                "reason": "cancelled at its nonce",
                "supersedingTx": "0xcancel",
            })
        );
        Ok(())
    }

    /// The adopt route takes the redemption id in the path and the replacement
    /// as the camelCase `replacementTx` the bot's body reads.
    #[tokio::test]
    async fn adopt_withdrawal_posts_the_replacement_and_reason()
    -> Result<(), Box<dyn std::error::Error>> {
        let request = request_for(Command::Debug(Debug::AdoptWithdrawal {
            id: "abc".to_owned(),
            replacement_tx: "0xspeedup".to_owned(),
            reason: "wallet sped up the withdrawal".to_owned(),
        }))
        .await?;
        assert_eq!(
            request_line(&request),
            "POST /liquidity-write/transfers/equity_redemption/abc/adopt-withdrawal HTTP/1.1"
        );
        assert_eq!(
            request_body(&request),
            serde_json::json!({
                "reason": "wallet sped up the withdrawal",
                "replacementTx": "0xspeedup",
            })
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
    async fn fail_equity_transfer_posts_the_kind_segment_and_reason()
    -> Result<(), Box<dyn std::error::Error>> {
        for (kind, segment) in [
            (EquityTransferKind::Mint, "equity_mint"),
            (EquityTransferKind::Redemption, "equity_redemption"),
        ] {
            let request = request_for(Command::Debug(Debug::FailEquityTransfer {
                kind,
                id: "abc".to_owned(),
                reason: "stuck at the issuer".to_owned(),
            }))
            .await?;
            assert_eq!(
                request_line(&request),
                format!("POST /liquidity-write/transfers/fail/{segment}/abc HTTP/1.1")
            );
            assert_eq!(
                request_body(&request),
                serde_json::json!({ "reason": "stuck at the issuer" })
            );
        }
        Ok(())
    }

    /// Omitting `--chain` sends no query, so the bot resolves the primary
    /// chain; an explicit secondary is sent as its `Chain` wire name.
    #[tokio::test]
    async fn process_tx_sends_the_chain_only_when_given() -> Result<(), Box<dyn std::error::Error>>
    {
        let primary = request_for(Command::Debug(Debug::ProcessTx {
            tx_hash: "0xabc".to_owned(),
            chain: None,
        }))
        .await?;
        assert_eq!(
            request_line(&primary),
            "POST /liquidity-write/transactions/0xabc/process HTTP/1.1"
        );

        let secondary = request_for(Command::Debug(Debug::ProcessTx {
            tx_hash: "0xabc".to_owned(),
            chain: Some(HedgedChain::Ethereum),
        }))
        .await?;
        assert_eq!(
            request_line(&secondary),
            "POST /liquidity-write/transactions/0xabc/process?chain=ethereum HTTP/1.1"
        );
        Ok(())
    }

    #[tokio::test]
    async fn cctp_complete_mint_posts_the_burn_and_source_chain()
    -> Result<(), Box<dyn std::error::Error>> {
        let request = request_for(Command::Debug(Debug::Cctp(Cctp::CompleteMint {
            burn_tx: "0xabc".to_owned(),
            source_chain: CctpSourceChain::Base,
        })))
        .await?;
        assert_eq!(
            request_line(&request),
            "POST /liquidity-write/cctp/complete-mint HTTP/1.1"
        );
        assert_eq!(
            request_body(&request),
            serde_json::json!({ "burnTx": "0xabc", "sourceChain": "base" })
        );
        Ok(())
    }

    #[tokio::test]
    async fn view_rebuild_posts_one_id() -> Result<(), Box<dyn std::error::Error>> {
        let request = request_for(Command::Debug(Debug::View(View::Rebuild(
            RebuildViewArgs {
                view: RebuildableView::Position,
                id: Some("AAPL".to_owned()),
                all: false,
            },
        ))))
        .await?;
        assert_eq!(
            request_line(&request),
            "POST /liquidity-write/views/position/rebuild HTTP/1.1"
        );
        assert_eq!(
            request_body(&request),
            serde_json::json!({ "id": "AAPL", "all": false })
        );
        Ok(())
    }

    #[tokio::test]
    async fn view_rebuild_posts_a_whole_model_without_an_id()
    -> Result<(), Box<dyn std::error::Error>> {
        let request = request_for(Command::Debug(Debug::View(View::Rebuild(
            RebuildViewArgs {
                view: RebuildableView::RebalanceTiming,
                id: None,
                all: true,
            },
        ))))
        .await?;
        assert_eq!(
            request_line(&request),
            "POST /liquidity-write/views/rebalance-timing/rebuild HTTP/1.1"
        );
        assert_eq!(request_body(&request), serde_json::json!({ "all": true }));
        Ok(())
    }

    #[tokio::test]
    async fn transfer_usdc_posts_the_direction_and_amount() -> Result<(), Box<dyn std::error::Error>>
    {
        for (direction, spelling) in [
            (TransferUsdcDirection::ToRaindex, "to-raindex"),
            (TransferUsdcDirection::ToAlpaca, "to-alpaca"),
        ] {
            let request = request_for(Command::Capital(Capital::TransferUsdc {
                direction,
                amount: "250.5".parse()?,
                chain: None,
            }))
            .await?;
            assert_eq!(
                request_line(&request),
                "POST /liquidity-write/capital/transfer-usdc HTTP/1.1"
            );
            assert_eq!(
                request_body(&request),
                serde_json::json!({ "direction": spelling, "amount": "250.5" })
            );
        }
        let request = request_for(Command::Capital(Capital::TransferUsdc {
            direction: TransferUsdcDirection::ToRaindex,
            amount: "250.5".parse()?,
            chain: Some(HedgedChain::Robinhood),
        }))
        .await?;
        assert_eq!(
            request_body(&request),
            serde_json::json!({ "direction": "to-raindex", "amount": "250.5", "chain": "robinhood" })
        );
        Ok(())
    }

    /// Both vault verbs send the same camelCase body, each to its own route.
    #[tokio::test]
    async fn vault_verbs_post_the_chain_token_vault_and_amount()
    -> Result<(), Box<dyn std::error::Error>> {
        let token = "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913";
        let vault_id = "0x00000000000000000000000000000000000000000000000000000000000000a1";
        let args = VaultArgs {
            amount: "1.5".parse()?,
            token: token.parse()?,
            vault_id: vault_id.parse()?,
            network: HedgedChain::Ethereum,
        };
        for (command, route) in [
            (Capital::VaultDeposit(args.clone()), "vault-deposit"),
            (Capital::VaultWithdraw(args), "vault-withdraw"),
        ] {
            let request = request_for(Command::Capital(command)).await?;
            assert_eq!(
                request_line(&request),
                format!("POST /liquidity-write/capital/{route} HTTP/1.1")
            );
            assert_eq!(
                request_body(&request),
                serde_json::json!({
                    "chain": "ethereum",
                    "token": "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
                    "vaultId": "0x00000000000000000000000000000000000000000000000000000000000000a1",
                    "amount": "1.5",
                })
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn vault_withdraw_usdc_posts_the_chain_and_amount()
    -> Result<(), Box<dyn std::error::Error>> {
        let request = request_for(Command::Capital(Capital::VaultWithdrawUsdc {
            amount: "100".parse()?,
            network: HedgedChain::Base,
        }))
        .await?;
        assert_eq!(
            request_line(&request),
            "POST /liquidity-write/capital/vault-withdraw-usdc HTTP/1.1"
        );
        assert_eq!(
            request_body(&request),
            serde_json::json!({ "chain": "base", "amount": "100" })
        );
        Ok(())
    }

    /// The body carries the operation id and either `amount` or `all: true`,
    /// never both keys and never `all: false`. A run without
    /// `--operation-id` sends a fresh id; one with it sends that id.
    #[tokio::test]
    async fn cctp_bridge_posts_either_an_amount_or_all() -> Result<(), Box<dyn std::error::Error>> {
        let amount = request_for(Command::Capital(Capital::CctpBridge {
            amount: Some("100".parse()?),
            all: false,
            from: CctpSourceChain::Ethereum,
            operation_id: None,
        }))
        .await?;
        assert_eq!(
            request_line(&amount),
            "POST /liquidity-write/capital/cctp-bridge HTTP/1.1"
        );
        let mut body = request_body(&amount);
        let generated = body
            .as_object_mut()
            .and_then(|body| body.remove("operationId"))
            .and_then(|id| id.as_str().map(str::parse::<uuid::Uuid>))
            .transpose()?;
        assert!(generated.is_some_and(|id| id.get_version_num() == 4));
        assert_eq!(
            body,
            serde_json::json!({ "from": "ethereum", "amount": "100" })
        );

        let id: uuid::Uuid = "6f1c2a1e-6c39-4a77-9a8e-1f0b7d6e8c11".parse()?;
        let all = request_for(Command::Capital(Capital::CctpBridge {
            amount: None,
            all: true,
            from: CctpSourceChain::Base,
            operation_id: Some(id),
        }))
        .await?;
        assert_eq!(
            request_body(&all),
            serde_json::json!({ "operationId": id.to_string(), "from": "base", "all": true })
        );
        Ok(())
    }

    #[tokio::test]
    async fn cctp_burn_supersede_posts_the_operation_and_the_superseding_tx()
    -> Result<(), Box<dyn std::error::Error>> {
        let id: uuid::Uuid = "6f1c2a1e-6c39-4a77-9a8e-1f0b7d6e8c11".parse()?;
        let request = request_for(Command::Capital(Capital::CctpBurnSupersede {
            operation_id: id,
            superseding_tx: "0xabc".to_owned(),
        }))
        .await?;
        assert_eq!(
            request_line(&request),
            "POST /liquidity-write/capital/cctp-burn-supersede HTTP/1.1"
        );
        assert_eq!(
            request_body(&request),
            serde_json::json!({ "operationId": id.to_string(), "supersedingTx": "0xabc" })
        );
        Ok(())
    }

    #[tokio::test]
    async fn reset_allowance_posts_the_chain() -> Result<(), Box<dyn std::error::Error>> {
        let request = request_for(Command::Capital(Capital::ResetAllowance {
            network: HedgedChain::Hyperevm,
        }))
        .await?;
        assert_eq!(
            request_line(&request),
            "POST /liquidity-write/capital/reset-allowance HTTP/1.1"
        );
        assert_eq!(
            request_body(&request),
            serde_json::json!({ "chain": "hyperevm" })
        );
        Ok(())
    }

    /// Only an answer that echoes this run's operation id and a status comes
    /// from a bot that records the burn; an older bot's answer, or one for
    /// another id, must trigger the do not rerun warning.
    #[test]
    fn only_an_answer_echoing_the_operation_id_and_a_status_is_recorded()
    -> Result<(), Box<dyn std::error::Error>> {
        let id: uuid::Uuid = "6f1c2a1e-6c39-4a77-9a8e-1f0b7d6e8c11".parse()?;
        let recorded = serde_json::json!({
            "operationId": id.to_string(),
            "burnTx": "0x01",
            "status": "pending",
        });
        let old_bot = serde_json::json!({
            "burnTx": "0x01",
            "sourceChain": "base",
            "destinationChain": "ethereum",
            "amountRaw": "1000000",
        });
        let other_id = serde_json::json!({
            "operationId": uuid::Uuid::new_v4().to_string(),
            "status": "pending",
        });

        assert!(records_operation(&recorded, id));
        assert!(!records_operation(&old_bot, id));
        assert!(!records_operation(&other_id, id));
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
