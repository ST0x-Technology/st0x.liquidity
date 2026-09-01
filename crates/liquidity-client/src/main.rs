//! Binary entrypoint: resolves the target environment, builds the auth-backed
//! transport, dispatches the parsed command, and maps failures to exit codes.

mod auth;
mod cli;
mod output;
mod target;
mod transport;

use clap::Parser;
use std::process::ExitCode;

use crate::auth::{Adc, AuthError, StaticToken, TokenSource};
use crate::cli::{Cli, Command, Debug, Read};
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
    match target.auth {
        Auth::OauthDesktop {
            client_id,
            client_secret,
        } => {
            let token = auth::desktop_id_token(
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
        Auth::Adc {
            read_audience,
            write_audience,
        } => {
            let read_auth = Adc::new(&read_audience).map_err(|error| Failure::Api {
                error: error.into(),
                logging_url: logging_url.clone(),
            })?;
            let write_auth = Adc::new(&write_audience).map_err(|error| Failure::Api {
                error: error.into(),
                logging_url: logging_url.clone(),
            })?;
            let client = Client::new(
                target.base_url,
                read_auth,
                write_auth,
                target.request_timeout,
                target.connect_timeout,
            )
            .map_err(Failure::Setup)?;
            dispatch(&client, cli.command)
                .await
                .map_err(|error| Failure::Api { error, logging_url })
        }
    }
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
    use crate::auth::StaticToken;
    use crate::cli::{
        Command, Debug, Read, ReadResource, ResourceArgs, TradeEventsArgs, TransferEventsArgs,
    };
    use crate::transport::Client;

    /// Accepts one connection, captures the raw request bytes, and replies with
    /// an empty JSON object.
    fn capture_server() -> std::io::Result<(u16, Receiver<String>)> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let port = listener.local_addr()?.port();
        let (sender, receiver) = channel();
        std::thread::spawn(move || {
            if let Ok((mut stream, _)) = listener.accept() {
                let mut buffer = [0u8; 4096];
                let read = stream.read(&mut buffer).unwrap_or(0);
                let _ = sender.send(String::from_utf8_lossy(&buffer[..read]).into_owned());
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

    #[test]
    fn exit_code_is_77_for_auth_and_access_denied() {
        use crate::auth::AuthError;
        use crate::transport::TransportError;

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
        use crate::output::OutputError;
        use crate::transport::TransportError;

        assert_eq!(
            ApiError::Transport(TransportError::Decode("x".to_owned())).exit_code(),
            1
        );
        assert_eq!(
            ApiError::Output(OutputError::Write(std::io::Error::other("x"))).exit_code(),
            1
        );
    }
}
