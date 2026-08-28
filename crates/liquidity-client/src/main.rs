mod auth;
mod cli;
mod error;
mod output;
mod target;
mod transport;

use std::process::ExitCode;

use clap::Parser;

use crate::auth::{Adc, StaticToken, TokenSource};
use crate::cli::{Cli, Command, Debug, Read};
use crate::error::Error;
use crate::target::Auth;
use crate::transport::Client;

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
        error: Error,
        logging_url: Option<String>,
    },
}

async fn execute(cli: Cli) -> Result<(), Failure> {
    let target = target::resolve(cli.env).map_err(Failure::Setup)?;
    let logging_url = target.logging_url;
    match target.auth {
        Auth::OauthDesktop {
            client_id,
            client_secret,
        } => {
            let token = auth::desktop_id_token(&client_id, &client_secret)
                .await
                .map_err(|error| Failure::Api {
                    error,
                    logging_url: logging_url.clone(),
                })?;
            let client = Client::new(
                target.base_url,
                StaticToken(token.clone()),
                StaticToken(token),
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
                error,
                logging_url: logging_url.clone(),
            })?;
            let write_auth = Adc::new(&write_audience).map_err(|error| Failure::Api {
                error,
                logging_url: logging_url.clone(),
            })?;
            let client =
                Client::new(target.base_url, read_auth, write_auth).map_err(Failure::Setup)?;
            dispatch(&client, cli.command)
                .await
                .map_err(|error| Failure::Api { error, logging_url })
        }
    }
}

async fn dispatch<A: TokenSource + Sync>(
    client: &Client<A>,
    command: Command,
) -> Result<(), Error> {
    let value = match command {
        Command::Read(Read::Resource(args)) => {
            client.get(args.resource.path(), &args.params).await?
        }
        Command::Read(Read::TradeEvents(args)) => {
            let path = format!("/trades/{}/{}/events", args.venue, args.aggregate_id);
            client.get(&path, &args.params).await?
        }
        Command::Read(Read::TransferEvents(args)) => {
            let path = format!("/transfers/{}/{}/events", args.kind, args.aggregate_id);
            client.get(&path, &args.params).await?
        }
        Command::Debug(Debug::Resume) => client.post("/transfers/resume").await?,
        Command::Debug(Debug::Recheck { kind, id }) => {
            client
                .post(&format!("/transfers/recheck/{kind}/{id}"))
                .await?
        }
    };
    output::print(&value)
}
