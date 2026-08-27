mod auth;
mod cli;
mod error;
mod output;
mod target;
mod transport;

use std::process::ExitCode;

use clap::Parser;

use crate::auth::Adc;
use crate::cli::{Capital, Cli, Command};
use crate::error::Error;
use crate::transport::Client;

#[tokio::main]
async fn main() -> ExitCode {
    match run(Cli::parse()).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(RunError::Setup(error)) => {
            eprintln!("error: {error:#}");
            ExitCode::from(2)
        }
        Err(RunError::Api(error)) => {
            eprintln!("error: {error}");
            ExitCode::from(error.exit_code())
        }
    }
}

enum RunError {
    Setup(anyhow::Error),
    Api(Error),
}

impl From<anyhow::Error> for RunError {
    fn from(error: anyhow::Error) -> Self {
        Self::Setup(error)
    }
}

impl From<Error> for RunError {
    fn from(error: Error) -> Self {
        Self::Api(error)
    }
}

async fn run(cli: Cli) -> Result<(), RunError> {
    let target = target::resolve(cli.env)?;
    let auth = Adc::new(&target.audience)?;
    let client = Client::new(target.base_url, auth)?;
    match cli.command {
        Command::Read(args) => {
            let value = client.get(args.resource.path(), &args.params).await?;
            output::print(&value)?;
        }
        Command::Capital(command) => {
            let value = match command {
                Capital::Resume => client.post("/transfers/resume").await?,
                Capital::Recheck { kind, id } => {
                    client
                        .post(&format!("/transfers/recheck/{kind}/{id}"))
                        .await?
                }
            };
            output::print(&value)?;
        }
    }
    Ok(())
}
