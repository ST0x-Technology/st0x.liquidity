//! Test-support operations shared by the workspace end-to-end suite.

use alloy::primitives::{Address, TxHash, U256};
use chrono::Utc;
use rain_math_float::Float;
use sqlx::SqlitePool;

use st0x_config::Ctx;
use st0x_execution::Symbol;
use st0x_tokenization::{IssuerRequestId, tokenization_request_id};

pub use crate::operator::equity_transfer::EquityTransferKind as TransferType;
use crate::operator::equity_transfer::{
    fail_transfer as fail_equity_transfer, recheck_transfer as recheck_equity_transfer,
};
use crate::test_utils::try_persist_event;
use crate::tokenized_equity_mint::{TokenizedEquityMint, TokenizedEquityMintEvent};

pub async fn fail_transfer(
    pool: &SqlitePool,
    transfer_type: TransferType,
    id: &str,
    reason: &str,
) -> anyhow::Result<()> {
    fail_equity_transfer(pool, transfer_type, id, reason).await
}

pub async fn recheck_transfer(
    ctx: &Ctx,
    transfer_type: TransferType,
    id: &str,
) -> anyhow::Result<()> {
    recheck_equity_transfer(ctx, transfer_type, id).await?;
    Ok(())
}

/// Seeds an interrupted mint fixture at the `TokensWrapped` state.
///
/// The end-to-end recovery scenario needs to start the server after the
/// interruption is persisted, so it cannot drive this setup through live
/// tokenization and wallet services.
pub async fn seed_mint_at_tokens_wrapped(
    pool: &SqlitePool,
    mint_id_str: &str,
    symbol_str: &str,
    wallet: Address,
    wrap_tx_hash: TxHash,
    wrapped_shares: U256,
    quantity: Float,
) -> anyhow::Result<()> {
    let symbol = Symbol::new(symbol_str.to_string())?;
    let mint_id: IssuerRequestId = mint_id_str.parse()?;
    let now = Utc::now();
    let events = [
        TokenizedEquityMintEvent::MintRequested {
            symbol: symbol.clone(),
            quantity,
            wallet,
            requested_at: now,
        },
        TokenizedEquityMintEvent::MintAccepted {
            issuer_request_id: mint_id.clone(),
            tokenization_request_id: tokenization_request_id("seeded-tokenization-request-id"),
            accepted_at: now,
        },
        TokenizedEquityMintEvent::TokensReceived {
            tx_hash: TxHash::random(),
            shares_minted: wrapped_shares,
            fees: None,
            received_at: now,
        },
        TokenizedEquityMintEvent::WrapSubmitted {
            wrap_tx_hash,
            submitted_at: now,
        },
        TokenizedEquityMintEvent::TokensWrapped {
            wrap_tx_hash,
            wrapped_shares,
            wrapped_at: now,
            wrap_block: None,
        },
    ];

    let IssuerRequestId(raw_id) = &mint_id;
    let aggregate_id = raw_id.to_string();
    for (index, event) in events.iter().enumerate() {
        try_persist_event::<TokenizedEquityMint>(
            pool,
            &aggregate_id,
            i64::try_from(index + 1)?,
            event,
        )
        .await?;
    }

    Ok(())
}
