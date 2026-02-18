//! CCTP bridge and recovery CLI commands.

use alloy::primitives::{B256, Bytes, U256};
use alloy::sol_types::SolCall;
use rust_decimal::Decimal;
use std::io::Write;

use st0x_bridge::cctp::{CctpBridge, CctpCtx, CctpError};
use st0x_bridge::{Attestation, Bridge, BridgeDirection};

use super::CctpChain;
use crate::bindings::IERC20;
use crate::config::Ctx;
use crate::onchain::{USDC_BASE, USDC_ETHEREUM};
use crate::rebalancing::RebalancingCtx;
use crate::threshold::Usdc;

impl CctpChain {
    /// Converts to the bridge direction (from this chain to its destination).
    const fn to_bridge_direction(self) -> BridgeDirection {
        match self {
            Self::Ethereum => BridgeDirection::EthereumToBase,
            Self::Base => BridgeDirection::BaseToEthereum,
        }
    }
}

pub(super) async fn cctp_bridge_command<W: Write>(
    stdout: &mut W,
    amount: Option<Usdc>,
    all: bool,
    from: CctpChain,
    ctx: &Ctx,
) -> anyhow::Result<()> {
    let rebalancing_ctx = ctx.rebalancing_ctx()?;
    let wallet = rebalancing_ctx.base_caller().address();

    let amount_u256 = if all {
        let balance = match from {
            CctpChain::Ethereum => {
                IERC20::IERC20Instance::new(
                    USDC_ETHEREUM,
                    rebalancing_ctx.ethereum_caller().provider(),
                )
                .balanceOf(wallet)
                .call()
                .await?
            }
            CctpChain::Base => {
                IERC20::IERC20Instance::new(USDC_BASE, rebalancing_ctx.base_caller().provider())
                    .balanceOf(wallet)
                    .call()
                    .await?
            }
        };
        if balance.is_zero() {
            anyhow::bail!("Balance is zero");
        }
        // Fee is 1 bps (0.01%). To use full balance: amount + fee = balance
        // amount * 1.0001 = balance => amount = balance * 10000 / 10001
        balance * U256::from(10000) / U256::from(10001)
    } else {
        let usdc_amount = amount.ok_or_else(|| anyhow::anyhow!("specify --amount or --all"))?;
        usdc_amount.to_u256_6_decimals()?
    };

    let dest = match from {
        CctpChain::Ethereum => CctpChain::Base,
        CctpChain::Base => CctpChain::Ethereum,
    };
    let amount_display = Decimal::from(amount_u256.to::<u128>()) / Decimal::from(1_000_000u64);
    writeln!(
        stdout,
        "CCTP Bridge: {from:?} -> {dest:?}, Amount: {amount_display} USDC"
    )?;
    writeln!(stdout, "   Wallet: {wallet}")?;

    let cctp_bridge = build_cctp_bridge(rebalancing_ctx)?;

    let direction = from.to_bridge_direction();

    writeln!(stdout, "\n1. Burning USDC on {from:?}...")?;
    let burn = cctp_bridge.burn(direction, amount_u256, wallet).await?;
    writeln!(stdout, "   Burn tx: {}, Amount: {}", burn.tx, burn.amount)?;

    writeln!(stdout, "\n2. Polling for attestation...")?;
    let response = cctp_bridge.poll_attestation(direction, burn.tx).await?;
    writeln!(
        stdout,
        "   Attestation received ({} bytes)",
        response.as_bytes().len()
    )?;

    writeln!(stdout, "\n3. Minting USDC on {dest:?}...")?;
    let mint_receipt = cctp_bridge.mint(direction, &response).await?;
    writeln!(
        stdout,
        "Bridge complete! Mint tx: {}\n  Amount received: {} (fee: {})",
        mint_receipt.tx, mint_receipt.amount, mint_receipt.fee
    )?;

    Ok(())
}

fn build_cctp_bridge(rebalancing: &RebalancingCtx) -> Result<CctpBridge, CctpError> {
    CctpBridge::try_from_ctx(CctpCtx {
        usdc_ethereum: USDC_ETHEREUM,
        usdc_base: USDC_BASE,
        ethereum_caller: rebalancing.ethereum_caller().clone(),
        base_caller: rebalancing.base_caller().clone(),
    })
}

pub(super) async fn cctp_recover_command<W: Write>(
    stdout: &mut W,
    burn_tx: B256,
    source_chain: CctpChain,
    ctx: &Ctx,
) -> anyhow::Result<()> {
    writeln!(stdout, "Recovering CCTP transfer")?;
    writeln!(stdout, "   Burn tx: {burn_tx}")?;
    writeln!(stdout, "   Source chain: {source_chain:?}")?;

    let rebalancing_ctx = ctx.rebalancing_ctx()?;

    let direction = source_chain.to_bridge_direction();
    let dest_chain = match source_chain {
        CctpChain::Base => CctpChain::Ethereum,
        CctpChain::Ethereum => CctpChain::Base,
    };
    writeln!(stdout, "   Destination chain: {dest_chain:?}")?;
    writeln!(stdout, "   Polling V2 attestation API...")?;

    let cctp_bridge = build_cctp_bridge(rebalancing_ctx)?;

    // Use the V2 API which returns both message and attestation from tx hash
    let response = cctp_bridge.poll_attestation(direction, burn_tx).await?;

    writeln!(
        stdout,
        "   Attestation received ({} bytes)",
        response.as_bytes().len()
    )?;

    writeln!(stdout, "   Calling receiveMessage on {dest_chain:?}...")?;
    let mint_receipt = cctp_bridge.mint(direction, &response).await?;
    writeln!(
        stdout,
        "CCTP transfer recovered! Mint tx: {}\n  Amount received: {} (fee: {})",
        mint_receipt.tx, mint_receipt.amount, mint_receipt.fee
    )?;

    Ok(())
}

pub(super) async fn reset_allowance_command<W: Write>(
    stdout: &mut W,
    chain: CctpChain,
    ctx: &Ctx,
) -> anyhow::Result<()> {
    let rebalancing_ctx = ctx.rebalancing_ctx()?;
    let owner = rebalancing_ctx.base_caller().address();

    let (usdc_address, spender, chain_name, caller) = match chain {
        CctpChain::Ethereum => (
            USDC_ETHEREUM,
            ctx.evm.orderbook,
            "Ethereum",
            rebalancing_ctx.ethereum_caller(),
        ),
        CctpChain::Base => (
            USDC_BASE,
            ctx.evm.orderbook,
            "Base",
            rebalancing_ctx.base_caller(),
        ),
    };

    writeln!(stdout, "Resetting USDC allowance on {chain_name}")?;
    writeln!(stdout, "   Owner: {owner}")?;
    writeln!(stdout, "   Spender (orderbook): {spender}")?;
    writeln!(stdout, "   USDC: {usdc_address}")?;

    let usdc = IERC20::IERC20Instance::new(usdc_address, caller.provider());
    let allowance = usdc.allowance(owner, spender).call().await?;
    writeln!(stdout, "   Current allowance: {allowance}")?;

    if allowance.is_zero() {
        writeln!(stdout, "Allowance already zero, nothing to reset")?;
        return Ok(());
    }

    writeln!(stdout, "   Sending approval tx via Fireblocks...")?;
    let calldata = IERC20::approveCall {
        spender,
        amount: U256::ZERO,
    };
    let encoded = Bytes::from(SolCall::abi_encode(&calldata));
    let receipt = caller
        .call_contract(usdc_address, encoded, "Reset USDC allowance")
        .await?;
    writeln!(stdout, "   Tx: {}", receipt.transaction_hash)?;

    let new_allowance = usdc.allowance(owner, spender).call().await?;
    writeln!(stdout, "Allowance reset to: {new_allowance}")?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{Address, B256, address};
    use alloy::providers::Provider;
    use rust_decimal::Decimal;
    use st0x_execution::{AlpacaAccountId, AlpacaBrokerApiCtx, AlpacaBrokerApiMode, TimeInForce};
    use std::str::FromStr;
    use url::Url;
    use uuid::uuid;

    use super::*;
    use crate::config::{BrokerCtx, LogLevel, TradingMode};
    use crate::onchain::EvmCtx;
    use crate::threshold::ExecutionThreshold;

    fn create_ctx_without_rebalancing() -> Ctx {
        Ctx {
            database_url: ":memory:".to_string(),
            log_level: LogLevel::Debug,
            server_port: 8080,
            evm: EvmCtx {
                ws_rpc_url: Url::parse("ws://localhost:8545").unwrap(),
                orderbook: address!("0x1234567890123456789012345678901234567890"),
                deployment_block: 1,
            },
            order_polling_interval: 15,
            order_polling_max_jitter: 5,
            broker: BrokerCtx::DryRun,
            telemetry: None,
            trading_mode: TradingMode::Standalone {
                order_owner: Address::ZERO,
            },
            execution_threshold: ExecutionThreshold::whole_share(),
        }
    }

    #[tokio::test]
    async fn test_cctp_bridge_requires_rebalancing_config() {
        let ctx = create_ctx_without_rebalancing();
        let amount = Some(Usdc(Decimal::from_str("100").unwrap()));

        let mut stdout = Vec::new();
        let result =
            cctp_bridge_command(&mut stdout, amount, false, CctpChain::Ethereum, &ctx).await;

        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("requires rebalancing configuration"),
            "Expected rebalancing config error, got: {err_msg}"
        );
    }

    #[tokio::test]
    async fn test_cctp_recover_requires_rebalancing_config() {
        let ctx = create_ctx_without_rebalancing();
        let burn_tx = B256::ZERO;

        let mut stdout = Vec::new();
        let result = cctp_recover_command(&mut stdout, burn_tx, CctpChain::Ethereum, &ctx).await;

        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("requires rebalancing configuration"),
            "Expected rebalancing config error, got: {err_msg}"
        );
    }

    #[tokio::test]
    async fn test_reset_allowance_requires_rebalancing_config() {
        let ctx = create_ctx_without_rebalancing();

        let mut stdout = Vec::new();
        let result = reset_allowance_command(&mut stdout, CctpChain::Ethereum, &ctx).await;

        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("requires rebalancing configuration"),
            "Expected rebalancing config error, got: {err_msg}"
        );
    }
}
