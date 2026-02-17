//! CCTP bridge and recovery CLI commands.

use alloy::network::EthereumWallet;
use alloy::primitives::{Address, B256, U256};
use alloy::providers::{Provider, ProviderBuilder};
use alloy::signers::local::PrivateKeySigner;
use rust_decimal::Decimal;
use std::io::Write;

use st0x_bridge::cctp::{CctpBridge, CctpCtx, CctpError};
use st0x_bridge::{Attestation, Bridge, BridgeDirection};

use super::CctpChain;
use crate::bindings::IERC20;
use crate::config::Ctx;
use crate::onchain::http_client_with_retry;
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

pub(super) async fn cctp_bridge_command<W: Write, BP: Provider + Clone + Send + Sync + 'static>(
    stdout: &mut W,
    amount: Option<Usdc>,
    all: bool,
    from: CctpChain,
    ctx: &Ctx,
    base_provider: BP,
) -> anyhow::Result<()> {
    let rebalancing = ctx
        .rebalancing
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("cctp-bridge requires rebalancing configuration"))?;

    let signer = PrivateKeySigner::from_bytes(&rebalancing.evm_private_key)?;
    let wallet = signer.address();

    let amount_u256 = if all {
        let balance = match from {
            CctpChain::Ethereum => {
                let provider = ProviderBuilder::new()
                    .connect_client(http_client_with_retry(rebalancing.ethereum_rpc_url.clone()));
                IERC20::IERC20Instance::new(USDC_ETHEREUM, provider)
                    .balanceOf(wallet)
                    .call()
                    .await?
            }
            CctpChain::Base => {
                IERC20::IERC20Instance::new(USDC_BASE, &base_provider)
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
        "CCTP Bridge: {from:?} → {dest:?}, Amount: {amount_display} USDC"
    )?;
    writeln!(stdout, "   Wallet: {wallet}")?;

    let cctp_bridge = build_cctp_bridge(rebalancing, base_provider, signer.clone())?;

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

fn build_cctp_bridge<BP: Provider + Clone + 'static>(
    rebalancing: &RebalancingCtx,
    base_provider: BP,
    signer: PrivateKeySigner,
) -> Result<CctpBridge<impl Provider + Clone + 'static, impl Provider + Clone + 'static>, CctpError>
{
    let owner = signer.address();

    let ethereum_provider = ProviderBuilder::new()
        .wallet(EthereumWallet::from(signer.clone()))
        .connect_client(http_client_with_retry(rebalancing.ethereum_rpc_url.clone()));
    let base_provider = ProviderBuilder::new()
        .wallet(EthereumWallet::from(signer))
        .connect_provider(base_provider);

    CctpBridge::try_from_ctx(CctpCtx {
        ethereum_provider,
        base_provider,
        owner,
        usdc_ethereum: USDC_ETHEREUM,
        usdc_base: USDC_BASE,
    })
}

pub(super) async fn cctp_recover_command<W: Write, BP: Provider + Clone + Send + Sync + 'static>(
    stdout: &mut W,
    burn_tx: B256,
    source_chain: CctpChain,
    ctx: &Ctx,
    base_provider: BP,
) -> anyhow::Result<()> {
    writeln!(stdout, "Recovering CCTP transfer")?;
    writeln!(stdout, "   Burn tx: {burn_tx}")?;
    writeln!(stdout, "   Source chain: {source_chain:?}")?;

    let rebalancing = ctx
        .rebalancing
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("cctp-recover requires rebalancing configuration"))?;
    let signer = PrivateKeySigner::from_bytes(&rebalancing.evm_private_key)?;

    let direction = source_chain.to_bridge_direction();
    let dest_chain = match source_chain {
        CctpChain::Base => CctpChain::Ethereum,
        CctpChain::Ethereum => CctpChain::Base,
    };
    writeln!(stdout, "   Destination chain: {dest_chain:?}")?;
    writeln!(stdout, "   Polling V2 attestation API...")?;

    let cctp_bridge = build_cctp_bridge(rebalancing, base_provider, signer)?;

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

pub(super) async fn reset_allowance_command<W: Write, BP: Provider + Clone>(
    stdout: &mut W,
    chain: CctpChain,
    ctx: &Ctx,
    base_provider: BP,
) -> anyhow::Result<()> {
    let rebalancing = ctx
        .rebalancing
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("reset-allowance requires rebalancing configuration"))?;

    let signer = PrivateKeySigner::from_bytes(&rebalancing.evm_private_key)?;
    let wallet = EthereumWallet::from(signer.clone());
    let owner = signer.address();

    let (usdc_address, spender, chain_name) = match chain {
        CctpChain::Ethereum => (USDC_ETHEREUM, ctx.evm.orderbook, "Ethereum"),
        CctpChain::Base => (USDC_BASE, ctx.evm.orderbook, "Base"),
    };

    writeln!(stdout, "Resetting USDC allowance on {chain_name}")?;
    writeln!(stdout, "   Owner: {owner}")?;
    writeln!(stdout, "   Spender (orderbook): {spender}")?;
    writeln!(stdout, "   USDC: {usdc_address}")?;

    match chain {
        CctpChain::Ethereum => {
            let provider = ProviderBuilder::new()
                .wallet(wallet)
                .connect_client(http_client_with_retry(rebalancing.ethereum_rpc_url.clone()));
            reset_allowance(stdout, usdc_address, owner, spender, &provider).await
        }
        CctpChain::Base => {
            let provider = ProviderBuilder::new()
                .wallet(wallet)
                .connect_provider(base_provider);
            reset_allowance(stdout, usdc_address, owner, spender, &provider).await
        }
    }
}

async fn reset_allowance<W: Write, P: Provider>(
    stdout: &mut W,
    usdc_address: Address,
    owner: Address,
    spender: Address,
    provider: &P,
) -> anyhow::Result<()> {
    let usdc = IERC20::new(usdc_address, provider);
    let allowance = usdc.allowance(owner, spender).call().await?;
    writeln!(stdout, "   Current allowance: {allowance}")?;

    if allowance.is_zero() {
        writeln!(stdout, "Allowance already zero, nothing to reset")?;
        return Ok(());
    }

    writeln!(stdout, "   Sending approval tx...")?;
    let receipt = usdc
        .approve(spender, U256::ZERO)
        .send()
        .await?
        .get_receipt()
        .await?;
    writeln!(stdout, "   Tx: {}", receipt.transaction_hash)?;

    let new_allowance = usdc.allowance(owner, spender).call().await?;
    writeln!(stdout, "Allowance reset to: {new_allowance}")?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{Address, B256, address};
    use alloy::providers::ProviderBuilder;
    use alloy::providers::mock::Asserter;
    use rust_decimal::Decimal;
    use st0x_execution::{AlpacaAccountId, AlpacaBrokerApiCtx, AlpacaBrokerApiMode, TimeInForce};
    use std::str::FromStr;
    use url::Url;
    use uuid::uuid;

    use super::*;
    use crate::config::{BrokerCtx, LogLevel};
    use crate::inventory::ImbalanceThreshold;
    use crate::onchain::EvmCtx;
    use crate::rebalancing::RebalancingCtx;
    use crate::rebalancing::trigger::UsdcRebalancing;
    use crate::threshold::ExecutionThreshold;
    use std::collections::HashMap;

    fn create_ctx_without_rebalancing() -> Ctx {
        Ctx {
            database_url: ":memory:".to_string(),
            log_level: LogLevel::Debug,
            server_port: 8080,
            evm: EvmCtx {
                ws_rpc_url: Url::parse("ws://localhost:8545").unwrap(),
                orderbook: address!("0x1234567890123456789012345678901234567890"),
                order_owner: Some(Address::ZERO),
                deployment_block: 1,
            },
            order_polling_interval: 15,
            order_polling_max_jitter: 5,
            broker: BrokerCtx::DryRun,
            telemetry: None,
            rebalancing: None,
            execution_threshold: ExecutionThreshold::whole_share(),
        }
    }

    fn create_ctx_with_rebalancing() -> Ctx {
        let mut ctx = create_ctx_without_rebalancing();
        ctx.rebalancing = Some(RebalancingCtx {
            evm_private_key: B256::ZERO,
            ethereum_rpc_url: Url::parse("http://localhost:8545").unwrap(),
            usdc_vault_id: B256::ZERO,
            redemption_wallet: Address::ZERO,
            market_maker_wallet: Address::ZERO,
            equity: ImbalanceThreshold {
                target: Decimal::from_str("0.5").unwrap(),
                deviation: Decimal::from_str("0.1").unwrap(),
            },
            usdc: UsdcRebalancing::Disabled,
            alpaca_broker_auth: AlpacaBrokerApiCtx {
                api_key: "test-key".to_string(),
                api_secret: "test-secret".to_string(),
                account_id: AlpacaAccountId::new(uuid!("904837e3-3b76-47ec-b432-046db621571b")),
                mode: Some(AlpacaBrokerApiMode::Sandbox),
                asset_cache_ttl: std::time::Duration::from_secs(3600),
                time_in_force: TimeInForce::default(),
            },
            equities: HashMap::new(),
        });
        ctx
    }

    fn create_mock_provider() -> impl Provider + Clone + 'static {
        let asserter = Asserter::new();
        ProviderBuilder::new().connect_mocked_client(asserter)
    }

    #[tokio::test]
    async fn test_cctp_bridge_requires_rebalancing_config() {
        let ctx = create_ctx_without_rebalancing();
        let provider = create_mock_provider();
        let amount = Some(Usdc(Decimal::from_str("100").unwrap()));

        let mut stdout = Vec::new();
        let result = cctp_bridge_command(
            &mut stdout,
            amount,
            false,
            CctpChain::Ethereum,
            &ctx,
            provider,
        )
        .await;

        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("requires rebalancing configuration"),
            "Expected rebalancing config error, got: {err_msg}"
        );
    }

    #[tokio::test]
    async fn test_cctp_recover_requires_rebalancing_config() {
        let ctx = create_ctx_without_rebalancing();
        let provider = create_mock_provider();
        let burn_tx = B256::ZERO;

        let mut stdout = Vec::new();
        let result =
            cctp_recover_command(&mut stdout, burn_tx, CctpChain::Ethereum, &ctx, provider).await;

        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("requires rebalancing configuration"),
            "Expected rebalancing config error, got: {err_msg}"
        );
    }

    #[tokio::test]
    async fn test_cctp_recover_writes_burn_tx_to_stdout() {
        let ctx = create_ctx_with_rebalancing();
        let provider = create_mock_provider();
        let burn_tx = B256::ZERO;

        let mut stdout = Vec::new();
        cctp_recover_command(&mut stdout, burn_tx, CctpChain::Ethereum, &ctx, provider)
            .await
            .unwrap_err();

        let output = String::from_utf8(stdout).unwrap();
        assert!(
            output.contains(&burn_tx.to_string()),
            "Expected burn tx in output, got: {output}"
        );
    }

    #[tokio::test]
    async fn test_reset_allowance_requires_rebalancing_config() {
        let ctx = create_ctx_without_rebalancing();
        let provider = create_mock_provider();

        let mut stdout = Vec::new();
        let result =
            reset_allowance_command(&mut stdout, CctpChain::Ethereum, &ctx, provider).await;

        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("requires rebalancing configuration"),
            "Expected rebalancing config error, got: {err_msg}"
        );
    }
}
