//! Alpaca CLI commands (deposit, withdraw, whitelist, transfers,
//! convert, journal).

use alloy::primitives::Address;
use std::io::Write;

use st0x_evm::{Evm, IntoErrorRegistry, Wallet};
use st0x_execution::{
    AlpacaAccountId, AlpacaBrokerApi, ConversionDirection, Executor, FractionalShares, Positive,
    Symbol,
};
use st0x_finance::Usdc;
use st0x_float_serde::format_float_with_fallback;

use super::ConvertDirection;
use crate::alpaca_wallet::{
    AlpacaWalletService, Network, TokenSymbol, TransferStatus, TravelRuleInfo, WhitelistEntry,
    WhitelistStatus,
};
use crate::bindings::IERC20;
use crate::config::{BrokerCtx, Ctx};
use crate::onchain::{USDC_ETHEREUM, USDC_ETHEREUM_SEPOLIA};

pub(super) async fn alpaca_deposit_command<Registry: IntoErrorRegistry, W: Write>(
    stdout: &mut W,
    amount: Usdc,
    ctx: &Ctx,
) -> anyhow::Result<()> {
    writeln!(stdout, "Depositing USDC directly to Alpaca")?;
    writeln!(stdout, "   Amount: {amount} USDC")?;

    let BrokerCtx::AlpacaBrokerApi(alpaca_auth) = &ctx.broker else {
        anyhow::bail!("alpaca-deposit requires Alpaca Broker API configuration");
    };

    let wallet_ctx = ctx.wallet()?;
    let sender_address = wallet_ctx.base_wallet().address();

    writeln!(stdout, "   Sender wallet: {sender_address}")?;

    let alpaca_wallet = AlpacaWalletService::new(
        alpaca_auth.base_url().to_string(),
        alpaca_auth.account_id,
        alpaca_auth.api_key.clone(),
        alpaca_auth.api_secret.clone(),
    );

    writeln!(stdout, "   Fetching Alpaca deposit address...")?;
    let usdc_symbol = TokenSymbol::new("USDC");
    let ethereum = Network::new("ethereum");
    let deposit_address = alpaca_wallet
        .get_wallet_address(&usdc_symbol, &ethereum)
        .await?;
    writeln!(stdout, "   Alpaca deposit address: {deposit_address}")?;

    let amount_u256 = amount.to_u256_6_decimals()?;
    writeln!(stdout, "   Amount (smallest unit): {amount_u256}")?;

    let (usdc_address, network) = if alpaca_auth.is_sandbox() {
        (USDC_ETHEREUM_SEPOLIA, "Ethereum Sepolia")
    } else {
        (USDC_ETHEREUM, "Ethereum Mainnet")
    };
    writeln!(stdout, "   Network: {network}")?;
    writeln!(stdout, "   USDC contract: {usdc_address}")?;
    let balance = wallet_ctx
        .ethereum_wallet()
        .call::<Registry, _>(
            usdc_address,
            IERC20::balanceOfCall {
                account: sender_address,
            },
        )
        .await?;
    writeln!(stdout, "   Current USDC balance: {balance}")?;

    if balance < amount_u256 {
        anyhow::bail!("Insufficient USDC balance: have {balance}, need {amount_u256}");
    }

    writeln!(stdout, "   Sending USDC transfer...")?;
    let ethereum_wallet = wallet_ctx.ethereum_wallet().clone();
    let tx_receipt = ethereum_wallet
        .submit::<Registry, _>(
            usdc_address,
            IERC20::transferCall {
                to: deposit_address,
                amount: amount_u256,
            },
            "USDC transfer to Alpaca",
        )
        .await?;

    let tx_hash = tx_receipt.transaction_hash;
    writeln!(stdout, "   Transaction hash: {tx_hash}")?;
    writeln!(stdout, "   Waiting for Alpaca to detect deposit...")?;

    let transfer = alpaca_wallet.poll_deposit_by_tx_hash(&tx_hash).await?;

    match transfer.status {
        TransferStatus::Complete => {
            writeln!(stdout, "Alpaca deposit completed successfully!")?;
            writeln!(stdout, "   Transfer ID: {}", transfer.id)?;
            writeln!(
                stdout,
                "   Amount: {} USDC",
                format_float_with_fallback(&transfer.amount)
            )?;
        }
        TransferStatus::Failed => {
            writeln!(stdout, "Alpaca deposit failed!")?;
            writeln!(stdout, "   Transfer ID: {}", transfer.id)?;
            anyhow::bail!("Deposit was detected but marked as failed by Alpaca");
        }
        status => {
            writeln!(
                stdout,
                "Unexpected transfer status after polling: {status:?}"
            )?;
            anyhow::bail!("Deposit ended with unexpected status: {status:?}");
        }
    }

    Ok(())
}

/// Prints whitelisted addresses for the given asset, splitting them into
/// approved (usable for withdrawals) and non-approved (pending/rejected).
///
/// Accepts a pre-fetched whitelist when available to avoid redundant API
/// calls. Falls back to fetching from the API when `None`.
async fn print_whitelisted_addresses<W: Write>(
    stdout: &mut W,
    alpaca_wallet: &AlpacaWalletService,
    asset: &TokenSymbol,
    prefetched: Option<&[WhitelistEntry]>,
) -> anyhow::Result<()> {
    let owned;
    let entries: &[WhitelistEntry] = if let Some(entries) = prefetched {
        entries
    } else {
        owned = alpaca_wallet.get_whitelisted_addresses().await?;
        &owned
    };

    // Filter by asset only -- chain comparison is intentionally omitted
    // because Alpaca's API returns inconsistent chain values ("ETH" vs
    // "ethereum"). See is_address_whitelisted_and_approved for context.
    let relevant: Vec<_> = entries
        .iter()
        .filter(|entry| entry.asset == *asset)
        .collect();

    if relevant.is_empty() {
        writeln!(stdout, "\nNo whitelisted addresses found for {asset}.")?;
        writeln!(
            stdout,
            "Use `alpaca-whitelist` to whitelist an address first."
        )?;
        return Ok(());
    }

    let (approved, other): (Vec<_>, Vec<_>) = relevant
        .into_iter()
        .partition(|entry| entry.status == WhitelistStatus::Approved);

    if approved.is_empty() {
        writeln!(
            stdout,
            "\nNo approved addresses available for {asset} withdrawal."
        )?;
    } else {
        writeln!(
            stdout,
            "\nApproved addresses (usable for {asset} withdrawal):"
        )?;

        for entry in &approved {
            writeln!(stdout, "   {}", entry.address)?;
        }
    }

    if !other.is_empty() {
        writeln!(stdout, "\nNot yet usable:")?;

        for entry in &other {
            writeln!(stdout, "   {} ({:?})", entry.address, entry.status,)?;
        }
    }

    Ok(())
}

pub(super) async fn alpaca_withdraw_command<Registry: IntoErrorRegistry, W: Write>(
    stdout: &mut W,
    amount: Usdc,
    to_address: Option<Address>,
    ctx: &Ctx,
) -> anyhow::Result<()> {
    writeln!(stdout, "Withdrawing USDC from Alpaca")?;
    writeln!(stdout, "   Amount: {amount} USDC")?;

    let BrokerCtx::AlpacaBrokerApi(alpaca_auth) = &ctx.broker else {
        anyhow::bail!("alpaca-withdraw requires Alpaca Broker API configuration");
    };

    let wallet_ctx = ctx.wallet()?;

    let alpaca_wallet = AlpacaWalletService::new(
        alpaca_auth.base_url().to_string(),
        alpaca_auth.account_id,
        alpaca_auth.api_key.clone(),
        alpaca_auth.api_secret.clone(),
    );

    let usdc_asset = TokenSymbol::new("USDC");

    let Some(to_address) = to_address else {
        writeln!(stdout, "\nNo --to address provided.")?;
        print_whitelisted_addresses(stdout, &alpaca_wallet, &usdc_asset, None).await?;
        anyhow::bail!("--to is required. See above for whitelisted addresses");
    };

    writeln!(stdout, "   Destination address: {to_address}")?;

    let positive_amount = Positive::new(amount)?;

    // Check whitelist before on-chain balance to fail fast on invalid destinations.
    // Chain comparison intentionally omitted -- Alpaca returns inconsistent chain
    // values ("ETH" vs "ethereum"). Matches is_address_whitelisted_and_approved.
    let whitelist = alpaca_wallet.get_whitelisted_addresses().await?;
    let is_whitelisted = whitelist.iter().any(|entry| {
        entry.address == to_address
            && entry.asset == usdc_asset
            && entry.status == WhitelistStatus::Approved
    });

    if !is_whitelisted {
        writeln!(
            stdout,
            "Error: address {to_address} is not whitelisted for USDC"
        )?;
        print_whitelisted_addresses(stdout, &alpaca_wallet, &usdc_asset, Some(&whitelist)).await?;
        anyhow::bail!(
            "Address {to_address} is not whitelisted. \
             See above for available addresses"
        );
    }

    let (usdc_address, network_name) = if alpaca_auth.is_sandbox() {
        (USDC_ETHEREUM_SEPOLIA, "Ethereum Sepolia")
    } else {
        (USDC_ETHEREUM, "Ethereum Mainnet")
    };

    writeln!(stdout, "   Network: {network_name}")?;
    writeln!(stdout, "   USDC contract: {usdc_address}")?;

    // Capture on-chain balance before the side-effecting withdrawal call
    let balance_before = wallet_ctx
        .ethereum_wallet()
        .call::<Registry, _>(
            usdc_address,
            IERC20::balanceOfCall {
                account: to_address,
            },
        )
        .await?;

    writeln!(stdout, "   Balance before: {balance_before}")?;

    writeln!(stdout, "   Initiating withdrawal...")?;
    let transfer = alpaca_wallet
        .initiate_withdrawal(positive_amount, &usdc_asset, &to_address)
        .await?;

    writeln!(stdout, "   Withdrawal initiated: {}", transfer.id)?;
    writeln!(stdout, "   Status: {:?}", transfer.status)?;
    writeln!(stdout, "   Waiting for withdrawal to complete...")?;

    let final_transfer = alpaca_wallet
        .poll_transfer_until_complete(&transfer.id)
        .await?;

    match final_transfer.status {
        TransferStatus::Complete => {
            writeln!(stdout, "Alpaca withdrawal completed successfully!")?;
            writeln!(stdout, "   Transfer ID: {}", final_transfer.id)?;
            writeln!(
                stdout,
                "   Amount: {} USDC",
                format_float_with_fallback(&final_transfer.amount)
            )?;

            if let Some(tx_hash) = final_transfer.tx {
                writeln!(stdout, "   Transaction hash: {tx_hash}")?;
            }

            let balance_after = wallet_ctx
                .ethereum_wallet()
                .call::<Registry, _>(
                    usdc_address,
                    IERC20::balanceOfCall {
                        account: to_address,
                    },
                )
                .await?;

            writeln!(stdout, "   Balance after: {balance_after}")?;
            let expected_increase = amount.to_u256_6_decimals()?;
            if balance_after >= balance_before + expected_increase {
                writeln!(stdout, "   Balance increased as expected!")?;
            } else {
                writeln!(
                    stdout,
                    "   Warning: Balance increase less than expected (may need to wait for confirmation)"
                )?;
            }
        }
        TransferStatus::Failed => {
            writeln!(stdout, "Alpaca withdrawal failed!")?;
            writeln!(stdout, "   Transfer ID: {}", final_transfer.id)?;
            anyhow::bail!("Withdrawal failed");
        }
        status => {
            writeln!(
                stdout,
                "Unexpected transfer status after polling: {status:?}"
            )?;
            anyhow::bail!("Withdrawal ended with unexpected status: {status:?}");
        }
    }

    Ok(())
}

pub(super) async fn alpaca_whitelist_command<W: Write>(
    stdout: &mut W,
    address: Option<Address>,
    ctx: &Ctx,
) -> anyhow::Result<()> {
    let BrokerCtx::AlpacaBrokerApi(alpaca_auth) = &ctx.broker else {
        anyhow::bail!("alpaca-whitelist requires Alpaca Broker API configuration");
    };

    let target_address = match address {
        Some(target_address) => target_address,
        None => ctx.wallet()?.ethereum_wallet().address(),
    };

    writeln!(stdout, "Whitelisting address for Alpaca withdrawals")?;
    writeln!(stdout, "   Address: {target_address}")?;
    writeln!(stdout, "   Asset: USDC")?;
    writeln!(stdout, "   Network: Ethereum")?;

    let travel_rule_config = ctx.travel_rule.as_ref().ok_or_else(|| {
        anyhow::anyhow!(
            "missing [broker.travel_rule] in config -- required for Alpaca whitelist creation"
        )
    })?;
    let travel_rule_info = TravelRuleInfo::from_config(travel_rule_config);

    let alpaca_wallet = AlpacaWalletService::new(
        alpaca_auth.base_url().to_string(),
        alpaca_auth.account_id,
        alpaca_auth.api_key.clone(),
        alpaca_auth.api_secret.clone(),
    );

    writeln!(stdout, "   Checking existing whitelist entries...")?;
    let existing = alpaca_wallet.get_whitelisted_addresses().await?;

    for entry in &existing {
        if entry.address == target_address && entry.asset.as_ref() == "USDC" {
            writeln!(stdout, "   Address already whitelisted!")?;
            writeln!(stdout, "   Status: {:?}", entry.status)?;
            writeln!(stdout, "   Created: {}", entry.created_at)?;
            return Ok(());
        }
    }

    writeln!(stdout, "   Creating whitelist entry...")?;
    let entry = alpaca_wallet
        .create_whitelist_entry(
            &target_address,
            &TokenSymbol::new("USDC"),
            &Network::new("ethereum"),
            &travel_rule_info,
        )
        .await?;

    writeln!(stdout, "Whitelist entry created!")?;
    writeln!(stdout, "   ID: {}", entry.id)?;
    writeln!(stdout, "   Status: {:?}", entry.status)?;
    writeln!(stdout, "   Created: {}", entry.created_at)?;

    if entry.status == WhitelistStatus::Pending {
        writeln!(
            stdout,
            "\nNote: Address is PENDING approval. In production this takes ~24 hours."
        )?;
        writeln!(
            stdout,
            "In sandbox, approval may be instant - try withdrawing."
        )?;
    }

    Ok(())
}

pub(super) async fn alpaca_whitelist_list_command<W: Write>(
    stdout: &mut W,
    ctx: &Ctx,
) -> anyhow::Result<()> {
    let BrokerCtx::AlpacaBrokerApi(alpaca_auth) = &ctx.broker else {
        anyhow::bail!("alpaca-whitelist-list requires Alpaca Broker API configuration");
    };

    let alpaca_wallet = AlpacaWalletService::new(
        alpaca_auth.base_url().to_string(),
        alpaca_auth.account_id,
        alpaca_auth.api_key.clone(),
        alpaca_auth.api_secret.clone(),
    );

    writeln!(stdout, "Fetching whitelisted addresses...")?;

    let entries = alpaca_wallet.get_whitelisted_addresses().await?;

    if entries.is_empty() {
        writeln!(stdout, "\nNo whitelisted addresses found.")?;
        return Ok(());
    }

    writeln!(stdout, "\nFound {} whitelist entries:\n", entries.len())?;

    for entry in &entries {
        writeln!(stdout, "Entry {}", entry.id)?;
        writeln!(stdout, "   Address: {}", entry.address)?;
        writeln!(stdout, "   Asset: {}", entry.asset.as_ref())?;
        writeln!(stdout, "   Chain: {}", entry.chain.as_ref())?;
        writeln!(stdout, "   Status: {:?}", entry.status)?;
        writeln!(stdout, "   Created: {}", entry.created_at)?;
        writeln!(stdout)?;
    }

    Ok(())
}

pub(super) async fn alpaca_whitelist_patch_travel_rule_command<W: Write>(
    stdout: &mut W,
    ctx: &Ctx,
) -> anyhow::Result<()> {
    let BrokerCtx::AlpacaBrokerApi(alpaca_auth) = &ctx.broker else {
        anyhow::bail!(
            "alpaca-whitelist-patch-travel-rule requires Alpaca Broker API configuration"
        );
    };

    let travel_rule_config = ctx.travel_rule.as_ref().ok_or_else(|| {
        anyhow::anyhow!(
            "missing [broker.travel_rule] in config -- required for travel rule patching"
        )
    })?;
    let travel_rule_info = TravelRuleInfo::from_config(travel_rule_config);

    writeln!(
        stdout,
        "Patching travel rule info on all whitelisted addresses"
    )?;

    let alpaca_wallet = AlpacaWalletService::new(
        alpaca_auth.base_url().to_string(),
        alpaca_auth.account_id,
        alpaca_auth.api_key.clone(),
        alpaca_auth.api_secret.clone(),
    );

    let patched = alpaca_wallet
        .patch_all_whitelist_travel_rules(&travel_rule_info)
        .await?;

    if patched.is_empty() {
        writeln!(stdout, "No whitelist entries found to patch.")?;
    } else {
        writeln!(stdout, "Patched {} whitelist entry/entries:", patched.len())?;

        for entry in &patched {
            writeln!(stdout, "   ID: {}", entry.id)?;
            writeln!(stdout, "   Address: {}", entry.address)?;
            writeln!(stdout, "   Asset: {}", entry.asset.as_ref())?;
            writeln!(stdout, "   Status: {:?}", entry.status)?;
        }
    }

    Ok(())
}

pub(super) async fn alpaca_unwhitelist_command<W: Write>(
    stdout: &mut W,
    address: Address,
    ctx: &Ctx,
) -> anyhow::Result<()> {
    let BrokerCtx::AlpacaBrokerApi(alpaca_auth) = &ctx.broker else {
        anyhow::bail!("alpaca-unwhitelist requires Alpaca Broker API configuration");
    };

    writeln!(stdout, "Removing address from Alpaca whitelist")?;
    writeln!(stdout, "   Address: {address}")?;

    let alpaca_wallet = AlpacaWalletService::new(
        alpaca_auth.base_url().to_string(),
        alpaca_auth.account_id,
        alpaca_auth.api_key.clone(),
        alpaca_auth.api_secret.clone(),
    );

    let removed = alpaca_wallet.remove_whitelist_entries(&address).await?;

    writeln!(stdout, "Removed {} whitelist entry/entries:", removed.len())?;
    for entry in &removed {
        writeln!(stdout, "   ID: {}", entry.id)?;
        writeln!(stdout, "   Asset: {}", entry.asset.as_ref())?;
        writeln!(stdout, "   Status was: {:?}", entry.status)?;
        writeln!(stdout, "   Created: {}", entry.created_at)?;
    }

    Ok(())
}

pub(super) async fn alpaca_transfers_command<W: Write>(
    stdout: &mut W,
    pending_only: bool,
    ctx: &Ctx,
) -> anyhow::Result<()> {
    let BrokerCtx::AlpacaBrokerApi(alpaca_auth) = &ctx.broker else {
        anyhow::bail!("alpaca-transfers requires Alpaca Broker API configuration");
    };

    let alpaca_wallet = AlpacaWalletService::new(
        alpaca_auth.base_url().to_string(),
        alpaca_auth.account_id,
        alpaca_auth.api_key.clone(),
        alpaca_auth.api_secret.clone(),
    );

    writeln!(stdout, "Fetching Alpaca crypto wallet transfers...")?;
    writeln!(stdout, "   Account: {}", alpaca_auth.account_id)?;

    let all_transfers = alpaca_wallet.list_all_transfers().await?;

    let transfers: Vec<_> = if pending_only {
        all_transfers
            .into_iter()
            .filter(|transfer| transfer.status.is_pending())
            .collect()
    } else {
        all_transfers
    };

    if transfers.is_empty() {
        if pending_only {
            writeln!(stdout, "\nNo pending transfers found.")?;
        } else {
            writeln!(stdout, "\nNo transfers found.")?;
        }
        return Ok(());
    }

    writeln!(stdout, "\nFound {} transfers:\n", transfers.len())?;

    for transfer in transfers {
        writeln!(stdout, "Transfer {}", transfer.id)?;
        writeln!(stdout, "   Direction: {:?}", transfer.direction)?;
        writeln!(
            stdout,
            "   Amount: {} {}",
            format_float_with_fallback(&transfer.amount),
            transfer.asset
        )?;
        writeln!(stdout, "   Status: {:?}", transfer.status)?;
        writeln!(stdout, "   Chain: {}", transfer.chain)?;
        writeln!(stdout, "   From: {}", transfer.from)?;
        writeln!(stdout, "   To: {}", transfer.to)?;
        if let Some(tx) = &transfer.tx {
            writeln!(stdout, "   Tx Hash: {tx}")?;
        }
        writeln!(stdout, "   Created: {}", transfer.created_at)?;
        writeln!(stdout)?;
    }

    Ok(())
}

pub(super) async fn alpaca_convert_command<W: Write>(
    stdout: &mut W,
    direction: ConvertDirection,
    amount: Usdc,
    ctx: &Ctx,
) -> anyhow::Result<()> {
    let direction_str = match direction {
        ConvertDirection::ToUsd => "USDC → USD",
        ConvertDirection::ToUsdc => "USD → USDC",
    };

    writeln!(stdout, "Converting {direction_str} on Alpaca")?;
    writeln!(stdout, "   Amount: {amount} USDC")?;

    let BrokerCtx::AlpacaBrokerApi(alpaca_auth) = &ctx.broker else {
        anyhow::bail!("alpaca-convert requires Alpaca Broker API configuration");
    };

    let executor = AlpacaBrokerApi::try_from_ctx(alpaca_auth.clone()).await?;

    let conversion_direction = match direction {
        ConvertDirection::ToUsd => ConversionDirection::UsdcToUsd,
        ConvertDirection::ToUsdc => ConversionDirection::UsdToUsdc,
    };

    let amount_exact = amount.inner();

    writeln!(stdout, "   Placing market order...")?;

    let order = executor
        .convert_usdc_usd(amount_exact, conversion_direction)
        .await?;

    writeln!(stdout, "Conversion completed successfully!")?;
    writeln!(stdout, "   Order ID: {}", order.id)?;
    writeln!(stdout, "   Symbol: {}", order.symbol)?;
    writeln!(
        stdout,
        "   Quantity: {}",
        format_float_with_fallback(&order.quantity)
    )?;
    writeln!(stdout, "   Status: {}", order.status_display())?;
    if let Some(price) = order.filled_average_price {
        writeln!(
            stdout,
            "   Filled Price: ${}",
            format_float_with_fallback(&price)
        )?;
    }
    if let Some(filled_qty) = order.filled_quantity {
        writeln!(
            stdout,
            "   Filled Quantity: {}",
            format_float_with_fallback(&filled_qty)
        )?;
    }
    if let (Some(price), Some(quantity)) = (order.filled_average_price, order.filled_quantity) {
        let usd_amount = (price * quantity)?;
        writeln!(
            stdout,
            "   USD Amount: ${}",
            format_float_with_fallback(&usd_amount)
        )?;
    }
    writeln!(stdout, "   Created: {}", order.created_at)?;

    Ok(())
}

pub(super) async fn alpaca_journal_command<W: Write>(
    stdout: &mut W,
    destination: AlpacaAccountId,
    symbol: Symbol,
    quantity: Positive<FractionalShares>,
    ctx: &Ctx,
) -> anyhow::Result<()> {
    let BrokerCtx::AlpacaBrokerApi(alpaca_auth) = &ctx.broker else {
        anyhow::bail!("alpaca-journal requires Alpaca Broker API configuration");
    };

    writeln!(stdout, "Journaling equities between Alpaca accounts")?;
    writeln!(stdout, "   Symbol: {symbol}")?;
    writeln!(stdout, "   Quantity: {quantity}")?;
    writeln!(stdout, "   Destination: {destination}")?;

    let executor = AlpacaBrokerApi::try_from_ctx(alpaca_auth.clone()).await?;

    let response = executor
        .create_journal(destination, &symbol, quantity)
        .await?;

    writeln!(stdout, "Journal created successfully!")?;
    writeln!(stdout, "   ID: {}", response.id)?;
    writeln!(stdout, "   Status: {}", response.status)?;
    writeln!(stdout, "   Symbol: {}", response.symbol)?;
    writeln!(stdout, "   Quantity: {}", response.quantity)?;

    if let Some(price) = response.price {
        writeln!(stdout, "   Price: ${}", format_float_with_fallback(&price))?;
    }

    if let Some(settle_date) = &response.settle_date {
        writeln!(stdout, "   Settle date: {settle_date}")?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{Address, address};
    use httpmock::prelude::*;
    use rain_math_float::Float;
    use serde_json::json;
    use url::Url;
    use uuid::uuid;

    use st0x_evm::NoOpErrorRegistry;
    use st0x_execution::{AlpacaAccountId, AlpacaBrokerApiCtx, AlpacaBrokerApiMode, TimeInForce};

    use super::*;
    use crate::cli::ConvertDirection;
    use crate::config::{AssetsConfig, EquitiesConfig, LogLevel, TradingMode};
    use crate::inventory::ImbalanceThreshold;
    use crate::onchain::EvmCtx;
    use crate::rebalancing::RebalancingCtx;
    use crate::threshold::ExecutionThreshold;
    use st0x_float_macro::float;

    fn create_ctx_without_alpaca() -> Ctx {
        Ctx {
            database_url: ":memory:".to_string(),
            log_level: LogLevel::Debug,
            log_dir: None,
            server_port: 8080,
            evm: EvmCtx {
                ws_rpc_url: Url::parse("ws://localhost:8545").unwrap(),
                orderbook: address!("0x1234567890123456789012345678901234567890"),
                deployment_block: 1,
            },
            order_polling_interval: 15,
            order_polling_max_jitter: 5,
            position_check_interval: 60,
            inventory_poll_interval: 60,
            apalis_finished_job_cleanup_interval_secs: 3600,
            broker: BrokerCtx::DryRun,
            telemetry: None,
            trading_mode: TradingMode::Standalone {
                order_owner: Address::ZERO,
            },
            wallet: None,
            execution_threshold: ExecutionThreshold::whole_share(),
            assets: AssetsConfig {
                equities: EquitiesConfig::default(),
                cash: None,
            },
            travel_rule: None,
            rest_api: None,
        }
    }

    const TEST_ACCOUNT_ID: &str = "904837e3-3b76-47ec-b432-046db621571b";

    fn create_alpaca_ctx_without_rebalancing() -> Ctx {
        let mut ctx = create_ctx_without_alpaca();
        ctx.broker = BrokerCtx::AlpacaBrokerApi(AlpacaBrokerApiCtx {
            api_key: "test-key".to_string(),
            api_secret: "test-secret".to_string(),
            account_id: AlpacaAccountId::new(uuid!("904837e3-3b76-47ec-b432-046db621571b")),
            mode: Some(AlpacaBrokerApiMode::Sandbox),
            asset_cache_ttl: std::time::Duration::from_secs(3600),
            time_in_force: TimeInForce::default(),
            counter_trade_slippage_bps: st0x_execution::DEFAULT_ALPACA_COUNTER_TRADE_SLIPPAGE_BPS,
        });
        ctx
    }

    fn create_mock_alpaca_ctx(base_url: String) -> Ctx {
        let mut ctx = create_ctx_without_alpaca();
        ctx.broker = BrokerCtx::AlpacaBrokerApi(AlpacaBrokerApiCtx {
            api_key: "test-key".to_string(),
            api_secret: "test-secret".to_string(),
            account_id: AlpacaAccountId::new(uuid!("904837e3-3b76-47ec-b432-046db621571b")),
            mode: Some(AlpacaBrokerApiMode::Mock(base_url)),
            asset_cache_ttl: std::time::Duration::from_secs(3600),
            time_in_force: TimeInForce::default(),
            counter_trade_slippage_bps: st0x_execution::DEFAULT_ALPACA_COUNTER_TRADE_SLIPPAGE_BPS,
        });
        ctx
    }

    fn create_full_alpaca_ctx() -> Ctx {
        create_full_alpaca_ctx_with_mode(AlpacaBrokerApiMode::Sandbox)
    }

    fn create_full_alpaca_ctx_with_mode(mode: AlpacaBrokerApiMode) -> Ctx {
        let alpaca_account_id = AlpacaAccountId::new(uuid!("904837e3-3b76-47ec-b432-046db621571b"));
        Ctx {
            database_url: ":memory:".to_string(),
            log_level: LogLevel::Debug,
            log_dir: None,
            server_port: 8080,
            evm: EvmCtx {
                ws_rpc_url: Url::parse("ws://localhost:8545").unwrap(),
                orderbook: address!("0x1234567890123456789012345678901234567890"),
                deployment_block: 1,
            },
            order_polling_interval: 15,
            order_polling_max_jitter: 5,
            position_check_interval: 60,
            inventory_poll_interval: 60,
            apalis_finished_job_cleanup_interval_secs: 3600,
            broker: BrokerCtx::AlpacaBrokerApi(AlpacaBrokerApiCtx {
                api_key: "test-key".to_string(),
                api_secret: "test-secret".to_string(),
                account_id: alpaca_account_id,
                mode: Some(mode),
                asset_cache_ttl: std::time::Duration::from_secs(3600),
                time_in_force: TimeInForce::default(),
                counter_trade_slippage_bps:
                    st0x_execution::DEFAULT_ALPACA_COUNTER_TRADE_SLIPPAGE_BPS,
            }),
            telemetry: None,
            assets: AssetsConfig {
                equities: EquitiesConfig::default(),
                cash: None,
            },
            trading_mode: TradingMode::Rebalancing(Box::new(
                RebalancingCtx::stub()
                    .equity(ImbalanceThreshold {
                        target: float!(0.5),
                        deviation: float!(0.1),
                    })
                    .usdc(ImbalanceThreshold {
                        target: Float::zero().unwrap(),
                        deviation: Float::zero().unwrap(),
                    })
                    .redemption_wallet(Address::ZERO)
                    .call(),
            )),
            wallet: Some(crate::wallet::OnchainWalletCtx::stub()),
            execution_threshold: ExecutionThreshold::whole_share(),
            travel_rule: None,
            rest_api: None,
        }
    }

    #[tokio::test]
    async fn test_alpaca_deposit_requires_wallet_config() {
        let ctx = create_alpaca_ctx_without_rebalancing();
        let amount = Usdc::new(float!(100));

        let mut stdout = Vec::new();
        let err_msg = alpaca_deposit_command::<NoOpErrorRegistry, _>(&mut stdout, amount, &ctx)
            .await
            .unwrap_err()
            .to_string();
        assert!(
            err_msg.contains("configured [wallet] section"),
            "Expected wallet config error, got: {err_msg}"
        );
    }

    #[tokio::test]
    async fn test_alpaca_deposit_writes_amount_to_stdout() {
        let ctx = create_full_alpaca_ctx();
        let amount = Usdc::new(float!(500.5));

        let mut stdout = Vec::new();
        alpaca_deposit_command::<NoOpErrorRegistry, _>(&mut stdout, amount, &ctx)
            .await
            .unwrap_err();

        let output = String::from_utf8(stdout).unwrap();
        assert!(
            output.contains("500.5 USDC"),
            "Expected amount in output, got: {output}"
        );
    }

    #[tokio::test]
    async fn test_alpaca_withdraw_requires_wallet_config() {
        let ctx = create_alpaca_ctx_without_rebalancing();
        let amount = Usdc::new(float!(100));
        let destination = address!("0x1234567890abcdef1234567890abcdef12345678");

        let mut stdout = Vec::new();
        let err_msg = alpaca_withdraw_command::<NoOpErrorRegistry, _>(
            &mut stdout,
            amount,
            Some(destination),
            &ctx,
        )
        .await
        .unwrap_err()
        .to_string();
        assert!(
            err_msg.contains("configured [wallet] section"),
            "Expected wallet config error, got: {err_msg}"
        );
    }

    #[tokio::test]
    async fn test_alpaca_whitelist_requires_wallet_config() {
        let ctx = create_alpaca_ctx_without_rebalancing();

        let mut stdout = Vec::new();
        let err_msg = alpaca_whitelist_command(&mut stdout, None, &ctx)
            .await
            .unwrap_err()
            .to_string();
        assert!(
            err_msg.contains("configured [wallet] section"),
            "Expected wallet config error, got: {err_msg}"
        );
    }

    #[tokio::test]
    async fn test_alpaca_convert_writes_direction_to_stdout() {
        let ctx = create_alpaca_ctx_without_rebalancing();
        let amount = Usdc::new(float!(500.5));

        let mut stdout = Vec::new();
        alpaca_convert_command(&mut stdout, ConvertDirection::ToUsd, amount, &ctx)
            .await
            .unwrap_err();

        let output = String::from_utf8(stdout).unwrap();
        assert!(
            output.contains("USDC → USD"),
            "Expected USDC to USD in output, got: {output}"
        );
        assert!(
            output.contains("500.5 USDC"),
            "Expected amount in output, got: {output}"
        );
    }

    #[tokio::test]
    async fn test_alpaca_convert_to_usdc_writes_direction_to_stdout() {
        let ctx = create_alpaca_ctx_without_rebalancing();
        let amount = Usdc::new(float!(250));

        let mut stdout = Vec::new();
        alpaca_convert_command(&mut stdout, ConvertDirection::ToUsdc, amount, &ctx)
            .await
            .unwrap_err();

        let output = String::from_utf8(stdout).unwrap();
        assert!(
            output.contains("USD → USDC"),
            "Expected USD to USDC in output, got: {output}"
        );
    }

    #[tokio::test]
    async fn test_alpaca_journal_requires_alpaca_config() {
        let ctx = create_ctx_without_alpaca();
        let destination = AlpacaAccountId::new(uuid!("11111111-2222-3333-4444-555555555555"));
        let symbol = Symbol::new("AAPL").unwrap();
        let quantity = Positive::new(FractionalShares::new(float!(10))).unwrap();

        let mut stdout = Vec::new();
        let err_msg = alpaca_journal_command(&mut stdout, destination, symbol, quantity, &ctx)
            .await
            .unwrap_err()
            .to_string();

        assert!(
            err_msg.contains("alpaca-journal requires Alpaca Broker API"),
            "Expected Alpaca config error, got: {err_msg}"
        );
    }

    #[tokio::test]
    async fn test_alpaca_journal_success() {
        let server = MockServer::start();
        let ctx = create_mock_alpaca_ctx(server.base_url());

        let account_mock = server.mock(|when, then| {
            when.method(GET)
                .path(format!("/v1/trading/accounts/{TEST_ACCOUNT_ID}/account"));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": TEST_ACCOUNT_ID,
                    "status": "ACTIVE"
                }));
        });

        let journal_mock = server.mock(|when, then| {
            when.method(POST).path("/v1/journals");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
                    "status": "pending",
                    "symbol": "TSLA",
                    "qty": "5.5",
                    "price": "250.00",
                    "from_account": TEST_ACCOUNT_ID,
                    "to_account": "11111111-2222-3333-4444-555555555555",
                    "settle_date": "2026-02-28",
                    "system_date": "2026-02-26",
                    "description": null
                }));
        });

        let destination = AlpacaAccountId::new(uuid!("11111111-2222-3333-4444-555555555555"));
        let symbol = Symbol::new("TSLA").unwrap();
        let quantity = Positive::new(FractionalShares::new(float!(5.5))).unwrap();

        let mut stdout = Vec::new();
        alpaca_journal_command(&mut stdout, destination, symbol, quantity, &ctx)
            .await
            .unwrap();

        account_mock.assert();
        journal_mock.assert();

        let output = String::from_utf8(stdout).unwrap();
        assert!(
            output.contains("TSLA"),
            "Expected symbol in output, got: {output}"
        );
        assert!(
            output.contains("5.5"),
            "Expected quantity in output, got: {output}"
        );
        assert!(
            output.contains("Journal created successfully!"),
            "Expected success message, got: {output}"
        );
        assert!(
            output.contains("$250"),
            "Expected price in output, got: {output}"
        );
    }

    fn mock_whitelist_endpoint(
        server: &MockServer,
        entries: serde_json::Value,
    ) -> httpmock::Mock<'_> {
        server.mock(|when, then| {
            when.method(GET)
                .path(format!("/v1/accounts/{TEST_ACCOUNT_ID}/wallets/whitelists"));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(entries);
        })
    }

    #[tokio::test]
    async fn test_alpaca_withdraw_no_to_lists_whitelisted_addresses() {
        let server = MockServer::start();
        let ctx = create_full_alpaca_ctx_with_mode(AlpacaBrokerApiMode::Mock(server.base_url()));

        let approved = address!("0x1234567890abcdef1234567890abcdef12345678");
        let pending = address!("0xabcdefabcdefabcdefabcdefabcdefabcdefabcd");

        let whitelist_mock = mock_whitelist_endpoint(
            &server,
            json!([
                {
                    "id": "wl-1",
                    "address": approved.to_string(),
                    "asset": "USDC",
                    "chain": "ethereum",
                    "status": "APPROVED",
                    "created_at": "2024-01-01T00:00:00Z"
                },
                {
                    "id": "wl-2",
                    "address": pending.to_string(),
                    "asset": "USDC",
                    "chain": "ethereum",
                    "status": "PENDING",
                    "created_at": "2024-01-02T00:00:00Z"
                }
            ]),
        );

        let amount = Usdc::new(float!(100));
        let mut stdout = Vec::new();

        let err_msg =
            alpaca_withdraw_command::<NoOpErrorRegistry, _>(&mut stdout, amount, None, &ctx)
                .await
                .unwrap_err()
                .to_string();

        whitelist_mock.assert_calls(1);

        let output = String::from_utf8(stdout).unwrap();

        assert!(
            err_msg.contains("--to is required"),
            "Expected --to required error, got: {err_msg}"
        );

        assert!(
            output.contains("Approved addresses"),
            "Expected approved section, got: {output}"
        );

        assert!(
            output.contains(&approved.to_string()),
            "Expected approved address in output, got: {output}"
        );

        assert!(
            output.contains("Not yet usable"),
            "Expected pending section, got: {output}"
        );

        assert!(
            output.contains(&pending.to_string()),
            "Expected pending address in output, got: {output}"
        );
    }

    #[tokio::test]
    async fn test_alpaca_withdraw_no_to_empty_whitelist() {
        let server = MockServer::start();
        let ctx = create_full_alpaca_ctx_with_mode(AlpacaBrokerApiMode::Mock(server.base_url()));

        let whitelist_mock = mock_whitelist_endpoint(&server, json!([]));

        let amount = Usdc::new(float!(100));
        let mut stdout = Vec::new();

        alpaca_withdraw_command::<NoOpErrorRegistry, _>(&mut stdout, amount, None, &ctx)
            .await
            .unwrap_err();

        whitelist_mock.assert_calls(1);

        let output = String::from_utf8(stdout).unwrap();

        assert!(
            output.contains("No whitelisted addresses found for USDC"),
            "Expected empty whitelist message, got: {output}"
        );

        assert!(
            output.contains("alpaca-whitelist"),
            "Expected alpaca-whitelist hint, got: {output}"
        );
    }

    #[tokio::test]
    async fn test_alpaca_withdraw_not_whitelisted_lists_addresses() {
        let server = MockServer::start();
        let ctx = create_full_alpaca_ctx_with_mode(AlpacaBrokerApiMode::Mock(server.base_url()));

        let approved = address!("0x1234567890abcdef1234567890abcdef12345678");
        let bad_destination = address!("0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef");

        // Whitelist endpoint is called once for the explicit whitelist check.
        // print_whitelisted_addresses reuses the prefetched whitelist slice.
        let whitelist_check = mock_whitelist_endpoint(
            &server,
            json!([{
                "id": "wl-1",
                "address": approved.to_string(),
                "asset": "USDC",
                "chain": "ethereum",
                "status": "APPROVED",
                "created_at": "2024-01-01T00:00:00Z"
            }]),
        );

        let amount = Usdc::new(float!(100));
        let mut stdout = Vec::new();

        let err_msg = alpaca_withdraw_command::<NoOpErrorRegistry, _>(
            &mut stdout,
            amount,
            Some(bad_destination),
            &ctx,
        )
        .await
        .unwrap_err()
        .to_string();

        whitelist_check.assert_calls(1);

        let output = String::from_utf8(stdout).unwrap();

        assert!(
            err_msg.contains("not whitelisted"),
            "Expected not-whitelisted error, got: {err_msg}"
        );

        assert!(
            output.contains("Approved addresses"),
            "Expected approved section in output, got: {output}"
        );

        assert!(
            output.contains(&approved.to_string()),
            "Expected approved address in output, got: {output}"
        );
    }
}
