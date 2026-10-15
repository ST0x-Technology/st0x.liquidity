//! Full-system e2e tests of the Robinhood Relay cash corridor.
//!
//! The bot runs with Base as its primary chain and Robinhood as a secondary
//! hedged chain whose cash vault rebalances with Alpaca over Relay's
//! USDG <-> USDC hop, against [`RobinhoodChain`], the CCTP harness's Ethereum
//! chain and [`RelayMock`]. Each test sets the vault and the Alpaca cash so
//! the corridor is out of balance at boot, and nothing else moves cash.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use alloy::primitives::{Address, B256, U256, utils::parse_units};
use alloy::providers::ext::AnvilApi as _;
use alloy::providers::{Provider, ProviderBuilder, RootProvider};
use rain_math_float::Float;
use st0x_float_macro::float;
use tokio::task::JoinHandle;

use st0x_bridge::corridor::{HopKind, UsdcCorridor};
use st0x_bridge::relay::RelayApiKey;
use st0x_config::{
    AlertsCtx, BrokerCtx, ChainCtx, Ctx, HedgedChain, InventoryMode, RelayHopCtx, UsdcCorridorCtx,
    UsdcCorridors,
};
use st0x_evm::{Chain, IERC20, Wallet};
use st0x_execution::alpaca_broker_api::{TEST_API_KEY, TEST_API_SECRET};
use st0x_execution::{AlpacaAccountId, AlpacaBrokerApiCtx, AlpacaBrokerApiMode, TimeInForce};
use st0x_finance::Usdc;
use st0x_hedge::{
    AllocationCtx, ChainAssets, ChainCashAsset, ImbalanceThreshold, OperationMode, RebalancingCtx,
};

use crate::assert::{StoredEvent, assert_event_subsequence, assert_single_clean_aggregate};
use crate::cctp::{CctpInfra, USDC_ETHEREUM};
use crate::poll::{connect_db, fetch_events_by_type, poll_for_events_with_timeout, spawn_bot};
use crate::rebalancing::assertions::test_wallet;
use crate::relay::{RelayMock, RobinhoodChain, relay_fill_amount};
use crate::test_infra::TestInfra;

const ROBINHOOD_RELAY: UsdcCorridor = UsdcCorridor::HubRouted {
    chain: Chain::Robinhood,
    hop: HopKind::Relay,
};

/// How long one Relay transfer may take end to end.
const TRANSFER_TIMEOUT: Duration = Duration::from_secs(180);

/// Failure events no clean Relay transfer emits.
const FAILURE_EVENTS: &[&str] = &[
    "WithdrawalFailed",
    "BridgingFailed",
    "DepositFailed",
    "ConversionFailed",
    "SwapFailed",
    "SwapRefunded",
    "SwapDepositReverted",
    "SwapEscrowUnresolved",
    "RedepositStarted",
];

/// The chain-to-Alpaca transfer's events, in order.
const ROBINHOOD_TO_ALPACA_EVENTS: &[&str] = &[
    "UsdcRebalanceEvent::Initiated",
    "UsdcRebalanceEvent::WithdrawalConfirmed",
    "UsdcRebalanceEvent::SwapQuoted",
    "UsdcRebalanceEvent::SwapDepositPrepared",
    "UsdcRebalanceEvent::SwapDeposited",
    "UsdcRebalanceEvent::RelayFillVerified",
    "UsdcRebalanceEvent::DepositInitiated",
    "UsdcRebalanceEvent::DepositConfirmed",
    "UsdcRebalanceEvent::ConversionInitiated",
    "UsdcRebalanceEvent::ConversionConfirmed",
];

/// The bot's world for one test: the shared harness (Base, the Alpaca
/// mock), the Ethereum chain, the Robinhood chain with its USDG vault, and
/// the Relay mock.
struct RelayCorridor<P> {
    infra: TestInfra<P>,
    cctp: CctpInfra,
    robinhood: RobinhoodChain,
    relay: RelayMock,
    usdg_vault: B256,
    _alpaca_deposit_watcher: JoinHandle<()>,
}

/// Raw units of a 6-decimal stable.
fn stable_units(amount: &str) -> U256 {
    parse_units(amount, 6)
        .unwrap_or_else(|error| panic!("{amount} is not a stable amount: {error}"))
        .into()
}

impl RelayCorridor<()> {
    /// Starts every service with `vault_usdg` in the Robinhood vault and
    /// `alpaca_cash` at Alpaca.
    async fn start(
        vault_usdg: &str,
        alpaca_cash: Float,
    ) -> anyhow::Result<RelayCorridor<impl Provider + Clone>> {
        let infra = Box::pin(TestInfra::start_with_cash(
            vec![("AAPL", float!(155))],
            Vec::new(),
            Some(alpaca_cash),
            None,
        ))
        .await?;
        let cctp = Box::pin(CctpInfra::start(&infra)).await?;
        let robinhood = RobinhoodChain::start().await?;
        let usdg_vault = robinhood
            .create_usdg_vault(stable_units(vault_usdg))
            .await?;
        let relay = RelayMock::start(&robinhood, cctp.ethereum_endpoint.clone()).await?;
        let ethereum = ProviderBuilder::new()
            .connect(&cctp.ethereum_endpoint)
            .await?;
        // The CCTP harness's Ethereum node mines only on a tx; a scan that
        // must see blocks past a confirmed tx needs blocks with none.
        ethereum.anvil_set_interval_mining(1).await?;

        // Credits Alpaca with the USDC our Ethereum wallet sends to its
        // deposit address.
        let alpaca_deposit_watcher = infra
            .broker_service
            .start_deposit_watcher(ethereum, USDC_ETHEREUM, robinhood.owner)
            .await?;

        Ok(RelayCorridor {
            infra,
            cctp,
            robinhood,
            relay,
            usdg_vault,
            _alpaca_deposit_watcher: alpaca_deposit_watcher,
        })
    }
}

impl<P: Provider + Clone> RelayCorridor<P> {
    /// The bot's config: hedging on Base, and the Robinhood Relay corridor
    /// as the only active cash corridor, balanced at 50% within 10%.
    async fn ctx(&self) -> anyhow::Result<Ctx> {
        let base = &self.infra.base_chain;
        let ethereum_endpoint = &self.cctp.ethereum_endpoint;
        let wallet = |endpoint: &str| -> anyhow::Result<Arc<dyn Wallet<Provider = RootProvider>>> {
            Ok(Arc::new(test_wallet(
                &base.owner_key,
                endpoint.parse()?,
                1,
            )?))
        };
        let ethereum_wallet = wallet(ethereum_endpoint)?;

        let mut rebalancing = RebalancingCtx::with_wallets()
            .allocation(AllocationCtx::single_chain_test(
                Chain::Base,
                float!(0.5),
                float!(100),
            )?)
            .call()
            .with_relay_api_base(self.relay.base_url());
        rebalancing.usdc = UsdcCorridors::for_test(
            OperationMode::Enabled,
            [UsdcCorridorCtx {
                corridor: ROBINHOOD_RELAY,
                threshold: ImbalanceThreshold {
                    target: float!(0.5),
                    deviation: float!(0.1),
                },
            }],
        )
        .with_relay_hop(
            Chain::Robinhood,
            RelayHopCtx {
                slippage_bps: 30,
                max_quote_loss_bps: 50,
                min_transfer: Usdc::new(float!(100)),
                max_transfer: Usdc::new(float!(50000)),
                quote_max_age: Duration::from_secs(60),
                fill_timeout: Duration::from_secs(1800),
                max_refund_retries: 3,
                max_deposit_revert_redrives: 5,
            },
        )
        .with_relay_api_key(RelayApiKey::from("e2e-relay-key".to_owned()));

        let mut ctx = Ctx::for_test()
            .database_url(self.infra.db_path.display().to_string())
            .rpc_url(base.endpoint().parse()?)
            .orderbook(base.orderbook)
            .deployment_block(base.provider.get_block_number().await?)
            .broker(alpaca_broker_ctx(&self.infra))
            .rebalancing(Box::new(rebalancing))
            .order_owner(base.owner)
            .wallet(st0x_config::OnchainWalletCtx::from_wallets(
                wallet(&base.endpoint())?,
                ethereum_wallet.clone(),
                ethereum_wallet,
                wallet(&self.robinhood.endpoint())?,
            ))
            .assets(self.infra.assets_config())
            .inventory_poll_interval_secs(2)
            .issuance(st0x_config::test_issuance_status_ctx(
                self.infra.issuance_base_url.clone(),
            ))
            .redemption_wallet(Address::random())
            .bot_gas_valuation(st0x_hedge::BotGasValuationConfig {
                chainlink_feed: base.mock_chainlink_feed,
            })
            .alerts(AlertsCtx::for_test(
                BTreeMap::from([
                    (Chain::Base, U256::from(1)),
                    (Chain::Ethereum, U256::from(1)),
                    (Chain::Robinhood, U256::from(1)),
                ]),
                Duration::from_secs(1),
                Duration::from_secs(1),
            ))
            .call()?;

        ctx.chains.insert_secondary(
            HedgedChain::test()
                .chain(Chain::Robinhood)
                .rpc_url(self.robinhood.endpoint().parse()?)
                .required_confirmations(1)
                .orderbook(self.robinhood.orderbook)
                .inventory(InventoryMode::Legacy)
                .vault_owner(self.robinhood.owner)
                .deployment_block(self.robinhood.provider.get_block_number().await?)
                .assets(ChainAssets {
                    equities: st0x_hedge::ChainEquities {
                        symbols: HashMap::new(),
                        operational_limit: None,
                    },
                    cash: Some(ChainCashAsset {
                        vault_ids: vec![self.usdg_vault],
                        rebalancing: OperationMode::Enabled,
                        operational_limit: None,
                    }),
                })
                .call(),
        );
        // The hub of the Relay hop and Alpaca's deposit and withdrawal chain.
        // A Relay end needs one confirmation at least: its deposit scan must
        // not read the raw head.
        ctx.chains.insert_transport(ChainCtx {
            chain: Chain::Ethereum,
            rpc_url: ethereum_endpoint.parse()?,
            required_confirmations: 1,
        });

        Ok(ctx)
    }

    async fn ethereum_wallet_usdc(&self) -> anyhow::Result<U256> {
        let ethereum = ProviderBuilder::new()
            .connect(&self.cctp.ethereum_endpoint)
            .await?;
        Ok(IERC20::new(USDC_ETHEREUM, &ethereum)
            .balanceOf(self.robinhood.owner)
            .call()
            .await?)
    }

    async fn usdc_rebalance_events(&self) -> anyhow::Result<Vec<StoredEvent>> {
        let pool = connect_db(&self.infra.db_path).await?;
        let events = fetch_events_by_type(&pool, "UsdcRebalance").await?;
        pool.close().await;
        Ok(events)
    }

    /// Asserts the money moved by one Relay fill of `amount_in`: the vault
    /// holds `vault_after`, Alpaca holds `alpaca_after`, and no stable is
    /// left in our Robinhood or Ethereum wallet.
    async fn assert_settled(
        &self,
        amount_in: &str,
        vault_after: U256,
        alpaca_after: Float,
    ) -> anyhow::Result<()> {
        let amount_in = stable_units(amount_in);
        assert_eq!(
            self.relay.deposits(),
            vec![(amount_in, relay_fill_amount(amount_in), true)],
            "Relay should have seen one deposit and filled it",
        );
        assert_eq!(
            self.robinhood.vault_usdg(self.usdg_vault).await?,
            vault_after
        );
        assert_eq!(self.robinhood.wallet_usdg().await?, U256::ZERO);
        assert_eq!(self.ethereum_wallet_usdc().await?, U256::ZERO);
        let alpaca_cash = self.infra.broker_service.cash_balance();
        assert!(
            alpaca_cash.eq(alpaca_after)?,
            "Alpaca cash {alpaca_cash:?}, expected {alpaca_after:?}",
        );

        Ok(())
    }
}

fn alpaca_broker_ctx<P>(infra: &TestInfra<P>) -> BrokerCtx {
    BrokerCtx::AlpacaBrokerApi(AlpacaBrokerApiCtx {
        auth: st0x_execution::AlpacaBrokerAuth::Basic {
            api_key: TEST_API_KEY.to_owned(),
            api_secret: TEST_API_SECRET.to_owned(),
        },
        account_id: AlpacaAccountId::new(uuid::uuid!("904837e3-3b76-47ec-b432-046db621571b")),
        mode: Some(AlpacaBrokerApiMode::Mock(infra.broker_service.base_url())),
        asset_cache_ttl: Duration::from_secs(3600),
        time_in_force: TimeInForce::Day,
        counter_trade_slippage_bps: st0x_execution::DEFAULT_ALPACA_COUNTER_TRADE_SLIPPAGE_BPS,
        hedge_floor: st0x_execution::HedgeFloor::default(),
    })
}

fn single_aggregate_id(events: &[StoredEvent]) -> String {
    assert_single_clean_aggregate(events, FAILURE_EVENTS);
    events[0].aggregate_id.clone()
}

/// 30,000 USDG in the Robinhood vault against 10,000 USD at Alpaca is 75%
/// onchain, above the 60% band edge: the bot withdraws the 10,000 USDG
/// above the 50% target, swaps it to USDC on Ethereum over Relay, deposits
/// the fill at Alpaca and converts it to USD.
#[test_log::test(tokio::test)]
async fn relay_corridor_moves_usdg_from_robinhood_to_alpaca() -> anyhow::Result<()> {
    let corridor = RelayCorridor::start("30000", float!(10000)).await?;
    let mut bot = spawn_bot(corridor.ctx().await?);

    poll_for_events_with_timeout(
        &mut bot,
        &corridor.infra.db_path,
        "UsdcRebalanceEvent::ConversionConfirmed",
        1,
        TRANSFER_TIMEOUT,
    )
    .await;

    let events = corridor.usdc_rebalance_events().await?;
    single_aggregate_id(&events);
    assert_event_subsequence(&events, ROBINHOOD_TO_ALPACA_EVENTS);
    corridor
        .assert_settled("10000", stable_units("20000"), float!(19995))
        .await?;

    bot.abort();
    Ok(())
}

/// 2,000 USDG in the Robinhood vault against 18,000 USD at Alpaca is 10%
/// onchain, below the 40% band edge: the bot buys 8,000 USDC at Alpaca,
/// withdraws it to Ethereum, swaps it to USDG on Robinhood over Relay and
/// deposits the fill into the vault.
#[test_log::test(tokio::test)]
async fn relay_corridor_moves_usdc_from_alpaca_to_robinhood() -> anyhow::Result<()> {
    let corridor = RelayCorridor::start("2000", float!(18000)).await?;
    let mut bot = spawn_bot(corridor.ctx().await?);

    poll_for_events_with_timeout(
        &mut bot,
        &corridor.infra.db_path,
        "UsdcRebalanceEvent::DepositConfirmed",
        1,
        TRANSFER_TIMEOUT,
    )
    .await;

    let events = corridor.usdc_rebalance_events().await?;
    single_aggregate_id(&events);
    assert_event_subsequence(
        &events,
        &[
            "UsdcRebalanceEvent::ConversionInitiated",
            "UsdcRebalanceEvent::ConversionConfirmed",
            "UsdcRebalanceEvent::Initiated",
            "UsdcRebalanceEvent::WithdrawalConfirmed",
            "UsdcRebalanceEvent::SwapQuoted",
            "UsdcRebalanceEvent::SwapDepositPrepared",
            "UsdcRebalanceEvent::SwapDeposited",
            "UsdcRebalanceEvent::RelayFillVerified",
            "UsdcRebalanceEvent::DepositInitiated",
            "UsdcRebalanceEvent::DepositConfirmed",
        ],
    );
    corridor
        .assert_settled(
            "8000",
            stable_units("2000") + relay_fill_amount(stable_units("8000")),
            float!(10000),
        )
        .await?;

    bot.abort();
    Ok(())
}
