//! The `Health` and `Settings` families: build identity, process start and
//! the operational settings, published once at boot.
//!
//! Settings are fixed for the life of the process: an accepted registry
//! reload ends the process, and the next one publishes again.

use std::time::{SystemTime, UNIX_EPOCH};

use alloy::primitives::Address;
use itertools::Itertools;
use rain_math_float::Float;
use tracing::warn;

use st0x_config::{BrokerCtx, Ctx, ExecutionThreshold, OperationMode, UsdcCorridorCtx};
use st0x_finance::{Positive, Symbol};

use super::{
    LiqFamilies, LiqFamily, LiqMetric, LiqSample, float_value, integer_value, push_sample,
    strip_prefix,
};

/// Publishes the `Health` and `Settings` families. Called once at boot, after
/// the config is loaded.
pub(crate) fn publish_boot_families(
    ctx: &Ctx,
    git_commit: &str,
    process_start: SystemTime,
    families: &LiqFamilies,
) {
    let now = SystemTime::now();
    families.replace(
        LiqFamily::Health,
        health_samples(git_commit, process_start),
        now,
    );
    families.replace(
        LiqFamily::Settings,
        settings_samples(&SettingsInput::from_ctx(ctx)),
        now,
    );
}

/// What the settings builder reads, owned by the metrics so it does not
/// depend on the dashboard DTO.
#[derive(Debug)]
pub(crate) struct SettingsInput {
    equity_target: Option<Float>,
    equity_deviation: Float,
    usdc_target: Option<Float>,
    usdc_deviation: Option<Float>,
    cash_reserved: Option<Float>,
    usdc_corridors: Vec<CorridorInput>,
    /// `None` for a share-count threshold: the series is dollars only.
    execution_threshold_usd: Option<Float>,
    assets: Vec<AssetInput>,
    wallet: Option<WalletInput>,
    log_level: String,
    server_port: u16,
    orderbook: Address,
    deployment_block: u64,
    broker: &'static str,
    order_polling_seconds: u64,
    inventory_poll_seconds: u64,
}

/// One `[rebalancing.usdc.corridors.<chain>]` table, listed whatever the
/// USDC mode: disabling the mode stops new transfers, not the tables. `active`
/// says whether the USDC trigger can start transfers on it: the mode is
/// enabled and the corridor's chain rebalances its cash asset.
#[derive(Debug)]
struct CorridorInput {
    usdc: UsdcCorridorCtx,
    active: bool,
}

#[derive(Debug)]
struct AssetInput {
    symbol: Symbol,
    counter_trading: AssetCounterTrading,
    rebalancing: bool,
}

/// Extended hours exists only while counter trading is enabled.
#[derive(Debug)]
enum AssetCounterTrading {
    Disabled,
    Enabled { extended_hours: bool },
}

#[derive(Debug)]
struct WalletInput {
    kind: String,
    address: Address,
    organization_id: Option<String>,
}

impl SettingsInput {
    /// Reads the same config the dashboard settings show: the primary
    /// chain's equity band, the band of the one corridor the USDC trigger can
    /// act on (or the primary chain's when it can act on several), and one
    /// row per symbol the primary chain lists.
    pub(crate) fn from_ctx(ctx: &Ctx) -> Self {
        let primary = ctx.chains.primary();
        let rebalancing = &ctx.rebalancing;

        // The USDC trigger skips a corridor whose chain does not rebalance
        // its cash asset.
        let rebalances_cash = |usdc: &&UsdcCorridorCtx| {
            ctx.chains
                .hedged_chain(usdc.corridor.chain())
                .and_then(|hedged| hedged.assets.rebalancing_cash())
                .is_some()
        };

        let usdc_band = rebalancing
            .usdc
            .active()
            .filter(rebalances_cash)
            .exactly_one()
            .ok()
            .or_else(|| {
                rebalancing
                    .usdc
                    .active()
                    .filter(rebalances_cash)
                    .find(|usdc| usdc.corridor.chain() == primary.chain)
            })
            .map(|usdc| &usdc.threshold);

        let execution_threshold_usd = match &ctx.execution_threshold {
            ExecutionThreshold::DollarValue(usd) => Some(usd.inner()),
            ExecutionThreshold::Shares(_) => None,
        };

        let assets = primary
            .assets
            .equities
            .symbols
            .iter()
            .map(|(symbol, config)| AssetInput {
                symbol: symbol.clone(),
                counter_trading: match config.trading {
                    OperationMode::Enabled => AssetCounterTrading::Enabled {
                        extended_hours: ctx.assets.is_extended_hours_enabled(symbol),
                    },
                    OperationMode::Disabled => AssetCounterTrading::Disabled,
                },
                rebalancing: config.rebalancing.starts_operations(),
            })
            .collect();

        let BrokerCtx::AlpacaBrokerApi(_) = &ctx.broker;

        Self {
            equity_target: rebalancing
                .allocation
                .targets
                .get(&primary.chain)
                .map(|target| target.inner()),
            equity_deviation: rebalancing.allocation.deviation.inner(),
            usdc_target: usdc_band.map(|band| band.target),
            usdc_deviation: usdc_band.map(|band| band.deviation),
            cash_reserved: cash_reserved(ctx),
            usdc_corridors: rebalancing
                .usdc
                .configured()
                .map(|usdc| CorridorInput {
                    usdc: *usdc,
                    active: rebalancing
                        .usdc
                        .active()
                        .filter(rebalances_cash)
                        .any(|active| active.corridor == usdc.corridor),
                })
                .collect(),
            execution_threshold_usd,
            assets,
            wallet: ctx.wallet_meta.as_ref().map(|meta| WalletInput {
                kind: meta.kind.clone(),
                address: meta.address,
                organization_id: meta.organization_id.clone(),
            }),
            log_level: format!("{:?}", ctx.log_level),
            server_port: ctx.server_port,
            orderbook: primary.orderbook,
            deployment_block: primary.deployment_block,
            broker: "alpaca",
            order_polling_seconds: ctx.order_polling_interval_secs,
            inventory_poll_seconds: ctx.inventory_poll_interval_secs,
        }
    }
}

/// The USD the broker account holds back, when configured.
pub(crate) fn cash_reserved(ctx: &Ctx) -> Option<Float> {
    ctx.assets
        .cash
        .as_ref()
        .map(|cash| Positive::inner(cash.reserved).inner())
}

/// `liq_bot_info` and `liq_bot_start_timestamp_seconds`.
pub(crate) fn health_samples(git_commit: &str, process_start: SystemTime) -> Vec<LiqSample> {
    let mut samples = Vec::new();
    let short_commit: String = git_commit.chars().take(12).collect();
    push_sample(
        &mut samples,
        LiqMetric::BotInfo,
        vec![("git_commit", short_commit)],
        Ok(1.0),
    );

    match process_start.duration_since(UNIX_EPOCH) {
        Ok(since_epoch) => push_sample(
            &mut samples,
            LiqMetric::BotStartTimestampSeconds,
            vec![],
            Ok(since_epoch.as_secs_f64()),
        ),
        Err(error) => warn!(%error, "Process start is before the Unix epoch"),
    }

    samples
}

/// Every `liq_settings_*` and `liq_asset_*` sample. An optional setting that
/// is not configured has no series.
pub(crate) fn settings_samples(input: &SettingsInput) -> Vec<LiqSample> {
    let mut samples = Vec::new();
    push_sample(
        &mut samples,
        LiqMetric::SettingsInfo,
        settings_info_labels(input),
        Ok(1.0),
    );

    let decimals = [
        (LiqMetric::SettingsEquityTarget, input.equity_target),
        (
            LiqMetric::SettingsEquityDeviation,
            Some(input.equity_deviation),
        ),
        (LiqMetric::SettingsUsdcTarget, input.usdc_target),
        (LiqMetric::SettingsUsdcDeviation, input.usdc_deviation),
        (LiqMetric::SettingsCashReserved, input.cash_reserved),
        (
            LiqMetric::SettingsExecutionThresholdUsd,
            input.execution_threshold_usd,
        ),
    ];
    for (metric, value) in decimals {
        if let Some(value) = value {
            push_sample(&mut samples, metric, vec![], float_value(value));
        }
    }

    let integers = [
        (
            LiqMetric::SettingsOrderPollingSeconds,
            input.order_polling_seconds,
        ),
        (
            LiqMetric::SettingsInventoryPollSeconds,
            input.inventory_poll_seconds,
        ),
        (LiqMetric::SettingsDeploymentBlock, input.deployment_block),
    ];
    for (metric, value) in integers {
        push_sample(&mut samples, metric, vec![], integer_value(value));
    }

    for CorridorInput { usdc, active } in &input.usdc_corridors {
        let labels = vec![("chain", usdc.corridor.chain().as_str().to_string())];
        let values = [
            (
                LiqMetric::UsdcCorridorTarget,
                float_value(usdc.threshold.target),
            ),
            (
                LiqMetric::UsdcCorridorDeviation,
                float_value(usdc.threshold.deviation),
            ),
            (
                LiqMetric::UsdcCorridorActive,
                Ok(if *active { 1.0 } else { 0.0 }),
            ),
        ];
        for (metric, value) in values {
            push_sample(&mut samples, metric, labels.clone(), value);
        }
    }

    for asset in &input.assets {
        asset_samples(&mut samples, asset);
    }

    samples
}

fn settings_info_labels(input: &SettingsInput) -> Vec<(&'static str, String)> {
    let (wallet_kind, wallet_address, turnkey_organization) =
        input
            .wallet
            .as_ref()
            .map_or_else(Default::default, |wallet| {
                (
                    wallet.kind.clone(),
                    format!("{:#x}", wallet.address),
                    wallet.organization_id.clone().unwrap_or_default(),
                )
            });

    // Port 0 published as an empty label, as the exporter did.
    let server_port = match input.server_port {
        0 => String::new(),
        port => port.to_string(),
    };

    vec![
        ("broker", input.broker.to_string()),
        ("log_level", input.log_level.clone()),
        ("orderbook", format!("{:#x}", input.orderbook)),
        ("server_port", server_port),
        // The settings carry no trading mode; the label stays for parity.
        ("trading_mode", String::new()),
        ("turnkey_organization", turnkey_organization),
        ("wallet_address", wallet_address),
        ("wallet_kind", wallet_kind),
    ]
}

fn asset_samples(samples: &mut Vec<LiqSample>, asset: &AssetInput) {
    let symbol = strip_prefix(asset.symbol.as_str()).to_string();
    let flag = |enabled: bool| Ok(if enabled { 1.0 } else { 0.0 });

    let counter_trading = match asset.counter_trading {
        AssetCounterTrading::Enabled { extended_hours } => {
            push_sample(
                samples,
                LiqMetric::AssetExtendedHours,
                vec![("symbol", symbol.clone())],
                flag(extended_hours),
            );
            true
        }
        AssetCounterTrading::Disabled => false,
    };

    push_sample(
        samples,
        LiqMetric::AssetCounterTrading,
        vec![("symbol", symbol.clone())],
        flag(counter_trading),
    );
    push_sample(
        samples,
        LiqMetric::AssetRebalancing,
        vec![("symbol", symbol)],
        flag(asset.rebalancing),
    );
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::time::Duration;

    use alloy::primitives::address;
    use serde_json::Value;

    use st0x_bridge::corridor::{HopKind, UsdcCorridor};
    use st0x_config::{
        CashHedgePolicy, ChainAssets, ChainCashAsset, ChainEquityAsset, EquityHedgePolicy,
        HedgedChain, ImbalanceThreshold, RebalancingMode, UsdcCorridors, WalletMeta,
        create_test_ctx_with_order_owner,
    };
    use st0x_evm::Chain;
    use st0x_finance::{Usd, Usdc};
    use st0x_float_macro::float;

    use super::*;
    use crate::dashboard::settings_from_ctx;
    use crate::metrics::liquidity::tests::{SeriesKey, parse_exposition, series};

    /// The goldens feed the exporter the dashboard DTO, so the same fixture
    /// reaches the builder through this adapter.
    fn input_from_dto(settings: &st0x_dto::Settings) -> SettingsInput {
        let decimal = |value: f64| Float::parse(value.to_string()).unwrap();
        assert_eq!(settings.broker, "alpaca");

        SettingsInput {
            equity_target: settings.equity_target.map(decimal),
            equity_deviation: decimal(settings.equity_deviation),
            usdc_target: settings.usdc_target.map(decimal),
            usdc_deviation: settings.usdc_deviation.map(decimal),
            cash_reserved: settings.cash_reserved.map(Usd::inner),
            // The dashboard settings carry one band, not each corridor's.
            usdc_corridors: Vec::new(),
            execution_threshold_usd: Float::parse(
                settings
                    .execution_threshold
                    .trim_start_matches('$')
                    .to_string(),
            )
            .ok(),
            assets: settings
                .assets
                .iter()
                .map(|asset| AssetInput {
                    symbol: asset.symbol.clone(),
                    counter_trading: match asset.counter_trading {
                        st0x_dto::CounterTrading::Disabled => AssetCounterTrading::Disabled,
                        st0x_dto::CounterTrading::Enabled { extended_hours } => {
                            AssetCounterTrading::Enabled { extended_hours }
                        }
                    },
                    rebalancing: asset.rebalancing,
                })
                .collect(),
            wallet: settings.wallet.as_ref().map(|wallet| WalletInput {
                kind: wallet.kind.clone(),
                address: wallet.address.parse().unwrap(),
                organization_id: wallet.organization_id.clone(),
            }),
            log_level: settings.log_level.clone(),
            server_port: settings.server_port,
            orderbook: settings.orderbook.parse().unwrap(),
            deployment_block: settings.deployment_block,
            broker: "alpaca",
            order_polling_seconds: settings.order_polling_interval,
            inventory_poll_seconds: settings.inventory_poll_interval,
        }
    }

    fn render(samples: Vec<LiqSample>, family: LiqFamily) -> BTreeMap<SeriesKey, f64> {
        let families = LiqFamilies::default();
        families.replace(family, samples, UNIX_EPOCH);
        let mut body = String::new();
        families.render_into(&mut body);

        parse_exposition(&body)
            .into_iter()
            .filter(|((name, _), _)| name != "liq_collector_last_success_ts_seconds")
            .collect()
    }

    fn full_settings() -> st0x_dto::Settings {
        let asset = |symbol: &str, counter_trading, rebalancing, limit: Option<&str>| {
            st0x_dto::AssetSettings {
                symbol: Symbol::new(symbol).unwrap(),
                counter_trading,
                rebalancing,
                operational_limit: limit.map(str::to_string),
            }
        };

        st0x_dto::Settings {
            equity_target: Some(0.5),
            equity_deviation: 0.2,
            usdc_target: Some(0.4),
            usdc_deviation: None,
            cash_reserved: Some(Usd::new(Float::parse("1000.25".to_string()).unwrap())),
            execution_threshold: "$2.5".to_string(),
            assets: vec![
                asset(
                    "tAAPL",
                    st0x_dto::CounterTrading::Enabled {
                        extended_hours: true,
                    },
                    true,
                    None,
                ),
                asset(
                    "wtTSLA",
                    st0x_dto::CounterTrading::Enabled {
                        extended_hours: false,
                    },
                    false,
                    Some("100"),
                ),
                asset("tSPYM", st0x_dto::CounterTrading::Disabled, true, None),
                asset(
                    "RKLB",
                    st0x_dto::CounterTrading::Enabled {
                        extended_hours: false,
                    },
                    false,
                    None,
                ),
            ],
            wallet: Some(st0x_dto::WalletSettings {
                kind: "turnkey".to_string(),
                address: "0x0000000000000000000000000000000000000001".to_string(),
                organization_id: Some("00000000-0000-0000-0000-000000000001".to_string()),
            }),
            log_level: "Debug".to_string(),
            server_port: 8080,
            orderbook: "0x0000000000000000000000000000000000000002".to_string(),
            deployment_block: 12_345_678,
            broker: "alpaca".to_string(),
            order_polling_interval: 5,
            inventory_poll_interval: 15,
        }
    }

    fn minimal_settings() -> st0x_dto::Settings {
        st0x_dto::Settings {
            equity_target: None,
            equity_deviation: 0.1,
            usdc_target: None,
            usdc_deviation: None,
            cash_reserved: None,
            execution_threshold: "10 shares".to_string(),
            assets: Vec::new(),
            wallet: None,
            log_level: "Info".to_string(),
            server_port: 0,
            orderbook: "0x0000000000000000000000000000000000000002".to_string(),
            deployment_block: 0,
            broker: "alpaca".to_string(),
            order_polling_interval: 1,
            inventory_poll_interval: 1,
        }
    }

    /// Compares the builder with the exporter's committed output for the
    /// same fixture, on the names the `Settings` family owns. The exporter
    /// also emits inventory and dropped names from the same collector; those
    /// belong to other families or to no one. Only samples are compared: the
    /// exporter writes no `# TYPE` lines and the store types every name as a
    /// gauge.
    fn assert_matches_exporter_golden(
        settings: &st0x_dto::Settings,
        fixture_json: &str,
        golden: &str,
    ) {
        let fixture: Value = serde_json::from_str(fixture_json).unwrap();
        assert_eq!(serde_json::to_value(settings).unwrap(), fixture);

        let settings_names: Vec<&str> = LiqMetric::ALL
            .into_iter()
            .filter(|metric| metric.family() == Some(LiqFamily::Settings))
            .map(LiqMetric::name)
            .collect();
        let expected: BTreeMap<SeriesKey, f64> = parse_exposition(golden)
            .into_iter()
            .filter(|((name, _), _)| settings_names.contains(&name.as_str()))
            .collect();

        let actual = render(
            settings_samples(&input_from_dto(settings)),
            LiqFamily::Settings,
        );

        assert_eq!(actual, expected);
    }

    #[test]
    fn settings_match_the_exporter_golden() {
        assert_matches_exporter_golden(
            &full_settings(),
            include_str!("testdata/settings.json"),
            include_str!("testdata/settings.prom"),
        );
    }

    #[test]
    fn minimal_settings_match_the_exporter_golden() {
        assert_matches_exporter_golden(
            &minimal_settings(),
            include_str!("testdata/settings-minimal.json"),
            include_str!("testdata/settings-minimal.prom"),
        );
    }

    #[test]
    fn full_settings_publish_every_series_with_literal_values() {
        let rendered = render(
            settings_samples(&input_from_dto(&full_settings())),
            LiqFamily::Settings,
        );

        assert_eq!(
            rendered,
            BTreeMap::from([
                (
                    series(
                        "liq_settings_info",
                        &[
                            ("broker", "alpaca"),
                            ("log_level", "Debug"),
                            ("orderbook", "0x0000000000000000000000000000000000000002"),
                            ("server_port", "8080"),
                            ("trading_mode", ""),
                            (
                                "turnkey_organization",
                                "00000000-0000-0000-0000-000000000001"
                            ),
                            (
                                "wallet_address",
                                "0x0000000000000000000000000000000000000001"
                            ),
                            ("wallet_kind", "turnkey"),
                        ],
                    ),
                    1.0,
                ),
                (series("liq_settings_equity_target", &[]), 0.5),
                (series("liq_settings_equity_deviation", &[]), 0.2),
                (series("liq_settings_usdc_target", &[]), 0.4),
                (series("liq_settings_cash_reserved", &[]), 1000.25),
                (series("liq_settings_execution_threshold_usd", &[]), 2.5),
                (series("liq_settings_order_polling_seconds", &[]), 5.0),
                (series("liq_settings_inventory_poll_seconds", &[]), 15.0),
                (series("liq_settings_deployment_block", &[]), 12_345_678.0),
                (
                    series("liq_asset_counter_trading", &[("symbol", "AAPL")]),
                    1.0
                ),
                (
                    series("liq_asset_extended_hours", &[("symbol", "AAPL")]),
                    1.0
                ),
                (series("liq_asset_rebalancing", &[("symbol", "AAPL")]), 1.0),
                (
                    series("liq_asset_counter_trading", &[("symbol", "TSLA")]),
                    1.0
                ),
                (
                    series("liq_asset_extended_hours", &[("symbol", "TSLA")]),
                    0.0
                ),
                (series("liq_asset_rebalancing", &[("symbol", "TSLA")]), 0.0),
                (
                    series("liq_asset_counter_trading", &[("symbol", "SPYM")]),
                    0.0
                ),
                (series("liq_asset_rebalancing", &[("symbol", "SPYM")]), 1.0),
                (
                    series("liq_asset_counter_trading", &[("symbol", "RKLB")]),
                    1.0
                ),
                (
                    series("liq_asset_extended_hours", &[("symbol", "RKLB")]),
                    0.0
                ),
                (series("liq_asset_rebalancing", &[("symbol", "RKLB")]), 0.0),
            ])
        );
    }

    #[test]
    fn unconfigured_settings_are_absent_and_empty_labels_stay_empty() {
        let rendered = render(
            settings_samples(&input_from_dto(&minimal_settings())),
            LiqFamily::Settings,
        );

        assert_eq!(
            rendered,
            BTreeMap::from([
                (
                    series(
                        "liq_settings_info",
                        &[
                            ("broker", "alpaca"),
                            ("log_level", "Info"),
                            ("orderbook", "0x0000000000000000000000000000000000000002"),
                            ("server_port", ""),
                            ("trading_mode", ""),
                            ("turnkey_organization", ""),
                            ("wallet_address", ""),
                            ("wallet_kind", ""),
                        ],
                    ),
                    1.0,
                ),
                (series("liq_settings_equity_deviation", &[]), 0.1),
                (series("liq_settings_order_polling_seconds", &[]), 1.0),
                (series("liq_settings_inventory_poll_seconds", &[]), 1.0),
                (series("liq_settings_deployment_block", &[]), 0.0),
            ])
        );
    }

    #[test]
    fn a_private_key_wallet_publishes_an_empty_turnkey_organization() {
        let mut settings = minimal_settings();
        settings.wallet = Some(st0x_dto::WalletSettings {
            kind: "private-key".to_string(),
            address: "0x0000000000000000000000000000000000000003".to_string(),
            organization_id: None,
        });

        let rendered = render(
            settings_samples(&input_from_dto(&settings)),
            LiqFamily::Settings,
        );

        let info = rendered
            .keys()
            .find(|(name, _)| name == "liq_settings_info")
            .unwrap();
        let labels: BTreeMap<&str, &str> = info
            .1
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str()))
            .collect();
        assert_eq!(labels["wallet_kind"], "private-key");
        assert_eq!(
            labels["wallet_address"],
            "0x0000000000000000000000000000000000000003"
        );
        assert_eq!(labels["turnkey_organization"], "");
    }

    #[test]
    fn an_integer_setting_beyond_exact_f64_range_is_absent() {
        let mut settings = minimal_settings();
        settings.deployment_block = (1 << 53) + 1;

        let rendered = render(
            settings_samples(&input_from_dto(&settings)),
            LiqFamily::Settings,
        );

        assert!(
            !rendered
                .keys()
                .any(|(name, _)| name == "liq_settings_deployment_block"),
            "{rendered:?}"
        );
    }

    /// The builder reads the config directly; the dashboard reads it through
    /// its own mapping. Both must describe the same settings.
    #[test]
    fn ctx_input_matches_the_dashboard_settings_for_the_same_config() {
        let mut ctx = create_test_ctx_with_order_owner(address!(
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        ));
        let primary = ctx.chains.primary().chain;
        for (symbol, trading, rebalancing, extended_hours) in [
            (
                "RKLB",
                OperationMode::Enabled,
                RebalancingMode::Disabled,
                OperationMode::Enabled,
            ),
            (
                "tAAPL",
                OperationMode::Disabled,
                RebalancingMode::Paused,
                OperationMode::Disabled,
            ),
        ] {
            let symbol = Symbol::new(symbol).unwrap();
            ctx.chains.primary_mut().assets.equities.symbols.insert(
                symbol.clone(),
                ChainEquityAsset {
                    tokenized_equity: address!("0x1111111111111111111111111111111111111111"),
                    tokenized_equity_derivative: address!(
                        "0x2222222222222222222222222222222222222222"
                    ),
                    vault_ids: Vec::new(),
                    trading,
                    rebalancing,
                    wrapped_equity_recovery: OperationMode::Disabled,
                    operational_limit: None,
                    target_share: None,
                },
            );
            ctx.assets.equities.symbols.insert(
                symbol,
                EquityHedgePolicy {
                    extended_hours_counter_trading: extended_hours,
                    hedge_floor_shares: None,
                },
            );
        }
        assert!(ctx.rebalancing.allocation.targets.contains_key(&primary));

        // The dashboard settings carry one USDC band, not each corridor's.
        let from_ctx: BTreeMap<SeriesKey, f64> = render(
            settings_samples(&SettingsInput::from_ctx(&ctx)),
            LiqFamily::Settings,
        )
        .into_iter()
        .filter(|((name, _), _)| !name.starts_with("liq_usdc_corridor_"))
        .collect();
        let from_dashboard = render(
            settings_samples(&input_from_dto(&settings_from_ctx(&ctx))),
            LiqFamily::Settings,
        );

        assert_eq!(from_ctx, from_dashboard);
        assert_eq!(
            from_ctx.get(&series("liq_asset_extended_hours", &[("symbol", "RKLB")])),
            Some(&1.0)
        );
        assert_eq!(
            from_ctx.get(&series("liq_asset_counter_trading", &[("symbol", "AAPL")])),
            Some(&0.0)
        );
    }

    /// The same parity on the config branches the default test config does
    /// not reach: each changes one thing the dashboard settings derive from.
    #[test]
    fn ctx_input_matches_the_dashboard_settings_on_every_config_branch() {
        let base = || {
            create_test_ctx_with_order_owner(address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"))
        };
        let usdc = |base_cash: OperationMode, corridors: Vec<UsdcCorridorCtx>| {
            let mut ctx = two_chain_ctx(base_cash);
            ctx.rebalancing.usdc = UsdcCorridors::for_test(OperationMode::Enabled, corridors);
            ctx
        };
        let enabled = OperationMode::Enabled;
        let base_corridor = corridor(Chain::Base, HopKind::Cctp, float!(0.5), float!(0.3));
        let hyperevm_corridor =
            corridor(Chain::HyperEvm, HopKind::Relay, float!(0.25), float!(0.125));

        let mut no_equity_target = base();
        let primary = no_equity_target.chains.primary().chain;
        no_equity_target
            .rebalancing
            .allocation
            .targets
            .remove(&primary);

        let mut dollar_threshold = base();
        dollar_threshold.execution_threshold =
            ExecutionThreshold::dollar_value(Usdc::new(float!(2.5))).unwrap();

        let mut wallet = base();
        wallet.wallet_meta = Some(WalletMeta {
            kind: "turnkey".to_string(),
            address: address!("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
            organization_id: Some("org-1".to_string()),
        });

        let mut cash_reserve = base();
        cash_reserve.assets.cash = Some(CashHedgePolicy {
            reserved: Positive::new(Usd::new(float!(1500))).unwrap(),
        });

        let mut port_zero = base();
        port_zero.server_port = 0;

        let variants = vec![
            ("one active corridor", usdc(enabled, vec![base_corridor])),
            (
                "one active corridor off the primary chain",
                usdc(enabled, vec![hyperevm_corridor]),
            ),
            (
                "several active corridors",
                usdc(enabled, vec![base_corridor, hyperevm_corridor]),
            ),
            (
                "several corridors, the primary chain's cash not rebalancing",
                usdc(
                    OperationMode::Disabled,
                    vec![base_corridor, hyperevm_corridor],
                ),
            ),
            ("no equity target", no_equity_target),
            ("a dollar execution threshold", dollar_threshold),
            ("a wallet", wallet),
            ("a cash reserve", cash_reserve),
            ("server port 0", port_zero),
        ];

        for (name, ctx) in variants {
            let from_ctx: BTreeMap<SeriesKey, f64> = render(
                settings_samples(&SettingsInput::from_ctx(&ctx)),
                LiqFamily::Settings,
            )
            .into_iter()
            .filter(|((name, _), _)| !name.starts_with("liq_usdc_corridor_"))
            .collect();
            let from_dashboard = render(
                settings_samples(&input_from_dto(&settings_from_ctx(&ctx))),
                LiqFamily::Settings,
            );
            assert_eq!(from_ctx, from_dashboard, "{name}");
        }
    }

    fn corridor(chain: Chain, hop: HopKind, target: Float, deviation: Float) -> UsdcCorridorCtx {
        UsdcCorridorCtx {
            corridor: UsdcCorridor::HubRouted { chain, hop },
            threshold: ImbalanceThreshold { target, deviation },
        }
    }

    fn cash(rebalancing: OperationMode) -> ChainCashAsset {
        ChainCashAsset {
            vault_ids: Vec::new(),
            rebalancing,
            operational_limit: None,
        }
    }

    /// The test config (Base primary, no cash asset) with Base's cash
    /// rebalancing set to `base` and HyperEvm added as a hedged chain whose
    /// cash rebalances.
    fn two_chain_ctx(base: OperationMode) -> Ctx {
        let mut ctx = create_test_ctx_with_order_owner(address!(
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        ));
        assert_eq!(ctx.chains.primary().chain, Chain::Base);
        ctx.chains.primary_mut().assets.cash = Some(cash(base));
        ctx.chains.insert_secondary(
            HedgedChain::test()
                .chain(Chain::HyperEvm)
                .assets(ChainAssets {
                    cash: Some(cash(OperationMode::Enabled)),
                    ..ChainAssets::default()
                })
                .call(),
        );
        ctx
    }

    fn corridor_series(ctx: &Ctx) -> BTreeMap<SeriesKey, f64> {
        render(
            settings_samples(&SettingsInput::from_ctx(ctx)),
            LiqFamily::Settings,
        )
        .into_iter()
        .filter(|((name, _), _)| name.starts_with("liq_usdc_corridor_"))
        .collect()
    }

    /// Each corridor table gets its own band, labelled by chain, while the
    /// single USDC band stays the primary chain's.
    #[test]
    fn every_usdc_corridor_band_is_published_by_chain() {
        let mut ctx = two_chain_ctx(OperationMode::Enabled);
        ctx.rebalancing.usdc = UsdcCorridors::for_test(
            OperationMode::Enabled,
            [
                corridor(Chain::HyperEvm, HopKind::Relay, float!(0.25), float!(0.125)),
                corridor(Chain::Base, HopKind::Cctp, float!(0.5), float!(0.3)),
            ],
        );

        assert_eq!(
            corridor_series(&ctx),
            BTreeMap::from([
                (
                    series("liq_usdc_corridor_target", &[("chain", "base")]),
                    0.5
                ),
                (
                    series("liq_usdc_corridor_deviation", &[("chain", "base")]),
                    0.3
                ),
                (
                    series("liq_usdc_corridor_active", &[("chain", "base")]),
                    1.0
                ),
                (
                    series("liq_usdc_corridor_target", &[("chain", "hyperevm")]),
                    0.25
                ),
                (
                    series("liq_usdc_corridor_deviation", &[("chain", "hyperevm")]),
                    0.125
                ),
                (
                    series("liq_usdc_corridor_active", &[("chain", "hyperevm")]),
                    1.0
                ),
            ])
        );
        let rendered = render(
            settings_samples(&SettingsInput::from_ctx(&ctx)),
            LiqFamily::Settings,
        );
        assert_eq!(
            rendered.get(&series("liq_settings_usdc_target", &[])),
            Some(&0.5)
        );
    }

    /// USDC mode disabled stops new transfers, not the corridor tables: their
    /// bands are still published, marked inactive, while the single USDC band
    /// is absent.
    #[test]
    fn usdc_corridor_bands_are_published_while_usdc_mode_is_disabled() {
        // Base rebalances its cash, so only the disabled mode can mark the
        // corridor inactive.
        let mut ctx = two_chain_ctx(OperationMode::Enabled);
        ctx.rebalancing.usdc = UsdcCorridors::for_test(
            OperationMode::Disabled,
            [corridor(
                Chain::Base,
                HopKind::Cctp,
                float!(0.5),
                float!(0.3),
            )],
        );

        assert_eq!(
            corridor_series(&ctx),
            BTreeMap::from([
                (
                    series("liq_usdc_corridor_target", &[("chain", "base")]),
                    0.5
                ),
                (
                    series("liq_usdc_corridor_deviation", &[("chain", "base")]),
                    0.3
                ),
                (
                    series("liq_usdc_corridor_active", &[("chain", "base")]),
                    0.0
                ),
            ])
        );
        let rendered = render(
            settings_samples(&SettingsInput::from_ctx(&ctx)),
            LiqFamily::Settings,
        );
        assert_eq!(rendered.get(&series("liq_settings_usdc_target", &[])), None);
        assert_eq!(
            rendered.get(&series("liq_settings_usdc_deviation", &[])),
            None
        );
    }

    /// The USDC trigger skips a corridor whose chain does not rebalance its
    /// cash asset, so that corridor is inactive while the USDC mode is enabled
    /// and the other chain's corridor stays active.
    #[test]
    fn a_corridor_is_inactive_while_its_chain_cash_rebalancing_is_disabled() {
        let mut ctx = two_chain_ctx(OperationMode::Disabled);
        ctx.rebalancing.usdc = UsdcCorridors::for_test(
            OperationMode::Enabled,
            [
                corridor(Chain::HyperEvm, HopKind::Relay, float!(0.25), float!(0.125)),
                corridor(Chain::Base, HopKind::Cctp, float!(0.5), float!(0.3)),
            ],
        );

        let rendered = corridor_series(&ctx);

        assert_eq!(
            rendered.get(&series("liq_usdc_corridor_active", &[("chain", "base")])),
            Some(&0.0)
        );
        assert_eq!(
            rendered.get(&series(
                "liq_usdc_corridor_active",
                &[("chain", "hyperevm")]
            )),
            Some(&1.0)
        );
        assert_eq!(
            rendered.get(&series("liq_usdc_corridor_target", &[("chain", "base")])),
            Some(&0.5)
        );
    }

    /// The single USDC band comes only from corridors the trigger can act on:
    /// with Base's cash not rebalancing, the HyperEvm corridor is the only
    /// one, so both the exporter and the dashboard settings show its band,
    /// not the primary chain's.
    #[test]
    fn the_single_usdc_band_skips_a_corridor_whose_chain_cash_does_not_rebalance() {
        let mut ctx = two_chain_ctx(OperationMode::Disabled);
        ctx.rebalancing.usdc = UsdcCorridors::for_test(
            OperationMode::Enabled,
            [
                corridor(Chain::HyperEvm, HopKind::Relay, float!(0.25), float!(0.125)),
                corridor(Chain::Base, HopKind::Cctp, float!(0.5), float!(0.3)),
            ],
        );

        let rendered = render(
            settings_samples(&SettingsInput::from_ctx(&ctx)),
            LiqFamily::Settings,
        );
        assert_eq!(
            rendered.get(&series("liq_settings_usdc_target", &[])),
            Some(&0.25)
        );
        assert_eq!(
            rendered.get(&series("liq_settings_usdc_deviation", &[])),
            Some(&0.125)
        );

        let settings = settings_from_ctx(&ctx);
        assert_eq!(settings.usdc_target, Some(0.25));
        assert_eq!(settings.usdc_deviation, Some(0.125));
    }

    #[test]
    fn health_publishes_the_short_commit_and_the_process_start() {
        let rendered = render(
            health_samples(
                "0123456789abcdef0123",
                UNIX_EPOCH + Duration::from_millis(1_700_000_000_500),
            ),
            LiqFamily::Health,
        );

        assert_eq!(
            rendered,
            BTreeMap::from([
                (
                    series("liq_bot_info", &[("git_commit", "0123456789ab")]),
                    1.0
                ),
                (
                    series("liq_bot_start_timestamp_seconds", &[]),
                    1_700_000_000.5
                ),
            ])
        );
    }

    #[test]
    fn publish_boot_families_publishes_health_and_settings() {
        let ctx = create_test_ctx_with_order_owner(address!(
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        ));
        let families = LiqFamilies::default();

        publish_boot_families(&ctx, "dev", UNIX_EPOCH, &families);

        let mut body = String::new();
        families.render_into(&mut body);
        let rendered = parse_exposition(&body);
        assert_eq!(
            rendered.get(&series("liq_bot_info", &[("git_commit", "dev")])),
            Some(&1.0)
        );
        assert_eq!(
            rendered.get(&series("liq_bot_start_timestamp_seconds", &[])),
            Some(&0.0)
        );
        let collectors: Vec<&str> = rendered
            .keys()
            .filter(|(name, _)| name == "liq_collector_last_success_ts_seconds")
            .map(|(_, labels)| labels[0].1.as_str())
            .collect();
        assert_eq!(collectors, ["health", "settings"]);
        assert_eq!(
            rendered.get(&series("liq_settings_deployment_block", &[])),
            Some(&integer_value(ctx.chains.primary().deployment_block).unwrap())
        );
    }
}
