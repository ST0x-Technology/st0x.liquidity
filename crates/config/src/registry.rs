//! The per-symbol tables, read from T0's token file in the bucket.
//!
//! One file, `t0/<env>.toml` in st0x.registry, holds every token's config
//! for every T0 service; its CI uploads it to
//! `gs://t0-artifacts-tokens/<env>/tokens.toml`. The bot takes from it
//! exactly the tables its own config used to carry per symbol:
//! `[chains.<c>.trading.assets.equities.<SYM>]` (addresses, vault ids and
//! the trading/rebalancing/recovery switches) and `[assets.equities.<SYM>]`
//! (the hedge policy). The chain-wide keys, cash, retired symbols and
//! everything else stay in the bot's own config.
//!
//! The projection runs on the parsed TOML table before it is deserialized
//! into `Config`, so the rows are exactly what the inline tables held and
//! every existing validation runs unchanged on the result.
//!
//! This reads at boot. A refresh loop (in the bot crate, where the metrics
//! live) re-reads the file and reports on a gauge when the bucket copy
//! differs from what this instance runs, or would be refused at boot; it
//! does not apply the change. So a token change still takes a roll, from
//! one file.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::Deserialize;
use thiserror::Error;
use toml::{Table, Value};
use url::Url;

pub const SCHEMA_VERSION: i64 = 1;

/// Most time boot may spend reading the file, retries included.
pub const BOOT_READ_BUDGET: Duration = Duration::from_secs(20);

const MAX_BODY: usize = 4 << 20;

/// The keys a chain row may carry, as `ChainEquityAsset` reads them
/// (`vault_id` is its alias for `vault_ids`).
const CHAIN_ROW_KEYS: [&str; 9] = [
    "tokenized_equity",
    "tokenized_equity_derivative",
    "vault_ids",
    "vault_id",
    "trading",
    "rebalancing",
    "wrapped_equity_recovery",
    "operational_limit",
    "target_share",
];

/// The keys a hedge policy may carry, as `EquityHedgePolicy` reads them.
const POLICY_KEYS: [&str; 2] = ["extended_hours_counter_trading", "hedge_floor_shares"];

/// `[registry]` in the bot config.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RegistrySource {
    /// `gs://bucket/object`, read with the VM's service account.
    pub url: String,
    /// Pin to one object generation, so every roll of a release runs the
    /// same tokens and a change ships only with a release. Absent = the
    /// latest copy.
    #[serde(default)]
    pub generation: Option<u64>,
}

#[derive(Debug, Error)]
pub enum RegistryError {
    #[error("[registry] takes `url` and, optionally, `generation`; nothing else")]
    Source(#[source] toml::de::Error),
    #[error("[registry] url {url:?} must be gs://<bucket>/<object>")]
    Url { url: String },
    #[error("token file is not UTF-8")]
    Utf8(#[source] std::str::Utf8Error),
    #[error("token file is not valid TOML")]
    Toml(#[source] toml::de::Error),
    #[error("token file schema_version must be {SCHEMA_VERSION}, got {got}")]
    SchemaVersion { got: String },
    #[error("token file: {what} is missing or not a table")]
    NotATable { what: String },
    #[error("token file: {what}.{key} must be \"enabled\" or \"disabled\"")]
    BadSwitch { what: String, key: &'static str },
    #[error("token file: {what}.{key} missing")]
    MissingAddress { what: String, key: &'static str },
    #[error("token file: no slot carries the bot's keys; refusing an empty universe")]
    EmptyUniverse,
    #[error(
        "token file lists hedged slots on chain {chain}, which this config does not declare \
         with a [chains.{chain}.trading] table; a chain needs an rpc_url and signing, which \
         are release-time facts"
    )]
    UndeclaredChain { chain: String },
    #[error(
        "the config reads the per-symbol tables from the bucket but also carries [{table}]; \
         keep one source"
    )]
    InlineTable { table: String },
    #[error("reading {}", path.display())]
    LocalRead {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("building the HTTP client")]
    Client(#[source] reqwest::Error),
    #[error("requesting a token from the metadata server")]
    MetadataToken(#[source] reqwest::Error),
    #[error("reading {url}")]
    Http {
        url: String,
        #[source]
        source: reqwest::Error,
    },
    #[error("reading {url}: {status}: {body}")]
    Status {
        url: String,
        status: reqwest::StatusCode,
        body: String,
    },
    #[error("reading {url}: larger than {MAX_BODY} bytes")]
    TooLarge { url: String },
    #[error("reading {url}: boot read exceeded {}s", BOOT_READ_BUDGET.as_secs())]
    BootTimeout {
        url: String,
        /// The last failed attempt before the budget ran out, if one ended.
        #[source]
        last: Option<Box<Self>>,
    },
}

impl RegistryError {
    /// A copy boot refuses as well, gone, refused to the service account or
    /// over the size cap, as opposed to a read that may succeed next time.
    pub fn copy_is_unusable(&self) -> bool {
        match self {
            Self::Status { status, .. } => matches!(
                *status,
                reqwest::StatusCode::NOT_FOUND
                    | reqwest::StatusCode::UNAUTHORIZED
                    | reqwest::StatusCode::FORBIDDEN
            ),
            Self::TooLarge { .. } => true,
            Self::Source(_)
            | Self::Url { .. }
            | Self::Utf8(_)
            | Self::Toml(_)
            | Self::SchemaVersion { .. }
            | Self::NotATable { .. }
            | Self::BadSwitch { .. }
            | Self::MissingAddress { .. }
            | Self::EmptyUniverse
            | Self::UndeclaredChain { .. }
            | Self::InlineTable { .. }
            | Self::LocalRead { .. }
            | Self::Client(_)
            | Self::MetadataToken(_)
            | Self::Http { .. }
            | Self::BootTimeout { .. } => false,
        }
    }
}

/// The token files the tests read, from `tests/fixtures`.
///
/// Two production copies exist on purpose. `tokens-production-migration.toml`
/// is frozen beside `production-inline.toml`, the pair that proves the
/// projection reproduces the inline tables the day they were replaced. The
/// copy production runs is the one `config/prod`'s pin names,
/// `tokens-production-<generation>.toml`; a pin bump adds that file.
#[cfg(test)]
pub(crate) mod fixtures {
    use std::path::{Path, PathBuf};

    fn root() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
    }

    pub(crate) fn read(name: &str) -> Vec<u8> {
        std::fs::read(root().join("tests/fixtures").join(name))
            .unwrap_or_else(|error| panic!("tests/fixtures/{name}: {error}"))
    }

    /// The copy `config/prod` pins, by generation.
    pub(crate) fn pinned_production_tokens() -> Vec<u8> {
        let deployed: toml::Table = toml::from_str(
            &std::fs::read_to_string(root().join("config/prod/st0x-hedge.toml")).unwrap(),
        )
        .unwrap();
        let pinned = super::source_of(&deployed)
            .unwrap()
            .unwrap()
            .generation
            .expect("config/prod pins a generation");
        let name = format!("tokens-production-{pinned}.toml");
        assert!(
            root().join("tests/fixtures").join(&name).is_file(),
            "config/prod pins generation {pinned}; copy that object to tests/fixtures/{name} \
             (gcloud storage cp 'gs://t0-artifacts-tokens/production/tokens.toml#{pinned}' ...)"
        );
        read(&name)
    }
}

/// How the token file reaches a parse of the config.
#[derive(Debug, Clone, Copy)]
pub enum TokenFile<'a> {
    /// The bytes of the file `[registry]` names, from the bucket at boot or
    /// from `--registry-file`.
    Bytes(&'a [u8]),
    /// Nothing fetched. A config that names `[registry]` is then judged
    /// without its per-symbol tables; only the offline config check does
    /// this, boot never does.
    Skipped,
}

/// What the bot takes from the file, as TOML tables ready to merge.
#[derive(Debug, Clone, PartialEq)]
pub struct Projection {
    /// chain name -> symbol -> `ChainEquityAsset` row.
    pub chain_rows: BTreeMap<String, BTreeMap<String, Table>>,
    /// symbol -> `EquityHedgePolicy` row.
    pub policies: BTreeMap<String, Table>,
}

impl Projection {
    /// The projection as the merge takes it: without the symbols the config
    /// retires. What runs and what the refresh loop compares are both this,
    /// so deleting a retired symbol's rows from the file is not a change.
    #[must_use]
    pub fn without_retired(mut self, config: &Table) -> Self {
        let retired = retired_symbols(config);
        for rows in self.chain_rows.values_mut() {
            rows.retain(|symbol, _| !retired.contains(symbol));
        }
        self.policies.retain(|symbol, _| !retired.contains(symbol));
        self
    }

    /// `chain/SYMBOL` of every hedged slot.
    pub fn slots(&self) -> BTreeSet<String> {
        self.chain_rows
            .iter()
            .flat_map(|(chain, rows)| rows.keys().map(move |symbol| format!("{chain}/{symbol}")))
            .collect()
    }
}

pub fn source_of(config: &Table) -> Result<Option<RegistrySource>, RegistryError> {
    match config.get("registry") {
        None => Ok(None),
        Some(value) => {
            let source: RegistrySource = value.clone().try_into().map_err(RegistryError::Source)?;
            parse_gs_url(&source.url)?;
            Ok(Some(source))
        }
    }
}

pub fn parse_gs_url(gs_url: &str) -> Result<(&str, &str), RegistryError> {
    match gs_url
        .strip_prefix("gs://")
        .and_then(|rest| rest.split_once('/'))
    {
        Some((bucket, object)) if !bucket.is_empty() && !object.is_empty() => Ok((bucket, object)),
        _ => Err(RegistryError::Url {
            url: gs_url.to_string(),
        }),
    }
}

pub fn parse(bytes: &[u8]) -> Result<Table, RegistryError> {
    let text = std::str::from_utf8(bytes).map_err(RegistryError::Utf8)?;
    toml::from_str(text).map_err(RegistryError::Toml)
}

fn table<'a>(value: Option<&'a Value>, what: &str) -> Result<&'a Table, RegistryError> {
    value
        .and_then(Value::as_table)
        .ok_or_else(|| RegistryError::NotATable {
            what: what.to_string(),
        })
}

fn is_switch(value: Option<&Value>) -> bool {
    matches!(value.and_then(Value::as_str), Some("enabled" | "disabled"))
}

/// Turn the token file into the bot's per-symbol tables.
///
/// A chain row is taken from every slot that carries any of the bot's own
/// keys; a slot that is only priced is not the bot's. A policy is taken from
/// every `[assets.equities.<SYM>]` that sets any of the bot's policy keys.
/// The bot reads its own keys and leaves the rest to the services that own
/// them: which keys may appear at all, and their spelling, is checked by
/// st0x.registry's CI (`t0/check.jq`) before the file is published, so a
/// new key of another service cannot fail a boot here. Anything malformed in
/// the bot's own keys is refused, never dropped.
pub fn project(file: &Table) -> Result<Projection, RegistryError> {
    match file.get("schema_version") {
        Some(Value::Integer(version)) if *version == SCHEMA_VERSION => {}
        other => {
            return Err(RegistryError::SchemaVersion {
                got: other.map_or_else(|| "nothing".to_string(), Value::to_string),
            });
        }
    }
    let chains = table(file.get("chains"), "chains")?;
    let equities = table(
        file.get("assets").and_then(|assets| assets.get("equities")),
        "assets.equities",
    )?;

    let mut chain_rows: BTreeMap<String, BTreeMap<String, Table>> = BTreeMap::new();
    for (name, chain) in chains {
        let chain = table(Some(chain), &format!("chains.{name}"))?;
        let Some(slots) = chain
            .get("assets")
            .and_then(|assets| assets.get("equities"))
        else {
            continue;
        };
        let slots = table(Some(slots), &format!("chains.{name}.assets.equities"))?;
        for (symbol, slot) in slots {
            let what = format!("chains.{name}.assets.equities.{symbol}");
            let slot = table(Some(slot), &what)?;
            let ours = [
                "trading",
                "rebalancing",
                "wrapped_equity_recovery",
                "tokenized_equity",
            ]
            .iter()
            .any(|key| slot.contains_key(*key));
            if !ours {
                continue;
            }
            for key in ["trading", "rebalancing", "wrapped_equity_recovery"] {
                if !is_switch(slot.get(key)) {
                    return Err(RegistryError::BadSwitch { what, key });
                }
            }
            for key in ["tokenized_equity", "tokenized_equity_derivative"] {
                if slot.get(key).and_then(Value::as_str).is_none() {
                    return Err(RegistryError::MissingAddress { what, key });
                }
            }
            let mut row = Table::new();
            for key in CHAIN_ROW_KEYS {
                if let Some(value) = slot.get(key) {
                    row.insert(key.to_string(), value.clone());
                }
            }
            chain_rows
                .entry(name.clone())
                .or_default()
                .insert(symbol.clone(), row);
        }
    }

    let mut policies = BTreeMap::new();
    for (symbol, policy) in equities {
        let what = format!("assets.equities.{symbol}");
        let policy = table(Some(policy), &what)?;
        if !POLICY_KEYS.iter().any(|key| policy.contains_key(*key)) {
            continue;
        }
        if !is_switch(policy.get("extended_hours_counter_trading")) {
            return Err(RegistryError::BadSwitch {
                what,
                key: "extended_hours_counter_trading",
            });
        }
        let mut row = Table::new();
        for key in POLICY_KEYS {
            if let Some(value) = policy.get(key) {
                row.insert(key.to_string(), value.clone());
            }
        }
        policies.insert(symbol.clone(), row);
    }

    if chain_rows.values().all(BTreeMap::is_empty) {
        return Err(RegistryError::EmptyUniverse);
    }
    Ok(Projection {
        chain_rows,
        policies,
    })
}

/// `parent.key` as a table, created when absent. A non-table value in the
/// way is a config error and is refused rather than overwritten.
fn subtable<'a>(
    parent: &'a mut Table,
    key: &str,
    what: &str,
) -> Result<&'a mut Table, RegistryError> {
    match parent
        .entry(key.to_string())
        .or_insert_with(|| Value::Table(Table::new()))
    {
        Value::Table(table) => Ok(table),
        _ => Err(RegistryError::NotATable {
            what: what.to_string(),
        }),
    }
}

/// Refuse a config that names `[registry]` and still carries a per-symbol
/// table of its own: one source of truth.
pub fn refuse_inline_tables(config: &Table) -> Result<(), RegistryError> {
    // Every key of an `equities` table that is itself a table is a symbol's
    // (the chain-wide keys, `operational_limit` and `retired_symbols`, are
    // not tables), whatever its spelling or case.
    let inline_symbol = |equities: Option<&Value>| {
        equities.and_then(Value::as_table).and_then(|equities| {
            equities
                .iter()
                .find(|(_, value)| value.is_table())
                .map(|(symbol, _)| symbol.clone())
        })
    };

    for (chain, chain_config) in config
        .get("chains")
        .and_then(Value::as_table)
        .into_iter()
        .flatten()
    {
        let equities = chain_config
            .get("trading")
            .and_then(|trading| trading.get("assets"))
            .and_then(|assets| assets.get("equities"));
        if let Some(symbol) = inline_symbol(equities) {
            return Err(RegistryError::InlineTable {
                table: format!("chains.{chain}.trading.assets.equities.{symbol}"),
            });
        }
    }
    let equities = config
        .get("assets")
        .and_then(|assets| assets.get("equities"));
    if let Some(symbol) = inline_symbol(equities) {
        return Err(RegistryError::InlineTable {
            table: format!("assets.equities.{symbol}"),
        });
    }
    Ok(())
}

/// The symbols the config lists under `[assets.equities] retired_symbols`.
fn retired_symbols(config: &Table) -> BTreeSet<String> {
    config
        .get("assets")
        .and_then(|assets| assets.get("equities"))
        .and_then(|equities| equities.get("retired_symbols"))
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(str::to_string)
        .collect()
}

/// Put the projected tables into a config table that reads `[registry]`.
///
/// The config must not carry any per-symbol table itself (one source of
/// truth). Chain-wide keys such as `operational_limit`, `cash`, and
/// `retired_symbols` are the config's own and are left as they are. A
/// symbol the config retires is not taken from the file at all, so a
/// retirement is one config change and the file's rows can go later. A
/// chain the file lists but the config does not declare with a `trading`
/// table is refused: a chain needs an rpc_url and signing, which are
/// release-time facts.
pub fn merge(config: &mut Table, projection: &Projection) -> Result<(), RegistryError> {
    refuse_inline_tables(config)?;
    let projection = projection.clone().without_retired(config);

    let chains = subtable(config, "chains", "chains")?;
    for (chain, rows) in &projection.chain_rows {
        if rows.is_empty() {
            continue;
        }
        let Some(trading) = chains
            .get_mut(chain)
            .and_then(|chain_config| chain_config.get_mut("trading"))
            .and_then(Value::as_table_mut)
        else {
            return Err(RegistryError::UndeclaredChain {
                chain: chain.clone(),
            });
        };
        let assets = subtable(trading, "assets", &format!("chains.{chain}.trading.assets"))?;
        let equities = subtable(
            assets,
            "equities",
            &format!("chains.{chain}.trading.assets.equities"),
        )?;
        for (symbol, row) in rows {
            equities.insert(symbol.clone(), Value::Table(row.clone()));
        }
    }

    let assets = subtable(config, "assets", "assets")?;
    let equities = subtable(assets, "equities", "assets.equities")?;
    for (symbol, row) in &projection.policies {
        equities.insert(symbol.clone(), Value::Table(row.clone()));
    }
    Ok(())
}

/// The JSON API URL of one object: `/b/<bucket>/o/<object>?alt=media`, the
/// object name as one path segment (`/` in it becomes `%2F`), plus
/// `generation` when pinned.
fn object_url(bucket: &str, object: &str, generation: Option<u64>) -> Result<Url, RegistryError> {
    let mut url = Url::parse("https://storage.googleapis.com/storage/v1/").map_err(|_| {
        RegistryError::Url {
            url: format!("gs://{bucket}/{object}"),
        }
    })?;
    url.path_segments_mut()
        .map_err(|()| RegistryError::Url {
            url: format!("gs://{bucket}/{object}"),
        })?
        .pop_if_empty()
        .extend(["b", bucket, "o", object]);
    {
        let mut query = url.query_pairs_mut();
        query.append_pair("alt", "media");
        if let Some(generation) = generation {
            query.append_pair("generation", &generation.to_string());
        }
    }
    Ok(url)
}

/// A client for the metadata server and Cloud Storage: no proxy (the bearer
/// token must travel only the direct link) and no redirects.
pub fn http_client() -> Result<reqwest::Client, RegistryError> {
    reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(4))
        .build()
        .map_err(RegistryError::Client)
}

/// The VM service account's access token.
/// See <https://cloud.google.com/compute/docs/access/authenticate-workloads#applications>.
async fn access_token(http: &reqwest::Client) -> Result<String, RegistryError> {
    #[derive(Deserialize)]
    struct Token {
        access_token: String,
    }
    let token: Token = http
        .get("http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token")
        .header("Metadata-Flavor", "Google")
        .send()
        .await
        .and_then(reqwest::Response::error_for_status)
        .map_err(RegistryError::MetadataToken)?
        .json()
        .await
        .map_err(RegistryError::MetadataToken)?;
    Ok(token.access_token)
}

/// One `objects.get` with `alt=media` as the VM's service account; with a
/// generation, exactly that immutable version of the object.
/// See <https://cloud.google.com/storage/docs/json_api/v1/objects/get>.
pub async fn fetch(
    http: &reqwest::Client,
    gs_url: &str,
    generation: Option<u64>,
) -> Result<Vec<u8>, RegistryError> {
    let (bucket, object) = parse_gs_url(gs_url)?;
    let url = object_url(bucket, object, generation)?;
    let token = access_token(http).await?;
    let named = || {
        generation.map_or_else(
            || gs_url.to_string(),
            |generation| format!("{gs_url}#{generation}"),
        )
    };
    let http_error = |source| RegistryError::Http {
        url: named(),
        source,
    };
    let mut response = http
        .get(url)
        .bearer_auth(token)
        .send()
        .await
        .map_err(http_error)?;
    if response
        .content_length()
        .is_some_and(|length| length > MAX_BODY as u64)
    {
        return Err(RegistryError::TooLarge { url: named() });
    }
    let status = response.status();
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(http_error)? {
        if body.len() + chunk.len() > MAX_BODY {
            return Err(RegistryError::TooLarge { url: named() });
        }
        body.extend_from_slice(&chunk);
    }
    if !status.is_success() {
        return Err(RegistryError::Status {
            url: named(),
            status,
            body: String::from_utf8_lossy(&body).chars().take(300).collect(),
        });
    }
    Ok(body)
}

/// The token file bytes for boot: a local file (`--registry-file`) or the
/// bucket, the latter bounded as a whole so a wedged read cannot stall
/// the roll's health gate.
pub async fn load_bytes(
    source: &RegistrySource,
    local: Option<&Path>,
) -> Result<Vec<u8>, RegistryError> {
    if let Some(path) = local {
        return std::fs::read(path).map_err(|source| RegistryError::LocalRead {
            path: path.to_path_buf(),
            source,
        });
    }
    let http = http_client()?;
    // No tracing subscriber exists yet at boot, so a failed attempt is kept
    // for the error rather than logged: the budget running out still names
    // why the attempts before it failed.
    let mut last = None;
    let read = async {
        let mut attempt = 1;
        loop {
            match fetch(&http, &source.url, source.generation).await {
                Ok(bytes) => break Ok(bytes),
                Err(error) if attempt < 3 => {
                    last = Some(Box::new(error));
                    tokio::time::sleep(Duration::from_secs(1 << attempt)).await;
                    attempt += 1;
                }
                Err(error) => break Err(error),
            }
        }
    };
    let outcome = tokio::time::timeout(BOOT_READ_BUDGET, read).await;
    outcome.map_err(|_| RegistryError::BootTimeout {
        url: source.url.clone(),
        last,
    })?
}

/// What changed between the running projection and a fresh one.
pub fn describe_change(live: &Projection, fresh: &Projection) -> Option<String> {
    let (live_slots, fresh_slots) = (live.slots(), fresh.slots());
    let mut parts = Vec::new();
    let added: Vec<_> = fresh_slots.difference(&live_slots).cloned().collect();
    let removed: Vec<_> = live_slots.difference(&fresh_slots).cloned().collect();
    if !added.is_empty() {
        parts.push(format!("added [{}]", added.join(",")));
    }
    if !removed.is_empty() {
        parts.push(format!("removed [{}]", removed.join(",")));
    }
    // Addresses compare case-blind: a checksum re-spelling is not a change.
    let canon = |row: &Table| -> Table {
        let mut row = row.clone();
        for key in ["tokenized_equity", "tokenized_equity_derivative"] {
            if let Some(Value::String(address)) = row.get(key) {
                row.insert(key.into(), Value::String(address.to_lowercase()));
            }
        }
        row
    };
    let changed: Vec<String> = live
        .chain_rows
        .iter()
        .flat_map(|(chain, rows)| {
            rows.iter().filter_map(move |(symbol, row)| {
                fresh
                    .chain_rows
                    .get(chain)
                    .and_then(|fresh_rows| fresh_rows.get(symbol))
                    .filter(|fresh_row| canon(fresh_row) != canon(row))
                    .map(|_| format!("{chain}/{symbol}"))
            })
        })
        .collect();
    if !changed.is_empty() {
        parts.push(format!("rows changed [{}]", changed.join(",")));
    }
    if live.policies != fresh.policies {
        parts.push("hedge policies changed".to_string());
    }
    (!parts.is_empty()).then(|| parts.join("; "))
}

/// What the bot keeps after boot, so the refresh loop can compare.
#[derive(Debug, Clone)]
pub struct RegistryLive {
    pub source: RegistrySource,
    /// The config table as parsed from disk, BEFORE the merge.
    pub static_config: Table,
    pub live: Projection,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::assets::{ChainEquityAsset, EquityHedgePolicy};

    fn fixture(name: &str) -> String {
        String::from_utf8(fixtures::read(name)).unwrap()
    }

    fn canon(file: &Table) -> String {
        let mut keys: Vec<_> = file.iter().collect();
        keys.sort_by_key(|(key, _)| key.as_str());
        keys.iter()
            .map(|(key, value)| format!("{key}={}", value.to_string().to_lowercase()))
            .collect::<Vec<_>>()
            .join(" ")
    }

    /// The registry file projects into exactly the per-symbol tables the
    /// inline configs carried the day they were replaced.
    #[test]
    fn registry_projects_to_the_inline_tables_it_replaced() {
        for env in ["staging", "production"] {
            let inline: Table = toml::from_str(&fixture(&format!("{env}-inline.toml"))).unwrap();
            let tokens = match env {
                "staging" => "tokens-staging.toml",
                _ => "tokens-production-migration.toml",
            };
            let projection = project(&parse(fixture(tokens).as_bytes()).unwrap()).unwrap();

            let mut want_rows: BTreeMap<String, String> = BTreeMap::new();
            for (chain, chain_config) in inline["chains"].as_table().unwrap() {
                let Some(equities) = chain_config
                    .get("trading")
                    .and_then(|file| file.get("assets"))
                    .and_then(|assets| assets.get("equities"))
                    .and_then(Value::as_table)
                else {
                    continue;
                };
                for (symbol, row) in equities {
                    let Some(row) = row
                        .as_table()
                        .filter(|row| row.contains_key("tokenized_equity"))
                    else {
                        continue;
                    };
                    let mut row = row.clone();
                    if let Some(value) = row.remove("vault_id") {
                        row.insert("vault_ids".into(), Value::Array(vec![value]));
                    }
                    want_rows.insert(format!("{chain}/{symbol}"), canon(&row));
                }
            }
            let got_rows: BTreeMap<String, String> = projection
                .chain_rows
                .iter()
                .flat_map(|(chain, rows)| {
                    rows.iter()
                        .map(move |(symbol, row)| (format!("{chain}/{symbol}"), canon(row)))
                })
                .collect();
            assert_eq!(got_rows, want_rows, "{env} chain rows");

            let want_pol: BTreeMap<String, String> = inline["assets"]["equities"]
                .as_table()
                .unwrap()
                .iter()
                .filter_map(|(symbol, value)| {
                    value
                        .as_table()
                        .filter(|file| file.contains_key("extended_hours_counter_trading"))
                        .map(|file| (symbol.clone(), canon(file)))
                })
                .collect();
            let got_pol: BTreeMap<String, String> = projection
                .policies
                .iter()
                .map(|(symbol, file)| (symbol.clone(), canon(file)))
                .collect();
            assert_eq!(got_pol, want_pol, "{env} policies");
        }
    }

    #[test]
    fn a_priced_only_slot_is_not_the_bots() {
        let projection = project(&parse(&fixtures::pinned_production_tokens()).unwrap()).unwrap();
        // Ethereum FTF is priced and quoted, but liquidity does not hedge it.
        assert!(!projection.slots().contains("ethereum/FTF"));
        assert!(projection.slots().contains("base/FGI"));
    }

    #[test]
    fn a_bad_switch_a_wrong_schema_and_an_undeclared_chain_are_refused() {
        let mut file = parse(fixture("tokens-staging.toml").as_bytes()).unwrap();
        file["chains"]["base"]["assets"]["equities"]["FGI"]
            .as_table_mut()
            .unwrap()
            .insert("trading".into(), Value::String("enable".into()));
        assert!(matches!(
            project(&file).unwrap_err(),
            RegistryError::BadSwitch { key: "trading", .. }
        ));

        let mut file = parse(fixture("tokens-staging.toml").as_bytes()).unwrap();
        file.insert("schema_version".into(), Value::Integer(2));
        assert!(matches!(
            project(&file).unwrap_err(),
            RegistryError::SchemaVersion { .. }
        ));

        let projection =
            project(&parse(fixture("tokens-staging.toml").as_bytes()).unwrap()).unwrap();
        let mut config: Table =
            toml::from_str("[registry]\nurl = \"gs://b/o\"\n[chains.base]\n").unwrap();
        assert!(matches!(
            merge(&mut config, &projection).unwrap_err(),
            RegistryError::UndeclaredChain { chain } if chain == "base"
        ));

        let mut file = parse(fixture("tokens-staging.toml").as_bytes()).unwrap();
        file["chains"]["base"]["assets"]["equities"]["FGI"]
            .as_table_mut()
            .unwrap()
            .remove("tokenized_equity_derivative");
        assert!(matches!(
            project(&file).unwrap_err(),
            RegistryError::MissingAddress {
                key: "tokenized_equity_derivative",
                ..
            }
        ));

        let mut file = parse(fixture("tokens-staging.toml").as_bytes()).unwrap();
        for chain in ["base", "robinhood"] {
            for (_, slot) in file["chains"][chain]["assets"]["equities"]
                .as_table_mut()
                .unwrap()
                .iter_mut()
            {
                let slot = slot.as_table_mut().unwrap();
                for key in [
                    "trading",
                    "rebalancing",
                    "wrapped_equity_recovery",
                    "tokenized_equity",
                ] {
                    slot.remove(key);
                }
            }
        }
        assert!(matches!(
            project(&file).unwrap_err(),
            RegistryError::EmptyUniverse
        ));
    }

    #[test]
    fn an_inline_copy_next_to_registry_is_refused() {
        let projection =
            project(&parse(fixture("tokens-staging.toml").as_bytes()).unwrap()).unwrap();
        let mut config: Table = toml::from_str(
            "[registry]\nurl = \"gs://b/o\"\n[chains.base]\n[chains.robinhood]\n[assets.equities.FGI]\nextended_hours_counter_trading = \"enabled\"\n",
        )
        .unwrap();
        assert!(matches!(
            merge(&mut config, &projection).unwrap_err(),
            RegistryError::InlineTable { table } if table == "assets.equities.FGI"
        ));
    }

    #[test]
    fn an_inline_row_on_a_chain_the_file_leaves_empty_is_refused() {
        let projection =
            project(&parse(fixture("tokens-staging.toml").as_bytes()).unwrap()).unwrap();
        let mut config: Table = toml::from_str(
            "[registry]\nurl = \"gs://b/o\"\n[chains.base.trading]\n[chains.robinhood.trading]\n\
             [chains.hyperevm.trading.assets.equities.FGI]\ntrading = \"enabled\"\n",
        )
        .unwrap();
        assert!(matches!(
            merge(&mut config, &projection).unwrap_err(),
            RegistryError::InlineTable { table } if table == "chains.hyperevm.trading.assets.equities.FGI"
        ));
    }

    #[test]
    fn rows_on_a_chain_without_a_trading_table_are_refused() {
        let projection =
            project(&parse(fixture("tokens-staging.toml").as_bytes()).unwrap()).unwrap();
        let mut config: Table = toml::from_str(
            "[registry]\nurl = \"gs://b/o\"\n[chains.base]\n[chains.robinhood.trading]\n",
        )
        .unwrap();
        assert!(matches!(
            merge(&mut config, &projection).unwrap_err(),
            RegistryError::UndeclaredChain { chain } if chain == "base"
        ));
    }

    /// Another service's key on a slot or policy the bot takes is left to
    /// that service: the token file's allowlist lives in st0x.registry's CI,
    /// so a new key there must not fail a boot here.
    #[test]
    fn a_key_the_bot_does_not_own_is_left_alone() {
        let mut file = parse(fixture("tokens-staging.toml").as_bytes()).unwrap();
        file["chains"]["base"]["assets"]["equities"]["FGI"]
            .as_table_mut()
            .unwrap()
            .insert("quote_ttl_secs".into(), Value::Integer(50));
        file["assets"]["equities"]["FGI"]
            .as_table_mut()
            .unwrap()
            .insert("max_half_spread_bps".into(), Value::Integer(1));
        let projection = project(&file).unwrap();
        assert!(!projection.chain_rows["base"]["FGI"].contains_key("quote_ttl_secs"));
        assert!(!projection.policies["FGI"].contains_key("max_half_spread_bps"));

        let mut file = parse(fixture("tokens-staging.toml").as_bytes()).unwrap();
        let policy = file["assets"]["equities"]["FGI"].as_table_mut().unwrap();
        policy.remove("extended_hours_counter_trading");
        policy.insert("hedge_floor_shares".into(), Value::Integer(1));
        assert!(matches!(
            project(&file).unwrap_err(),
            RegistryError::BadSwitch {
                key: "extended_hours_counter_trading",
                ..
            }
        ));
    }

    /// A retired symbol is not taken from the file: a retirement is one
    /// config change, and the file's rows for it can go at any time after.
    #[test]
    fn a_retired_symbol_is_not_taken_from_the_file() {
        let projection =
            project(&parse(fixture("tokens-staging.toml").as_bytes()).unwrap()).unwrap();
        assert!(projection.slots().contains("base/FGI"));
        let mut config: Table = toml::from_str(
            "[registry]\nurl = \"gs://b/o\"\n[chains.base.trading]\n[chains.robinhood.trading]\n\
             [assets.equities]\nretired_symbols = [\"FGI\"]\n",
        )
        .unwrap();
        merge(&mut config, &projection).unwrap();
        let base = config["chains"]["base"]["trading"]["assets"]["equities"]
            .as_table()
            .unwrap();
        assert!(!base.contains_key("FGI"));
        assert!(base.contains_key("RKLB"));
        let policies = config["assets"]["equities"].as_table().unwrap();
        assert!(!policies.contains_key("FGI"));
        assert!(policies.contains_key("RKLB"));
    }

    /// Deleting a retired symbol's rows from the file is not a change: the
    /// running projection never had them.
    #[test]
    fn a_retired_symbols_rows_leaving_the_file_is_not_a_change() {
        let config: Table = toml::from_str(
            "[registry]\nurl = \"gs://b/o\"\n[chains.base.trading]\n[chains.robinhood.trading]\n\
             [assets.equities]\nretired_symbols = [\"FGI\"]\n",
        )
        .unwrap();
        let live = project(&parse(fixture("tokens-staging.toml").as_bytes()).unwrap())
            .unwrap()
            .without_retired(&config);
        let mut file = parse(fixture("tokens-staging.toml").as_bytes()).unwrap();
        file["chains"]["base"]["assets"]["equities"]
            .as_table_mut()
            .unwrap()
            .remove("FGI");
        file["assets"]["equities"]
            .as_table_mut()
            .unwrap()
            .remove("FGI");
        let fresh = project(&file).unwrap().without_retired(&config);
        assert_eq!(describe_change(&live, &fresh), None);
    }

    /// A lowercase or oddly spelled inline table is still an inline table.
    #[test]
    fn a_lowercase_inline_symbol_table_is_refused_too() {
        let config: Table = toml::from_str(
            "[registry]\nurl = \"gs://b/o\"\n[assets.equities.aapl]\nextended_hours_counter_trading = \"enabled\"\n",
        )
        .unwrap();
        assert!(matches!(
            refuse_inline_tables(&config).unwrap_err(),
            RegistryError::InlineTable { table } if table == "assets.equities.aapl"
        ));
    }

    /// The object name is one path segment: its `/` must reach the API as
    /// `%2F`, or the request names a different object.
    #[test]
    fn the_object_name_is_one_path_segment() {
        let url = object_url("t0-artifacts-tokens", "production/tokens.toml", Some(7)).unwrap();
        assert_eq!(
            url.as_str(),
            "https://storage.googleapis.com/storage/v1/b/t0-artifacts-tokens/o/production%2Ftokens.toml?alt=media&generation=7"
        );
    }

    #[test]
    fn a_bad_registry_url_or_key_is_refused_offline() {
        for text in [
            "[registry]\nurl = \"gcs://b/o\"\n",
            "[registry]\nurl = \"gs://bucket\"\n",
        ] {
            let config: Table = toml::from_str(text).unwrap();
            assert!(
                matches!(source_of(&config).unwrap_err(), RegistryError::Url { .. }),
                "{text:?}"
            );
        }
        for text in [
            "[registry]\nurl = \"gs://b/o\"\ngeneraton = 1\n",
            "[registry]\nurl = \"gs://b/o\"\nrefresh_secs = 60\n",
        ] {
            let config: Table = toml::from_str(text).unwrap();
            assert!(
                matches!(source_of(&config).unwrap_err(), RegistryError::Source(_)),
                "{text:?}"
            );
        }
    }

    #[test]
    fn a_change_of_address_case_alone_is_not_a_change() {
        let projection =
            project(&parse(fixture("tokens-staging.toml").as_bytes()).unwrap()).unwrap();
        let mut changed = projection.clone();
        for rows in changed.chain_rows.values_mut() {
            for row in rows.values_mut() {
                for key in ["tokenized_equity", "tokenized_equity_derivative"] {
                    if let Some(Value::String(value)) = row.get(key) {
                        row.insert(key.into(), Value::String(value.to_uppercase()));
                    }
                }
            }
        }
        assert_eq!(describe_change(&projection, &changed), None);
        changed.chain_rows.get_mut("base").unwrap().remove("FGI");
        assert!(
            describe_change(&projection, &changed)
                .unwrap()
                .contains("removed [base/FGI]")
        );
    }

    /// The key lists `project` copies are the fields the bot deserializes.
    /// A row holding every listed key must deserialize (so no listed key is
    /// stale), and the destructures below list every field without `..`, so
    /// a new field fails to compile here until it is added to the lists.
    #[test]
    fn the_projected_keys_are_the_structs_fields() {
        let row: Table = toml::from_str(
            "tokenized_equity = \"0x0000000000000000000000000000000000000001\"\n\
             tokenized_equity_derivative = \"0x0000000000000000000000000000000000000002\"\n\
             vault_ids = [\"0x01\"]\n\
             trading = \"enabled\"\n\
             rebalancing = \"disabled\"\n\
             wrapped_equity_recovery = \"disabled\"\n\
             operational_limit = 5\n\
             target_share = 0.5\n",
        )
        .unwrap();
        let mut keys: Vec<_> = row.keys().map(String::as_str).chain(["vault_id"]).collect();
        let mut listed = CHAIN_ROW_KEYS.to_vec();
        keys.sort_unstable();
        listed.sort_unstable();
        assert_eq!(keys, listed);
        let ChainEquityAsset {
            tokenized_equity: _,
            tokenized_equity_derivative: _,
            vault_ids: _,
            trading: _,
            rebalancing: _,
            wrapped_equity_recovery: _,
            operational_limit: _,
            target_share: _,
        } = row.try_into().unwrap();

        let policy: Table = toml::from_str(
            "extended_hours_counter_trading = \"enabled\"\nhedge_floor_shares = 1\n",
        )
        .unwrap();
        let mut keys: Vec<_> = policy.keys().map(String::as_str).collect();
        let mut listed = POLICY_KEYS.to_vec();
        keys.sort_unstable();
        listed.sort_unstable();
        assert_eq!(keys, listed);
        let EquityHedgePolicy {
            extended_hours_counter_trading: _,
            hedge_floor_shares: _,
        } = policy.try_into().unwrap();
    }

    #[test]
    fn an_added_symbol_a_changed_row_and_a_changed_policy_are_changes() {
        let live = project(&parse(fixture("tokens-staging.toml").as_bytes()).unwrap()).unwrap();

        let mut fresh = live.clone();
        let row = fresh.chain_rows["base"]["FGI"].clone();
        fresh
            .chain_rows
            .get_mut("base")
            .unwrap()
            .insert("NEWSYM".into(), row);
        assert_eq!(
            describe_change(&live, &fresh).as_deref(),
            Some("added [base/NEWSYM]")
        );

        let mut fresh = live.clone();
        fresh
            .chain_rows
            .get_mut("base")
            .unwrap()
            .get_mut("FGI")
            .unwrap()
            .insert("trading".into(), Value::String("enabled".into()));
        assert_eq!(
            describe_change(&live, &fresh).as_deref(),
            Some("rows changed [base/FGI]")
        );

        let mut fresh = live.clone();
        fresh.policies.get_mut("FGI").unwrap().insert(
            "extended_hours_counter_trading".into(),
            Value::String("enabled".into()),
        );
        assert_eq!(
            describe_change(&live, &fresh).as_deref(),
            Some("hedge policies changed")
        );
    }

    /// A copy that is gone or refused to the service account fails every
    /// boot until someone acts; a rate limit or a server error may not.
    #[test]
    fn a_lost_grant_is_unusable_and_a_server_error_is_transient() {
        for (status, unusable) in [
            (reqwest::StatusCode::UNAUTHORIZED, true),
            (reqwest::StatusCode::FORBIDDEN, true),
            (reqwest::StatusCode::NOT_FOUND, true),
            (reqwest::StatusCode::TOO_MANY_REQUESTS, false),
            (reqwest::StatusCode::INTERNAL_SERVER_ERROR, false),
            (reqwest::StatusCode::SERVICE_UNAVAILABLE, false),
        ] {
            let error = RegistryError::Status {
                url: "gs://bucket/tokens.toml".into(),
                status,
                body: String::new(),
            };
            assert_eq!(error.copy_is_unusable(), unusable, "{status}");
        }
    }
}
