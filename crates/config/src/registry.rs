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
use std::path::Path;
use std::time::Duration;

use serde::Deserialize;
use thiserror::Error;
use toml::{Table, Value};

pub const SCHEMA_VERSION: i64 = 1;

/// Most time boot may spend reading the file, retries included.
pub const BOOT_READ_BUDGET: Duration = Duration::from_secs(20);

const MAX_BODY: usize = 4 << 20;

/// The keys a chain row may carry, as `ChainEquityAsset` reads them.
const CHAIN_ROW_KEYS: [&str; 8] = [
    "tokenized_equity",
    "tokenized_equity_derivative",
    "vault_ids",
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
    #[serde(default = "default_refresh_secs")]
    pub refresh_secs: u64,
}

fn default_refresh_secs() -> u64 {
    60
}

#[derive(Debug, Error)]
pub enum RegistryError {
    #[error("[registry] {0}")]
    Source(String),
    #[error("token file: {0}")]
    File(String),
    #[error("token file: reading {0}: {1}")]
    Read(String, String),
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
    /// `chain/SYMBOL` of every hedged slot.
    pub fn slots(&self) -> BTreeSet<String> {
        self.chain_rows
            .iter()
            .flat_map(|(c, rows)| rows.keys().map(move |s| format!("{c}/{s}")))
            .collect()
    }
}

pub fn source_of(config: &Table) -> Result<Option<RegistrySource>, RegistryError> {
    match config.get("registry") {
        None => Ok(None),
        Some(v) => {
            let source: RegistrySource = v.clone().try_into().map_err(|e| {
                RegistryError::Source(format!(
                    "takes `url` and, optionally, `generation` and `refresh_secs`; nothing else: {e}"
                ))
            })?;
            parse_gs_url(&source.url)?;
            Ok(Some(source))
        }
    }
}

pub fn parse_gs_url(gs_url: &str) -> Result<(&str, &str), RegistryError> {
    let rest = gs_url
        .strip_prefix("gs://")
        .ok_or_else(|| RegistryError::Source(format!("url {gs_url:?} is not a gs:// url")))?;
    match rest.split_once('/') {
        Some((b, o)) if !b.is_empty() && !o.is_empty() => Ok((b, o)),
        _ => Err(RegistryError::Source(format!(
            "url {gs_url:?} must be gs://<bucket>/<object>"
        ))),
    }
}

pub fn parse(bytes: &[u8]) -> Result<Table, RegistryError> {
    let text = std::str::from_utf8(bytes).map_err(|_| RegistryError::File("not UTF-8".into()))?;
    toml::from_str(text).map_err(|e| RegistryError::File(format!("not valid TOML: {e}")))
}

fn table<'a>(v: Option<&'a Value>, what: &str) -> Result<&'a Table, RegistryError> {
    v.and_then(Value::as_table)
        .ok_or_else(|| RegistryError::File(format!("{what} is missing or not a table")))
}

fn is_switch(v: Option<&Value>) -> bool {
    matches!(v.and_then(Value::as_str), Some("enabled" | "disabled"))
}

/// Turn the token file into the bot's per-symbol tables.
///
/// A chain row is taken from every slot that carries the bot's own keys
/// (`trading` present); a slot that is only priced is not the bot's.
/// A policy is taken from every `[assets.equities.<SYM>]` that carries
/// `extended_hours_counter_trading`. Pricing's keys on either level are
/// ignored. Anything malformed is refused, never dropped.
pub fn project(file: &Table) -> Result<Projection, RegistryError> {
    match file.get("schema_version") {
        Some(Value::Integer(v)) if *v == SCHEMA_VERSION => {}
        other => {
            return Err(RegistryError::File(format!(
                "schema_version must be {SCHEMA_VERSION}, got {}",
                other.map_or_else(|| "nothing".to_string(), Value::to_string)
            )));
        }
    }
    let chains = table(file.get("chains"), "chains")?;
    let equities = table(
        file.get("assets").and_then(|a| a.get("equities")),
        "assets.equities",
    )?;

    let mut chain_rows: BTreeMap<String, BTreeMap<String, Table>> = BTreeMap::new();
    for (name, chain) in chains {
        let chain = table(Some(chain), &format!("chains.{name}"))?;
        let Some(slots) = chain.get("assets").and_then(|a| a.get("equities")) else {
            continue;
        };
        let slots = table(Some(slots), &format!("chains.{name}.assets.equities"))?;
        for (sym, slot) in slots {
            let what = format!("chains.{name}.assets.equities.{sym}");
            let slot = table(Some(slot), &what)?;
            let ours = [
                "trading",
                "rebalancing",
                "wrapped_equity_recovery",
                "tokenized_equity",
            ]
            .iter()
            .any(|k| slot.contains_key(*k));
            if !ours {
                continue;
            }
            for k in ["trading", "rebalancing", "wrapped_equity_recovery"] {
                if !is_switch(slot.get(k)) {
                    return Err(RegistryError::File(format!(
                        "{what}.{k} must be \"enabled\" or \"disabled\""
                    )));
                }
            }
            for k in ["tokenized_equity", "tokenized_equity_derivative"] {
                if slot.get(k).and_then(Value::as_str).is_none() {
                    return Err(RegistryError::File(format!("{what}.{k} missing")));
                }
            }
            let mut row = Table::new();
            for k in CHAIN_ROW_KEYS {
                if let Some(v) = slot.get(k) {
                    row.insert(k.to_string(), v.clone());
                }
            }
            chain_rows
                .entry(name.clone())
                .or_default()
                .insert(sym.clone(), row);
        }
    }

    let mut policies = BTreeMap::new();
    for (sym, a) in equities {
        let a = table(Some(a), &format!("assets.equities.{sym}"))?;
        if !a.contains_key("extended_hours_counter_trading") {
            continue;
        }
        if !is_switch(a.get("extended_hours_counter_trading")) {
            return Err(RegistryError::File(format!(
                "assets.equities.{sym}.extended_hours_counter_trading must be \"enabled\" or \"disabled\""
            )));
        }
        let mut row = Table::new();
        for k in POLICY_KEYS {
            if let Some(v) = a.get(k) {
                row.insert(k.to_string(), v.clone());
            }
        }
        policies.insert(sym.clone(), row);
    }

    if chain_rows.values().all(BTreeMap::is_empty) {
        return Err(RegistryError::File(
            "no slot carries the bot's keys; refusing an empty universe".into(),
        ));
    }
    Ok(Projection {
        chain_rows,
        policies,
    })
}

fn subtable<'a>(t: &'a mut Table, key: &str) -> &'a mut Table {
    if !t.get(key).is_some_and(Value::is_table) {
        t.insert(key.to_string(), Value::Table(Table::new()));
    }
    match t.get_mut(key) {
        Some(Value::Table(table)) => table,
        // Unreachable: the key was just made a table above.
        _ => unreachable!("{key} was just made a table"),
    }
}

/// Put the projected tables into a config table that reads `[registry]`.
///
/// The config must not carry any per-symbol table itself (one source of
/// truth). Chain-wide keys such as `operational_limit`, `cash`, and
/// `retired_symbols` are the config's own and are left as they are. A
/// chain the file lists but the config does not declare is refused: a
/// chain needs an rpc_url and signing, which are release-time facts.
pub fn merge(config: &mut Table, p: &Projection) -> Result<(), RegistryError> {
    let is_symbol = |k: &str| k.chars().next().is_some_and(|c| c.is_ascii_uppercase());

    let declared: BTreeSet<String> = config
        .get("chains")
        .and_then(Value::as_table)
        .map(|c| c.keys().cloned().collect())
        .unwrap_or_default();
    for (chain, rows) in &p.chain_rows {
        if rows.is_empty() {
            continue;
        }
        if !declared.contains(chain) {
            return Err(RegistryError::File(format!(
                "lists hedged slots on chain {chain}, which this config does not declare under [chains]; \
                 a chain needs an rpc_url and signing, which are release-time facts"
            )));
        }
    }

    let chains = subtable(config, "chains");
    for (chain, rows) in &p.chain_rows {
        let Some(ct) = chains.get_mut(chain).and_then(Value::as_table_mut) else {
            continue;
        };
        let equities = subtable(subtable(subtable(ct, "trading"), "assets"), "equities");
        if let Some(k) = equities.keys().find(|k| is_symbol(k)) {
            return Err(RegistryError::Source(format!(
                "reads the per-symbol tables from the bucket but the config also carries \
                 [chains.{chain}.trading.assets.equities.{k}]; keep one source"
            )));
        }
        for (sym, row) in rows {
            equities.insert(sym.clone(), Value::Table(row.clone()));
        }
    }

    let equities = subtable(subtable(config, "assets"), "equities");
    if let Some(k) = equities.keys().find(|k| is_symbol(k)) {
        return Err(RegistryError::Source(format!(
            "reads the per-symbol tables from the bucket but the config also carries \
             [assets.equities.{k}]; keep one source"
        )));
    }
    for (sym, row) in &p.policies {
        equities.insert(sym.clone(), Value::Table(row.clone()));
    }
    Ok(())
}

fn percent(segment: &str) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(segment.len());
    for b in segment.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char);
            }
            _ => {
                let _ = write!(out, "%{b:02X}");
            }
        }
    }
    out
}

async fn access_token(http: &reqwest::Client) -> Result<String, RegistryError> {
    #[derive(Deserialize)]
    struct Token {
        access_token: String,
    }
    let t: Token = http
        .get("http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token")
        .header("Metadata-Flavor", "Google")
        .timeout(Duration::from_secs(4))
        .send()
        .await
        .and_then(reqwest::Response::error_for_status)
        .map_err(|e| RegistryError::Read("metadata server".into(), e.to_string()))?
        .json()
        .await
        .map_err(|e| RegistryError::Read("metadata token".into(), e.to_string()))?;
    Ok(t.access_token)
}

/// One `objects.get` as the VM's service account.
pub async fn fetch(
    http: &reqwest::Client,
    gs_url: &str,
    generation: Option<u64>,
) -> Result<Vec<u8>, RegistryError> {
    let (bucket, object) = parse_gs_url(gs_url)?;
    let token = access_token(http).await?;
    let generation_query = generation.map_or(String::new(), |g| format!("&generation={g}"));
    let url = format!(
        "https://storage.googleapis.com/storage/v1/b/{}/o/{}?alt=media{generation_query}",
        percent(bucket),
        percent(object)
    );
    let response = http
        .get(&url)
        .bearer_auth(token)
        .timeout(Duration::from_secs(4))
        .send()
        .await
        .map_err(|e| RegistryError::Read(gs_url.into(), e.to_string()))?;
    let status = response.status();
    if response
        .content_length()
        .is_some_and(|length| length > MAX_BODY as u64)
    {
        return Err(RegistryError::Read(
            gs_url.into(),
            format!("larger than {MAX_BODY} bytes"),
        ));
    }
    let body = response
        .bytes()
        .await
        .map_err(|e| RegistryError::Read(gs_url.into(), e.to_string()))?;
    if !status.is_success() {
        return Err(RegistryError::Read(
            format!(
                "{gs_url}{}",
                generation.map_or(String::new(), |g| format!(" generation {g}"))
            ),
            format!(
                "{status}: {}",
                String::from_utf8_lossy(&body)
                    .chars()
                    .take(300)
                    .collect::<String>()
            ),
        ));
    }
    if body.len() > MAX_BODY {
        return Err(RegistryError::Read(
            gs_url.into(),
            format!("larger than {MAX_BODY} bytes"),
        ));
    }
    Ok(body.to_vec())
}

/// The token file bytes for boot: a local file (`--registry-file`) or the
/// bucket, the latter bounded as a whole so a wedged read cannot stall
/// the roll's health gate.
pub async fn load_bytes(
    source: &RegistrySource,
    local: Option<&Path>,
) -> Result<Vec<u8>, RegistryError> {
    if let Some(path) = local {
        return std::fs::read(path)
            .map_err(|e| RegistryError::Read(path.display().to_string(), e.to_string()));
    }
    let http = reqwest::Client::new();
    let read = async {
        let mut attempt = 1;
        loop {
            match fetch(&http, &source.url, source.generation).await {
                Ok(b) => break Ok(b),
                Err(e) if attempt < 3 => {
                    tracing::warn!(attempt, error = %e, "token file: boot read failed; retrying");
                    tokio::time::sleep(Duration::from_secs(1 << attempt)).await;
                    attempt += 1;
                }
                Err(e) => break Err(e),
            }
        }
    };
    tokio::time::timeout(BOOT_READ_BUDGET, read)
        .await
        .map_err(|_| {
            RegistryError::Read(
                source.url.clone(),
                format!("boot read exceeded {}s", BOOT_READ_BUDGET.as_secs()),
            )
        })?
}

/// What changed between the running projection and a fresh one.
pub fn describe_change(live: &Projection, fresh: &Projection) -> String {
    let (a, b) = (live.slots(), fresh.slots());
    let mut parts = Vec::new();
    let added: Vec<_> = b.difference(&a).cloned().collect();
    let removed: Vec<_> = a.difference(&b).cloned().collect();
    if !added.is_empty() {
        parts.push(format!("added [{}]", added.join(",")));
    }
    if !removed.is_empty() {
        parts.push(format!("removed [{}]", removed.join(",")));
    }
    let canon = |t: &Table| -> Table {
        let mut t = t.clone();
        for k in ["tokenized_equity", "tokenized_equity_derivative"] {
            if let Some(Value::String(v)) = t.get(k) {
                t.insert(k.into(), Value::String(v.to_lowercase()));
            }
        }
        t
    };
    let changed: Vec<String> = live
        .chain_rows
        .iter()
        .flat_map(|(c, rows)| {
            rows.iter().filter_map(move |(s, row)| {
                fresh
                    .chain_rows
                    .get(c)
                    .and_then(|r| r.get(s))
                    .filter(|f| canon(f) != canon(row))
                    .map(|_| format!("{c}/{s}"))
            })
        })
        .collect();
    if !changed.is_empty() {
        parts.push(format!("rows changed [{}]", changed.join(",")));
    }
    if live.policies != fresh.policies {
        parts.push("hedge policies changed".to_string());
    }
    if parts.is_empty() {
        "no difference".to_string()
    } else {
        parts.join("; ")
    }
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

    fn fixture(name: &str) -> String {
        std::fs::read_to_string(format!(
            "{}/../../tests/fixtures/{name}",
            env!("CARGO_MANIFEST_DIR")
        ))
        .unwrap_or_else(|e| panic!("{name}: {e}"))
    }

    fn canon(t: &Table) -> String {
        let mut keys: Vec<_> = t.iter().collect();
        keys.sort_by_key(|(k, _)| k.as_str());
        keys.iter()
            .map(|(k, v)| format!("{k}={}", v.to_string().to_lowercase()))
            .collect::<Vec<_>>()
            .join(" ")
    }

    /// The registry file projects into exactly the per-symbol tables the
    /// inline configs carried the day they were replaced.
    #[test]
    fn registry_projects_to_the_inline_tables_it_replaced() {
        for env in ["staging", "production"] {
            let inline: Table = toml::from_str(&fixture(&format!("{env}-inline.toml"))).unwrap();
            let p = project(&parse(fixture(&format!("tokens-{env}.toml")).as_bytes()).unwrap())
                .unwrap();

            let mut want_rows: BTreeMap<String, String> = BTreeMap::new();
            for (c, ct) in inline["chains"].as_table().unwrap() {
                let Some(eq) = ct
                    .get("trading")
                    .and_then(|t| t.get("assets"))
                    .and_then(|a| a.get("equities"))
                    .and_then(Value::as_table)
                else {
                    continue;
                };
                for (s, row) in eq {
                    let Some(row) = row
                        .as_table()
                        .filter(|r| r.contains_key("tokenized_equity"))
                    else {
                        continue;
                    };
                    let mut row = row.clone();
                    if let Some(v) = row.remove("vault_id") {
                        row.insert("vault_ids".into(), Value::Array(vec![v]));
                    }
                    want_rows.insert(format!("{c}/{s}"), canon(&row));
                }
            }
            let got_rows: BTreeMap<String, String> = p
                .chain_rows
                .iter()
                .flat_map(|(c, rows)| {
                    rows.iter()
                        .map(move |(s, r)| (format!("{c}/{s}"), canon(r)))
                })
                .collect();
            assert_eq!(got_rows, want_rows, "{env} chain rows");

            let want_pol: BTreeMap<String, String> = inline["assets"]["equities"]
                .as_table()
                .unwrap()
                .iter()
                .filter_map(|(s, v)| {
                    v.as_table()
                        .filter(|t| t.contains_key("extended_hours_counter_trading"))
                        .map(|t| (s.clone(), canon(t)))
                })
                .collect();
            let got_pol: BTreeMap<String, String> = p
                .policies
                .iter()
                .map(|(s, t)| (s.clone(), canon(t)))
                .collect();
            assert_eq!(got_pol, want_pol, "{env} policies");
        }
    }

    #[test]
    fn a_priced_only_slot_is_not_the_bots() {
        let p = project(&parse(fixture("tokens-production.toml").as_bytes()).unwrap()).unwrap();
        // Ethereum FTF is priced and quoted, but liquidity does not hedge it.
        assert!(!p.slots().contains("ethereum/FTF"));
        assert!(p.slots().contains("base/FGI"));
    }

    #[test]
    fn a_bad_switch_a_wrong_schema_and_an_undeclared_chain_are_refused() {
        let mut t = parse(fixture("tokens-staging.toml").as_bytes()).unwrap();
        t["chains"]["base"]["assets"]["equities"]["FGI"]
            .as_table_mut()
            .unwrap()
            .insert("trading".into(), Value::String("enable".into()));
        assert!(
            project(&t)
                .unwrap_err()
                .to_string()
                .contains("trading must be")
        );

        let mut t = parse(fixture("tokens-staging.toml").as_bytes()).unwrap();
        t.insert("schema_version".into(), Value::Integer(2));
        assert!(
            project(&t)
                .unwrap_err()
                .to_string()
                .contains("schema_version")
        );

        let p = project(&parse(fixture("tokens-staging.toml").as_bytes()).unwrap()).unwrap();
        let mut config: Table =
            toml::from_str("[registry]\nurl = \"gs://b/o\"\n[chains.base]\n").unwrap();
        assert!(
            merge(&mut config, &p)
                .unwrap_err()
                .to_string()
                .contains("does not declare")
        );
    }

    #[test]
    fn an_inline_copy_next_to_registry_is_refused() {
        let p = project(&parse(fixture("tokens-staging.toml").as_bytes()).unwrap()).unwrap();
        let mut config: Table = toml::from_str(
            "[registry]\nurl = \"gs://b/o\"\n[chains.base]\n[chains.robinhood]\n[assets.equities.FGI]\nextended_hours_counter_trading = \"enabled\"\n",
        )
        .unwrap();
        assert!(
            merge(&mut config, &p)
                .unwrap_err()
                .to_string()
                .contains("keep one source")
        );
    }

    #[test]
    fn a_bad_registry_url_or_key_is_refused_offline() {
        for text in [
            "[registry]\nurl = \"gcs://b/o\"\n",
            "[registry]\nurl = \"gs://bucket\"\n",
            "[registry]\nurl = \"gs://b/o\"\ngeneraton = 1\n",
        ] {
            let t: Table = toml::from_str(text).unwrap();
            assert!(source_of(&t).is_err(), "{text:?}");
        }
    }

    #[test]
    fn a_change_of_address_case_alone_is_not_a_change() {
        let p = project(&parse(fixture("tokens-staging.toml").as_bytes()).unwrap()).unwrap();
        let mut q = p.clone();
        for rows in q.chain_rows.values_mut() {
            for row in rows.values_mut() {
                for k in ["tokenized_equity", "tokenized_equity_derivative"] {
                    if let Some(Value::String(v)) = row.get(k) {
                        row.insert(k.into(), Value::String(v.to_uppercase()));
                    }
                }
            }
        }
        assert_eq!(describe_change(&p, &q), "no difference");
        q.chain_rows.get_mut("base").unwrap().remove("FGI");
        assert!(describe_change(&p, &q).contains("removed [base/FGI]"));
    }
}
