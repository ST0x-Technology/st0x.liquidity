//! Deploy-time verification that Turnkey policies cover every startup approve
//! and every call a served cash corridor signs on either end.

use std::collections::BTreeSet;
use std::fmt::{Display, Formatter};
use std::path::Path;

use alloy::primitives::{Address, Selector};
use alloy::sol_types::SolCall;
use st0x_bridge::corridor::{FirstArgument, HopCall, NoRelayDepository, UsdcCorridor};
use st0x_config::{ChainApprovalInputs, Ctx, InventoryMode, TokenFile};
use st0x_evm::turnkey::{
    TurnkeyPolicy, TurnkeyPolicyClient, TurnkeyPolicyEffect, TurnkeyPolicyError,
    TurnkeyPolicySnapshot,
};
use st0x_evm::{Chain, IERC20};

use crate::bindings::IRaindexInventory;
use crate::onchain::approvals::{ApprovalTarget, build_approval_targets};

/// Successful result of a deploy-time policy verification.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApprovalPolicyVerification {
    /// The validated wallet backend is not Turnkey, so no Turnkey query is
    /// applicable.
    SkippedNonTurnkey,
    /// Every startup approval target and every served corridor's hop call is
    /// covered by at least one allow policy.
    Verified {
        target_count: usize,
        corridor_call_count: usize,
        policy_count: usize,
    },
}

/// Every startup approval target on one hedged chain for which no provably
/// matching allow policy was returned by Turnkey.
#[derive(Debug)]
pub struct MissingPolicyCoverage {
    chain: Chain,
    missing: Vec<ApprovalTarget>,
}

impl MissingPolicyCoverage {
    fn new(chain: Chain, missing: Vec<ApprovalTarget>) -> Self {
        Self { chain, missing }
    }
}

impl Display for MissingPolicyCoverage {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        writeln!(
            formatter,
            "Turnkey policies do not cover {} startup approval target(s) on {}:",
            self.missing.len(),
            self.chain,
        )?;

        let stable = self.chain.settlement_stable().symbol;

        for target in &self.missing {
            let symbol = target
                .symbol
                .as_ref()
                .map_or(stable, st0x_execution::Symbol::as_str);
            writeln!(
                formatter,
                "- {symbol}: token {}, spender {}, purpose {:?}",
                target.token, target.spender, target.purpose
            )?;
        }

        Ok(())
    }
}

impl std::error::Error for MissingPolicyCoverage {}

/// Every call one served corridor signs, on either end, for which no
/// provably matching allow policy was returned by Turnkey.
#[derive(Debug)]
pub struct MissingCorridorCoverage {
    corridor: UsdcCorridor,
    missing: Vec<HopCall>,
}

impl Display for MissingCorridorCoverage {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        writeln!(
            formatter,
            "Turnkey policies do not cover {} call(s) the {} corridor signs:",
            self.missing.len(),
            self.corridor,
        )?;

        for call in &self.missing {
            write!(
                formatter,
                "- {}: {} ({}) on {}",
                call.chain, call.function, call.selector, call.to
            )?;

            if let FirstArgument::Address(spender) = call.first_argument {
                write!(formatter, ", spender {spender}")?;
            }

            writeln!(formatter)?;
        }

        Ok(())
    }
}

impl std::error::Error for MissingCorridorCoverage {}

/// Failure from config validation, Turnkey policy listing, or policy coverage.
#[derive(Debug, thiserror::Error)]
pub enum ApprovalPolicyVerificationError {
    #[error(transparent)]
    Config(#[from] st0x_config::CtxError),
    #[error(transparent)]
    Turnkey(#[from] TurnkeyPolicyError),
    #[error(transparent)]
    Coverage(#[from] ChainCoverageError),
}

/// Why one hedged chain's startup approvals, or one served corridor's hop
/// calls, are not provably covered.
#[derive(Debug, thiserror::Error)]
pub enum ChainCoverageError {
    #[error(transparent)]
    MissingCoverage(#[from] MissingPolicyCoverage),
    #[error(transparent)]
    MissingCorridorCoverage(#[from] MissingCorridorCoverage),
    #[error(transparent)]
    NoRelayDepository(#[from] NoRelayDepository),
    #[error("the {corridor} corridor's chain is not a hedged chain")]
    CorridorChainNotHedged { corridor: UsdcCorridor },
}

/// Validates deploy inputs, lists Turnkey policies, and fails unless every
/// startup MAX approval and every served corridor's hop call has a provably
/// matching allow policy.
pub async fn verify_turnkey_approval_policies(
    config_path: &Path,
    secrets_path: &Path,
    tokens: TokenFile<'_>,
) -> Result<ApprovalPolicyVerification, ApprovalPolicyVerificationError> {
    let inputs = Ctx::load_turnkey_approval_policy_inputs(config_path, secrets_path, tokens)?;
    verify_approval_inputs(inputs).await
}

/// Registry changes that need no new grants must not depend on Turnkey availability.
pub(crate) async fn verify_registry_approval_inputs(
    running: Option<st0x_config::TurnkeyApprovalPolicyInputs>,
    candidate: Option<st0x_config::TurnkeyApprovalPolicyInputs>,
) -> Result<(), ApprovalPolicyVerificationError> {
    if let (Some(running), Some(candidate)) = (&running, &candidate)
        && running.wallet_address == candidate.wallet_address
        && running.organization_id.as_str() == candidate.organization_id.as_str()
        && candidate.corridors.is_subset(&running.corridors)
        && !additional_approval_targets(&running.hedged, &candidate.hedged)
    {
        return Ok(());
    }
    verify_approval_inputs(candidate).await?;
    Ok(())
}

fn additional_approval_targets(
    running: &[ChainApprovalInputs],
    candidate: &[ChainApprovalInputs],
) -> bool {
    candidate.iter().any(|chain| {
        let targets = build_approval_targets(
            chain.role,
            chain.inventory,
            &chain.assets,
            chain.orderbook,
            chain.chain.settlement_stable().address,
        );
        let previous = running
            .iter()
            .find(|previous| previous.chain == chain.chain);
        let previous_targets = previous.map(|previous| {
            build_approval_targets(
                previous.role,
                previous.inventory,
                &previous.assets,
                previous.orderbook,
                previous.chain.settlement_stable().address,
            )
        });
        targets.iter().any(|target| {
            previous_targets.as_ref().is_none_or(|previous| {
                !previous
                    .iter()
                    .any(|old| old.token == target.token && old.spender == target.spender)
            })
        })
    })
}

pub(crate) async fn verify_approval_inputs(
    inputs: Option<st0x_config::TurnkeyApprovalPolicyInputs>,
) -> Result<ApprovalPolicyVerification, ApprovalPolicyVerificationError> {
    let Some(inputs) = inputs else {
        return Ok(ApprovalPolicyVerification::SkippedNonTurnkey);
    };
    let client = TurnkeyPolicyClient::new(
        inputs.organization_id,
        inputs.kms_api_key,
        inputs.api_private_key,
    )
    .await?;
    let snapshot = client.list_policies().await?;
    let target_count = verify_hedged_chains(&inputs.hedged, &snapshot, inputs.wallet_address)?;
    let corridor_call_count = verify_corridor_ends(
        &inputs.corridors,
        &inputs.hedged,
        &snapshot,
        inputs.wallet_address,
    )?;

    Ok(ApprovalPolicyVerification::Verified {
        target_count,
        corridor_call_count,
        policy_count: snapshot.policies.len(),
    })
}

/// Every hedged chain's startup targets, each checked against the policies
/// on that chain's own id; the covered target count on success. The first
/// chain with an uncovered target fails the gate naming that chain.
fn verify_hedged_chains(
    hedged: &[ChainApprovalInputs],
    snapshot: &TurnkeyPolicySnapshot,
    wallet_address: Address,
) -> Result<usize, ChainCoverageError> {
    let mut target_count = 0;

    for chain_inputs in hedged {
        let chain = chain_inputs.chain;
        let targets = build_approval_targets(
            chain_inputs.role,
            chain_inputs.inventory,
            &chain_inputs.assets,
            chain_inputs.orderbook,
            chain.settlement_stable().address,
        );
        let context = ApprovalPolicyContext {
            user_id: &snapshot.user_id,
            user_tags: &snapshot.user_tags,
            wallet_address,
            chain_id: chain.chain_id(),
        };
        let missing = missing_policy_coverage(&targets, &snapshot.policies, &context);

        if !missing.is_empty() {
            return Err(MissingPolicyCoverage::new(chain, missing).into());
        }

        target_count += targets.len();
    }

    Ok(target_count)
}

/// Every served corridor's calls, hop and cash legs, each checked against the
/// policies on its own end's chain id; the covered call count on success. The
/// first corridor with an uncovered call fails the gate, naming every such
/// call on both of its ends.
fn verify_corridor_ends(
    corridors: &BTreeSet<UsdcCorridor>,
    hedged: &[ChainApprovalInputs],
    snapshot: &TurnkeyPolicySnapshot,
    wallet_address: Address,
) -> Result<usize, ChainCoverageError> {
    let mut call_count = 0;

    for corridor in corridors {
        let mut calls = corridor.hop_calls()?;
        calls.extend(cash_leg_calls(*corridor, hedged)?);
        let missing: Vec<HopCall> = calls
            .iter()
            .filter(|call| {
                let context = ApprovalPolicyContext {
                    user_id: &snapshot.user_id,
                    user_tags: &snapshot.user_tags,
                    wallet_address,
                    chain_id: call.chain.chain_id(),
                };
                let required = RequiredCall {
                    to: call.to,
                    selector: call.selector,
                    first_address: match call.first_argument {
                        FirstArgument::Address(address) => Some(address),
                        FirstArgument::Signer => Some(wallet_address),
                        FirstArgument::ChosenAtRunTime | FirstArgument::NotAnAddress => None,
                    },
                };

                !policies_cover_call(&snapshot.policies, &required, &context)
            })
            .copied()
            .collect();

        if !missing.is_empty() {
            return Err(MissingCorridorCoverage {
                corridor: *corridor,
                missing,
            }
            .into());
        }

        call_count += calls.len();
    }

    Ok(call_count)
}

/// The calls a corridor signs around its hop: the vault `withdraw4` and
/// `deposit4` of the stable on the corridor chain's inventory, and the hub's
/// USDC `transfer` to Alpaca's deposit address, which only a run knows.
fn cash_leg_calls(
    corridor: UsdcCorridor,
    hedged: &[ChainApprovalInputs],
) -> Result<[HopCall; 3], ChainCoverageError> {
    let chain = corridor.chain();
    let inputs = hedged
        .iter()
        .find(|inputs| inputs.chain == chain)
        .ok_or(ChainCoverageError::CorridorChainNotHedged { corridor })?;
    let inventory = match inputs.inventory {
        InventoryMode::Legacy => inputs.orderbook,
        InventoryMode::Managed { inventory } => inventory,
    };
    let vault_call = |selector: [u8; 4], function| HopCall {
        chain,
        to: inventory,
        selector: selector.into(),
        first_argument: FirstArgument::Address(chain.settlement_stable().address),
        function,
    };

    Ok([
        vault_call(IRaindexInventory::withdraw4Call::SELECTOR, "withdraw4"),
        vault_call(IRaindexInventory::deposit4Call::SELECTOR, "deposit4"),
        HopCall {
            chain: Chain::Ethereum,
            to: Chain::Ethereum.settlement_stable().address,
            selector: IERC20::transferCall::SELECTOR.into(),
            first_argument: FirstArgument::ChosenAtRunTime,
            function: "transfer",
        },
    ])
}

fn missing_policy_coverage(
    targets: &[ApprovalTarget],
    policies: &[TurnkeyPolicy],
    context: &ApprovalPolicyContext<'_>,
) -> Vec<ApprovalTarget> {
    targets
        .iter()
        .filter(|target| {
            let required = RequiredCall {
                to: target.token,
                selector: IERC20::approveCall::SELECTOR.into(),
                first_address: Some(target.spender),
            };

            !policies_cover_call(policies, &required, context)
        })
        .cloned()
        .collect()
}

/// One transaction a policy must allow: its target, its selector, and its
/// first ABI word when that word is an address (`None` leaves any policy
/// term on it unprovable).
#[derive(Debug, Clone, Copy)]
struct RequiredCall {
    to: Address,
    selector: Selector,
    first_address: Option<Address>,
}

#[derive(Debug, Clone, Copy)]
struct ApprovalPolicyContext<'a> {
    user_id: &'a str,
    user_tags: &'a [String],
    wallet_address: Address,
    chain_id: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Applicability {
    Applies,
    DoesNotApply,
    Unknown,
}

fn policies_cover_call(
    policies: &[TurnkeyPolicy],
    call: &RequiredCall,
    context: &ApprovalPolicyContext<'_>,
) -> bool {
    let mut has_allow = false;

    for policy in policies {
        let applicability = policy_applicability(policy, call, context);

        match (policy.effect, applicability) {
            (TurnkeyPolicyEffect::Deny, Applicability::Applies | Applicability::Unknown) => {
                return false;
            }
            (TurnkeyPolicyEffect::Allow, Applicability::Applies) => has_allow = true,
            _ => {}
        }
    }

    has_allow
}

fn policy_applicability(
    policy: &TurnkeyPolicy,
    call: &RequiredCall,
    context: &ApprovalPolicyContext<'_>,
) -> Applicability {
    let condition = condition_applicability(policy.condition.as_deref(), call, context);

    // Consensus identifies who can approve an allowed activity. A matching
    // deny condition wins independently of which users could approve it.
    if policy.effect == TurnkeyPolicyEffect::Deny {
        return condition;
    }

    conjunction([
        consensus_applicability(policy.consensus.as_deref(), context),
        condition,
    ])
}

fn consensus_applicability(
    consensus: Option<&str>,
    context: &ApprovalPolicyContext<'_>,
) -> Applicability {
    let Some(consensus) = compact_expression(consensus) else {
        return Applicability::Applies;
    };

    if let Some(consensus_user_id) = any_approver_user_id(&consensus) {
        return equality_applicability(consensus_user_id, context.user_id);
    }

    trim_grouping_parentheses(&consensus)
        .strip_prefix("approvers.any(user,user.tags.contains('")
        .and_then(|value| value.strip_suffix("'))"))
        .map_or(Applicability::Unknown, |tag_id| {
            if context.user_tags.iter().any(|user_tag| user_tag == tag_id) {
                Applicability::Applies
            } else {
                Applicability::DoesNotApply
            }
        })
}

fn condition_applicability(
    condition: Option<&str>,
    call: &RequiredCall,
    context: &ApprovalPolicyContext<'_>,
) -> Applicability {
    let Some(condition) = compact_expression(condition) else {
        return Applicability::Applies;
    };

    if condition.contains("||") {
        return Applicability::Unknown;
    }

    conjunction(
        condition
            .split("&&")
            .map(|term| policy_term_applicability(term, call, context)),
    )
}

fn compact_expression(expression: Option<&str>) -> Option<String> {
    let expression = expression?;
    let mut compact = String::with_capacity(expression.len());
    let mut in_quoted_literal = false;
    let mut escaped = false;

    for character in expression.chars() {
        if in_quoted_literal {
            compact.push(character);

            if escaped {
                escaped = false;
            } else if character == '\\' {
                escaped = true;
            } else if character == '\'' {
                in_quoted_literal = false;
            }
        } else if character == '\'' {
            in_quoted_literal = true;
            compact.push(character);
        } else if !character.is_ascii_whitespace() {
            compact.push(character);
        }
    }

    (!compact.is_empty()).then_some(compact)
}

fn conjunction(applicabilities: impl IntoIterator<Item = Applicability>) -> Applicability {
    applicabilities
        .into_iter()
        .fold(Applicability::Applies, |combined, applicability| {
            match (combined, applicability) {
                (Applicability::DoesNotApply, _) | (_, Applicability::DoesNotApply) => {
                    Applicability::DoesNotApply
                }
                (Applicability::Unknown, _) | (_, Applicability::Unknown) => Applicability::Unknown,
                _ => Applicability::Applies,
            }
        })
}

fn any_approver_user_id(consensus: &str) -> Option<&str> {
    let consensus = trim_grouping_parentheses(consensus);

    consensus
        .strip_prefix("approvers.any(user,user.id=='")
        .and_then(|value| value.strip_suffix("')"))
        .or_else(|| {
            consensus
                .strip_prefix("approvers.any(user,'")
                .and_then(|value| value.strip_suffix("'==user.id)"))
        })
}

fn policy_term_applicability(
    term: &str,
    call: &RequiredCall,
    context: &ApprovalPolicyContext<'_>,
) -> Applicability {
    let term = trim_grouping_parentheses(term);

    if let Some(value) = equality_value(term, "eth.tx.to") {
        return lowercase_address_applicability(value, call.to, true);
    }

    if let Some(values) = membership_values(term, "eth.tx.to") {
        return address_membership_applicability(&values, call.to, true);
    }

    let selector = format!("{:#x}", call.selector);

    if let Some(value) = equality_value(term, "eth.tx.data[0..10]") {
        return equality_applicability(value, &selector);
    }

    if let Some(value) = equality_value(term, "eth.tx.data[2..10]") {
        return equality_applicability(value, &selector[2..]);
    }

    if let Some(value) = equality_value(term, "activity.kind") {
        return equality_applicability(value, "SIGN_TRANSACTION");
    }

    if let Some(value) = equality_value(term, "activity.type") {
        return equality_applicability(value, "ACTIVITY_TYPE_SIGN_TRANSACTION_V2");
    }

    if let Some(value) = term.strip_prefix("eth.tx.chain_id==").or_else(|| {
        term.strip_suffix("==eth.tx.chain_id")
            .filter(|value| !value.is_empty())
    }) {
        return value
            .parse::<u64>()
            .map_or(Applicability::Unknown, |value| {
                if value == context.chain_id {
                    Applicability::Applies
                } else {
                    Applicability::DoesNotApply
                }
            });
    }

    if let Some(value) = equality_value(term, "wallet_account.address") {
        return checksummed_address_applicability(value, context.wallet_address);
    }

    if let Some(value) = equality_value(term, "eth.tx.data[34..74]") {
        return call
            .first_address
            .map_or(Applicability::Unknown, |address| {
                lowercase_address_applicability(value, address, false)
            });
    }

    if let Some(values) = membership_values(term, "eth.tx.data[34..74]") {
        return call
            .first_address
            .map_or(Applicability::Unknown, |address| {
                address_membership_applicability(&values, address, false)
            });
    }

    // ABI-derived fields only have meaning when Turnkey has a matching smart
    // contract interface. The verifier cannot prove that interface exists, so
    // these and all other condition shapes remain unknown.
    Applicability::Unknown
}

fn address_membership_applicability(
    values: &[&str],
    expected: Address,
    prefixed: bool,
) -> Applicability {
    let mut saw_unknown = false;

    for value in values {
        match lowercase_address_applicability(value, expected, prefixed) {
            Applicability::Applies => return Applicability::Applies,
            Applicability::Unknown => saw_unknown = true,
            Applicability::DoesNotApply => {}
        }
    }

    if saw_unknown {
        Applicability::Unknown
    } else {
        Applicability::DoesNotApply
    }
}

fn lowercase_address_applicability(
    actual: &str,
    expected: Address,
    prefixed: bool,
) -> Applicability {
    if actual != actual.to_ascii_lowercase() {
        return Applicability::Unknown;
    }

    let value = if prefixed {
        actual.to_owned()
    } else {
        format!("0x{actual}")
    };

    value
        .parse::<Address>()
        .map_or(Applicability::Unknown, |value| {
            if value == expected {
                Applicability::Applies
            } else {
                Applicability::DoesNotApply
            }
        })
}

fn checksummed_address_applicability(actual: &str, expected: Address) -> Applicability {
    Address::parse_checksummed(actual, None).map_or(Applicability::Unknown, |value| {
        if value == expected {
            Applicability::Applies
        } else {
            Applicability::DoesNotApply
        }
    })
}

fn equality_applicability(actual: &str, expected: &str) -> Applicability {
    if actual == expected {
        Applicability::Applies
    } else {
        Applicability::DoesNotApply
    }
}

fn equality_value<'a>(term: &'a str, field: &str) -> Option<&'a str> {
    term.strip_prefix(&format!("{field}=='"))
        .and_then(|value| value.strip_suffix('\''))
        .or_else(|| {
            term.strip_prefix('\'')
                .and_then(|value| value.strip_suffix(&format!("'=={field}")))
        })
}

fn membership_values<'a>(term: &'a str, field: &str) -> Option<Vec<&'a str>> {
    let mut remaining = term
        .strip_prefix(&format!("{field}in["))?
        .strip_suffix(']')?;
    let mut values = Vec::new();

    while !remaining.is_empty() {
        remaining = remaining.strip_prefix('\'')?;
        let literal_end = remaining.find('\'')?;
        values.push(&remaining[..literal_end]);
        remaining = &remaining[literal_end + 1..];

        if remaining.is_empty() {
            break;
        }

        remaining = remaining.strip_prefix(',')?;
    }

    (!values.is_empty()).then_some(values)
}

fn trim_grouping_parentheses(mut term: &str) -> &str {
    while term.starts_with('(') && term.ends_with(')') {
        term = &term[1..term.len() - 1];
    }
    term
}

#[cfg(test)]
mod tests {
    use alloy::primitives::address;
    use std::collections::{BTreeSet, HashMap};

    use st0x_bridge::corridor::{HopKind, UsdcCorridor};
    use st0x_config::{
        ChainAssets, ChainEquities, ChainEquityAsset, ChainRole, InventoryMode, OperationMode,
        RebalancingMode,
    };
    use st0x_evm::turnkey::{TurnkeyPolicy, TurnkeyPolicyEffect, TurnkeyPolicySnapshot};
    use st0x_evm::{USDC_BASE, USDC_ETHEREUM, USDC_HYPEREVM};

    use super::*;
    use crate::onchain::approvals::{ApprovalPurpose, ApprovalTarget};

    const USER_ID: &str = "user-bot";
    const USER_TAG_ID: &str = "tag-liquidity-bot";

    fn target() -> ApprovalTarget {
        ApprovalTarget {
            token: "0x1111111111111111111111111111111111111111"
                .parse()
                .unwrap(),
            spender: "0x2222222222222222222222222222222222222222"
                .parse()
                .unwrap(),
            symbol: Some("AAPL".parse().unwrap()),
            purpose: ApprovalPurpose::WrapUnderlying,
        }
    }

    fn context(user_tags: &[String]) -> ApprovalPolicyContext<'_> {
        ApprovalPolicyContext {
            user_id: USER_ID,
            user_tags,
            wallet_address: "0x52908400098527886E0F7030069857D2E4169EE7"
                .parse()
                .unwrap(),
            chain_id: Chain::Base.chain_id(),
        }
    }

    fn missing(targets: &[ApprovalTarget], policies: &[TurnkeyPolicy]) -> Vec<ApprovalTarget> {
        missing_policy_coverage(targets, policies, &context(&[USER_TAG_ID.to_string()]))
    }

    fn allow(condition: Option<&str>) -> TurnkeyPolicy {
        TurnkeyPolicy {
            effect: TurnkeyPolicyEffect::Allow,
            consensus: Some("approvers.any(user, user.id == 'user-bot')".to_string()),
            condition: condition.map(str::to_owned),
        }
    }

    fn tag_allow(condition: &str) -> TurnkeyPolicy {
        TurnkeyPolicy {
            effect: TurnkeyPolicyEffect::Allow,
            consensus: Some(
                "approvers.any(user, user.tags.contains('tag-liquidity-bot'))".to_string(),
            ),
            condition: Some(condition.to_string()),
        }
    }

    fn p2_condition() -> &'static str {
        "activity.type == 'ACTIVITY_TYPE_SIGN_TRANSACTION_V2' && \
         eth.tx.chain_id == 8453 && \
         eth.tx.to in ['0x1111111111111111111111111111111111111111', \
                       '0x3333333333333333333333333333333333333333'] && \
         eth.tx.data[2..10] == '095ea7b3' && \
         wallet_account.address == '0x52908400098527886E0F7030069857D2E4169EE7' && \
         eth.tx.data[34..74] in ['2222222222222222222222222222222222222222', \
                                '4444444444444444444444444444444444444444']"
    }

    fn p6_condition() -> &'static str {
        "activity.type == 'ACTIVITY_TYPE_SIGN_TRANSACTION_V2' && \
         eth.tx.chain_id == 8453 && \
         eth.tx.to in ['0x3333333333333333333333333333333333333333', \
                       '0x5555555555555555555555555555555555555555'] && \
         eth.tx.data[2..10] == '095ea7b3' && \
         wallet_account.address == '0x52908400098527886E0F7030069857D2E4169EE7' && \
         eth.tx.data[34..74] in ['4444444444444444444444444444444444444444', \
                                '7777777777777777777777777777777777777777']"
    }

    #[test]
    fn p2_tag_policy_covers_underlying_to_wrapper_approval() {
        let target = target();

        assert!(missing(&[target], &[tag_allow(p2_condition())]).is_empty());
    }

    #[test]
    fn p2_lowercase_address_literals_with_hex_letters_cover_target() {
        let target = ApprovalTarget {
            token: "0xf6744fd94e27c2f58f6110aa9fdc77a87e41766b"
                .parse()
                .unwrap(),
            spender: "0xf4f8c66085910d583c01f3b4e44bf731d4e2c565"
                .parse()
                .unwrap(),
            symbol: Some("RKLB".parse().unwrap()),
            purpose: ApprovalPurpose::WrapUnderlying,
        };
        let condition = "activity.type == 'ACTIVITY_TYPE_SIGN_TRANSACTION_V2' && \
                         eth.tx.chain_id == 8453 && \
                         eth.tx.to in \
                         ['0xf6744fd94e27c2f58f6110aa9fdc77a87e41766b'] && \
                         eth.tx.data[2..10] == '095ea7b3' && \
                         wallet_account.address == \
                         '0x52908400098527886E0F7030069857D2E4169EE7' && \
                         eth.tx.data[34..74] in \
                         ['f4f8c66085910d583c01f3b4e44bf731d4e2c565']";

        assert!(missing(&[target], &[tag_allow(condition)]).is_empty());
    }

    #[test]
    fn p6_tag_policy_covers_wrapped_token_to_orderbook_approval() {
        let target = ApprovalTarget {
            token: "0x3333333333333333333333333333333333333333"
                .parse()
                .unwrap(),
            spender: "0x4444444444444444444444444444444444444444"
                .parse()
                .unwrap(),
            symbol: Some("AAPL".parse().unwrap()),
            purpose: ApprovalPurpose::DepositWrappedEquity,
        };

        assert!(missing(&[target], &[tag_allow(p6_condition())]).is_empty());
    }

    #[test]
    fn p2_tag_policy_does_not_apply_without_the_authenticated_user_tag() {
        let target = target();
        let policies = [tag_allow(p2_condition())];
        let no_tags = [];

        assert_eq!(
            missing_policy_coverage(std::slice::from_ref(&target), &policies, &context(&no_tags),),
            vec![target]
        );
    }

    #[test]
    fn p2_tag_policy_rejects_each_mismatched_transaction_constraint() {
        let target = target();
        let cases = [
            ("chain", p2_condition().replace("8453", "1")),
            (
                "token",
                p2_condition().replace(
                    "0x1111111111111111111111111111111111111111",
                    "0x9999999999999999999999999999999999999999",
                ),
            ),
            ("selector", p2_condition().replace("095ea7b3", "a9059cbb")),
            (
                "wallet",
                p2_condition().replace(
                    "0x52908400098527886E0F7030069857D2E4169EE7",
                    "0x8888888888888888888888888888888888888888",
                ),
            ),
            (
                "spender",
                p2_condition().replace(
                    "2222222222222222222222222222222222222222",
                    "8888888888888888888888888888888888888888",
                ),
            ),
        ];

        for (constraint, condition) in cases {
            assert_eq!(
                missing(std::slice::from_ref(&target), &[tag_allow(&condition)]),
                vec![target.clone()],
                "mismatched {constraint} must not authorize the approval"
            );
        }
    }

    #[test]
    fn exact_token_allow_policy_covers_startup_approve() {
        let target = target();
        let policies = [allow(Some(
            "activity.kind == 'SIGN_TRANSACTION' && \
             eth.tx.to == '0x1111111111111111111111111111111111111111'",
        ))];

        assert!(missing(&[target], &policies).is_empty());
    }

    #[test]
    fn deny_and_transfer_only_policies_do_not_cover_approve() {
        let target = target();
        let policies = [
            TurnkeyPolicy {
                effect: TurnkeyPolicyEffect::Deny,
                consensus: Some("approvers.any(user, user.id == 'user-bot')".to_string()),
                condition: Some(
                    "eth.tx.to == '0x1111111111111111111111111111111111111111'".to_string(),
                ),
            },
            allow(Some(
                "eth.tx.to == '0x1111111111111111111111111111111111111111' && \
                 eth.tx.function_name == 'transfer'",
            )),
        ];

        assert_eq!(
            missing(std::slice::from_ref(&target), &policies),
            vec![target]
        );
    }

    #[test]
    fn raw_approve_selector_policy_covers_startup_approve() {
        let target = target();
        let policies = [allow(Some(
            "eth.tx.to == '0x1111111111111111111111111111111111111111' && \
             eth.tx.data[0..10] == '0x095ea7b3'",
        ))];

        assert!(missing(&[target], &policies).is_empty());
    }

    #[test]
    fn uppercase_address_policy_is_rejected_fail_closed() {
        let target = target();
        let policies = [allow(Some(
            "eth.tx.to == '0x111111111111111111111111111111111111111A'",
        ))];

        assert_eq!(
            missing(std::slice::from_ref(&target), &policies),
            vec![target]
        );
    }

    #[test]
    fn whitespace_inside_quoted_literal_is_preserved() {
        let target = target();
        let policies = [allow(Some(
            "eth.tx.to == '0x1111111111111111111111111111111111111111 '",
        ))];

        assert_eq!(
            missing(std::slice::from_ref(&target), &policies),
            vec![target]
        );
    }

    #[test]
    fn unrestricted_allow_policy_covers_every_target() {
        let target = target();

        assert!(missing(&[target], &[allow(None)]).is_empty());
    }

    #[test]
    fn allow_policy_for_another_user_does_not_cover_startup_approve() {
        let target = target();
        let policies = [TurnkeyPolicy {
            effect: TurnkeyPolicyEffect::Allow,
            consensus: Some("approvers.any(user, user.id == 'user-someone-else')".to_string()),
            condition: Some(
                "eth.tx.to == '0x1111111111111111111111111111111111111111'".to_string(),
            ),
        }];

        assert_eq!(
            missing(std::slice::from_ref(&target), &policies),
            vec![target]
        );
    }

    #[test]
    fn multi_approver_consensus_fails_closed() {
        let target = target();
        let policies = [TurnkeyPolicy {
            effect: TurnkeyPolicyEffect::Allow,
            consensus: Some("approvers.count() >= 2".to_string()),
            condition: Some(
                "eth.tx.to == '0x1111111111111111111111111111111111111111'".to_string(),
            ),
        }];

        assert_eq!(
            missing(std::slice::from_ref(&target), &policies),
            vec![target]
        );
    }

    #[test]
    fn applicable_deny_overrides_matching_allow() {
        let target = target();
        let policies = [
            allow(Some(
                "eth.tx.to == '0x1111111111111111111111111111111111111111'",
            )),
            TurnkeyPolicy {
                effect: TurnkeyPolicyEffect::Deny,
                consensus: Some("approvers.any(user, user.id == 'user-bot')".to_string()),
                condition: Some(
                    "eth.tx.to == '0x1111111111111111111111111111111111111111'".to_string(),
                ),
            },
        ];

        assert_eq!(
            missing(std::slice::from_ref(&target), &policies),
            vec![target]
        );
    }

    #[test]
    fn unknown_deny_applicability_blocks_matching_allow() {
        let target = target();
        let policies = [
            allow(Some(
                "eth.tx.to == '0x1111111111111111111111111111111111111111'",
            )),
            TurnkeyPolicy {
                effect: TurnkeyPolicyEffect::Deny,
                consensus: None,
                condition: Some("eth.tx.function_name == 'transfer'".to_string()),
            },
        ];

        assert_eq!(
            missing(std::slice::from_ref(&target), &policies),
            vec![target]
        );
    }

    #[test]
    fn deny_consensus_does_not_limit_deny_precedence() {
        let target = target();
        let policies = [
            allow(Some(
                "eth.tx.to == '0x1111111111111111111111111111111111111111'",
            )),
            TurnkeyPolicy {
                effect: TurnkeyPolicyEffect::Deny,
                consensus: Some("approvers.any(user, user.id == 'user-someone-else')".to_string()),
                condition: Some(
                    "eth.tx.to == '0x1111111111111111111111111111111111111111'".to_string(),
                ),
            },
        ];

        assert_eq!(
            missing(std::slice::from_ref(&target), &policies),
            vec![target]
        );
    }

    #[test]
    fn provably_unrelated_deny_does_not_block_matching_allow() {
        let target = target();
        let policies = [
            allow(Some(
                "eth.tx.to == '0x1111111111111111111111111111111111111111'",
            )),
            TurnkeyPolicy {
                effect: TurnkeyPolicyEffect::Deny,
                consensus: None,
                condition: Some(
                    "eth.tx.to == '0x2222222222222222222222222222222222222222'".to_string(),
                ),
            },
        ];

        assert!(missing(&[target], &policies).is_empty());
    }

    #[test]
    fn abi_derived_function_name_fails_closed_without_interface_proof() {
        let target = target();
        let policies = [allow(Some(
            "eth.tx.to == '0x1111111111111111111111111111111111111111' && \
             eth.tx.function_name == 'approve'",
        ))];

        assert_eq!(
            missing(std::slice::from_ref(&target), &policies),
            vec![target]
        );
    }

    #[test]
    fn missing_coverage_error_names_chain_symbol_token_and_spender() {
        let target = target();
        let error = MissingPolicyCoverage::new(Chain::Ethereum, vec![target.clone()]);
        let message = error.to_string();

        assert!(message.contains("ethereum"));
        assert!(message.contains("AAPL"));
        assert!(message.contains(&target.token.to_string()));
        assert!(message.contains(&target.spender.to_string()));
    }

    fn snapshot(policies: Vec<TurnkeyPolicy>) -> TurnkeyPolicySnapshot {
        TurnkeyPolicySnapshot {
            user_id: USER_ID.to_string(),
            user_tags: vec![USER_TAG_ID.to_string()],
            policies,
        }
    }

    /// One hedged chain listing a trading-only AAPL: underlying 0x11..
    /// wraps into vault 0x22.., which deposits into orderbook 0x33.. -- the
    /// same addresses the policy fixtures above name. The inventory mode
    /// decides which spender the two deposit grants must be covered for.
    fn chain_inputs(
        chain: Chain,
        role: ChainRole,
        inventory: InventoryMode,
    ) -> ChainApprovalInputs {
        ChainApprovalInputs {
            chain,
            role,
            inventory,
            orderbook: address!("0x3333333333333333333333333333333333333333"),
            assets: ChainAssets {
                equities: ChainEquities {
                    operational_limit: None,
                    symbols: HashMap::from([(
                        "AAPL".parse().unwrap(),
                        ChainEquityAsset {
                            tokenized_equity: address!(
                                "0x1111111111111111111111111111111111111111"
                            ),
                            tokenized_equity_derivative: address!(
                                "0x2222222222222222222222222222222222222222"
                            ),
                            vault_ids: Vec::new(),
                            trading: OperationMode::Enabled,
                            rebalancing: RebalancingMode::Disabled,
                            wrapped_equity_recovery: OperationMode::Disabled,
                            operational_limit: None,
                            target_share: None,
                        },
                    )]),
                },
                cash: None,
            },
        }
    }

    #[tokio::test]
    async fn registry_approval_removals_do_not_query_turnkey() {
        let running = st0x_config::TurnkeyApprovalPolicyInputs {
            organization_id: st0x_evm::turnkey::TurnkeyOrganizationId::try_new(
                "00000000-0000-4000-8000-000000000001".into(),
            )
            .unwrap(),
            kms_api_key: None,
            api_private_key: None,
            wallet_address: Address::ZERO,
            hedged: vec![chain_inputs(
                Chain::Base,
                ChainRole::Primary,
                InventoryMode::Legacy,
            )],
            corridors: BTreeSet::from([UsdcCorridor::BASE_CCTP]),
        };
        let mut candidate = running.clone();
        let asset = candidate.hedged[0]
            .assets
            .equities
            .symbols
            .values_mut()
            .next()
            .unwrap();
        asset.trading = OperationMode::Disabled;
        verify_registry_approval_inputs(Some(running.clone()), Some(running.clone()))
            .await
            .unwrap();
        verify_registry_approval_inputs(Some(running.clone()), Some(candidate.clone()))
            .await
            .unwrap();
        let error = verify_registry_approval_inputs(Some(candidate), Some(running))
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            ApprovalPolicyVerificationError::Turnkey(TurnkeyPolicyError::MissingCredentials)
        ));
    }

    #[test]
    fn registry_approval_additions_are_compared_on_the_actual_chain_and_spender() {
        let running = vec![chain_inputs(
            Chain::Base,
            ChainRole::Primary,
            InventoryMode::Legacy,
        )];
        let mut candidate = running.clone();
        candidate[0].inventory = InventoryMode::Managed {
            inventory: address!("0x4444444444444444444444444444444444444444"),
        };
        assert!(additional_approval_targets(&running, &candidate));
        let candidate = vec![chain_inputs(
            Chain::Ethereum,
            ChainRole::Primary,
            InventoryMode::Legacy,
        )];
        assert!(additional_approval_targets(&running, &candidate));
        let mut disabled = running.clone();
        disabled[0]
            .assets
            .equities
            .symbols
            .values_mut()
            .next()
            .unwrap()
            .trading = OperationMode::Disabled;
        assert!(additional_approval_targets(&disabled, &running));
    }

    /// Each hedged chain's targets are checked on that chain's own id: a
    /// policy pinned to Base's chain id covers Base's three targets and
    /// refuses an Ethereum trading table outright, naming the chain. The
    /// hedge-only secondary's one uncovered target is Ethereum's own USDC:
    /// its trading-only AAPL has no wrapper there to approve.
    #[test]
    fn hedged_chain_targets_are_checked_on_their_own_chain_id() {
        let policies = snapshot(vec![tag_allow(
            "activity.type == 'ACTIVITY_TYPE_SIGN_TRANSACTION_V2' && \
             eth.tx.chain_id == 8453 && \
             eth.tx.data[2..10] == '095ea7b3'",
        )]);
        let wallet_address = address!("0x52908400098527886E0F7030069857D2E4169EE7");

        let covered = verify_hedged_chains(
            &[chain_inputs(
                Chain::Base,
                ChainRole::Primary,
                InventoryMode::Legacy,
            )],
            &policies,
            wallet_address,
        )
        .unwrap();
        assert_eq!(covered, 3);

        let error = verify_hedged_chains(
            &[
                chain_inputs(Chain::Base, ChainRole::Primary, InventoryMode::Legacy),
                chain_inputs(Chain::Ethereum, ChainRole::Secondary, InventoryMode::Legacy),
            ],
            &policies,
            wallet_address,
        )
        .unwrap_err();

        let ChainCoverageError::MissingCoverage(missing) = error else {
            panic!("expected missing startup coverage, got {error:?}");
        };
        assert_eq!(missing.chain, Chain::Ethereum);
        assert_eq!(
            missing
                .missing
                .iter()
                .map(|target| (target.token, target.purpose))
                .collect::<Vec<_>>(),
            vec![(USDC_ETHEREUM, ApprovalPurpose::DepositUsdc)],
            "Ethereum's uncovered target must be Ethereum's own USDC, not Base's"
        );
    }

    #[test]
    fn hedged_hyperevm_requires_policy_for_its_canonical_usdc() {
        let hedged = [chain_inputs(
            Chain::HyperEvm,
            ChainRole::Secondary,
            InventoryMode::Legacy,
        )];
        let wallet_address = address!("0x52908400098527886E0F7030069857D2E4169EE7");
        let condition = format!(
            "eth.tx.chain_id == 999 && eth.tx.to == '{USDC_HYPEREVM:#x}' && \
             eth.tx.data[2..10] == '095ea7b3'"
        );

        for policies in [
            Vec::new(),
            vec![allow(Some(&condition.replace("999", "8453")))],
            vec![allow(Some(&condition.replace(
                &format!("{USDC_HYPEREVM:#x}"),
                &format!("{USDC_ETHEREUM:#x}"),
            )))],
        ] {
            let error =
                verify_hedged_chains(&hedged, &snapshot(policies), wallet_address).unwrap_err();
            let ChainCoverageError::MissingCoverage(missing) = error else {
                panic!("expected missing startup coverage, got {error:?}");
            };

            assert_eq!(missing.chain, Chain::HyperEvm);
            assert_eq!(
                missing.missing,
                vec![ApprovalTarget {
                    token: USDC_HYPEREVM,
                    spender: hedged[0].orderbook,
                    symbol: None,
                    purpose: ApprovalPurpose::DepositUsdc,
                }]
            );
        }

        assert_eq!(
            verify_hedged_chains(
                &hedged,
                &snapshot(vec![allow(Some(&condition))]),
                wallet_address,
            )
            .unwrap(),
            1
        );
    }

    /// The gate proves the spender startup actually approves, so a managed
    /// chain's two deposit grants must be covered for its inventory: a policy
    /// that only authorizes the orderbook as spender leaves both uncovered,
    /// and the inventory-spender policy covers all three targets.
    #[test]
    fn managed_chain_coverage_follows_the_inventory_spender() {
        let inventory = address!("0x4444444444444444444444444444444444444444");
        let hedged = [chain_inputs(
            Chain::Base,
            ChainRole::Primary,
            InventoryMode::Managed { inventory },
        )];
        let wallet_address = address!("0x52908400098527886E0F7030069857D2E4169EE7");
        let spender_allow = |spender: &str| {
            tag_allow(&format!(
                "activity.type == 'ACTIVITY_TYPE_SIGN_TRANSACTION_V2' && \
                 eth.tx.chain_id == 8453 && \
                 eth.tx.data[2..10] == '095ea7b3' && \
                 eth.tx.data[34..74] in \
                 ['2222222222222222222222222222222222222222', '{spender}']"
            ))
        };

        let error = verify_hedged_chains(
            &hedged,
            &snapshot(vec![spender_allow(
                "3333333333333333333333333333333333333333",
            )]),
            wallet_address,
        )
        .unwrap_err();
        let ChainCoverageError::MissingCoverage(missing) = error else {
            panic!("expected missing startup coverage, got {error:?}");
        };

        assert_eq!(
            missing
                .missing
                .iter()
                .map(|target| (target.token, target.spender, target.purpose))
                .collect::<Vec<_>>(),
            vec![
                (
                    address!("0x2222222222222222222222222222222222222222"),
                    inventory,
                    ApprovalPurpose::DepositWrappedEquity
                ),
                (USDC_BASE, inventory, ApprovalPurpose::DepositUsdc),
            ],
            "the orderbook-spender policy must not cover the inventory grants"
        );
        assert_eq!(
            verify_hedged_chains(
                &hedged,
                &snapshot(vec![spender_allow(
                    "4444444444444444444444444444444444444444",
                )]),
                wallet_address,
            )
            .unwrap(),
            3
        );
    }

    const ROBINHOOD_RELAY: UsdcCorridor = UsdcCorridor::HubRouted {
        chain: Chain::Robinhood,
        hop: HopKind::Relay,
    };

    const WALLET: Address = address!("0x52908400098527886E0F7030069857D2E4169EE7");

    /// The corridor chains as hedged secondaries; their vault calls go to
    /// orderbook 0x33.. in legacy inventory mode.
    fn corridor_chains() -> [ChainApprovalInputs; 2] {
        [Chain::Base, Chain::Robinhood]
            .map(|chain| chain_inputs(chain, ChainRole::Secondary, InventoryMode::Legacy))
    }

    /// The vault calls on the corridor chain and the hub's send to Alpaca.
    fn cash_leg_policies() -> Vec<TurnkeyPolicy> {
        let withdraw = Selector::from(IRaindexInventory::withdraw4Call::SELECTOR);
        let deposit = Selector::from(IRaindexInventory::deposit4Call::SELECTOR);

        [
            (
                "0x3333333333333333333333333333333333333333",
                format!("{withdraw:x}"),
            ),
            (
                "0x3333333333333333333333333333333333333333",
                format!("{deposit:x}"),
            ),
            (
                "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
                "a9059cbb".to_string(),
            ),
        ]
        .into_iter()
        .map(|(to, selector)| {
            tag_allow(&format!(
                "eth.tx.to == '{to}' && eth.tx.data[2..10] == '{selector}'"
            ))
        })
        .collect()
    }

    /// A policy that covers both Relay approves (USDG on Robinhood, USDC on
    /// Ethereum, each to the depository) but no call to the depository itself.
    #[test]
    fn relay_corridor_end_without_policy_for_its_deposit_is_refused() {
        let depository = Chain::Robinhood.relay_depository().unwrap();
        let usdg = Chain::Robinhood.settlement_stable().address;
        let approvals_only = tag_allow(&format!(
            "activity.type == 'ACTIVITY_TYPE_SIGN_TRANSACTION_V2' && \
             eth.tx.to in ['{usdg:#x}', '{USDC_ETHEREUM:#x}'] && \
             eth.tx.data[2..10] == '095ea7b3' && \
             eth.tx.data[34..74] == '{depository:x}'"
        ));

        let error = verify_corridor_ends(
            &BTreeSet::from([ROBINHOOD_RELAY]),
            &corridor_chains(),
            &snapshot([vec![approvals_only], cash_leg_policies()].concat()),
            WALLET,
        )
        .unwrap_err();
        let ChainCoverageError::MissingCorridorCoverage(missing) = error else {
            panic!("expected missing corridor coverage, got {error:?}");
        };

        assert_eq!(missing.corridor, ROBINHOOD_RELAY);
        assert_eq!(
            missing
                .missing
                .iter()
                .map(|call| (call.chain, call.to, call.function))
                .collect::<Vec<_>>(),
            vec![
                (Chain::Robinhood, depository, "depositErc20"),
                (Chain::Ethereum, depository, "depositErc20"),
            ],
            "both Relay ends must be refused for their uncovered deposit"
        );
        assert_eq!(
            missing.to_string(),
            format!(
                "Turnkey policies do not cover 2 call(s) the robinhood via relay corridor \
                 signs:\n\
                 - robinhood: depositErc20 (0xe8017952) on {depository}\n\
                 - ethereum: depositErc20 (0xe8017952) on {depository}\n"
            )
        );
    }

    /// Both Relay ends' approves and deposits, the deposit only with
    /// `depositor` as its depositor, plus the cash legs.
    fn relay_policies(depositor: Address) -> Vec<TurnkeyPolicy> {
        let depository = Chain::Robinhood.relay_depository().unwrap();
        let usdg = Chain::Robinhood.settlement_stable().address;

        let mut policies = cash_leg_policies();
        policies.push(tag_allow(&format!(
            "eth.tx.to in ['{usdg:#x}', '{USDC_ETHEREUM:#x}'] && \
             eth.tx.data[2..10] == '095ea7b3' && \
             eth.tx.data[34..74] == '{depository:x}'"
        )));
        policies.push(tag_allow(&format!(
            "eth.tx.to == '{depository:#x}' && \
             eth.tx.data[0..10] == '0xe8017952' && \
             eth.tx.data[34..74] == '{depositor:x}'"
        )));
        policies
    }

    #[test]
    fn fully_covered_relay_corridor_passes_and_counts_every_call() {
        assert_eq!(
            verify_corridor_ends(
                &BTreeSet::from([ROBINHOOD_RELAY]),
                &corridor_chains(),
                &snapshot(relay_policies(WALLET)),
                WALLET,
            )
            .unwrap(),
            7
        );
    }

    #[test]
    fn relay_deposit_policy_for_another_depositor_does_not_cover_ours() {
        let other = address!("0x9999999999999999999999999999999999999999");

        let error = verify_corridor_ends(
            &BTreeSet::from([ROBINHOOD_RELAY]),
            &corridor_chains(),
            &snapshot(relay_policies(other)),
            WALLET,
        )
        .unwrap_err();
        let ChainCoverageError::MissingCorridorCoverage(missing) = error else {
            panic!("expected missing corridor coverage, got {error:?}");
        };

        assert_eq!(
            missing
                .missing
                .iter()
                .map(|call| (call.chain, call.function))
                .collect::<Vec<_>>(),
            vec![
                (Chain::Robinhood, "depositErc20"),
                (Chain::Ethereum, "depositErc20"),
            ]
        );
    }

    /// A deny on the raw deposit selector overrides the allow for both
    /// Relay deposits and nothing else.
    #[test]
    fn deny_on_a_hop_selector_refuses_that_call_on_both_ends() {
        let mut policies = relay_policies(WALLET);
        policies.push(TurnkeyPolicy {
            effect: TurnkeyPolicyEffect::Deny,
            consensus: None,
            condition: Some("eth.tx.data[2..10] == 'e8017952'".to_string()),
        });

        let error = verify_corridor_ends(
            &BTreeSet::from([ROBINHOOD_RELAY]),
            &corridor_chains(),
            &snapshot(policies),
            WALLET,
        )
        .unwrap_err();
        let ChainCoverageError::MissingCorridorCoverage(missing) = error else {
            panic!("expected missing corridor coverage, got {error:?}");
        };

        assert_eq!(
            missing
                .missing
                .iter()
                .map(|call| (call.chain, call.function))
                .collect::<Vec<_>>(),
            vec![
                (Chain::Robinhood, "depositErc20"),
                (Chain::Ethereum, "depositErc20"),
            ]
        );
    }

    /// Base via CCTP signs on Base and on Ethereum: a policy pinned to
    /// Base's chain id leaves the Ethereum end's calls uncovered, and one
    /// more pinned to Ethereum's covers all nine.
    #[test]
    fn cctp_corridor_ends_are_checked_on_their_own_chain_ids() {
        let corridors = BTreeSet::from([UsdcCorridor::BASE_CCTP]);
        let on_chain = |chain_id: u64| {
            tag_allow(&format!(
                "activity.type == 'ACTIVITY_TYPE_SIGN_TRANSACTION_V2' && \
                 eth.tx.chain_id == {chain_id}"
            ))
        };

        let error = verify_corridor_ends(
            &corridors,
            &corridor_chains(),
            &snapshot(vec![on_chain(8453)]),
            WALLET,
        )
        .unwrap_err();
        let ChainCoverageError::MissingCorridorCoverage(missing) = error else {
            panic!("expected missing corridor coverage, got {error:?}");
        };

        assert_eq!(missing.corridor, UsdcCorridor::BASE_CCTP);
        assert_eq!(
            missing
                .missing
                .iter()
                .map(|call| (call.chain, call.function))
                .collect::<Vec<_>>(),
            vec![
                (Chain::Ethereum, "approve"),
                (Chain::Ethereum, "depositForBurn"),
                (Chain::Ethereum, "receiveMessage"),
                (Chain::Ethereum, "transfer"),
            ]
        );
        assert_eq!(
            verify_corridor_ends(
                &corridors,
                &corridor_chains(),
                &snapshot(vec![on_chain(8453), on_chain(1)]),
                WALLET,
            )
            .unwrap(),
            9
        );
    }

    /// The vault calls go to the chain's deposit spender, so in managed mode
    /// a policy for the orderbook does not cover them.
    #[test]
    fn corridor_vault_calls_follow_the_inventory_spender() {
        let inventory = address!("0x4444444444444444444444444444444444444444");
        let hedged = [chain_inputs(
            Chain::Base,
            ChainRole::Primary,
            InventoryMode::Managed { inventory },
        )];
        let hop = UsdcCorridor::BASE_CCTP.hop_calls().unwrap();
        let (messenger, transmitter) = (hop[1].to, hop[2].to);
        let policies = snapshot(vec![
            tag_allow("eth.tx.chain_id == 1"),
            tag_allow(&format!(
                "eth.tx.chain_id == 8453 && \
                 eth.tx.to in ['{USDC_BASE:#x}', '{messenger:#x}', '{transmitter:#x}', \
                               '0x3333333333333333333333333333333333333333']"
            )),
        ]);

        let error = verify_corridor_ends(
            &BTreeSet::from([UsdcCorridor::BASE_CCTP]),
            &hedged,
            &policies,
            WALLET,
        )
        .unwrap_err();
        let ChainCoverageError::MissingCorridorCoverage(missing) = error else {
            panic!("expected missing corridor coverage, got {error:?}");
        };

        assert_eq!(
            missing
                .missing
                .iter()
                .map(|call| (call.to, call.function))
                .collect::<Vec<_>>(),
            vec![(inventory, "withdraw4"), (inventory, "deposit4")]
        );
    }

    #[test]
    fn corridor_on_a_chain_that_is_not_hedged_is_refused() {
        let error = verify_corridor_ends(
            &BTreeSet::from([ROBINHOOD_RELAY]),
            &[chain_inputs(
                Chain::Base,
                ChainRole::Primary,
                InventoryMode::Legacy,
            )],
            &snapshot(vec![tag_allow("eth.tx.chain_id == 1")]),
            WALLET,
        )
        .unwrap_err();

        assert!(matches!(
            error,
            ChainCoverageError::CorridorChainNotHedged {
                corridor: ROBINHOOD_RELAY
            }
        ));
    }

    /// `depositForBurn`'s first word is the amount, `receiveMessage`'s an
    /// offset and the Alpaca `transfer`'s a recipient only a run knows, so a
    /// policy constraining that word proves nothing about those calls.
    #[test]
    fn first_word_constraint_on_an_unknown_argument_fails_closed() {
        let messenger = UsdcCorridor::BASE_CCTP.hop_calls().unwrap()[1].to;
        let policies = snapshot(vec![tag_allow(&format!(
            "eth.tx.data[34..74] == '{messenger:x}'"
        ))]);

        let error = verify_corridor_ends(
            &BTreeSet::from([UsdcCorridor::BASE_CCTP]),
            &corridor_chains(),
            &policies,
            WALLET,
        )
        .unwrap_err();
        let ChainCoverageError::MissingCorridorCoverage(missing) = error else {
            panic!("expected missing corridor coverage, got {error:?}");
        };

        assert!(
            missing
                .missing
                .iter()
                .all(|call| call.function != "approve"),
            "the approves name the messenger as their first word"
        );
        for (chain, function) in [
            (Chain::Base, "depositForBurn"),
            (Chain::Base, "receiveMessage"),
            (Chain::Ethereum, "depositForBurn"),
            (Chain::Ethereum, "receiveMessage"),
            (Chain::Ethereum, "transfer"),
        ] {
            assert!(
                missing
                    .missing
                    .iter()
                    .any(|call| call.chain == chain && call.function == function),
                "{function} on {chain} must stay uncovered"
            );
        }
    }

    #[tokio::test]
    async fn registry_reload_with_an_added_corridor_queries_turnkey() {
        let running = st0x_config::TurnkeyApprovalPolicyInputs {
            organization_id: st0x_evm::turnkey::TurnkeyOrganizationId::try_new(
                "00000000-0000-4000-8000-000000000001".into(),
            )
            .unwrap(),
            kms_api_key: None,
            api_private_key: None,
            wallet_address: Address::ZERO,
            hedged: vec![chain_inputs(
                Chain::Base,
                ChainRole::Primary,
                InventoryMode::Legacy,
            )],
            corridors: BTreeSet::from([UsdcCorridor::BASE_CCTP]),
        };
        let mut candidate = running.clone();
        candidate.corridors.insert(ROBINHOOD_RELAY);

        let error = verify_registry_approval_inputs(Some(running), Some(candidate))
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            ApprovalPolicyVerificationError::Turnkey(TurnkeyPolicyError::MissingCredentials)
        ));
    }
}
