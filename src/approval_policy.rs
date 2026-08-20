//! Deploy-time verification that Turnkey policies cover every startup approve.

use std::fmt::{Display, Formatter};
use std::path::Path;

use alloy::primitives::Address;
use st0x_config::Ctx;
use st0x_evm::USDC_BASE;
use st0x_evm::turnkey::{
    TurnkeyPolicy, TurnkeyPolicyClient, TurnkeyPolicyEffect, TurnkeyPolicyError,
};

use crate::onchain::approvals::{ApprovalTarget, build_approval_targets};

const BASE_CHAIN_ID: u64 = 8_453;

/// Successful result of a deploy-time policy verification.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApprovalPolicyVerification {
    /// The validated wallet backend is not Turnkey, so no Turnkey query is
    /// applicable.
    SkippedNonTurnkey,
    /// Every startup approval target is covered by at least one allow policy.
    Verified {
        target_count: usize,
        policy_count: usize,
    },
}

/// Every startup approval target for which no provably matching allow policy
/// was returned by Turnkey.
#[derive(Debug)]
pub struct MissingPolicyCoverage {
    missing: Vec<ApprovalTarget>,
}

impl MissingPolicyCoverage {
    fn new(missing: Vec<ApprovalTarget>) -> Self {
        Self { missing }
    }
}

impl Display for MissingPolicyCoverage {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        writeln!(
            formatter,
            "Turnkey policies do not cover {} startup approval target(s):",
            self.missing.len()
        )?;

        for target in &self.missing {
            let symbol = target
                .symbol
                .as_ref()
                .map_or("USDC", st0x_execution::Symbol::as_str);
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

/// Failure from config validation, Turnkey policy listing, or policy coverage.
#[derive(Debug, thiserror::Error)]
pub enum ApprovalPolicyVerificationError {
    #[error(transparent)]
    Config(#[from] st0x_config::CtxError),
    #[error(transparent)]
    Turnkey(#[from] TurnkeyPolicyError),
    #[error(transparent)]
    MissingCoverage(#[from] MissingPolicyCoverage),
}

/// Validates deploy inputs, lists Turnkey policies, and fails unless every
/// startup MAX approval has a provably matching allow policy.
pub async fn verify_turnkey_approval_policies(
    config_path: &Path,
    secrets_path: &Path,
) -> Result<ApprovalPolicyVerification, ApprovalPolicyVerificationError> {
    let Some(inputs) = Ctx::load_turnkey_approval_policy_inputs(config_path, secrets_path)? else {
        return Ok(ApprovalPolicyVerification::SkippedNonTurnkey);
    };
    let targets = build_approval_targets(&inputs.assets, inputs.orderbook, USDC_BASE);
    let client = TurnkeyPolicyClient::new(
        inputs.organization_id,
        inputs.kms_api_key,
        inputs.api_private_key,
    )
    .await?;
    let snapshot = client.list_policies().await?;
    let context = ApprovalPolicyContext {
        user_id: &snapshot.user_id,
        user_tags: &snapshot.user_tags,
        wallet_address: inputs.wallet_address,
        chain_id: BASE_CHAIN_ID,
    };
    let missing = missing_policy_coverage(&targets, &snapshot.policies, &context);

    if !missing.is_empty() {
        return Err(MissingPolicyCoverage::new(missing).into());
    }

    Ok(ApprovalPolicyVerification::Verified {
        target_count: targets.len(),
        policy_count: snapshot.policies.len(),
    })
}

fn missing_policy_coverage(
    targets: &[ApprovalTarget],
    policies: &[TurnkeyPolicy],
    context: &ApprovalPolicyContext<'_>,
) -> Vec<ApprovalTarget> {
    targets
        .iter()
        .filter(|target| !policies_cover_target(policies, target, context))
        .cloned()
        .collect()
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

fn policies_cover_target(
    policies: &[TurnkeyPolicy],
    target: &ApprovalTarget,
    context: &ApprovalPolicyContext<'_>,
) -> bool {
    let mut has_allow = false;

    for policy in policies {
        let applicability = policy_applicability(policy, target, context);

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
    target: &ApprovalTarget,
    context: &ApprovalPolicyContext<'_>,
) -> Applicability {
    let condition = condition_applicability(policy.condition.as_deref(), target, context);

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
    target: &ApprovalTarget,
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
            .map(|term| policy_term_applicability(term, target, context)),
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
    target: &ApprovalTarget,
    context: &ApprovalPolicyContext<'_>,
) -> Applicability {
    let term = trim_grouping_parentheses(term);

    if let Some(value) = equality_value(term, "eth.tx.to") {
        return lowercase_address_applicability(value, target.token, true);
    }

    if let Some(values) = membership_values(term, "eth.tx.to") {
        return address_membership_applicability(&values, target.token, true);
    }

    if let Some(value) = equality_value(term, "eth.tx.data[0..10]") {
        return equality_applicability(value, "0x095ea7b3");
    }

    if let Some(value) = equality_value(term, "eth.tx.data[2..10]") {
        return equality_applicability(value, "095ea7b3");
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
        return lowercase_address_applicability(value, target.spender, false);
    }

    if let Some(values) = membership_values(term, "eth.tx.data[34..74]") {
        return address_membership_applicability(&values, target.spender, false);
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
    use st0x_evm::turnkey::{TurnkeyPolicy, TurnkeyPolicyEffect};

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
            chain_id: BASE_CHAIN_ID,
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
    fn missing_coverage_error_names_symbol_token_and_spender() {
        let target = target();
        let error = MissingPolicyCoverage::new(vec![target.clone()]);
        let message = error.to_string();

        assert!(message.contains("AAPL"));
        assert!(message.contains(&target.token.to_string()));
        assert!(message.contains(&target.spender.to_string()));
    }
}
