//! Deploy-time verification that Turnkey policies cover every startup approve.

use std::fmt::{Display, Formatter};
use std::path::Path;

use st0x_config::Ctx;
use st0x_evm::USDC_BASE;
use st0x_evm::turnkey::{
    TurnkeyPolicy, TurnkeyPolicyClient, TurnkeyPolicyEffect, TurnkeyPolicyError,
};

use crate::onchain::approvals::{ApprovalTarget, build_approval_targets};

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
    let client = TurnkeyPolicyClient::new(inputs.organization_id, &inputs.api_private_key)?;
    let snapshot = client.list_policies().await?;
    let missing = missing_policy_coverage(&targets, &snapshot.policies, &snapshot.user_id);

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
    user_id: &str,
) -> Vec<ApprovalTarget> {
    targets
        .iter()
        .filter(|target| !policies_cover_target(policies, target, user_id))
        .cloned()
        .collect()
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
    user_id: &str,
) -> bool {
    let mut has_allow = false;

    for policy in policies {
        let applicability = policy_applicability(policy, target, user_id);

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
    user_id: &str,
) -> Applicability {
    let condition = condition_applicability(policy.condition.as_deref(), target);

    // Consensus identifies who can approve an allowed activity. A matching
    // deny condition wins independently of which users could approve it.
    if policy.effect == TurnkeyPolicyEffect::Deny {
        return condition;
    }

    conjunction([
        consensus_applicability(policy.consensus.as_deref(), user_id),
        condition,
    ])
}

fn consensus_applicability(consensus: Option<&str>, user_id: &str) -> Applicability {
    let Some(consensus) = compact_expression(consensus) else {
        return Applicability::Applies;
    };

    let Some(consensus_user_id) = any_approver_user_id(&consensus) else {
        return Applicability::Unknown;
    };

    if consensus_user_id == user_id {
        Applicability::Applies
    } else {
        Applicability::DoesNotApply
    }
}

fn condition_applicability(condition: Option<&str>, target: &ApprovalTarget) -> Applicability {
    let Some(condition) = compact_expression(condition) else {
        return Applicability::Applies;
    };

    if condition.contains("||") {
        return Applicability::Unknown;
    }

    conjunction(
        condition
            .split("&&")
            .map(|term| policy_term_applicability(term, target)),
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

fn policy_term_applicability(term: &str, target: &ApprovalTarget) -> Applicability {
    let term = trim_grouping_parentheses(term);
    let token = format!("{:#x}", target.token);

    if let Some(value) = equality_value(term, "eth.tx.to") {
        return equality_applicability(value, &token);
    }

    if let Some(value) = equality_value(term, "eth.tx.data[0..10]") {
        return equality_applicability(value, "0x095ea7b3");
    }

    if let Some(value) = equality_value(term, "activity.kind") {
        return equality_applicability(value, "SIGN_TRANSACTION");
    }

    if let Some(value) = equality_value(term, "activity.type") {
        return equality_applicability(value, "ACTIVITY_TYPE_SIGN_TRANSACTION_V2");
    }

    // ABI-derived fields only have meaning when Turnkey has a matching smart
    // contract interface. The verifier cannot prove that interface exists, so
    // these and all other condition shapes remain unknown.
    Applicability::Unknown
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

fn trim_grouping_parentheses(mut term: &str) -> &str {
    while term.starts_with('(') && term.ends_with(')') {
        term = &term[1..term.len() - 1];
    }
    term
}

#[cfg(test)]
mod tests {
    use alloy::primitives::Address;

    use st0x_evm::turnkey::{TurnkeyPolicy, TurnkeyPolicyEffect};

    use super::*;
    use crate::onchain::approvals::{ApprovalPurpose, ApprovalTarget};

    const USER_ID: &str = "user-bot";

    fn target() -> ApprovalTarget {
        ApprovalTarget {
            token: "0x1111111111111111111111111111111111111111"
                .parse()
                .unwrap(),
            spender: Address::random(),
            symbol: Some("AAPL".parse().unwrap()),
            purpose: ApprovalPurpose::WrapUnderlying,
        }
    }

    fn allow(condition: Option<&str>) -> TurnkeyPolicy {
        TurnkeyPolicy {
            effect: TurnkeyPolicyEffect::Allow,
            consensus: Some("approvers.any(user, user.id == 'user-bot')".to_string()),
            condition: condition.map(str::to_owned),
        }
    }

    #[test]
    fn exact_token_allow_policy_covers_startup_approve() {
        let target = target();
        let policies = [allow(Some(
            "activity.kind == 'SIGN_TRANSACTION' && \
             eth.tx.to == '0x1111111111111111111111111111111111111111'",
        ))];

        assert!(missing_policy_coverage(&[target], &policies, USER_ID).is_empty());
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
            missing_policy_coverage(std::slice::from_ref(&target), &policies, USER_ID),
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

        assert!(missing_policy_coverage(&[target], &policies, USER_ID).is_empty());
    }

    #[test]
    fn uppercase_address_policy_is_rejected_fail_closed() {
        let target = target();
        let policies = [allow(Some(
            "eth.tx.to == '0x111111111111111111111111111111111111111A'",
        ))];

        assert_eq!(
            missing_policy_coverage(std::slice::from_ref(&target), &policies, USER_ID),
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
            missing_policy_coverage(std::slice::from_ref(&target), &policies, USER_ID),
            vec![target]
        );
    }

    #[test]
    fn unrestricted_allow_policy_covers_every_target() {
        let target = target();

        assert!(missing_policy_coverage(&[target], &[allow(None)], USER_ID).is_empty());
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
            missing_policy_coverage(std::slice::from_ref(&target), &policies, USER_ID),
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
            missing_policy_coverage(std::slice::from_ref(&target), &policies, USER_ID),
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
            missing_policy_coverage(std::slice::from_ref(&target), &policies, USER_ID),
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
            missing_policy_coverage(std::slice::from_ref(&target), &policies, USER_ID),
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
            missing_policy_coverage(std::slice::from_ref(&target), &policies, USER_ID),
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

        assert!(missing_policy_coverage(&[target], &policies, USER_ID).is_empty());
    }

    #[test]
    fn abi_derived_function_name_fails_closed_without_interface_proof() {
        let target = target();
        let policies = [allow(Some(
            "eth.tx.to == '0x1111111111111111111111111111111111111111' && \
             eth.tx.function_name == 'approve'",
        ))];

        assert_eq!(
            missing_policy_coverage(std::slice::from_ref(&target), &policies, USER_ID),
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
