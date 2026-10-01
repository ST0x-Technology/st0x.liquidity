//! What a Relay fill or refund must look like on chain, checked against a
//! mined transaction and its receipt.
//!
//! Relay's solver pays by calling the stable's `transferFrom(solver, recipient,
//! amount)` with the order id appended as a trailing 32-byte word. Nothing
//! else names the order, and the sender is a rotating relayer, so the proof
//! rests on the calldata and the stable's `Transfer` log alone.

use alloy::primitives::{Address, B256, FixedBytes, U256};
use alloy::rpc::types::{Log, TransactionReceipt};
use alloy::sol;
use alloy::sol_types::{SolCall, SolEvent};

use st0x_evm::IERC20;

use super::RelayOrderId;

sol! {
    #[derive(Debug)]
    /// Emitted by Relay's depository on every ERC-20 deposit. No field is
    /// indexed, so a scan matches on the decoded `id`.
    event RelayErc20Deposit(address from, address token, uint256 amount, bytes32 id);
}

/// `transferFrom`'s selector, its three words, and the trailing order id.
const PAYMENT_CALLDATA_LEN: usize = 4 + 4 * 32;

const ORDER_ID_OFFSET: usize = 4 + 3 * 32;

/// Why a tx Relay names is not the fill or refund it claims to be.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UnverifiedReason {
    /// Relay named no tx, or more than one.
    TxCount {
        count: usize,
    },
    /// The tx is on both chains, so its side is ambiguous.
    OnBothChains,
    /// A fill found on the origin chain: only a refund can pay there.
    OnOriginChain,
    Reverted,
    /// The tx does not call the stable of the side it is on.
    NotTheStable {
        to: Option<Address>,
    },
    CalldataLength {
        length: usize,
    },
    NotTransferFrom {
        selector: FixedBytes<4>,
    },
    /// The `transferFrom` words carry dirty high bits.
    CalldataUndecodable,
    /// The calldata pays someone other than our wallet.
    Recipient {
        recipient: Address,
    },
    /// The trailing calldata word is another order's id.
    OrderId {
        found: B256,
    },
    /// Not exactly one `Transfer` of the stable to our wallet.
    TransferCount {
        count: usize,
    },
    /// The `Transfer` comes from another account than the calldata's `from`.
    TransferSender {
        calldata: Address,
        logged: Address,
    },
    /// The `Transfer` moved another amount than the calldata names.
    TransferAmount {
        calldata: U256,
        logged: U256,
    },
    /// A fill below the quote's minimum.
    BelowMinimum {
        amount: U256,
        minimum: U256,
    },
    /// A refund larger than the deposit.
    AboveDeposit {
        amount: U256,
        deposited: U256,
    },
}

/// What a payment must match on the side it landed on.
#[derive(Debug, Clone, Copy)]
pub(super) struct PaymentTerms {
    pub(super) stable: Address,
    pub(super) recipient: Address,
    pub(super) order_id: RelayOrderId,
}

/// Checks a mined tx against `terms` and returns the amount it paid.
pub(super) fn check_payment(
    terms: &PaymentTerms,
    to: Option<Address>,
    input: &[u8],
    receipt: &TransactionReceipt,
) -> Result<U256, UnverifiedReason> {
    if !receipt.status() {
        return Err(UnverifiedReason::Reverted);
    }

    if to != Some(terms.stable) {
        return Err(UnverifiedReason::NotTheStable { to });
    }

    let (call, order_id) = decode_payment_calldata(input)?;

    if call.to != terms.recipient {
        return Err(UnverifiedReason::Recipient { recipient: call.to });
    }

    let RelayOrderId(expected) = terms.order_id;
    if order_id != expected {
        return Err(UnverifiedReason::OrderId { found: order_id });
    }

    let transfers = receipt
        .inner
        .logs()
        .iter()
        .filter_map(|log| transfer_to(log, terms))
        .collect::<Vec<_>>();

    let [transfer] = transfers.as_slice() else {
        return Err(UnverifiedReason::TransferCount {
            count: transfers.len(),
        });
    };

    if transfer.from != call.from {
        return Err(UnverifiedReason::TransferSender {
            calldata: call.from,
            logged: transfer.from,
        });
    }

    if transfer.value != call.amount {
        return Err(UnverifiedReason::TransferAmount {
            calldata: call.amount,
            logged: transfer.value,
        });
    }

    Ok(call.amount)
}

/// Splits payment calldata into the `transferFrom` call and the order id.
fn decode_payment_calldata(
    input: &[u8],
) -> Result<(IERC20::transferFromCall, B256), UnverifiedReason> {
    if input.len() != PAYMENT_CALLDATA_LEN {
        return Err(UnverifiedReason::CalldataLength {
            length: input.len(),
        });
    }

    let (selector, arguments) = input.split_at(4);
    let selector = FixedBytes::<4>::from_slice(selector);

    if selector != IERC20::transferFromCall::SELECTOR {
        return Err(UnverifiedReason::NotTransferFrom { selector });
    }

    let (words, order_id) = arguments.split_at(ORDER_ID_OFFSET - 4);

    let call = IERC20::transferFromCall::abi_decode_raw_validate(words)
        .map_err(|_| UnverifiedReason::CalldataUndecodable)?;

    Ok((call, B256::from_slice(order_id)))
}

/// The stable's `Transfer` to our wallet in `log`, if it is one. A log that
/// carries the `Transfer` topic but does not decode is not counted.
fn transfer_to(log: &Log, terms: &PaymentTerms) -> Option<IERC20::Transfer> {
    if log.address() != terms.stable || log.topic0() != Some(&IERC20::Transfer::SIGNATURE_HASH) {
        return None;
    }

    let transfer = log.log_decode::<IERC20::Transfer>().ok()?.inner.data;

    (transfer.to == terms.recipient).then_some(transfer)
}

/// The `RelayErc20Deposit` in `log` from `depository`, if it is one.
pub(super) fn deposit_event(log: &Log, depository: Address) -> Option<RelayErc20Deposit> {
    if log.address() != depository || log.topic0() != Some(&RelayErc20Deposit::SIGNATURE_HASH) {
        return None;
    }

    log.log_decode::<RelayErc20Deposit>()
        .ok()
        .map(|decoded| decoded.inner.data)
}

#[cfg(test)]
mod tests {
    use alloy::consensus::Transaction as _;
    use alloy::primitives::{address, b256};
    use alloy::rpc::types::Transaction;

    use super::*;

    const FUNDED_WALLET: Address = address!("0xe385C5EE42d7B81A6a51E759FaaFca6159Fd04B6");

    const SOLVER: Address = address!("0xf70da97812cb96acdf810712aa562db8dfa3dbef");

    const ROBINHOOD_USDG: Address = address!("0x5fc5360d0400a0fd4f2af552add042d716f1d168");

    const ETHEREUM_USDC: Address = address!("0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48");

    const DEPOSITORY: Address = address!("0x4cd00e387622c35bddb9b4c962c136462338bc31");

    /// The funded test's Robinhood -> Ethereum order, filled on Ethereum.
    const FILLED_ORDER: RelayOrderId = RelayOrderId(b256!(
        "0x266b12442f9b86ef731fae285c34f489217acdfcedd755422ce47db429992d85"
    ));

    /// The funded test's forced refund, paid on Robinhood.
    const REFUNDED_ORDER: RelayOrderId = RelayOrderId(b256!(
        "0x705cf340fd2bd8b1a4c563e63eaf5f1e8611efe2b448e3509253bf303a5e0a2b"
    ));

    fn fixture_tx(body: &str) -> Transaction {
        serde_json::from_str(body).unwrap()
    }

    fn fixture_receipt(body: &str) -> TransactionReceipt {
        serde_json::from_str(body).unwrap()
    }

    fn fill() -> (Transaction, TransactionReceipt) {
        (
            fixture_tx(include_str!("../../relay-fixtures/fill_tx_ethereum.json")),
            fixture_receipt(include_str!(
                "../../relay-fixtures/fill_receipt_ethereum.json"
            )),
        )
    }

    fn refund() -> (Transaction, TransactionReceipt) {
        (
            fixture_tx(include_str!(
                "../../relay-fixtures/refund_tx_robinhood.json"
            )),
            fixture_receipt(include_str!(
                "../../relay-fixtures/refund_receipt_robinhood.json"
            )),
        )
    }

    fn terms(stable: Address, order_id: RelayOrderId) -> PaymentTerms {
        PaymentTerms {
            stable,
            recipient: FUNDED_WALLET,
            order_id,
        }
    }

    #[test]
    fn real_fill_calldata_is_transfer_from_with_trailing_order_id() {
        let (tx, _) = fill();

        let (call, order_id) = decode_payment_calldata(tx.input()).unwrap();

        assert_eq!(tx.input().len(), 132);
        assert_eq!(call.from, SOLVER);
        assert_eq!(call.to, FUNDED_WALLET);
        assert_eq!(call.amount, U256::from(4_763_755));
        assert_eq!(RelayOrderId(order_id), FILLED_ORDER);
    }

    #[test]
    fn real_refund_calldata_has_the_fill_layout() {
        let (tx, _) = refund();

        let (call, order_id) = decode_payment_calldata(tx.input()).unwrap();

        assert_eq!(call.from, SOLVER);
        assert_eq!(call.to, FUNDED_WALLET);
        assert_eq!(call.amount, U256::from(2_995_154));
        assert_eq!(RelayOrderId(order_id), REFUNDED_ORDER);
    }

    #[test]
    fn real_fill_proves_on_ethereum() {
        let (tx, receipt) = fill();

        let amount = check_payment(
            &terms(ETHEREUM_USDC, FILLED_ORDER),
            tx.to(),
            tx.input(),
            &receipt,
        )
        .unwrap();

        assert_eq!(amount, U256::from(4_763_755));
    }

    #[test]
    fn real_refund_proves_on_robinhood() {
        let (tx, receipt) = refund();

        let amount = check_payment(
            &terms(ROBINHOOD_USDG, REFUNDED_ORDER),
            tx.to(),
            tx.input(),
            &receipt,
        )
        .unwrap();

        assert_eq!(amount, U256::from(2_995_154));
    }

    #[test]
    fn real_fill_for_another_order_is_unverified() {
        let (tx, receipt) = fill();

        let reason = check_payment(
            &terms(ETHEREUM_USDC, REFUNDED_ORDER),
            tx.to(),
            tx.input(),
            &receipt,
        )
        .unwrap_err();

        let RelayOrderId(found) = FILLED_ORDER;
        assert_eq!(reason, UnverifiedReason::OrderId { found });
    }

    #[test]
    fn real_refund_checked_as_ethereum_usdc_is_not_the_stable() {
        let (tx, receipt) = refund();

        let reason = check_payment(
            &terms(ETHEREUM_USDC, REFUNDED_ORDER),
            tx.to(),
            tx.input(),
            &receipt,
        )
        .unwrap_err();

        assert_eq!(
            reason,
            UnverifiedReason::NotTheStable {
                to: Some(ROBINHOOD_USDG)
            }
        );
    }

    #[test]
    fn payment_to_another_wallet_is_unverified() {
        let (tx, receipt) = fill();
        let other = address!("0x1111111111111111111111111111111111111111");

        let reason = check_payment(
            &PaymentTerms {
                recipient: other,
                ..terms(ETHEREUM_USDC, FILLED_ORDER)
            },
            tx.to(),
            tx.input(),
            &receipt,
        )
        .unwrap_err();

        assert_eq!(
            reason,
            UnverifiedReason::Recipient {
                recipient: FUNDED_WALLET
            }
        );
    }

    #[test]
    fn calldata_without_the_order_id_word_is_unverified() {
        let (tx, receipt) = fill();
        let truncated = &tx.input()[..100];

        let reason = check_payment(
            &terms(ETHEREUM_USDC, FILLED_ORDER),
            tx.to(),
            truncated,
            &receipt,
        )
        .unwrap_err();

        assert_eq!(reason, UnverifiedReason::CalldataLength { length: 100 });
    }

    #[test]
    fn other_selector_is_unverified() {
        let (tx, receipt) = fill();
        let mut input = tx.input().to_vec();
        input[..4].copy_from_slice(&IERC20::transferCall::SELECTOR);

        let reason = check_payment(
            &terms(ETHEREUM_USDC, FILLED_ORDER),
            tx.to(),
            &input,
            &receipt,
        )
        .unwrap_err();

        assert_eq!(
            reason,
            UnverifiedReason::NotTransferFrom {
                selector: IERC20::transferCall::SELECTOR.into()
            }
        );
    }

    #[test]
    fn real_deposit_logs_decode_to_their_order() {
        let receipt = fixture_receipt(include_str!(
            "../../relay-fixtures/deposit_receipt_robinhood.json"
        ));

        let deposits = receipt
            .inner
            .logs()
            .iter()
            .filter_map(|log| deposit_event(log, DEPOSITORY))
            .collect::<Vec<_>>();

        let [deposit] = deposits.as_slice() else {
            panic!("expected one deposit event, got {deposits:?}");
        };
        assert_eq!(deposit.from, FUNDED_WALLET);
        assert_eq!(deposit.token, ROBINHOOD_USDG);
        assert_eq!(deposit.amount, U256::from(5_000_000));
        assert_eq!(RelayOrderId(deposit.id), FILLED_ORDER);
    }
}
