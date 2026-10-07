//! `POST /quote/v2`: the request we send, the quote we accept, and the checks
//! that bind Relay's approve and deposit steps to the transfer we asked for.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use alloy::primitives::{Address, B256, Bytes, U256};
use alloy::sol;
use alloy::sol_types::SolCall;
use serde::de::IgnoredAny;
use serde::{Deserialize, Serialize};

use st0x_evm::Chain;

use super::acceptance::{BasisPoints, QuoteAmounts};

sol! {
    function approve(address spender, uint256 amount) external returns (bool);

    /// Relay depository v2 entry point. The `id` is the order id that every
    /// later fill or refund carries, not the quote's request id.
    function depositErc20(address depositor, address token, uint256 amount, bytes32 id) external;
}

/// An exact-input quote for moving `amount` of `origin`'s settlement stable to
/// `destination`'s settlement stable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuoteRequest {
    pub origin: Chain,
    pub destination: Chain,
    /// Smallest unit of the origin stable.
    pub amount: U256,
    /// The wallet that signs the approve and the deposit.
    pub user: Address,
    pub recipient: Address,
    /// Always explicit: Relay's docs disagree on what an unset `refundTo` does.
    pub refund_to: Address,
    /// Always explicit: an unset slippage makes Relay floor the fill 2% below
    /// the expected amount.
    pub slippage: BasisPoints,
    /// How long the fill may take, sent as Relay's `ttl` in whole seconds.
    /// Relay does not shorten the order deadline to it (see
    /// [`RelayQuote::deadline`]).
    pub ttl: Duration,
}

/// Relay's handle for a quote, used only to read its status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize)]
#[serde(transparent)]
pub struct RelayRequestId(pub B256);

impl std::fmt::Display for RelayRequestId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self(id) = self;
        write!(formatter, "{id}")
    }
}

/// The deposit's `bytes32 id`: the key the depository logs and the trailing
/// calldata word of the fill or refund that settles it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RelayOrderId(pub B256);

impl std::fmt::Display for RelayOrderId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self(id) = self;
        write!(formatter, "{id}")
    }
}

/// A quote whose approve and deposit steps were checked against the request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelayQuote {
    pub request_id: RelayRequestId,
    /// Decoded from the deposit step's calldata.
    pub order_id: RelayOrderId,
    pub amounts: QuoteAmounts,
    /// The order's `output.deadline`: until then the solver may still fill.
    /// Relay sets it a week after the quote whatever `ttl` asks for.
    pub deadline: SystemTime,
    pub fees: QuoteFees,
    /// `None` when Relay sends no approve step.
    pub approve: Option<StepTransaction>,
    pub deposit: StepTransaction,
}

/// Fees Relay quoted, both paid on the origin chain.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QuoteFees {
    /// In the origin stable's smallest unit, already out of the expected amount.
    pub relayer: U256,
    /// In the origin chain's native token (wei): Relay's estimate of our gas.
    pub gas: U256,
}

/// A transaction Relay asks the user to send.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StepTransaction {
    pub chain_id: u64,
    pub to: Address,
    pub data: Bytes,
    pub value: U256,
}

/// The step of a quote a check refers to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuoteStep {
    Approve,
    Deposit,
}

/// A value in a quote that must equal what the request implies.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuoteField {
    /// `protocol.v2.orderData.output.chainId`: where the solver pays.
    OutputChain,
    /// `protocol.v2.paymentDetails.chainId`: where we deposit.
    PaymentDetailsChain,
    /// The contract the approve step calls.
    ApproveTarget,
    ApproveSpender,
    ApproveAmount,
    /// The contract the deposit step calls.
    DepositTarget,
    Depositor,
    DepositToken,
    DepositAmount,
    /// `details.currencyIn.amount`.
    InputAmount,
    /// `details.recipient`.
    Recipient,
    /// `protocol.v2.orderData.inputs[0].payment.chainId`: where the order says
    /// we pay.
    InputPaymentChain,
    InputPaymentCurrency,
    InputPaymentAmount,
    PaymentRecipient,
    PaymentCurrency,
    PaymentMinimum,
    PaymentExpected,
    RefundRecipient,
    /// A refund option's token, which must be the stable of the chain it pays on.
    RefundCurrency,
    PaymentDetailsDepository,
    PaymentDetailsCurrency,
    PaymentDetailsAmount,
}

/// How a quote differs from the transfer it was requested for.
#[derive(Debug, thiserror::Error)]
pub enum QuoteMismatch {
    #[error("Relay has no depository pinned on {chain}")]
    NoDepository { chain: Chain },
    #[error("quote has no deposit step")]
    MissingDeposit,
    #[error("quote step {position} is neither approve nor deposit")]
    UnexpectedStep { position: usize },
    #[error("quote has more than one {step:?} step")]
    DuplicateStep { step: QuoteStep },
    #[error("{step:?} step has {count} items, expected one")]
    ItemCount { step: QuoteStep, count: usize },
    #[error("{step:?} step is on chain {actual}, expected {expected}")]
    StepChain {
        step: QuoteStep,
        expected: u64,
        actual: u64,
    },
    #[error("{step:?} step sends {value} native value, expected none")]
    StepValue { step: QuoteStep, value: U256 },
    #[error("{step:?} step calldata does not decode: {source}")]
    StepCalldata {
        step: QuoteStep,
        #[source]
        source: alloy::sol_types::Error,
    },
    #[error("Relay has no chain name pinned for {chain}")]
    UnnamedChain { chain: Chain },
    #[error("quote {field:?} is {actual}, expected {expected}")]
    ChainMismatch {
        field: QuoteField,
        expected: &'static str,
        actual: String,
    },
    #[error("quote {field:?} is {actual}, expected {expected}")]
    AddressMismatch {
        field: QuoteField,
        expected: Address,
        actual: Address,
    },
    #[error("quote {field:?} is {actual}, expected {expected}")]
    AmountMismatch {
        field: QuoteField,
        expected: U256,
        actual: U256,
    },
    #[error("quote input is {actual}, expected {expected}")]
    InputCurrency {
        expected: QuotedCurrency,
        actual: QuotedCurrency,
    },
    #[error("quote output is {actual}, expected {expected}")]
    OutputCurrency {
        expected: QuotedCurrency,
        actual: QuotedCurrency,
    },
    #[error("quote gives {currency} {actual} decimals, expected {expected}")]
    CurrencyDecimals {
        currency: QuotedCurrency,
        expected: u8,
        actual: u8,
    },
    #[error("relayer fee is in {actual}, expected {expected}")]
    RelayerFeeCurrency {
        expected: QuotedCurrency,
        actual: QuotedCurrency,
    },
    #[error("gas fee is on chain {actual}, expected {expected}")]
    GasFeeChain { expected: u64, actual: u64 },
    #[error("order has no refund option")]
    MissingRefund,
    #[error("refund is on {actual}, expected {origin} or {destination}")]
    RefundChain {
        origin: &'static str,
        destination: &'static str,
        actual: String,
    },
    #[error("order has {count} inputs, expected one")]
    InputCount { count: usize },
    #[error("order carries {count} fees, expected none")]
    OrderFees { count: usize },
    #[error("order output carries {count} calls, expected none")]
    OutputCalls { count: usize },
    #[error("order has {count} output payments, expected one")]
    PaymentCount { count: usize },
    #[error("quote names order {quoted}, the deposit calldata {calldata}")]
    OrderIdMismatch {
        quoted: RelayOrderId,
        calldata: RelayOrderId,
    },
    #[error("order data hashes to order {derived}, the quote names order {quoted}")]
    OrderIdNotDerived {
        derived: RelayOrderId,
        quoted: RelayOrderId,
    },
    #[error("order deadline {seconds} is not a representable time")]
    Deadline { seconds: u64 },
}

/// A token on a chain, as a quote names it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QuotedCurrency {
    pub chain_id: u64,
    pub address: Address,
}

impl QuotedCurrency {
    fn settlement_stable(chain: Chain) -> Self {
        Self {
            chain_id: chain.chain_id(),
            address: chain.settlement_stable().address,
        }
    }
}

impl std::fmt::Display for QuotedCurrency {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{} on chain {}", self.address, self.chain_id)
    }
}

/// Wire body of `POST /quote/v2`. Amounts go as decimal strings: alloy's serde
/// would send `U256` as hex, which Relay does not read.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct QuoteRequestBody {
    user: Address,
    origin_chain_id: u64,
    destination_chain_id: u64,
    origin_currency: Address,
    destination_currency: Address,
    amount: String,
    trade_type: &'static str,
    recipient: Address,
    refund_to: Address,
    slippage_tolerance: String,
    ttl: u64,
}

impl From<&QuoteRequest> for QuoteRequestBody {
    fn from(request: &QuoteRequest) -> Self {
        let BasisPoints(slippage) = request.slippage;

        Self {
            user: request.user,
            origin_chain_id: request.origin.chain_id(),
            destination_chain_id: request.destination.chain_id(),
            origin_currency: request.origin.settlement_stable().address,
            destination_currency: request.destination.settlement_stable().address,
            amount: request.amount.to_string(),
            trade_type: "EXACT_INPUT",
            recipient: request.recipient,
            refund_to: request.refund_to,
            slippage_tolerance: slippage.to_string(),
            ttl: request.ttl.as_secs(),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct QuoteResponse {
    request_id: RelayRequestId,
    steps: Vec<RawStep>,
    fees: RawFees,
    details: RawDetails,
    protocol: RawProtocol,
}

#[derive(Debug, Deserialize)]
struct RawStep {
    id: String,
    items: Vec<RawStepItem>,
}

#[derive(Debug, Deserialize)]
struct RawStepItem {
    data: RawStepData,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawStepData {
    to: Address,
    data: Bytes,
    #[serde(with = "decimal")]
    value: U256,
    chain_id: u64,
}

#[derive(Debug, Deserialize)]
struct RawFees {
    gas: RawAmount,
    relayer: RawAmount,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawDetails {
    recipient: Address,
    currency_in: RawAmount,
    currency_out: RawAmount,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawAmount {
    currency: RawCurrency,
    #[serde(with = "decimal")]
    amount: U256,
    #[serde(with = "decimal")]
    minimum_amount: U256,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawCurrency {
    chain_id: u64,
    address: Address,
    decimals: u8,
}

/// The order the deposit commits to: who the solver pays, who a refund pays,
/// and what the depository is told to expect.
#[derive(Debug, Deserialize)]
struct RawProtocol {
    v2: RawProtocolV2,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawProtocolV2 {
    /// Must equal the deposit calldata's `id`, or the checked order is not the
    /// one the deposit funds.
    order_id: B256,
    order_data: RawOrderData,
    payment_details: RawPaymentDetails,
}

/// Every live quote carries empty `fees` and `output.calls`; their entries are
/// only counted, as no shape for them has been seen.
#[derive(Debug, Deserialize)]
struct RawOrderData {
    inputs: Vec<RawOrderInput>,
    fees: Vec<IgnoredAny>,
    output: RawOrderOutput,
}

#[derive(Debug, Deserialize)]
struct RawOrderInput {
    payment: RawInputPayment,
    refunds: Vec<RawRefund>,
}

/// What the order says we pay in.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawInputPayment {
    /// Relay's chain name, not the numeric id.
    chain_id: String,
    currency: Address,
    #[serde(with = "decimal")]
    amount: U256,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawRefund {
    /// Relay's chain name, not the numeric id.
    chain_id: String,
    recipient: Address,
    currency: Address,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawOrderOutput {
    /// Relay's chain name, not the numeric id.
    chain_id: String,
    payments: Vec<RawPayment>,
    calls: Vec<IgnoredAny>,
    /// Unix seconds.
    deadline: u64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawPayment {
    recipient: Address,
    currency: Address,
    #[serde(with = "decimal")]
    minimum_amount: U256,
    #[serde(with = "decimal")]
    expected_amount: U256,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawPaymentDetails {
    /// Relay's chain name, not the numeric id.
    chain_id: String,
    depository: Address,
    currency: Address,
    #[serde(with = "decimal")]
    amount: U256,
}

impl From<&RawCurrency> for QuotedCurrency {
    fn from(currency: &RawCurrency) -> Self {
        Self {
            chain_id: currency.chain_id,
            address: currency.address,
        }
    }
}

impl QuoteResponse {
    /// Checks the quote against `request` and the origin's pinned `depository`,
    /// and reads the order id from the deposit calldata, which must match
    /// `protocol.v2.orderId`.
    pub(super) fn validate(
        self,
        request: &QuoteRequest,
        depository: Address,
    ) -> Result<RelayQuote, QuoteMismatch> {
        let origin = request.origin;
        let origin_stable = origin.settlement_stable().address;

        check_currency(
            &self.details.currency_in.currency,
            origin,
            |expected, actual| QuoteMismatch::InputCurrency { expected, actual },
        )?;
        check_currency(
            &self.details.currency_out.currency,
            request.destination,
            |expected, actual| QuoteMismatch::OutputCurrency { expected, actual },
        )?;

        check_amount(
            QuoteField::InputAmount,
            request.amount,
            self.details.currency_in.amount,
        )?;

        check_fees(&self.fees, origin)?;
        check_order_input(&self.protocol.v2.order_data, request)?;
        check_output_payment(&self.details, &self.protocol.v2.order_data, request)?;
        check_refunds(&self.protocol.v2.order_data, request)?;
        check_payment_details(&self.protocol.v2.payment_details, request, depository)?;

        let (approve, deposit) = split_steps(self.steps)?;

        let approve = approve
            .map(|approve| {
                check_approve(&approve, origin, origin_stable, depository, request.amount)?;
                Ok::<_, QuoteMismatch>(approve)
            })
            .transpose()?;

        let order_id = check_deposit(&deposit, request, origin_stable, depository)?;

        let quoted = RelayOrderId(self.protocol.v2.order_id);
        if quoted != order_id {
            return Err(QuoteMismatch::OrderIdMismatch {
                quoted,
                calldata: order_id,
            });
        }

        let seconds = self.protocol.v2.order_data.output.deadline;
        let deadline = UNIX_EPOCH
            .checked_add(Duration::from_secs(seconds))
            .ok_or(QuoteMismatch::Deadline { seconds })?;

        Ok(RelayQuote {
            request_id: self.request_id,
            order_id,
            amounts: QuoteAmounts {
                amount_in: request.amount,
                expected_out: self.details.currency_out.amount,
                minimum_out: self.details.currency_out.minimum_amount,
                slippage: request.slippage,
            },
            deadline,
            fees: QuoteFees {
                relayer: self.fees.relayer.amount,
                gas: self.fees.gas.amount,
            },
            approve,
            deposit,
        })
    }
}

fn check_currency(
    currency: &RawCurrency,
    chain: Chain,
    mismatch: impl FnOnce(QuotedCurrency, QuotedCurrency) -> QuoteMismatch,
) -> Result<(), QuoteMismatch> {
    let expected = QuotedCurrency::settlement_stable(chain);
    let actual = QuotedCurrency::from(currency);

    if actual != expected {
        return Err(mismatch(expected, actual));
    }

    let decimals = chain.settlement_stable().decimals;
    if currency.decimals != decimals {
        return Err(QuoteMismatch::CurrencyDecimals {
            currency: actual,
            expected: decimals,
            actual: currency.decimals,
        });
    }

    Ok(())
}

/// Both fees are paid on the origin chain: the relayer fee in the origin
/// stable, the gas in its native token.
fn check_fees(fees: &RawFees, origin: Chain) -> Result<(), QuoteMismatch> {
    let expected = QuotedCurrency::settlement_stable(origin);
    let actual = QuotedCurrency::from(&fees.relayer.currency);

    if actual != expected {
        return Err(QuoteMismatch::RelayerFeeCurrency { expected, actual });
    }

    if fees.gas.currency.chain_id != origin.chain_id() {
        return Err(QuoteMismatch::GasFeeChain {
            expected: origin.chain_id(),
            actual: fees.gas.currency.chain_id,
        });
    }

    Ok(())
}

/// The order takes exactly one input, the requested amount of the origin
/// stable on the origin chain, and carries no extra fee.
fn check_order_input(order: &RawOrderData, request: &QuoteRequest) -> Result<(), QuoteMismatch> {
    let [input] = order.inputs.as_slice() else {
        return Err(QuoteMismatch::InputCount {
            count: order.inputs.len(),
        });
    };

    check_chain(
        QuoteField::InputPaymentChain,
        request.origin,
        &input.payment.chain_id,
    )?;
    check_address(
        QuoteField::InputPaymentCurrency,
        request.origin.settlement_stable().address,
        input.payment.currency,
    )?;
    check_amount(
        QuoteField::InputPaymentAmount,
        request.amount,
        input.payment.amount,
    )?;

    if !order.fees.is_empty() {
        return Err(QuoteMismatch::OrderFees {
            count: order.fees.len(),
        });
    }

    Ok(())
}

/// The solver pays exactly one output: the destination stable to our
/// recipient, at the amounts `details` quotes, with no call attached.
fn check_output_payment(
    details: &RawDetails,
    order: &RawOrderData,
    request: &QuoteRequest,
) -> Result<(), QuoteMismatch> {
    check_address(QuoteField::Recipient, request.recipient, details.recipient)?;
    check_chain(
        QuoteField::OutputChain,
        request.destination,
        &order.output.chain_id,
    )?;

    if !order.output.calls.is_empty() {
        return Err(QuoteMismatch::OutputCalls {
            count: order.output.calls.len(),
        });
    }

    let [payment] = order.output.payments.as_slice() else {
        return Err(QuoteMismatch::PaymentCount {
            count: order.output.payments.len(),
        });
    };

    check_address(
        QuoteField::PaymentRecipient,
        request.recipient,
        payment.recipient,
    )?;
    check_address(
        QuoteField::PaymentCurrency,
        request.destination.settlement_stable().address,
        payment.currency,
    )?;
    check_amount(
        QuoteField::PaymentMinimum,
        details.currency_out.minimum_amount,
        payment.minimum_amount,
    )?;
    check_amount(
        QuoteField::PaymentExpected,
        details.currency_out.amount,
        payment.expected_amount,
    )
}

/// Relay offers a refund on either end: in the origin stable on the origin
/// chain or in the destination stable on the destination chain. Every option
/// must pay our `refundTo`, and there must be at least one.
fn check_refunds(order: &RawOrderData, request: &QuoteRequest) -> Result<(), QuoteMismatch> {
    let origin = request
        .origin
        .relay_name()
        .ok_or(QuoteMismatch::UnnamedChain {
            chain: request.origin,
        })?;
    let destination = request
        .destination
        .relay_name()
        .ok_or(QuoteMismatch::UnnamedChain {
            chain: request.destination,
        })?;

    let mut refunds = order
        .inputs
        .iter()
        .flat_map(|input| &input.refunds)
        .peekable();

    if refunds.peek().is_none() {
        return Err(QuoteMismatch::MissingRefund);
    }

    refunds.try_for_each(|refund| {
        check_address(
            QuoteField::RefundRecipient,
            request.refund_to,
            refund.recipient,
        )?;

        let chain = if refund.chain_id == origin {
            request.origin
        } else if refund.chain_id == destination {
            request.destination
        } else {
            return Err(QuoteMismatch::RefundChain {
                origin,
                destination,
                actual: refund.chain_id.clone(),
            });
        };

        check_address(
            QuoteField::RefundCurrency,
            chain.settlement_stable().address,
            refund.currency,
        )
    })
}

fn check_payment_details(
    payment: &RawPaymentDetails,
    request: &QuoteRequest,
    depository: Address,
) -> Result<(), QuoteMismatch> {
    check_chain(
        QuoteField::PaymentDetailsChain,
        request.origin,
        &payment.chain_id,
    )?;
    check_address(
        QuoteField::PaymentDetailsDepository,
        depository,
        payment.depository,
    )?;
    check_address(
        QuoteField::PaymentDetailsCurrency,
        request.origin.settlement_stable().address,
        payment.currency,
    )?;
    check_amount(
        QuoteField::PaymentDetailsAmount,
        request.amount,
        payment.amount,
    )
}

fn check_chain(field: QuoteField, expected: Chain, actual: &str) -> Result<(), QuoteMismatch> {
    let name = expected
        .relay_name()
        .ok_or(QuoteMismatch::UnnamedChain { chain: expected })?;

    if actual == name {
        Ok(())
    } else {
        Err(QuoteMismatch::ChainMismatch {
            field,
            expected: name,
            actual: actual.to_owned(),
        })
    }
}

fn check_address(
    field: QuoteField,
    expected: Address,
    actual: Address,
) -> Result<(), QuoteMismatch> {
    if actual == expected {
        Ok(())
    } else {
        Err(QuoteMismatch::AddressMismatch {
            field,
            expected,
            actual,
        })
    }
}

fn check_amount(field: QuoteField, expected: U256, actual: U256) -> Result<(), QuoteMismatch> {
    if actual == expected {
        Ok(())
    } else {
        Err(QuoteMismatch::AmountMismatch {
            field,
            expected,
            actual,
        })
    }
}

/// Splits the steps into the optional approve and the required deposit,
/// refusing any other step: an unknown step is a flow this code never signs.
fn split_steps(
    steps: Vec<RawStep>,
) -> Result<(Option<StepTransaction>, StepTransaction), QuoteMismatch> {
    let mut approve = None;
    let mut deposit = None;

    for (position, step) in steps.into_iter().enumerate() {
        let (kind, slot) = match step.id.as_str() {
            "approve" => (QuoteStep::Approve, &mut approve),
            "deposit" => (QuoteStep::Deposit, &mut deposit),
            _ => return Err(QuoteMismatch::UnexpectedStep { position }),
        };

        if slot.is_some() {
            return Err(QuoteMismatch::DuplicateStep { step: kind });
        }

        let [item] =
            <[RawStepItem; 1]>::try_from(step.items).map_err(|items| QuoteMismatch::ItemCount {
                step: kind,
                count: items.len(),
            })?;

        *slot = Some(StepTransaction {
            chain_id: item.data.chain_id,
            to: item.data.to,
            data: item.data.data,
            value: item.data.value,
        });
    }

    let deposit = deposit.ok_or(QuoteMismatch::MissingDeposit)?;

    Ok((approve, deposit))
}

fn check_step_envelope(
    step: QuoteStep,
    transaction: &StepTransaction,
    origin: Chain,
    target_field: QuoteField,
    expected_target: Address,
) -> Result<(), QuoteMismatch> {
    if transaction.chain_id != origin.chain_id() {
        return Err(QuoteMismatch::StepChain {
            step,
            expected: origin.chain_id(),
            actual: transaction.chain_id,
        });
    }

    check_address(target_field, expected_target, transaction.to)?;

    if !transaction.value.is_zero() {
        return Err(QuoteMismatch::StepValue {
            step,
            value: transaction.value,
        });
    }

    Ok(())
}

fn check_approve(
    approve: &StepTransaction,
    origin: Chain,
    origin_stable: Address,
    depository: Address,
    amount: U256,
) -> Result<(), QuoteMismatch> {
    check_step_envelope(
        QuoteStep::Approve,
        approve,
        origin,
        QuoteField::ApproveTarget,
        origin_stable,
    )?;

    let call =
        approveCall::abi_decode(&approve.data).map_err(|source| QuoteMismatch::StepCalldata {
            step: QuoteStep::Approve,
            source,
        })?;

    check_address(QuoteField::ApproveSpender, depository, call.spender)?;
    check_amount(QuoteField::ApproveAmount, amount, call.amount)
}

fn check_deposit(
    deposit: &StepTransaction,
    request: &QuoteRequest,
    origin_stable: Address,
    depository: Address,
) -> Result<RelayOrderId, QuoteMismatch> {
    check_step_envelope(
        QuoteStep::Deposit,
        deposit,
        request.origin,
        QuoteField::DepositTarget,
        depository,
    )?;

    let call = depositErc20Call::abi_decode(&deposit.data).map_err(|source| {
        QuoteMismatch::StepCalldata {
            step: QuoteStep::Deposit,
            source,
        }
    })?;

    check_address(QuoteField::Depositor, request.user, call.depositor)?;
    check_address(QuoteField::DepositToken, origin_stable, call.token)?;
    check_amount(QuoteField::DepositAmount, request.amount, call.amount)?;

    Ok(RelayOrderId(call.id))
}

/// Relay writes amounts as base-10 strings.
mod decimal {
    use alloy::primitives::U256;
    use serde::{Deserialize, Deserializer, de::Error};

    pub(super) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<U256, D::Error> {
        let text = String::deserialize(deserializer)?;
        U256::from_str_radix(&text, 10).map_err(D::Error::custom)
    }
}

#[cfg(test)]
pub(super) mod tests {
    use alloy::primitives::{address, b256};
    use serde_json::{Value, json};

    use super::*;
    use crate::relay::acceptance::QuoteBounds;

    pub(in crate::relay) const FUNDED_QUOTE: &str =
        include_str!("../../relay-fixtures/quote_funded_robinhood_to_ethereum.json");

    pub(in crate::relay) const FUNDED_WALLET: Address =
        address!("0xe385C5EE42d7B81A6a51E759FaaFca6159Fd04B6");

    const DEPOSITORY: Address = address!("0x4cd00e387622c35bddb9b4c962c136462338bc31");

    /// The request behind the funded test quote: 5 USDG Robinhood to
    /// Ethereum, 30 bps slippage.
    pub(in crate::relay) fn funded_request() -> QuoteRequest {
        QuoteRequest {
            origin: Chain::Robinhood,
            destination: Chain::Ethereum,
            amount: U256::from(5_000_000),
            user: FUNDED_WALLET,
            recipient: FUNDED_WALLET,
            refund_to: FUNDED_WALLET,
            slippage: BasisPoints::new(30).unwrap(),
            ttl: Duration::from_secs(1800),
        }
    }

    /// A 1000-unit request for the `0xdead` user, as the unfunded fixtures
    /// were quoted.
    fn dead_request(origin: Chain, destination: Chain, slippage: u16) -> QuoteRequest {
        let dead = address!("0x000000000000000000000000000000000000dEaD");

        QuoteRequest {
            origin,
            destination,
            amount: U256::from(1_000_000_000),
            user: dead,
            recipient: dead,
            refund_to: dead,
            slippage: BasisPoints::new(slippage).unwrap(),
            ttl: Duration::from_secs(1800),
        }
    }

    fn validate(body: &Value, request: &QuoteRequest) -> Result<RelayQuote, QuoteMismatch> {
        serde_json::from_value::<QuoteResponse>(body.clone())
            .unwrap()
            .validate(request, request.origin.relay_depository().unwrap())
    }

    fn funded_body() -> Value {
        serde_json::from_str(FUNDED_QUOTE).unwrap()
    }

    /// Validates the funded quote against its request after `mutate` edits it.
    fn refusal(mutate: impl FnOnce(&mut Value)) -> QuoteMismatch {
        let mut body = funded_body();
        mutate(&mut body);
        validate(&body, &funded_request()).unwrap_err()
    }

    const OTHER: Address = address!("0x1111111111111111111111111111111111111111");

    const ROBINHOOD_USDG: Address = address!("0x5fc5360d0400a0fd4f2af552add042d716f1d168");

    const ETHEREUM_USDC: Address = address!("0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48");

    #[test]
    fn request_body_sends_decimal_amount_and_explicit_slippage() {
        let body = serde_json::to_value(QuoteRequestBody::from(&funded_request())).unwrap();

        assert_eq!(
            body,
            json!({
                "user": "0xe385c5ee42d7b81a6a51e759faafca6159fd04b6",
                "originChainId": 4663,
                "destinationChainId": 1,
                "originCurrency": "0x5fc5360d0400a0fd4f2af552add042d716f1d168",
                "destinationCurrency": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
                "amount": "5000000",
                "tradeType": "EXACT_INPUT",
                "recipient": "0xe385c5ee42d7b81a6a51e759faafca6159fd04b6",
                "refundTo": "0xe385c5ee42d7b81a6a51e759faafca6159fd04b6",
                "slippageTolerance": "30",
                "ttl": 1800,
            })
        );
    }

    #[test]
    fn order_id_is_the_deposit_calldata_id_not_request_id() {
        let quote = validate(&funded_body(), &funded_request()).unwrap();

        assert_eq!(
            quote.order_id,
            RelayOrderId(b256!(
                "0x266b12442f9b86ef731fae285c34f489217acdfcedd755422ce47db429992d85"
            ))
        );
        assert_eq!(
            quote.request_id,
            RelayRequestId(b256!(
                "0x1790875063e29a43c3a2201049fa8f6e6542e940fe2060ed03ea9107630b5a49"
            ))
        );
    }

    #[test]
    fn deposit_calldata_for_another_order_is_refused() {
        let error = refusal(|body| {
            let calldata = body["steps"][1]["items"][0]["data"]["data"]
                .as_str()
                .unwrap()
                .to_owned();
            let (head, _) = calldata.split_at(calldata.len() - 64);
            body["steps"][1]["items"][0]["data"]["data"] =
                json!(format!("{head}{}", "11".repeat(32)));
        });

        assert!(
            matches!(
                error,
                QuoteMismatch::OrderIdMismatch { quoted, calldata }
                    if quoted == RelayOrderId(b256!(
                        "0x266b12442f9b86ef731fae285c34f489217acdfcedd755422ce47db429992d85"
                    ))
                        && calldata == RelayOrderId(B256::repeat_byte(0x11))
            ),
            "{error:?}"
        );
    }

    #[test]
    fn order_data_the_order_id_does_not_commit_to_is_refused() {
        let error = refusal(|body| body["protocol"]["v2"]["orderData"]["solver"] = json!(OTHER));

        assert!(
            matches!(
                error,
                QuoteMismatch::OrderIdNotDerived { derived, quoted }
                    if quoted == RelayOrderId(b256!(
                        "0x266b12442f9b86ef731fae285c34f489217acdfcedd755422ce47db429992d85"
                    ))
                        && derived != quoted
            ),
            "{error:?}"
        );
    }

    #[test]
    fn funded_quote_reads_amounts_fees_and_steps() {
        let quote = validate(&funded_body(), &funded_request()).unwrap();

        assert_eq!(
            quote.amounts,
            QuoteAmounts {
                amount_in: U256::from(5_000_000),
                expected_out: U256::from(4_763_755),
                minimum_out: U256::from(4_749_464),
                slippage: BasisPoints::new(30).unwrap(),
            }
        );
        assert_eq!(
            quote.fees,
            QuoteFees {
                relayer: U256::from(235_904),
                gas: U256::from(2_382_592_438_800_u64),
            }
        );

        let approve = quote.approve.unwrap();
        assert_eq!(approve.chain_id, 4663);
        assert_eq!(
            approve.to,
            address!("0x5fc5360D0400a0Fd4f2af552ADD042D716F1d168")
        );
        assert_eq!(approve.value, U256::ZERO);
        assert_eq!(&approve.data[..4], &[0x09, 0x5e, 0xa7, 0xb3]);

        assert_eq!(quote.deposit.chain_id, 4663);
        assert_eq!(quote.deposit.to, DEPOSITORY);
        assert_eq!(quote.deposit.value, U256::ZERO);
        assert_eq!(&quote.deposit.data[..4], &[0xe8, 0x01, 0x79, 0x52]);
    }

    /// Both directions at 1000 units and 50 bps, quoted for the `0xdead` user.
    #[test]
    fn both_directions_validate() {
        for (fixture, origin, destination, order_id, expected_out, minimum_out) in [
            (
                include_str!("../../relay-fixtures/quote_robinhood_to_ethereum.json"),
                Chain::Robinhood,
                Chain::Ethereum,
                b256!("0xae7a78753d0ba847d89a6bfd50782bd4a5cb632a73fda16ee904ec321cda1b56"),
                999_303_522_u64,
                994_307_004_u64,
            ),
            (
                include_str!("../../relay-fixtures/quote_ethereum_to_robinhood.json"),
                Chain::Ethereum,
                Chain::Robinhood,
                b256!("0xbac7900c347acd50a3adc855c4dff3cae98fef07fb2d4c3a1b32ba34d5389cb4"),
                999_595_489,
                994_597_512,
            ),
        ] {
            let request = dead_request(origin, destination, 50);

            let quote = validate(&serde_json::from_str(fixture).unwrap(), &request).unwrap();

            assert_eq!(quote.order_id, RelayOrderId(order_id));
            assert_eq!(quote.amounts.expected_out, U256::from(expected_out));
            assert_eq!(quote.amounts.minimum_out, U256::from(minimum_out));
        }
    }

    /// Relay rounds its `minimumAmount` down, so each live quote must clear
    /// the slippage floor it was requested at.
    #[test]
    fn every_live_quote_is_accepted_at_its_own_slippage() {
        let bounds = QuoteBounds {
            max_loss: BasisPoints::new(500).unwrap(),
            downstream_minimum: U256::ZERO,
        };

        for (fixture, request) in [
            (FUNDED_QUOTE, funded_request()),
            (
                include_str!("../../relay-fixtures/quote_robinhood_to_ethereum.json"),
                dead_request(Chain::Robinhood, Chain::Ethereum, 50),
            ),
            (
                include_str!("../../relay-fixtures/quote_ethereum_to_robinhood.json"),
                dead_request(Chain::Ethereum, Chain::Robinhood, 50),
            ),
            (
                include_str!("../../relay-fixtures/quote_ttl_robinhood_to_ethereum.json"),
                dead_request(Chain::Robinhood, Chain::Ethereum, 30),
            ),
        ] {
            let quote = validate(&serde_json::from_str(fixture).unwrap(), &request).unwrap();

            assert_eq!(quote.amounts.accept(&bounds), Ok(()), "{:?}", quote.amounts);
        }
    }

    #[test]
    fn approve_to_another_spender_is_refused() {
        let mut body = funded_body();
        let other_spender = "0x095ea7b3\
            0000000000000000000000001111111111111111111111111111111111111111\
            00000000000000000000000000000000000000000000000000000000004c4b40";
        body["steps"][0]["items"][0]["data"]["data"] = json!(other_spender);

        let error = validate(&body, &funded_request()).unwrap_err();

        assert!(
            matches!(
                error,
                QuoteMismatch::AddressMismatch {
                    field: QuoteField::ApproveSpender,
                    expected,
                    actual,
                }
                    if expected == DEPOSITORY
                        && actual == address!("0x1111111111111111111111111111111111111111")
            ),
            "{error:?}"
        );
    }

    #[test]
    fn approve_above_the_amount_is_refused() {
        let mut body = funded_body();
        let unlimited = "0x095ea7b3\
            0000000000000000000000004cd00e387622c35bddb9b4c962c136462338bc31\
            ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff";
        body["steps"][0]["items"][0]["data"]["data"] = json!(unlimited);

        let error = validate(&body, &funded_request()).unwrap_err();

        assert!(
            matches!(
                error,
                QuoteMismatch::AmountMismatch {
                    field: QuoteField::ApproveAmount,
                    expected,
                    actual,
                }
                    if expected == U256::from(5_000_000) && actual == U256::MAX
            ),
            "{error:?}"
        );
    }

    #[test]
    fn deposit_to_another_contract_is_refused() {
        let mut body = funded_body();
        body["steps"][1]["items"][0]["data"]["to"] =
            json!("0x2222222222222222222222222222222222222222");

        let error = validate(&body, &funded_request()).unwrap_err();

        assert!(
            matches!(
                error,
                QuoteMismatch::AddressMismatch {
                    field: QuoteField::DepositTarget,
                    expected,
                    actual,
                }
                    if expected == DEPOSITORY
                        && actual == address!("0x2222222222222222222222222222222222222222")
            ),
            "{error:?}"
        );
    }

    #[test]
    fn quote_for_another_amount_is_refused() {
        let request = QuoteRequest {
            amount: U256::from(4_000_000),
            ..funded_request()
        };

        let error = validate(&funded_body(), &request).unwrap_err();

        assert!(
            matches!(
                error,
                QuoteMismatch::AmountMismatch {
                    field: QuoteField::InputAmount,
                    expected,
                    actual,
                }
                    if expected == U256::from(4_000_000) && actual == U256::from(5_000_000)
            ),
            "{error:?}"
        );
    }

    #[test]
    fn deposit_crediting_another_depositor_is_refused() {
        let request = QuoteRequest {
            user: address!("0x3333333333333333333333333333333333333333"),
            ..funded_request()
        };

        let error = validate(&funded_body(), &request).unwrap_err();

        assert!(
            matches!(
                error,
                QuoteMismatch::AddressMismatch {
                    field: QuoteField::Depositor,
                    expected,
                    actual,
                }
                    if expected == address!("0x3333333333333333333333333333333333333333")
                        && actual == FUNDED_WALLET
            ),
            "{error:?}"
        );
    }

    #[test]
    fn quote_for_the_wrong_output_currency_is_refused() {
        let request = QuoteRequest {
            destination: Chain::Base,
            ..funded_request()
        };

        let error = validate(&funded_body(), &request).unwrap_err();

        assert!(
            matches!(
                error,
                QuoteMismatch::OutputCurrency { expected, actual }
                    if expected.chain_id == 8453 && actual.chain_id == 1
            ),
            "{error:?}"
        );
    }

    #[test]
    fn missing_approve_is_allowed_and_missing_deposit_is_refused() {
        let mut body = funded_body();
        body["steps"].as_array_mut().unwrap().remove(0);

        let quote = validate(&body, &funded_request()).unwrap();
        assert_eq!(quote.approve, None);

        body["steps"] = json!([]);
        let error = validate(&body, &funded_request()).unwrap_err();
        assert!(matches!(error, QuoteMismatch::MissingDeposit), "{error:?}");
    }

    #[test]
    fn unknown_step_is_refused() {
        let mut body = funded_body();
        body["steps"][0]["id"] = json!("permit");

        let error = validate(&body, &funded_request()).unwrap_err();

        assert!(
            matches!(error, QuoteMismatch::UnexpectedStep { position: 0 }),
            "{error:?}"
        );
    }

    #[test]
    fn quote_naming_another_recipient_is_refused() {
        let error = refusal(|body| body["details"]["recipient"] = json!(OTHER));

        assert!(
            matches!(
                error,
                QuoteMismatch::AddressMismatch {
                    field: QuoteField::Recipient,
                    expected,
                    actual,
                }
                    if expected == FUNDED_WALLET && actual == OTHER
            ),
            "{error:?}"
        );
    }

    #[test]
    fn second_output_payment_is_refused() {
        let error = refusal(|body| {
            let payments = &mut body["protocol"]["v2"]["orderData"]["output"]["payments"];
            let payment = payments[0].clone();
            payments.as_array_mut().unwrap().push(payment);
        });

        assert!(
            matches!(error, QuoteMismatch::PaymentCount { count: 2 }),
            "{error:?}"
        );
    }

    #[test]
    fn output_payment_to_another_recipient_is_refused() {
        let error = refusal(|body| {
            body["protocol"]["v2"]["orderData"]["output"]["payments"][0]["recipient"] =
                json!(OTHER);
        });

        assert!(
            matches!(
                error,
                QuoteMismatch::AddressMismatch {
                    field: QuoteField::PaymentRecipient,
                    expected,
                    actual,
                }
                    if expected == FUNDED_WALLET && actual == OTHER
            ),
            "{error:?}"
        );
    }

    #[test]
    fn output_payment_in_another_currency_is_refused() {
        let error = refusal(|body| {
            body["protocol"]["v2"]["orderData"]["output"]["payments"][0]["currency"] = json!(OTHER);
        });

        assert!(
            matches!(
                error,
                QuoteMismatch::AddressMismatch {
                    field: QuoteField::PaymentCurrency,
                    expected,
                    actual,
                }
                    if expected == ETHEREUM_USDC && actual == OTHER
            ),
            "{error:?}"
        );
    }

    #[test]
    fn output_payment_minimum_below_the_quote_is_refused() {
        let error = refusal(|body| {
            body["protocol"]["v2"]["orderData"]["output"]["payments"][0]["minimumAmount"] =
                json!("1");
        });

        assert!(
            matches!(
                error,
                QuoteMismatch::AmountMismatch {
                    field: QuoteField::PaymentMinimum,
                    expected,
                    actual,
                }
                    if expected == U256::from(4_749_464) && actual == U256::from(1)
            ),
            "{error:?}"
        );
    }

    #[test]
    fn output_payment_expected_below_the_quote_is_refused() {
        let error = refusal(|body| {
            body["protocol"]["v2"]["orderData"]["output"]["payments"][0]["expectedAmount"] =
                json!("4749464");
        });

        assert!(
            matches!(
                error,
                QuoteMismatch::AmountMismatch {
                    field: QuoteField::PaymentExpected,
                    expected,
                    actual,
                }
                    if expected == U256::from(4_763_755) && actual == U256::from(4_749_464)
            ),
            "{error:?}"
        );
    }

    #[test]
    fn refund_to_another_recipient_is_refused() {
        let error = refusal(|body| {
            body["protocol"]["v2"]["orderData"]["inputs"][0]["refunds"][1]["recipient"] =
                json!(OTHER);
        });

        assert!(
            matches!(
                error,
                QuoteMismatch::AddressMismatch {
                    field: QuoteField::RefundRecipient,
                    expected,
                    actual,
                }
                    if expected == FUNDED_WALLET && actual == OTHER
            ),
            "{error:?}"
        );
    }

    #[test]
    fn second_order_input_is_refused() {
        let error = refusal(|body| {
            let inputs = &mut body["protocol"]["v2"]["orderData"]["inputs"];
            let input = inputs[0].clone();
            inputs.as_array_mut().unwrap().push(input);
        });

        assert!(
            matches!(error, QuoteMismatch::InputCount { count: 2 }),
            "{error:?}"
        );
    }

    #[test]
    fn input_payment_on_another_chain_is_refused() {
        let error = refusal(|body| {
            body["protocol"]["v2"]["orderData"]["inputs"][0]["payment"]["chainId"] =
                json!("ethereum");
        });

        assert!(
            matches!(
                error,
                QuoteMismatch::ChainMismatch {
                    field: QuoteField::InputPaymentChain,
                    expected: "robinhood",
                    ref actual,
                } if actual == "ethereum"
            ),
            "{error:?}"
        );
    }

    #[test]
    fn input_payment_in_another_currency_is_refused() {
        let error = refusal(|body| {
            body["protocol"]["v2"]["orderData"]["inputs"][0]["payment"]["currency"] = json!(OTHER);
        });

        assert!(
            matches!(
                error,
                QuoteMismatch::AddressMismatch {
                    field: QuoteField::InputPaymentCurrency,
                    expected,
                    actual,
                }
                    if expected == ROBINHOOD_USDG && actual == OTHER
            ),
            "{error:?}"
        );
    }

    #[test]
    fn input_payment_for_another_amount_is_refused() {
        let error = refusal(|body| {
            body["protocol"]["v2"]["orderData"]["inputs"][0]["payment"]["amount"] =
                json!("3000000");
        });

        assert!(
            matches!(
                error,
                QuoteMismatch::AmountMismatch {
                    field: QuoteField::InputPaymentAmount,
                    expected,
                    actual,
                }
                    if expected == U256::from(5_000_000) && actual == U256::from(3_000_000)
            ),
            "{error:?}"
        );
    }

    #[test]
    fn order_fee_is_refused() {
        let error = refusal(|body| {
            body["protocol"]["v2"]["orderData"]["fees"] =
                json!([{"recipient": OTHER, "amount": "1"}]);
        });

        assert!(
            matches!(error, QuoteMismatch::OrderFees { count: 1 }),
            "{error:?}"
        );
    }

    #[test]
    fn output_call_is_refused() {
        let error = refusal(|body| {
            body["protocol"]["v2"]["orderData"]["output"]["calls"] = json!(["0xdeadbeef"]);
        });

        assert!(
            matches!(error, QuoteMismatch::OutputCalls { count: 1 }),
            "{error:?}"
        );
    }

    #[test]
    fn output_on_another_chain_is_refused() {
        let error = refusal(|body| {
            body["protocol"]["v2"]["orderData"]["output"]["chainId"] = json!("robinhood");
        });

        assert!(
            matches!(
                error,
                QuoteMismatch::ChainMismatch {
                    field: QuoteField::OutputChain,
                    expected: "ethereum",
                    ref actual,
                } if actual == "robinhood"
            ),
            "{error:?}"
        );
    }

    #[test]
    fn payment_details_on_another_chain_are_refused() {
        let error =
            refusal(|body| body["protocol"]["v2"]["paymentDetails"]["chainId"] = json!("ethereum"));

        assert!(
            matches!(
                error,
                QuoteMismatch::ChainMismatch {
                    field: QuoteField::PaymentDetailsChain,
                    expected: "robinhood",
                    ref actual,
                } if actual == "ethereum"
            ),
            "{error:?}"
        );
    }

    #[test]
    fn order_without_a_refund_option_is_refused() {
        let error = refusal(|body| {
            body["protocol"]["v2"]["orderData"]["inputs"][0]["refunds"] = json!([]);
        });

        assert!(matches!(error, QuoteMismatch::MissingRefund), "{error:?}");
    }

    #[test]
    fn refund_on_a_third_chain_is_refused() {
        let error = refusal(|body| {
            body["protocol"]["v2"]["orderData"]["inputs"][0]["refunds"][1]["chainId"] =
                json!("base");
        });

        assert!(
            matches!(
                error,
                QuoteMismatch::RefundChain {
                    origin: "robinhood",
                    destination: "ethereum",
                    ref actual,
                } if actual == "base"
            ),
            "{error:?}"
        );
    }

    #[test]
    fn refund_in_another_token_is_refused() {
        let error = refusal(|body| {
            body["protocol"]["v2"]["orderData"]["inputs"][0]["refunds"][0]["currency"] =
                json!(OTHER);
        });

        assert!(
            matches!(
                error,
                QuoteMismatch::AddressMismatch {
                    field: QuoteField::RefundCurrency,
                    expected,
                    actual,
                }
                    if expected == Chain::Robinhood.settlement_stable().address && actual == OTHER
            ),
            "{error:?}"
        );
    }

    #[test]
    fn payment_details_naming_another_depository_is_refused() {
        let error =
            refusal(|body| body["protocol"]["v2"]["paymentDetails"]["depository"] = json!(OTHER));

        assert!(
            matches!(
                error,
                QuoteMismatch::AddressMismatch {
                    field: QuoteField::PaymentDetailsDepository,
                    expected,
                    actual,
                }
                    if expected == DEPOSITORY && actual == OTHER
            ),
            "{error:?}"
        );
    }

    #[test]
    fn payment_details_in_another_currency_are_refused() {
        let error =
            refusal(|body| body["protocol"]["v2"]["paymentDetails"]["currency"] = json!(OTHER));

        assert!(
            matches!(
                error,
                QuoteMismatch::AddressMismatch {
                    field: QuoteField::PaymentDetailsCurrency,
                    expected,
                    actual,
                }
                    if expected == ROBINHOOD_USDG && actual == OTHER
            ),
            "{error:?}"
        );
    }

    #[test]
    fn payment_details_for_another_amount_are_refused() {
        let error =
            refusal(|body| body["protocol"]["v2"]["paymentDetails"]["amount"] = json!("3000000"));

        assert!(
            matches!(
                error,
                QuoteMismatch::AmountMismatch {
                    field: QuoteField::PaymentDetailsAmount,
                    expected,
                    actual,
                }
                    if expected == U256::from(5_000_000) && actual == U256::from(3_000_000)
            ),
            "{error:?}"
        );
    }

    #[test]
    fn relayer_fee_in_another_currency_is_refused() {
        let error = refusal(|body| body["fees"]["relayer"]["currency"]["address"] = json!(OTHER));

        assert!(
            matches!(
                error,
                QuoteMismatch::RelayerFeeCurrency { expected, actual }
                    if expected == QuotedCurrency { chain_id: 4663, address: ROBINHOOD_USDG }
                        && actual == QuotedCurrency { chain_id: 4663, address: OTHER }
            ),
            "{error:?}"
        );
    }

    #[test]
    fn gas_fee_on_another_chain_is_refused() {
        let error = refusal(|body| body["fees"]["gas"]["currency"]["chainId"] = json!(1));

        assert!(
            matches!(
                error,
                QuoteMismatch::GasFeeChain {
                    expected: 4663,
                    actual: 1
                }
            ),
            "{error:?}"
        );
    }

    #[test]
    fn currency_on_another_decimal_grid_is_refused() {
        let error =
            refusal(|body| body["details"]["currencyOut"]["currency"]["decimals"] = json!(18));

        assert!(
            matches!(
                error,
                QuoteMismatch::CurrencyDecimals { currency, expected: 6, actual: 18 }
                    if currency == QuotedCurrency { chain_id: 1, address: ETHEREUM_USDC }
            ),
            "{error:?}"
        );
    }

    /// The acceptance math compares input and output units, so every chain
    /// Relay can move between must settle on the same decimal grid.
    #[test]
    fn relay_chains_share_one_stable_decimal_grid() {
        let decimals: Vec<u8> = Chain::ALL
            .into_iter()
            .filter(|chain| chain.relay_depository().is_some())
            .map(|chain| chain.settlement_stable().decimals)
            .collect();

        assert_eq!(decimals, [6, 6]);
    }

    /// Quoted live at Unix 1,790,882,607 with `ttl: 1800`: the deadline still
    /// lands 604,801 s later, as with no `ttl` at all.
    #[test]
    fn deadline_is_read_from_the_order_and_ignores_the_ttl() {
        let quote = validate(
            &serde_json::from_str(include_str!(
                "../../relay-fixtures/quote_ttl_robinhood_to_ethereum.json"
            ))
            .unwrap(),
            &dead_request(Chain::Robinhood, Chain::Ethereum, 30),
        )
        .unwrap();

        assert_eq!(
            quote.deadline,
            UNIX_EPOCH + Duration::from_secs(1_791_487_408)
        );
    }

    #[test]
    fn unrepresentable_deadline_is_refused() {
        let error = refusal(|body| {
            body["protocol"]["v2"]["orderData"]["output"]["deadline"] = json!(u64::MAX);
        });

        assert!(
            matches!(error, QuoteMismatch::Deadline { seconds: u64::MAX }),
            "{error:?}"
        );
    }
}
