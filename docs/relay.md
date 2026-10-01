# Relay

Relay moves the Robinhood corridor's cash between USDG on Robinhood (chain 4663)
and USDC on Ethereum (chain 1). We approve and deposit on the origin chain into
Relay's depository; Relay's solver pays the recipient on the destination chain
from its own funds. The client is `st0x_bridge::relay`, behind the `relay`
feature.

Facts here come from live quotes and status reads (2026-09-22) and the funded
test of 2026-10-01 (RAI-2586: 5 USDG to Ethereum, the USDC back, one forced
refund). Response bodies from those reads and that test are pinned in
`crates/bridge/relay-fixtures/`.

## API

Base URL `https://api.relay.link`. An `x-api-key` header is optional:
`RelayClient::new(None)` warns once and runs on unauthenticated limits.

### `POST /quote/v2`

Body (amounts are base-10 strings in the token's smallest unit):

```json
{
  "user": "0x...",
  "originChainId": 4663,
  "destinationChainId": 1,
  "originCurrency": "0x5fc5...d168",
  "destinationCurrency": "0xa0b8...eb48",
  "amount": "5000000",
  "tradeType": "EXACT_INPUT",
  "recipient": "0x...",
  "refundTo": "0x...",
  "slippageTolerance": "30",
  "ttl": 1800
}
```

The response carries `requestId`, `steps[]`, `fees`, `details` and `protocol`.
The client reads:

- `details.currencyIn` / `details.currencyOut`: chain id, token, `amount`, and
  on the output `minimumAmount`, the least the solver may pay before it refunds.
- `fees.relayer` (origin stable, already out of the expected amount) and
  `fees.gas` (origin native token, Relay's estimate of our gas).
- `steps`: `approve` then `deposit`, one item each, with `to`, `data`, `value`
  and `chainId`.

A refused quote answers 4xx/5xx with `{"message", "errorCode", "requestId"}`.
Codes: `AMOUNT_TOO_LOW`, `AMOUNT_TOO_HIGH`, `INSUFFICIENT_LIQUIDITY`,
`CHAIN_DISABLED`, `ROUTE_TEMPORARILY_RESTRICTED`, `NO_QUOTES`,
`NO_SWAP_ROUTES_FOUND` (seen live, not in the docs), `SWAP_IMPACT_TOO_HIGH`, and
the transient `PRICE_FETCH_FAILED`, `SERVICE_UNAVAILABLE`, `REQUEST_TIMED_OUT`,
`RPC_HTTP_ERROR`. The client maps them to `RelayError::QuoteRefused { code }`
and retries only the transient ones, twice, within one call.

### `GET /intents/status/v3?requestId=`

Statuses: `waiting` (no deposit seen; the body is only `status` and
`quoteCreatedAt`), `depositing`, `pending`, `submitted`, `delayed`, `success`,
`refund`, `failure`. `inTxHashes[]` are the deposits, `txHashes[]` the fills or
refunds, `failReason` and `refundFailReason` a code or `"N/A"`. Both hash fields
are arrays; the client keeps every entry.

A `refund` counts as a paid refund (`IntentStatus::Refund`) only with at least
one refund tx and no `refundFailReason`. With no refund tx it is
`IntentStatus::Refunding` (not terminal); with a `refundFailReason` it is
`IntentStatus::RefundFailed`, which carries that `refund_fail_reason` (terminal:
the refund will not be paid).

A `success` with no tx in `txHashes` is `IntentStatus::Filling` (not terminal):
settlement needs a fill tx to prove.

Terminal statuses are `Success`, a paid `Refund`, `RefundFailed` and `Failure`;
`Filling` and `Refunding` are not. A status name the client does not know is
`IntentStatus::Unknown` and stays non-terminal, so the transfer keeps waiting
rather than settling on a guess. Known fail reasons are `SLIPPAGE`,
`TOO_LITTLE_RECEIVED`, `SOLVER_CAPACITY_EXCEEDED`, `TTL_EXPIRED`,
`DEPOSIT_CONFIRMATION_TIMEOUT`, `DEPOSIT_REORGED`, `BLOCKED_WALLET`,
`TRANSACTION_NOT_INCLUDED`, plus `DEPOSITED_AMOUNT_TOO_LOW_TO_FILL` (seen on the
forced refund, not in the docs).

### Rate limits

Documented per key: `/quote` 50 per minute, other endpoints 200 per minute.
Unauthenticated limits are not documented; the funded test hit none. A 429 maps
to `RelayError::RateLimited { retry_after }` from the `Retry-After` header
(seconds) and is never retried inside the client: the caller backs off.

## Traps

- **2% default slippage.** A quote without `slippageTolerance` gets
  `slippageTolerance.total = "200"`: Relay may pay 2% below the expected amount
  and call it a fill. `QuoteRequest` makes the slippage a required field.
- **Deposit id, not request id.** `requestId` is only the status API's handle.
  The key Relay settles by is the deposit's `bytes32 id` (also
  `protocol.v2.orderId`), the last word of the deposit calldata
  `depositErc20(address depositor, address token, uint256 amount, bytes32 id)`
  (selector `0xe8017952`). The client reads `RelayQuote::order_id` from that
  calldata and refuses the quote unless it equals `protocol.v2.orderId`, so the
  order it checks is the one the deposit funds.
- **`ttl` does not bound the deadline.** The order's
  `protocol.v2.orderData.output.deadline` (and each refund's `deadline`) lands a
  week after the quote with `ttl` unset, `1800` or `60` (live, 2026-10-01; the
  `ttl: 1800` body is `quote_ttl_robinhood_to_ethereum.json`). The solver may
  fill until then, long after the caller gave up. `QuoteRequest` still sends
  `ttl`; `RelayQuote::deadline` exposes the real deadline.
- **`refundTo` is always sent.** The quote docs say an unset `refundTo` falls
  back to the recipient or user; the refunds page says it disables automatic
  refunds.

## Quote checks

`RelayClient::quote` refuses an origin with no `Chain::relay_depository()`
before it sends anything, then refuses a quote unless:

- the input and output are the two chains' settlement stables on their pinned
  decimals (every Relay chain's stable has 6, which a test pins, so input and
  output amounts compare unit for unit), and the input amount is the requested
  amount;
- the relayer fee is in the origin stable and the gas fee is on the origin
  chain;
- `protocol.v2.orderData.inputs[]` has exactly one input, whose `payment` is the
  requested amount of the origin stable on the origin chain, and
  `orderData.fees` is empty;
- `details.recipient` is our recipient, and `protocol.v2.orderData.output` is on
  the destination chain, has no `calls` and exactly one payment: the destination
  stable to our recipient, at the quoted `minimumAmount` and expected amount;
- `protocol.v2.orderData.inputs[].refunds[]` has at least one refund option, and
  every option pays our `refundTo` on the origin or the destination chain;
- `protocol.v2.paymentDetails` is on the origin chain and names the pinned
  depository, the origin stable and the requested amount;
- the `approve` step, when present, is on the origin chain, calls the origin
  stable with no value, and approves exactly the amount to the depository
  (`Chain::relay_depository()`, `0x4cd0...bc31` on both chains);
- the `deposit` step is on the origin chain, calls the depository with no value,
  and its calldata names our wallet as depositor, the origin stable, and the
  exact amount;
- there is no other step.

The order's `chainId` fields carry Relay's chain names (`"robinhood"`,
`"ethereum"`), not numeric ids; `Chain::relay_name()` pins them.

The approve is optional because Relay may leave it out when the allowance
already covers the amount (not observed; the funded test always had one). With
no approve step the caller must recheck the allowance before skipping the
approve: another transfer from the same wallet can consume a standing allowance.

`QuoteAmounts::accept` then checks the amounts against `QuoteBounds`, in checked
integer basis points (an overflow is `QuoteAcceptanceError::Overflow`, never a
panic). The loss bound rounds up, so it never sits below its exact value. The
slippage floor rounds down: Relay rounds its own `minimumAmount` down (999303522
at 50 bps is 994307004.39, and Relay quotes 994307004), so a floor rounded up
would refuse an honest quote.

- `expected_out >= amount_in * (10000 - max_loss) / 10000`, else
  `QuoteLossExceedsBound`;
- `minimum_out >= expected_out * (10000 - slippage) / 10000`, else
  `QuoteFloorBelowBound`: Relay's floor is not looser than the slippage we sent,
  which `QuoteAmounts` carries from the request;
- `minimum_out >= downstream_minimum`, else `QuoteBelowDownstreamMinimum`;
- `minimum_out <= expected_out`, else `MinimumAboveExpected`.

## On chain

- **Deposit.** The depository emits
  `RelayErc20Deposit(address from, address token, uint256 amount, bytes32 id)`
  with **no indexed fields**: finding our deposit means decoding every log in a
  block range and matching `id`.
- **Fill.** No event carries the id and no Relay contract is on the destination
  side. The fill tx calls the destination stable's
  `transferFrom(solver, recipient, amount)` (selector `0x23b872dd`) with the
  order id appended as a trailing 32-byte word (4 + 4 x 32 bytes of calldata).
  The solver was `0xf70da97812cb96acdf810712aa562db8dfa3dbef` on both chains;
  the tx sender was a different relayer EOA every time, so it is never trusted.
  The only log is the stable's `Transfer(solver, recipient, amount)`. One fill
  tx per transfer in every run.
- **Refund.** Same shape, paid from the solver to `refundTo`; the depository
  emits nothing. Every quote offers two refund options in
  `protocol.v2.orderData.inputs[].refunds[]`: the origin stable on the origin
  chain and the destination stable on the destination chain, so a refund proof
  must look on both. The forced refund (3 USDG deposited against a 5 USDG quote)
  was paid on the origin chain in USDG: 2.995154 USDG 15 s after the quote, with
  `failReason: DEPOSITED_AMOUNT_TOO_LOW_TO_FILL`.

Both fills paid exactly the quoted expected amount, in 32 and 59 s.
