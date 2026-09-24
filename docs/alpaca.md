# Shared Alpaca client integration

Liquidity pins the released `st0x-alpaca` crate by tag in workspace
dependencies; `st0x-execution` and `st0x-tokenization` select their respective
features. The shared crate owns Broker, Market Data, wallet, and tokenization
HTTP requests and wire types. Liquidity's executor keeps its hedge floor,
buying-power reservation, and slippage policy; the tokenization adapter keeps
onchain redemption submission and mint-receipt verification.

The executor's `Direction`, `ClientOrderId`, order state, and error variants are
part of Liquidity's public contract. Convert them at the adapter boundary. In
particular, map shared API errors and uncertain conversion outcomes to the
existing local variants: the rebalance and retry paths match those variants to
decide whether an order can be submitted again.

The shared crate uses `reqwest` 0.13 while Liquidity still uses 0.12. Their
`reqwest::Error` types are distinct. A test that constructs a shared Market Data
transport error must use the renamed `reqwest_alpaca` dev dependency.

The shared broker mock's order lookup omits `extended_hours`. Recovery of that
mock order must report `IncompleteOrder { field: ExtendedHours }`, as it would
for an incomplete provider response; a lookup for reporting can still return the
order. Do not treat the missing value as `false`.

When updating the shared crate pin, update its git source hash in `rust.nix` so
crane can vendor the release in Nix builds.
