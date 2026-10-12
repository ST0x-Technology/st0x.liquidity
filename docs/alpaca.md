# Shared Alpaca client integration

Liquidity pins two crates from st0x.alpaca by git rev in workspace dependencies,
`st0x-alpaca` and `st0x-alpaca-gateway-api`, both at the same rev.
`st0x-execution` and `st0x-tokenization` select their respective `st0x-alpaca`
features, and `st0x-liquidity-client` uses both crates. The shared crate owns
Broker, Market Data, wallet, and tokenization HTTP requests and wire types.
Liquidity's executor keeps its hedge floor, buying-power reservation, and
slippage policy; the tokenization adapter keeps onchain redemption submission
and mint-receipt verification.

The executor's `Direction`, `ClientOrderId`, order state, and error variants are
part of Liquidity's public contract. Convert them at the adapter boundary. In
particular, map shared API errors and uncertain conversion outcomes to the
existing local variants: the rebalance and retry paths match those variants to
decide whether an order can be submitted again.

The shared wallet, tokenization, and conversion polls retry every read error
that the shared error's `permanence()` classifies Transient, honoring its
`Retry-After`, as
[docs/parity.md](https://github.com/ST0x-Technology/st0x.alpaca/blob/0ca02fd33e4ba4ba3aa0fb86c1a59ed11b822c50/docs/parity.md)
in st0x.alpaca states. Liquidity relies on that rule and must not override it
with a local poll loop or a zero retry budget. The retry is bounded. The wallet
transfer and deposit polls retry a failed read up to
`PollingConfig::max_retries` times (10 by default) and then return the error;
the deposit path then records `FailDeposit` unless the error is a classified
429. The wallet tx hash poll, the tokenization polls, and the conversion poll
retry until their deadline. A deadline reached during a retry wait ends the poll
with its timeout, which the deposit path also records as `FailDeposit`.

The shared crate uses `reqwest` 0.13 while Liquidity still uses 0.12. Their
`reqwest::Error` types are distinct. A test that constructs a shared Market Data
transport error must use the renamed `reqwest_alpaca` dev dependency.

The shared broker mock's order lookup omits `extended_hours`. Recovery of that
mock order must report `IncompleteOrder { field: ExtendedHours }`, as it would
for an incomplete provider response; a lookup for reporting can still return the
order. Do not treat the missing value as `false`.

When updating the shared crate pin, move `st0x-alpaca` and
`st0x-alpaca-gateway-api` to the same rev in one change. The gateway API crate
depends on `st0x-alpaca` by path, so split revs make Cargo resolve a second git
source that has no `outputHashes` entry, and build a second `st0x-alpaca` whose
gateway types disagree with the first. Then update the git source hash in
`rust.nix` so crane can vendor the pinned rev in Nix builds. Compute it with
`nix-prefetch-git --fetch-submodules --fetch-lfs`, the fetcher options crane
uses for `outputHashes`, and key the entry by the exact `Cargo.lock` source.
