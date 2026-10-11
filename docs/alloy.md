# Alloy Patterns and Conventions

Quick reference for common alloy usage patterns in this codebase.

## Imports

Most types come from `alloy::primitives`:

```rust
use alloy::primitives::{Address, TxHash, U256, B256, Bytes};
```

**Use semantic type aliases, not raw bytes:**

- `TxHash` for transaction hashes (not `B256`)
- `BlockHash` for block hashes (not `B256`)
- `Address` for addresses (not `FixedBytes<20>`)

All of these are just type aliases over `FixedBytes<N>`, but using the semantic
name makes code clearer.

## FixedBytes Aliases and `::random()`

All common alloy types are aliases for `FixedBytes<N>`:

- `Address` = `FixedBytes<20>`
- `B256` / `TxHash` / `BlockHash` = `FixedBytes<32>`
- `FixedBytes<4>` for function selectors, short IDs, etc.

Because they share the same underlying type, all methods available on
`FixedBytes` work on every alias. In particular, with the `rand` feature enabled
on alloy:

```rust
use alloy::primitives::{Address, B256, TxHash};

let random_addr = Address::random();
let random_hash = B256::random();
let random_tx = TxHash::random();  // same as B256::random()
```

Use `::random()` in tests instead of constructing bytes manually.
`FixedBytes::right_padding_from`, `FixedBytes::from([0xAB; 32])`, or similar
manual constructions are unnecessary when you just need a unique value.

## Compile-Time Macros

Use macros for compile-time checked literals:

```rust
use alloy::primitives::{address, b256, fixed_bytes};

let addr = address!("0x1234567890123456789012345678901234567890");
let hash = b256!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
let bytes = fixed_bytes!("0x1234");
```

**Never construct these manually with `from_slice` or string parsing at
runtime** when the value is known at compile time.

**In tests**, prefer `address!()`, `b256!()`, and `fixed_bytes!()` for
deterministic fixture values. Use `::random()` when you just need a unique value
and don't care about the specific bytes.

## Mock Providers for Testing

Use `Asserter` with `ProviderBuilder` for mocking RPC responses:

```rust
use alloy::network::EthereumWallet;
use alloy::providers::ProviderBuilder;
use alloy::providers::mock::Asserter;
use alloy::signers::local::PrivateKeySigner;

let asserter = Asserter::new();

// Push responses in order they'll be consumed
asserter.push_success(&vec![some_log]);           // First RPC call
asserter.push_success(&Vec::<Log>::new());        // Second RPC call
asserter.push_success(&balance.to_be_bytes::<32>()); // Third RPC call (eth_call)

let provider = ProviderBuilder::new()
    .wallet(EthereumWallet::from(PrivateKeySigner::random()))
    .connect_mocked_client(asserter);
```

Responses are consumed in FIFO order regardless of which RPC method is called.

## ABI Encoding

**Never manually encode ABI data.** Use alloy's generated types:

```rust
use alloy::sol_types::SolEvent;

// For events - use encode_log_data()
let event = MyContract::Transfer { from, to, value };
let log_data = event.encode_log_data();

// For function calls - use the generated call builders
let call = contract.transfer(to, value);
```

## Event Decoding

```rust
use alloy::sol_types::SolEvent;

let event = MyContract::Transfer::decode_log(&log.inner)?;
// event.from, event.to, event.value are now available
```

## Contract Bindings

Generated via `sol!` macro from ABI JSON:

```rust
sol!(
    #![sol(all_derives = true, rpc)]
    MyContract,
    "path/to/Contract.json"
);

// Use the contract
let contract = MyContract::new(address, &provider);
let result = contract.someFunction(arg1, arg2).call().await?;
```

## Filter Builders

Use the generated filter builders for event subscriptions:

```rust
let contract = MyContract::new(address, &provider);

// Generated filter builder with type-safe topic setters
let filter = contract
    .Transfer_filter()
    .topic1(from_address)  // indexed param 1
    .topic2(to_address)    // indexed param 2
    .filter;

let logs = provider.get_logs(&filter).await?;
```

## Anvil Test Nodes

Anvil keeps recent block states in memory, then writes older states to
`~/.foundry/anvil/tmp`. Its default disk tier holds up to 3,600 complete state
snapshots, and those temporary directories remain after a normal exit. A test
using interval mining can therefore leave several gigabytes per run.

Every interval-mining test node must disable the disk tier:

```rust
let anvil = Anvil::new()
    .block_time(1)
    .arg("--max-persisted-states")
    .arg("0")
    .spawn();
```

This retains recent state in memory and prevents historical snapshots from being
written to the user's Foundry directory. Do not replace it with
`--prune-history`; that also changes the available in-memory history.

## Canonical State on Load-Balanced RPCs

Separate transaction, head, and `latest` account-state reads can hit different
backends. A fresh head response does not authenticate an earlier missing
transaction response or a later `latest` nonce response. Pin the state read to
the observed header's canonical hash:

```rust
let next_nonce = provider
    .get_transaction_count(sender)
    .block_id(BlockId::hash_canonical(header.hash))
    .await?;
```

`BlockId::hash_canonical` requests EIP-1898 `requireCanonical = true`. A backend
that cannot serve that block must fail rather than silently answer from its
older state. Missing headers or unsupported/failed hash-pinned state reads are
inconclusive; never fall back to `latest` to qualify a dropped transaction. If
this state's nonce exceeds the known transaction nonce, that transaction may
already be mined but hidden by a stale receipt lookup. Even an unused nonce does
not prove absence from every backend's mempool: suspected-drop policy still
needs submission-boundary, head-progress, grace, and consecutive-miss checks.
HTTP tests should assert the canonical hash parameter, not just FIFO mock
response order.

Do not count frozen-head polls as consecutive qualified misses merely because
the head later advances once. Wallet waits and burn checks both begin counting
only beyond the post-grace reference margin. A lagging or consumed-nonce poll
resets that reference and miss run, but a bounded burn check keeps polling in
the same observation window instead of restarting grace on every redrive.

For timer-driven tests using only mocked RPC responses, use a paused Tokio clock
and `tokio::time::Instant` for the elapsed-time policy as well as Tokio timers.
A short wall-clock grace can expire before mock polling advances the head on a
busy runner. Pausing timers alone does not freeze `std::time::Instant`, so
mixing the two clocks still leaves that race. Do not pause real-I/O recovery
tests where auto-advancing timers can race network futures.

Bracket each generic send attempt inside the shared send lock, refreshing the
pre-send head for every nonce or fee recovery retry. A head read before waiting
for the lock or before a rejected attempt is not the accepted send's boundary.
Gas estimation and signing must finish before the pre-send read, immediately
before the raw-envelope broadcast. Bracketing those earlier operations can leave
the floor stale when a remote signer is slow and the post-send backend lags.
Bracket send, prepare, and each prepared broadcast with head observations and
retain the highest submission boundary, never the lowest: a lagging backend must
not lower a previously seen floor. Preparation may precede broadcast or restart
recovery by many blocks; its boundary cannot substitute for a broadcast-time
observation. A failed post-operation read must not turn an already signed or
broadcast transaction into a submission failure or discard its nonce ownership.
During a prepared broadcast, an old preparation boundary is not usable until the
broadcast-time observations complete. Failure or cancellation keeps the boundary
unknown, while retaining the highest observation for a later successful retry;
it must not fall back to the stale preparation floor.

Keep preparation's observation under the same send lock as nonce allocation and
signing. Cancellation cleanup must synchronously release only the fresh unused
preparation's reservation and hash, invalidate cached allocation, and preserve
earlier occupied nonces; do not spawn asynchronous cleanup from a drop guard.
Bound optional post-head reads by the existing receipt-poll interval, returning
accepted identity with unknown freshness on expiry. Receipt drop checks need
live submission evidence before and after canonical RPC reads, resetting
miss/head progress when it changes; a wait-start snapshot is unsafe across
rebroadcasts.

A deterministic JSON-RPC rejection of hash-pinned state is not an RPC outage.
Clear the absence progress and keep waiting for the receipt; only transient
transport failures count toward the outage cap. Repeated unavailable canonical
state must end in a retryable receipt timeout, not a terminal transport error.

CCTP recovery separates submission evidence from nonce ownership: a
confirmation-time suspected drop can release generic wallet ownership, but the
endpoint retains one exact-hash submission context for fresh requalification on
retry. Never retain a sticky drop verdict or synthesize a missing sender, nonce
or boundary. A recorded burn's recovery scan must target that hash, not another
identical `DepositForBurn` in an old scan window. Optional positive scans while
the burn is Pending do not turn RPC rejection into terminal transfer failure;
strict confirmation validation still applies after finding the exact hash.
Hashless or confirmed-revert recovery also checks retained transfer history
before adopting a fingerprint match: an earlier transfer's burn can fall inside
a stale scan window, even with a single-flight corridor. Missing ownership
evidence must fail closed, not silently bypass this check. An empty exact-hash
log scan needs fresh canonical unused-nonce and head-progress qualification
after the scan; a separate numeric head does not prove the log backend is caught
up. Generic recovery needs the complete candidate list before excluding foreign
owners: the newest foreign match must not hide an older eligible burn, and
filtering a partial positive response cannot establish an authoritative empty
result that permits retrying a confirmed-reverted burn.

Anvil's transaction automining does not advance an idle chain. In CCTP restart
tests, cancellation can race past the mint into `Bridged` before a deposit send
is recorded. That recovery path needs a finality-qualified empty deposit scan;
without empty blocks it can repeatedly return `ScanInconclusive` and exhaust the
job's retries. Mine the required two Ethereum blocks after the first bot stops
and before restarting it, rather than weakening the scan or increasing retries.

## Common Pitfalls

1. **Don't use `B256` for tx hashes** - use `TxHash`
2. **Don't manually ABI-encode** - use `SolEvent::encode_log_data()` or call
   builders
3. **Don't parse literals at runtime** - use `address!()`, `b256!()` macros
4. **Don't construct Filters manually** - use generated `*_filter()` builders
5. **Mock responses are FIFO** - push them in the order RPC calls happen
6. **OR-topic ordering is not stable** - `Filter` stores topic alternatives in a
   set. HTTP tests must compare the deserialized request filter or topic sets,
   rather than matching the serialized OR-topic array in a fixed order. Two
   equivalent filters can serialize their alternatives in different orders.
