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

## Common Pitfalls

1. **Don't use `B256` for tx hashes** - use `TxHash`
2. **Don't manually ABI-encode** - use `SolEvent::encode_log_data()` or call
   builders
3. **Don't parse literals at runtime** - use `address!()`, `b256!()` macros
4. **Don't construct Filters manually** - use generated `*_filter()` builders
5. **Mock responses are FIFO** - push them in the order RPC calls happen
