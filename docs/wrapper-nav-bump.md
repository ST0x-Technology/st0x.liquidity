# wtStock wrapper NAV bump

A dividend or stock split is applied to a wtStock wrapper (`wtCOIN`, `wtAAPL`,
...) by a **plain ERC-20 transfer of the underlying token into the wrapper
contract**. This note records why, with on-chain evidence, so the dividend
donate step ([RAI-896] / [RAI-1040]) uses the right call.

## TL;DR

- The wrapper is a stock-standard OpenZeppelin **ERC-4626** vault. Its
  `totalAssets()` is the wrapper's own balance of the underlying token, so
  sending underlying in raises `totalAssets()` without minting shares, which
  raises `convertToAssets(1e18)` -- the NAV/ratio the bot reads via
  `crates/wrapper` (`ratio.rs`, `service.rs`).
- **Donate = `underlying.transfer(wrapperAddress, amount)`.** There is no
  `donate()` / `addAssets()` entrypoint, and none is needed; deposit-then-burn
  is unnecessary.

## The contract

The deployed wrapper for each symbol is configured as
`[assets.equities.<SYM>].tokenized_equity_derivative` (addresses originate from
the external `st0x.registry`). On-chain it is an OpenZeppelin **BeaconProxy**
whose implementation is **`StoxWrappedTokenVault`** -- an `ERC4626Upgradeable`
(plus `ICloneableV2` for clone-factory init) that overrides **only** `name()`
and `symbol()`. It does not override `totalAssets()`, `deposit`, `_deposit`, or
`_decimalsOffset`, so it inherits OpenZeppelin's ERC-4626 accounting verbatim:

```solidity
// OpenZeppelin ERC4626Upgradeable
function totalAssets() public view virtual returns (uint256) {
    return IERC20(asset()).balanceOf(address(this));
}

function _convertToAssets(uint256 shares, Math.Rounding rounding)
    internal view virtual returns (uint256)
{
    return shares.mulDiv(totalAssets() + 1, totalSupply() + 10 ** _decimalsOffset(), rounding);
}
```

`_decimalsOffset()` is the default `0`. The wrapper's `asset()` is the 1:1
tokenized share (the issuance-side `StoxReceiptVault` token, e.g. `tCOIN`).

`StoxWrappedTokenVault`'s own NatSpec states the design intent:

> dividends and stock splits both revalue the underlying asset ... The wrapper
> token as a vault never produces yield or rebases due to off-chain events,
> therefore it captures the value in its price on-chain rather than in its
> supply.

## Why a bare transfer bumps NAV

`convertToAssets(shares) = shares * (totalAssets() + 1) / (totalSupply() + 1)`,
with `totalAssets() = underlying.balanceOf(wrapper)`. Transferring `X`
underlying into the wrapper raises the numerator and leaves `totalSupply()`
unchanged, so every share is worth more -- the standard ERC-4626 "donation".
Depositing via `deposit()` would instead mint shares 1:1 at the current ratio
and **not** move the NAV.

## Implication for the donate step ([RAI-896] / [RAI-1040])

Buy the equity with the dividend cash, tokenize it into the underlying tokenized
share, then `transfer` that share into the wrapper address. No bespoke vault
call is involved.

## Evidence / how to verify

Worked example on Base (chainid 8453), verified 2026-06-16:

| role                                     | address                                      |
| ---------------------------------------- | -------------------------------------------- |
| wtCOIN (BeaconProxy)                     | `0x5cda0e1ca4ce2af96315f7f8963c85399c172204` |
| beacon                                   | `0x4c2d2d3bf1232bf0d3fb7123007a9b8444637bc8` |
| implementation (`StoxWrappedTokenVault`) | `0x80a79767f2d7c24a0577f791ec2af74a7c9a1ed1` |
| underlying `tCOIN` (`asset()`)           | `0x626757e6f50675d17fcad312e82f989ae7a23d38` |

On-chain reads (current state, ratio 1:1): `asset()` returns `tCOIN`;
`totalAssets() == tCOIN.balanceOf(wrapper)`; `convertToAssets(1e18) == 1e18`.
The implementation source is verified on Sourcify; the wrapper source is not
vendored in this repo (Rain "Stox\*" contracts, DCL-1.0).

To prove the bump on a Base fork: read `convertToAssets(1e18)`, `transfer` some
underlying into the wrapper, and re-read -- the value rises proportionally.

[RAI-896]: https://linear.app/makeitrain/issue/RAI-896
[RAI-1040]: https://linear.app/makeitrain/issue/RAI-1040
