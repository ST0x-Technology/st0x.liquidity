# ADR 0019: Resolve close-flatten prices through a fallback chain

- Status: Accepted
- Date: 2026-08-10
- Updated: 2026-08-11 after Alpaca recommended current bid/ask market data as
  the primary limit-order reference and the position mark as fallback

## Context

Close-flatten mode (ADR-less, introduced by PR #1045 for RAI-1404) prices its
hedges by crossing the SIP quote: buys take the ask plus
`counter_trade_slippage_bps`, sells take the bid minus it. `SPEC.md` states the
rule and its guard explicitly:

> Quote requests explicitly select Alpaca's SIP feed so a subscription-dependent
> default cannot silently substitute an IEX-only quote. Missing SIP access and
> missing, non-positive, or crossed quotes are retryable errors; the bot never
> falls back to a latest-trade or stale position price.

`docs/conductor.md` repeats the same prohibition. So a position-price fallback
was considered when close-flatten was designed, and was ruled out on purpose.

On 2026-08-07, the first multi-day gap after that config reached production, the
design failed in the worst available way. Every quote request returned
`403 {"message":"subscription does not permit querying recent SIP data"}`. The
`PlaceHedge` job exhausted its retries, which failed `hedge-worker-0`, which
exited the process. systemd restarted the bot 12 times between 23:49:42 and
23:59:45 UTC, and the loop only ended because the session closed. Eight
trade-enabled symbols carried roughly 9.7k USD of net short exposure through the
weekend. Nothing had verified the SIP entitlement before shipping: the tests use
`httpmock`, so CI passed and the entitlement was first exercised in production
three weeks later.

Direct probes against the production Broker API key establish what we actually
have:

| `feed`        | Result                                                     |
| ------------- | ---------------------------------------------------------- |
| `sip`         | 403, subscription does not permit querying recent SIP data |
| `iex`         | 200                                                        |
| `delayed_sip` | 200, real consolidated NBBO, 15 minutes delayed            |
| `boats`       | 403                                                        |
| `overnight`   | 403                                                        |
| `otc`         | 403                                                        |

Both the documented Broker API auth scheme (HTTP Basic) and the retail
`APCA-API-*` header scheme return identical results, so this is an entitlement
boundary and not a request defect. Alpaca's Broker API tiers (Standard through
StandardPlus10000, up to 2,000 USD/month) are all documented as "real time IEX
or 15 mins delayed SIP", so real-time SIP is not self-serve for a Broker API
partner and requires a custom arrangement.

Two facts decide this ADR.

**IEX is not a usable substitute inside the window.** IEX stops quoting between
roughly 16:00 and 16:40 ET, while the close-flatten window runs 19:45 to 20:00
ET. Friday's closing IEX quotes, against the true mark:

| Symbol | IEX bid | IEX ask | Spread | Mark   |
| ------ | ------- | ------- | ------ | ------ |
| SKHY   | 117.65  | 155.25  | 27.6%  | 138.14 |
| COIN   | 143.79  | 161.86  | 11.8%  | 154.06 |
| CEG    | 255.80  | 283.12  | 10.1%  | 269.70 |
| NVDA   | 210.44  | 232.74  | 10.1%  | 223.80 |
| SPYM   | 88.00   | 0       | no ask | 91.01  |

`LatestQuote::new` rejects only crossed quotes, so a 27%-wide stub validates
cleanly. Crossing these produces marketable limits 5% to 16% through the real
market, on the order of 700 USD given away per Friday. That converts a loud
crash into a silent, certain loss.

**The spec's staleness premise is backwards.** The prohibition names the "stale
position price" as the thing to avoid. Measurement shows the position mark is
the freshest source available to us. It is fed by CTA and UTP, the two
consolidated tapes that constitute the SIP, and it updates continuously through
the extended session:

```
19:59:34Z  NVDA=224.01     15:59 ET, just before regular close
21:43:42Z  NVDA=223.116
23:59:08Z  NVDA=223.7417   19:59 ET, last tick of extended session
00:00:37Z  NVDA=223.8      20:00 ET, freezes at the close
```

The genuinely stale source is `fetch_latest_trade_price`, which sends no `feed`
parameter, resolves to the IEX default, and therefore freezes at roughly 16:00
ET. That call prices every ordinary extended-hours hedge, so the same data gap
affects 16:00 to 20:00 ET every weekday, not only the flatten window.

The operating constraint is that flattening before a multi-day gap is mandatory.
A wide spread is always cheaper than carrying directional exposure across a
weekend. Any design where a missing quote results in no flatten is unacceptable,
which is what both the current code and the current spec produce.

## Decision

1. **Close-flatten resolves prices through an ordered source chain:** an
   optional current bid/ask quote source first, the position mark second, and
   the hardcoded `delayed_sip` quote last. A buy takes the primary quote's ask
   and a sell takes its bid. Alpaca recommends market data as the primary basis
   for limit orders and the mark as fallback. The primary capability is
   deliberately optional because this account does not currently have a usable
   real-time market-data entitlement and may never use Alpaca SIP. With no
   provider installed, the chain starts effectively at the mark, preserving the
   production behaviour this ADR introduced. A missing or failed primary lookup
   always falls through to the mark.

2. **The cross widens with elapsed time inside the window** instead of applying
   a single flat band. It ramps linearly from `counter_trade_slippage_bps` at
   the window's start to the configured ceiling `close_flatten_cross_max_bps` at
   the extended-session close. The ramp is anchored to the window rather than to
   a per-order attempt count, so the cross is a pure function of the window and
   the current time: restart-safe, identical across retries, and independent of
   how many reprice cycles land. A hedge that first becomes ready mid-window
   opens partway up the ramp, because it has less time left to flatten. The
   dedicated `close_flatten_reprice_timeout_secs` setting is 60 seconds inside
   the 900-second window, making each later attempt sample the ramp further
   along so the cross converges toward a fill without needing a spread we cannot
   observe in real time. The ordinary `extended_hours_reprice_timeout_secs`
   remains 300 seconds outside the window: cancelling and replacing at the same
   flat cross every minute would lose broker time priority and add calls without
   improving fill probability. Both values remain explicit deployment settings.
   The ramp deliberately reuses `counter_trade_slippage_bps` as its floor; that
   setting also sizes the ordinary buy-side preflight buffer. The shared floor
   keeps the first close-flatten attempt identical to ordinary extended-hours
   policy instead of introducing a discontinuity and another tuning knob, so
   operators must treat changes to it as affecting both behaviors.

3. **A missing price never skips the flatten.** Dead-lettering remains the
   terminal behaviour, but only once every reference source has failed, not on
   the first failed lookup. The order is optional primary quote, position mark,
   `delayed_sip` quote, then dead-letter. Failure of a preferred source never
   suppresses a usable fallback. This promise is specific to price sources. A
   calendar/session lookup failure has no safe fallback: without a trusted
   extended-session close the bot cannot know whether the venue accepts the
   order or where the ramp lies. A queued hedge surfaces that as a
   process-scoped failure; scan-time and immediate-fill paths count and skip the
   current attempt, then retry on a later scan. No path fabricates a session
   boundary or cross.

4. **There is no market-data feed configuration.** The fallback quote request
   stays hardcoded to `feed=delayed_sip`; no `[broker] market_data_feed` key or
   `AlpacaMarketDataFeed` enum is introduced. The probe table above establishes
   why there is no runtime choice: `sip` is rejected, `iex` publishes stub
   quotes once it stops trading around 16:00 ET, and `delayed_sip` is the only
   value that both answers and returns a real book. A required key with exactly
   one correct value would only make the bot misconfigurable. The executor
   instead exposes an optional primary-quote capability whose default is
   unavailable. A future provider is selected by wiring an implementation, not
   by exposing a setting that is invalid in the current deployment.

5. **Ordinary extended-hours hedges use the same source chain**, replacing
   `fetch_latest_trade_price`. That call resolves to the plan default feed and
   freezes at roughly 16:00 ET, so every hedge between then and the 20:00 ET
   close had been priced from a stale reference. Sharing one resolver prevents
   ordinary and close-flatten pricing from acquiring different fallback rules.
   This replacement is scoped to extended-hours hedge pricing and its
   exact-limit buy preflight. The regular-hours market-order buy preflight keeps
   the latest-trade lookup because IEX is live during that session; it does not
   expose the stale 16:00-to-20:00 behavior this decision removes.

## Consequences

- We lose the marketability guarantee that crossing a live NBBO provided. A mark
  plus a band is an estimate of where the book is, not a price derived from it.
  The widening cross recovers this probabilistically rather than absolutely: an
  attempt can fail to fill, and only a later, wider one corrects it.
- A flatten can still end the session unfilled if the ramp's ceiling is below
  the true spread. This is a real residual risk and is the reason the ceiling is
  configurable rather than fixed.
- Attempt fill rate inside each close-flatten window is computed from
  `close_flatten_outcomes_total{symbol,direction,outcome}` as `filled` divided
  by all terminal outcomes. Partial fills take their eventual terminal outcome;
  cancelled and failed attempts stay in the denominator. Compare it with
  `close_flatten_placements_total{symbol,direction,cross_bucket}` over the same
  windows. Low fill rate while placements concentrate in the ceiling bucket is
  the evidence for tuning `close_flatten_cross_max_bps`; fills at lower buckets
  argue against widening it. PR #1180 implements both counters, and `SPEC.md`
  defines their observation semantics.
- The mark is only available for symbols where a broker position exists. The
  `delayed_sip` fallback covers the gap, at the cost of a 15-minute-old price.
  Because that delay equals the default 900-second flatten window, every delayed
  quote used during the window describes a book from before the window began.
  The ramp does not start wider for this source: the same configured ceiling
  bounds loss across all sources. This residual is accepted because the delayed
  quote is reached only after the mark is unavailable and a bounded cross around
  a genuine consolidated book is preferable to carrying the exposure through a
  multi-day closure.
- Until a primary quote provider is wired, we remain unable to observe a
  real-time spread and cannot detect a genuinely dislocated book at the moment
  we trade into it.
- Adding a real-time SIP entitlement or another quote provider later is a small
  executor implementation change rather than a configuration flip. The resolver
  and its mark fallback do not change.

## Alternatives considered

**Buy real-time SIP now.** The cleanest source on the merits. Rejected as an
immediate dependency because it is not self-serve for a Broker API partner,
pricing is bespoke and currently unknown, and the exposure recurs every Friday
while procurement runs. The optional primary-quote boundary preserves this path
without making the entitlement a prerequisite for the present fix.

**Configure `feed=iex`.** What the branch under review currently does. Rejected:
the table above shows it prices 5% to 16% through the real market inside the
exact window it is needed, which is worse than the crash it replaces.

**Keep the feed configurable, restricted to `Sip | DelayedSip`.** Keeps the
config path open for a future entitlement while removing the value that loses
money. Rejected as carrying a required key whose only valid setting today is
`DelayedSip`. That fallback is uncommon for close-flatten sells because a broker
position normally supplies a mark, but it is expected to be common for ordinary
extended-hours buys that open exposure in a symbol with no broker position yet.
The higher call volume does not create a meaningful choice between feeds: the
entitlement and stub-book constraints are the same, so the knob still does not
earn a config surface.

**Reject quotes whose spread exceeds a bound.** Considered as protection against
stub books, and drafted into an earlier revision of this ADR. Rejected on the
grounds that it contradicts the operating constraint: rejecting a wide quote
produces no flatten, and no flatten is the outcome this whole change exists to
prevent. Its original purpose also disappears with decision 4, since
`delayed_sip` returns a real consolidated book rather than the single-venue
stubs that motivated the guard. A wide but genuine quote should be crossed, not
refused.

**Size the cross from the `delayed_sip` spread.** Attractive because it uses a
real NBBO width rather than a guess. Rejected for now as the primary mechanism:
a 15-minute-old spread is least trustworthy exactly when it matters, in a fast
or dislocating book, and it adds a second required data dependency. The widening
cross achieves the same convergence without trusting a stale width. Worth
revisiting as a way to choose where the ramp starts.

**Move the flatten before the 16:00 ET regular close.** Costs nothing and gets
deep liquidity and live IEX quotes. Rejected as a replacement because onchain
fills continue until 20:00 ET, so the exposure it leaves behind is precisely the
exposure close-flatten exists to remove. Viable as a complement.
