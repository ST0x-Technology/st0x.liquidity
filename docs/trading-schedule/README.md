# Trading schedule rollout

These TOML fragments are examples, not deployed configuration. The runtime
configs omit `[pricing.trading_schedule]` because older bot images reject it.
Tests check each fragment against its environment's current asset eligibility.

1. Release a bot binary that accepts the optional schedule block.
2. Verify pricing's schedule API, Google identity access, scope membership, and
   eligibility profiles for the target environment.
3. Add the matching fragment to that environment's runtime config in a separate
   config change. Keep `mode = "observe"` and pass the released-image validator.
4. Demonstrate the schedule and restart behavior in staging. Verify signed-order
   expiry enforcement and review missing pricing coverage before enabling it.

The production fragment isolates FTF, CBRS, and QSEP in unpriced scopes. The
staging fragment isolates QSEP. Broker fallback is degraded coverage, not a
complete pricing schedule. Recheck these memberships before rollout.

Observation does not change hedge behavior. Enforcement requires separate
approval; neither a stopped quote stream nor a deferred hedge proves the bot is
flat. See [ADR 0021](../../adrs/0021-consume-pricing-trading-schedule.md).
