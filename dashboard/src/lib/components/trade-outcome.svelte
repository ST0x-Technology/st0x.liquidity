<script lang="ts">
  import type { TradeOutcome } from '$lib/api/TradeOutcome'
  import { decimalIsZero, formatDecimal } from '$lib/decimal'
  import {
    tradeFailureReason,
    tradeOutcomeShares,
    tradeOutcomeClass,
    tradeOutcomeLabel
  } from '$lib/trade'

  // Compact is the trade-history row: label only, so a failed row keeps the
  // same height as a filled one. The reason and the share breakdown live in
  // the detail panel behind the row's info button.
  let { outcome, compact = true }: { outcome: TradeOutcome; compact?: boolean } = $props()

  const failureReason = $derived(tradeFailureReason(outcome))
  const outcomeShares = $derived(tradeOutcomeShares(outcome))
  const quantity = (value: string | null): string =>
    value === null ? 'unknown' : formatDecimal(value, 9)
</script>

<div class="font-medium {tradeOutcomeClass(outcome)}">
  {tradeOutcomeLabel(outcome)}
</div>
{#if !compact && failureReason !== null}
  <div class="break-all text-destructive">
    {failureReason}
  </div>
{/if}
{#if !compact && outcomeShares !== null}
  <div class="text-muted-foreground">
    Accepted {quantity(outcomeShares.accepted)} · Filled {quantity(outcomeShares.filled)}
    {#if outcomeShares.remaining !== null}
      · Unfilled {formatDecimal(outcomeShares.remaining, 9)}
    {/if}
  </div>
  {#if outcomeShares.excess !== null && !decimalIsZero(outcomeShares.excess)}
    <div class="text-destructive">
      Excess fill {formatDecimal(outcomeShares.excess, 9)}
    </div>
  {/if}
{/if}
