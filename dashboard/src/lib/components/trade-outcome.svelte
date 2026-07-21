<script lang="ts">
  import type { TradeOutcome } from '$lib/api/TradeOutcome'
  import { decimalIsZero, formatDecimal } from '$lib/decimal'
  import {
    tradeFailureReason,
    tradeOutcomeShares,
    tradeOutcomeClass,
    tradeOutcomeLabel
  } from '$lib/trade'

  let { outcome, compact = true }: { outcome: TradeOutcome; compact?: boolean } = $props()

  const failureReason = $derived(tradeFailureReason(outcome))
  const outcomeShares = $derived(tradeOutcomeShares(outcome))
  const quantity = (value: string | null): string =>
    value === null ? 'unknown' : formatDecimal(value, 9)
</script>

<div class="font-medium {tradeOutcomeClass(outcome)}">
  {tradeOutcomeLabel(outcome)}
</div>
{#if failureReason !== null}
  <div
    class={compact ? 'truncate text-destructive/80' : 'break-all text-destructive'}
    title={failureReason}
  >
    {failureReason}
  </div>
{/if}
{#if outcomeShares !== null}
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
