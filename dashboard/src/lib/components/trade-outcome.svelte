<script lang="ts">
  import type { TradeOutcome } from '$lib/api/TradeOutcome'
  import { decimalIsZero, formatDecimal } from '$lib/decimal'
  import {
    tradeFailureReason,
    tradeFailureShares,
    tradeOutcomeClass,
    tradeOutcomeLabel
  } from '$lib/trade'

  let { outcome, compact = true }: { outcome: TradeOutcome; compact?: boolean } = $props()

  const failureReason = $derived(tradeFailureReason(outcome))
  const failureShares = $derived(tradeFailureShares(outcome))
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
{#if failureShares !== null}
  <div class="text-muted-foreground">
    Accepted {quantity(failureShares.accepted)} · Filled {quantity(failureShares.filled)}
    {#if failureShares.remaining !== null}
      · Unfilled {formatDecimal(failureShares.remaining, 9)}
    {/if}
  </div>
  {#if failureShares.excess !== null && !decimalIsZero(failureShares.excess)}
    <div class="text-destructive">
      Excess fill {formatDecimal(failureShares.excess, 9)}
    </div>
  {/if}
{/if}
