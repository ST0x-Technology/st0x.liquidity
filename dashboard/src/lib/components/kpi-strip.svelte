<script lang="ts">
  import { createQuery } from '@tanstack/svelte-query'
  import * as Card from '$lib/components/ui/card'
  import type { PerformanceMetrics } from '$lib/api/PerformanceMetrics'

  const metrics = createQuery<PerformanceMetrics>(() => ({
    queryKey: ['metrics'],
    enabled: false
  }))

  const allTime = $derived(metrics.data?.all)

  const kpis = $derived([
    { label: 'P&L', value: allTime ? `$${allTime.pnl.absolute}` : '--' },
    { label: 'Volume', value: allTime?.volume ? `$${allTime.volume}` : '--' },
    { label: 'Trades', value: allTime?.tradeCount != null ? String(allTime.tradeCount) : '--' },
    {
      label: 'Hedge Lag',
      value: allTime?.hedgeLagMs != null ? `${String(allTime.hedgeLagMs)}ms` : '--'
    },
    { label: 'Uptime', value: allTime?.uptimePercent ? `${allTime.uptimePercent}%` : '--' },
    { label: 'Sharpe', value: allTime?.sharpeRatio ?? '--' }
  ])
</script>

<div class="grid shrink-0 grid-cols-3 gap-2 md:grid-cols-6 md:gap-4">
  {#each kpis as kpi (kpi.label)}
    <Card.Root class="py-2 md:py-3">
      <Card.Content class="px-3 py-0 text-center">
        <p class="text-muted-foreground text-xs">{kpi.label}</p>
        <p class="text-sm font-semibold md:text-base">{kpi.value}</p>
      </Card.Content>
    </Card.Root>
  {/each}
</div>
