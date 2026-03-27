<script lang="ts">
  import { createQuery } from '@tanstack/svelte-query'
  import * as Card from '$lib/components/ui/card'
  import * as Table from '$lib/components/ui/table'
  import type { Trade } from '$lib/api/Trade'

  const tradesQuery = createQuery<Trade[]>(() => ({
    queryKey: ['trades'],
    enabled: false
  }))

  const trades = $derived(tradesQuery.data ?? [])

  const venueLabel = (venue: Trade['venue']): string => {
    switch (venue) {
      case 'raindex': return 'Raindex'
      case 'alpaca': return 'Alpaca'
      case 'schwab': return 'Schwab'
      case 'dry_run': return 'DryRun'
    }
  }

  const formatTime = (iso: string): string => {
    const date = new Date(iso)
    return date.toLocaleTimeString('en-US', { hour12: false })
  }

  const directionColor = (direction: Trade['direction']): string =>
    direction === 'buy' ? 'text-green-500' : 'text-red-500'

  const tradesWithDislocation = $derived.by(() => {
    const net = new Map<string, number>()
    const sorted = [...trades].sort(
      (lhs, rhs) => new Date(lhs.filledAt).getTime() - new Date(rhs.filledAt).getTime()
    )

    return sorted.map((trade) => {
      const shares = parseFloat(trade.shares)
      const delta = trade.direction === 'buy' ? shares : -shares
      const prev = net.get(trade.symbol) ?? 0
      const updated = prev + delta
      net.set(trade.symbol, updated)

      return { ...trade, delta, netDislocation: updated }
    }).reverse()
  })

  const fmtSigned = (value: number): string => {
    const abs = Math.abs(value)
    const sign = value >= 0 ? '+' : '-'
    const formatted = abs === 0 ? '0' : abs >= 1 ? abs.toFixed(2) : abs.toPrecision(4)
    return `${sign}${formatted}`
  }
</script>

<Card.Root class="flex h-full min-h-56 flex-col overflow-hidden">
  <Card.Header class="shrink-0 pb-0">
    <Card.Title class="flex items-center justify-between">
      <span>Trade History</span>
      <span class="text-sm font-normal text-muted-foreground">
        {trades.length} fills
      </span>
    </Card.Title>
  </Card.Header>
  <Card.Content class="relative min-h-0 flex-1 overflow-auto px-6 pt-0">
    {#if trades.length === 0}
      <div class="flex h-full items-center justify-center text-muted-foreground">
        No trades yet
      </div>
    {:else}
      <Table.Root>
        <Table.Header>
          <Table.Row>
            <Table.Head>Time</Table.Head>
            <Table.Head>Venue</Table.Head>
            <Table.Head>Side</Table.Head>
            <Table.Head class="text-right">Dislocation</Table.Head>
            <Table.Head>Underlying</Table.Head>
          </Table.Row>
        </Table.Header>
        <Table.Body>
          {#each tradesWithDislocation as trade (trade.filledAt + trade.symbol + trade.venue)}
            <Table.Row>
              <Table.Cell class="font-mono text-xs text-muted-foreground">
                {formatTime(trade.filledAt)}
              </Table.Cell>

              <Table.Cell class="text-xs">
                {venueLabel(trade.venue)}
              </Table.Cell>

              <Table.Cell class="text-xs font-medium {directionColor(trade.direction)}">
                {trade.direction === 'buy' ? 'Buy' : 'Sell'}
              </Table.Cell>

              <Table.Cell class="text-right font-mono text-xs">
                <span class={directionColor(trade.direction)}>
                  {fmtSigned(trade.delta)}
                </span>
                {' '}
                <span class="text-muted-foreground">
                  (net: {fmtSigned(trade.netDislocation)})
                </span>
              </Table.Cell>

              <Table.Cell class="font-mono text-xs font-medium">
                {trade.symbol}
              </Table.Cell>
            </Table.Row>
          {/each}
        </Table.Body>
      </Table.Root>
      <div
        class="pointer-events-none sticky bottom-0 h-8 bg-gradient-to-t from-card to-transparent"
      ></div>
    {/if}
  </Card.Content>
</Card.Root>
