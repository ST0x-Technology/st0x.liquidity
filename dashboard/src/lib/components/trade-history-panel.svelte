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
      case 'dry_run': return 'DryRun'
    }
  }

  const formatTime = (iso: string): string => {
    const date = new Date(iso)
    const now = new Date()
    const isToday =
      date.getFullYear() === now.getFullYear() &&
      date.getMonth() === now.getMonth() &&
      date.getDate() === now.getDate()

    if (isToday) {
      return date.toLocaleTimeString('en-US', { hour12: false })
    }

    return date.toLocaleString('en-US', {
      hour12: false,
      month: 'short',
      day: 'numeric',
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit'
    })
  }

  const directionColor = (direction: Trade['direction']): string =>
    direction === 'buy' ? 'text-green-500' : 'text-red-500'

  const fmtSize = (value: string): string => {
    const num = parseFloat(value)
    if (num === 0) return '0'
    if (num >= 1) return num.toFixed(2)
    return num.toPrecision(4)
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
            <Table.Head class="text-right">Time</Table.Head>
            <Table.Head>Asset</Table.Head>
            <Table.Head class="text-right">Venue</Table.Head>
            <Table.Head class="text-right">Side</Table.Head>
            <Table.Head class="text-center">Size</Table.Head>
          </Table.Row>
        </Table.Header>
        <Table.Body>
          {#each trades as trade (trade.id)}
            <Table.Row>
              <Table.Cell class="text-right font-mono text-xs text-muted-foreground">
                {formatTime(trade.filledAt)}
              </Table.Cell>

              <Table.Cell class="font-mono text-xs font-medium">
                {trade.symbol}
              </Table.Cell>

              <Table.Cell class="text-right text-xs">
                {venueLabel(trade.venue)}
              </Table.Cell>

              <Table.Cell class="text-right text-xs font-medium {directionColor(trade.direction)}">
                {trade.direction === 'buy' ? 'Buy' : 'Sell'}
              </Table.Cell>

              <Table.Cell class="text-center font-mono text-xs">
                {fmtSize(trade.shares)}
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
