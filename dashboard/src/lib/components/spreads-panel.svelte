<script lang="ts">
  import { createQuery } from '@tanstack/svelte-query'
  import * as Card from '$lib/components/ui/card'
  import * as Table from '$lib/components/ui/table'
  import type { SpreadSummary } from '$lib/api/SpreadSummary'

  const spreads = createQuery<SpreadSummary[]>(() => ({
    queryKey: ['spreads'],
    enabled: false
  }))
</script>

<Card.Root class="flex min-h-0 flex-col">
  <Card.Header class="shrink-0 pb-2">
    <Card.Title class="text-sm">Spreads</Card.Title>
  </Card.Header>

  <Card.Content class="min-h-0 flex-1 overflow-auto px-3 pb-3">
    {#if spreads.data && spreads.data.length > 0}
      <Table.Root>
        <Table.Header>
          <Table.Row>
            <Table.Head>Symbol</Table.Head>
            <Table.Head class="text-right">Buy</Table.Head>
            <Table.Head class="text-right">Sell</Table.Head>
            <Table.Head class="text-right">Pyth</Table.Head>
            <Table.Head class="text-right">Spread</Table.Head>
          </Table.Row>
        </Table.Header>
        <Table.Body>
          {#each spreads.data as spread (spread.symbol)}
            <Table.Row>
              <Table.Cell class="font-medium">{spread.symbol}</Table.Cell>
              <Table.Cell class="text-right">${spread.lastBuyPrice}</Table.Cell>
              <Table.Cell class="text-right">${spread.lastSellPrice}</Table.Cell>
              <Table.Cell class="text-right">${spread.pythPrice}</Table.Cell>
              <Table.Cell class="text-right">{spread.spreadBps} bps</Table.Cell>
            </Table.Row>
          {/each}
        </Table.Body>
      </Table.Root>
    {:else}
      <p class="text-muted-foreground py-8 text-center text-sm">No spread data available</p>
    {/if}
  </Card.Content>
</Card.Root>
