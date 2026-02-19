<script lang="ts">
  import { createQuery } from '@tanstack/svelte-query'
  import * as Card from '$lib/components/ui/card'
  import * as Table from '$lib/components/ui/table'
  import type { Inventory } from '$lib/api/Inventory'

  const inventory = createQuery<Inventory>(() => ({
    queryKey: ['inventory'],
    enabled: false
  }))
</script>

<Card.Root class="flex min-h-0 flex-col">
  <Card.Header class="shrink-0 pb-2">
    <Card.Title class="text-sm">Inventory</Card.Title>
  </Card.Header>

  <Card.Content class="min-h-0 flex-1 overflow-auto px-3 pb-3">
    {#if inventory.data}
      <Table.Root>
        <Table.Header>
          <Table.Row>
            <Table.Head>Symbol</Table.Head>
            <Table.Head class="text-right">Onchain</Table.Head>
            <Table.Head class="text-right">Offchain</Table.Head>
          </Table.Row>
        </Table.Header>
        <Table.Body>
          {#each inventory.data.perSymbol as symbol (symbol.symbol)}
            <Table.Row>
              <Table.Cell class="font-medium">{symbol.symbol}</Table.Cell>
              <Table.Cell class="text-right">{symbol.onchain}</Table.Cell>
              <Table.Cell class="text-right">{symbol.offchain}</Table.Cell>
            </Table.Row>
          {/each}
          <Table.Row class="border-t-2">
            <Table.Cell class="font-medium">USDC</Table.Cell>
            <Table.Cell class="text-right">${inventory.data.usdc.onchain}</Table.Cell>
            <Table.Cell class="text-right">${inventory.data.usdc.offchain}</Table.Cell>
          </Table.Row>
        </Table.Body>
      </Table.Root>
    {:else}
      <p class="text-muted-foreground py-8 text-center text-sm">No inventory data</p>
    {/if}
  </Card.Content>
</Card.Root>
