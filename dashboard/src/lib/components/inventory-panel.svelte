<script lang="ts">
  import { createQuery } from '@tanstack/svelte-query'
  import AvailableInventory from '$lib/components/available-inventory.svelte'
  import * as Card from '$lib/components/ui/card'
  import type { Inventory } from '$lib/api/Inventory'
  import type { Position } from '$lib/api/Position'
  import type { Settings } from '$lib/api/Settings'

  const inventoryQuery = createQuery<Inventory>(() => ({
    queryKey: ['inventory'],
    enabled: false
  }))

  const positionsQuery = createQuery<Position[]>(() => ({
    queryKey: ['positions'],
    enabled: false
  }))

  const settingsQuery = createQuery<Settings>(() => ({
    queryKey: ['settings'],
    enabled: false
  }))

  const inventory = $derived(inventoryQuery.data)
  const symbols = $derived(inventory?.perSymbol ?? [])
  const usdc = $derived(inventory?.usdc)
  const positions = $derived(positionsQuery.data ?? [])
  const settings = $derived(settingsQuery.data)
</script>

<Card.Root class="flex shrink-0 flex-col overflow-hidden">
  <Card.Header class="shrink-0 pb-3">
    <Card.Title>Overview</Card.Title>
  </Card.Header>
  <Card.Content class="relative min-h-0 flex-1 overflow-auto px-6 pt-0">
    {#if !inventory}
      <div class="flex h-full items-center justify-center text-muted-foreground">
        Waiting for inventory data…
      </div>
    {:else}
      <AvailableInventory {symbols} {usdc} {positions} {settings} />
    {/if}
  </Card.Content>
</Card.Root>
