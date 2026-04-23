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

<Card.Root class="flex h-full flex-col overflow-hidden border-l-4 border-l-blue-500/50">
  <Card.Header class="shrink-0 pb-3">
    <Card.Title class="flex items-center gap-1.5">
      Inventory
      <span class="group relative cursor-help text-muted-foreground">
        <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 20 20" fill="currentColor" class="h-3.5 w-3.5"><path fill-rule="evenodd" d="M18 10a8 8 0 1 1-16 0 8 8 0 0 1 16 0Zm-7-4a1 1 0 1 1-2 0 1 1 0 0 1 2 0ZM9 9a.75.75 0 0 0 0 1.5h.253a.25.25 0 0 1 .244.304l-.459 2.066A1.75 1.75 0 0 0 10.747 15H11a.75.75 0 0 0 0-1.5h-.253a.25.25 0 0 1-.244-.304l.459-2.066A1.75 1.75 0 0 0 9.253 9H9Z" clip-rule="evenodd" /></svg>
        <span class="pointer-events-none absolute left-0 top-full z-50 mt-1 hidden w-56 rounded bg-popover px-3 py-2 text-xs font-normal text-popover-foreground shadow-lg group-hover:block">
          Asset balances across Alpaca (offchain) and Raindex (onchain), with allocation ratios and directional exposure.
        </span>
      </span>
    </Card.Title>
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
