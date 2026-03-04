<script lang="ts">
  import { createQuery } from '@tanstack/svelte-query'
  import * as Card from '$lib/components/ui/card'
  import type { Trade } from '$lib/api/Trade'

  const trades = createQuery<Trade[]>(() => ({
    queryKey: ['trades'],
    enabled: false
  }))
</script>

<Card.Root class="flex min-h-0 flex-col">
  <Card.Header class="shrink-0 pb-2">
    <Card.Title class="text-sm">Trade Log</Card.Title>
  </Card.Header>

  <Card.Content class="min-h-0 flex-1 overflow-auto px-3 pb-3">
    {#if trades.data && trades.data.length > 0}
      <ul class="space-y-1">
        {#each trades.data as trade (trade.id)}
          <li class="text-muted-foreground border-b py-1 text-xs last:border-0">
            {trade.id}
          </li>
        {/each}
      </ul>
    {:else}
      <p class="text-muted-foreground py-8 text-center text-sm">No trades yet</p>
    {/if}
  </Card.Content>
</Card.Root>
