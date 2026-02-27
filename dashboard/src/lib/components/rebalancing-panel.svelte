<script lang="ts">
  import { createQuery } from '@tanstack/svelte-query'
  import * as Card from '$lib/components/ui/card'
  import type { RebalanceOperation } from '$lib/api/RebalanceOperation'

  const activeRebalances = createQuery<RebalanceOperation[]>(() => ({
    queryKey: ['rebalances', 'active'],
    enabled: false
  }))

  const recentRebalances = createQuery<RebalanceOperation[]>(() => ({
    queryKey: ['rebalances', 'recent'],
    enabled: false
  }))
</script>

<Card.Root class="flex min-h-0 flex-col">
  <Card.Header class="shrink-0 pb-2">
    <Card.Title class="text-sm">Rebalancing</Card.Title>
  </Card.Header>

  <Card.Content class="min-h-0 flex-1 overflow-auto px-3 pb-3">
    {#if activeRebalances.data && activeRebalances.data.length > 0}
      <div class="mb-3">
        <p class="text-muted-foreground mb-1 text-xs font-medium">Active</p>
        <ul class="space-y-1">
          {#each activeRebalances.data as rebalance (rebalance.id)}
            <li class="rounded border px-2 py-1 text-xs">{rebalance.id}</li>
          {/each}
        </ul>
      </div>
    {/if}

    {#if recentRebalances.data && recentRebalances.data.length > 0}
      <div>
        <p class="text-muted-foreground mb-1 text-xs font-medium">Recent</p>
        <ul class="space-y-1">
          {#each recentRebalances.data as rebalance (rebalance.id)}
            <li class="text-muted-foreground border-b py-1 text-xs last:border-0">
              {rebalance.id}
            </li>
          {/each}
        </ul>
      </div>
    {/if}

    {#if (!activeRebalances.data || activeRebalances.data.length === 0) && (!recentRebalances.data || recentRebalances.data.length === 0)}
      <p class="text-muted-foreground py-8 text-center text-sm">No rebalancing activity</p>
    {/if}
  </Card.Content>
</Card.Root>
