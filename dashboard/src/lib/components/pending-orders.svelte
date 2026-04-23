<script lang="ts">
  import { onMount } from 'svelte'
  import * as Card from '$lib/components/ui/card'
  import { reactive } from '$lib/frp.svelte'
  import { getApiBaseUrl } from '$lib/env'
  import { formatUtc, FETCH_TIMEOUT_MS } from '$lib/time'

  type PendingOrder = {
    viewId: string
    status: string
    symbol: string
    direction: string
    shares: string
    executor: string
    placedAt: string
    submittedAt: string | null
    sharesFilled: string | null
    avgPrice: string | null
  }

  const POLL_INTERVAL_MS = 5000

  const orders = reactive<PendingOrder[]>([])
  const error = reactive<string | null>(null)

  const fetchOrders = async () => {
    try {
      const baseUrl = getApiBaseUrl()
      const response = await fetch(`${baseUrl}/orders/pending`, {
        signal: AbortSignal.timeout(FETCH_TIMEOUT_MS)
      })

      if (!response.ok) {
        error.update(() => `HTTP ${String(response.status)}`)
        return
      }

      const data: PendingOrder[] = await response.json() as PendingOrder[]
      orders.update(() => data)
      error.update(() => null)
    } catch (fetchError) {
      error.update(() => fetchError instanceof Error ? fetchError.message : 'Unknown error')
    }
  }

  onMount(() => {
    void fetchOrders()
    const interval = setInterval(() => { void fetchOrders() }, POLL_INTERVAL_MS)
    return () => { clearInterval(interval) }
  })

  const statusColor = (status: string): string => {
    switch (status) {
      case 'Pending': return 'text-yellow-500'
      case 'Submitted': return 'text-blue-400'
      case 'PartiallyFilled': return 'text-blue-400'
      default: return ''
    }
  }

  const dotColor = (status: string): string => {
    switch (status) {
      case 'Pending': return 'bg-yellow-500'
      case 'Submitted': return 'bg-blue-400'
      case 'PartiallyFilled': return 'bg-blue-400'
      default: return 'bg-muted-foreground'
    }
  }

  const directionColor = (direction: string): string =>
    direction === 'Buy' ? 'text-green-500' : 'text-red-500'

</script>

<Card.Root class="flex h-full flex-col overflow-hidden border-l-4 border-l-yellow-500/50">
  <Card.Header class="shrink-0 pb-3">
    <Card.Title class="flex items-center justify-between">
      <span class="flex items-center gap-1.5">
        Pending Orders
        <span class="group relative cursor-help text-muted-foreground">
          <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 20 20" fill="currentColor" class="h-3.5 w-3.5"><path fill-rule="evenodd" d="M18 10a8 8 0 1 1-16 0 8 8 0 0 1 16 0Zm-7-4a1 1 0 1 1-2 0 1 1 0 0 1 2 0ZM9 9a.75.75 0 0 0 0 1.5h.253a.25.25 0 0 1 .244.304l-.459 2.066A1.75 1.75 0 0 0 10.747 15H11a.75.75 0 0 0 0-1.5h-.253a.25.25 0 0 1-.244-.304l.459-2.066A1.75 1.75 0 0 0 9.253 9H9Z" clip-rule="evenodd" /></svg>
          <span class="pointer-events-none absolute left-0 top-full z-50 mt-1 hidden w-56 rounded bg-popover px-3 py-2 text-xs font-normal text-popover-foreground shadow-lg group-hover:block">
            Hedge orders being executed on Alpaca. When someone takes a Raindex order onchain, the bot places an offsetting trade here to hedge exposure.
          </span>
        </span>
      </span>
      <span class="text-sm font-normal text-muted-foreground">
        {orders.current.length} in-flight
      </span>
    </Card.Title>
  </Card.Header>
  <Card.Content class="min-h-0 flex-1 overflow-auto px-6 pt-0">
    {#if error.current}
      <div class="text-xs text-destructive">{error.current}</div>
    {:else if orders.current.length === 0}
      <div class="text-xs text-muted-foreground">No pending orders</div>
    {:else}
      <div class="space-y-1">
        {#each orders.current as order, idx (order.viewId)}
          <div class="flex items-center gap-3 rounded px-2 py-1 text-xs {idx % 2 === 0 ? 'bg-muted/40' : ''}">
            <span class="inline-flex items-center gap-1.5 {statusColor(order.status)}">
              <span class="inline-block h-1.5 w-1.5 rounded-full {dotColor(order.status)}"></span>
              <span class="font-mono">{order.status}</span>
            </span>

            <span class="font-mono font-medium {directionColor(order.direction)}">
              {order.direction}
            </span>

            <span class="font-mono font-medium">{order.symbol}</span>

            <span class="font-mono">{order.shares}</span>

            {#if order.sharesFilled}
              <span class="text-muted-foreground">filled: {order.sharesFilled}</span>
            {/if}

            <span class="ml-auto text-muted-foreground">{formatUtc(order.placedAt)}</span>
          </div>
        {/each}
      </div>
    {/if}
  </Card.Content>
</Card.Root>
