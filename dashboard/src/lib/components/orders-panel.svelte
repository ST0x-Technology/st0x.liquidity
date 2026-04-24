<script lang="ts">
  import { onMount } from 'svelte'
  import * as Card from '$lib/components/ui/card'
  import { reactive } from '$lib/frp.svelte'
  import { getApiBaseUrl } from '$lib/env'
  import { FETCH_TIMEOUT_MS } from '$lib/time'
  import { formatBalance, formatTimestamp } from '$lib/format'

  type TokenRef = {
    address: string
    symbol: string
    decimals: number
  }

  type OrderSummary = {
    orderHash: string
    owner: string
    inputToken: TokenRef
    outputToken: TokenRef
    outputVaultBalance: string
    ioRatio: string
    createdAt: number
    orderbookId: string
  }

  type OrdersListResponse = {
    orders: OrderSummary[]
    pagination: {
      page: number
      pageSize: number
      totalOrders: number
      totalPages: number
      hasMore: boolean
    }
  }

  type UnavailableResponse = {
    unavailable: true
    reason: string
  }

  type ApiResponse = OrdersListResponse | UnavailableResponse

  const isUnavailable = (response: ApiResponse): response is UnavailableResponse =>
    'unavailable' in response && response.unavailable

  const data = reactive<ApiResponse | null>(null)
  const loading = reactive(false)
  const error = reactive<string | null>(null)

  const fetchOrders = async () => {
    loading.update(() => true)
    error.update(() => null)

    try {
      const baseUrl = getApiBaseUrl()
      const response = await fetch(
        `${baseUrl}/orders/raindex`,
        { signal: AbortSignal.timeout(FETCH_TIMEOUT_MS) },
      )

      if (!response.ok) {
        error.update(() => `HTTP ${String(response.status)}`)
        return
      }

      const result: unknown = await response.json()

      if (typeof result === 'object' && result !== null && 'unavailable' in result) {
        data.update(() => result as UnavailableResponse)
        return
      }

      if (
        typeof result !== 'object' ||
        result === null ||
        !('orders' in result) ||
        !Array.isArray((result as OrdersListResponse).orders)
      ) {
        error.update(() => 'Unexpected response format from REST API')
        return
      }

      data.update(() => result as OrdersListResponse)
    } catch (fetchError) {
      error.update(() =>
        fetchError instanceof Error ? fetchError.message : 'Unknown error',
      )
    } finally {
      loading.update(() => false)
    }
  }

  onMount(() => {
    void fetchOrders()
  })


</script>

<Card.Root class="flex h-full flex-col overflow-hidden border-l-4 border-l-violet-500/50">
  <Card.Header class="shrink-0 pb-2">
    <Card.Title class="flex items-center justify-between">
      <span class="flex items-center gap-1.5">
        Raindex Orders
        <span class="group relative cursor-help text-muted-foreground">
          <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 20 20" fill="currentColor" class="h-3.5 w-3.5"><path fill-rule="evenodd" d="M18 10a8 8 0 1 1-16 0 8 8 0 0 1 16 0Zm-7-4a1 1 0 1 1-2 0 1 1 0 0 1 2 0ZM9 9a.75.75 0 0 0 0 1.5h.253a.25.25 0 0 1 .244.304l-.459 2.066A1.75 1.75 0 0 0 10.747 15H11a.75.75 0 0 0 0-1.5h-.253a.25.25 0 0 1-.244-.304l.459-2.066A1.75 1.75 0 0 0 9.253 9H9Z" clip-rule="evenodd" /></svg>
          <span class="pointer-events-none absolute left-0 top-full z-50 mt-1 hidden w-72 rounded bg-popover px-3 py-2 text-xs font-normal text-popover-foreground shadow-lg group-hover:block">
            Active onchain orders on the Raindex orderbook owned by the bot wallet.
            <strong>Output</strong> is the token the order offers (leaves the vault when taken).
            <strong>Input</strong> is the token received in exchange.
            <strong>Vault Balance</strong> is the output token amount currently available.
            <strong>IO Ratio</strong> is the current price (output per input).
          </span>
        </span>
      </span>

      <div class="flex items-center gap-2">
        {#if data.current && !isUnavailable(data.current)}
          <span class="text-xs text-muted-foreground">
            {data.current.orders.length} of {data.current.pagination.totalOrders}
          </span>
        {/if}

        <button
          class="rounded border bg-background px-2 py-1 text-xs hover:bg-accent"
          onclick={() => { void fetchOrders() }}
          disabled={loading.current}
        >
          {loading.current ? 'Loading...' : 'Refresh'}
        </button>
      </div>
    </Card.Title>
  </Card.Header>

  <Card.Content class="relative min-h-0 flex-1 overflow-auto px-4 pt-0">
    {#if error.current && data.current}
      <div class="mb-2 rounded border border-destructive bg-destructive/10 px-3 py-1.5 text-xs text-destructive">
        Refresh failed: {error.current}
      </div>
    {/if}

    {#if error.current && !data.current}
      <div class="flex h-full items-center justify-center text-destructive">
        Failed to load orders: {error.current}
      </div>
    {:else if !data.current && loading.current}
      <div class="flex h-full items-center justify-center text-muted-foreground">
        Loading...
      </div>
    {:else if data.current && isUnavailable(data.current)}
      <div class="flex h-full items-center justify-center text-muted-foreground">
        {data.current.reason}
      </div>
    {:else if data.current && !isUnavailable(data.current)}
      {#if data.current.orders.length === 0}
        <div class="flex h-full items-center justify-center text-muted-foreground">
          No active orders found for this owner.
        </div>
      {:else}
        <table class="w-full text-sm">
          <thead>
            <tr class="border-b text-left text-xs text-muted-foreground">
              <th class="pb-2 pr-4 font-medium">Output</th>
              <th class="pb-2 pr-4 font-medium">Input</th>
              <th class="pb-2 pr-4 text-right font-medium">Vault Balance</th>
              <th class="pb-2 pr-4 text-right font-medium">IO Ratio</th>
              <th class="pb-2 pr-4 font-medium">Order Hash</th>
              <th class="pb-2 font-medium">Created</th>
            </tr>
          </thead>

          <tbody>
            {#each data.current.orders as order (order.orderHash)}
              <tr class="border-b border-border/30 hover:bg-accent/30">
                <td class="py-2 pr-4 font-medium">{order.outputToken.symbol}</td>

                <td class="py-2 pr-4 font-medium">{order.inputToken.symbol}</td>

                <td class="py-2 pr-4 text-right font-mono">
                  {formatBalance(order.outputVaultBalance, order.outputToken.decimals)}
                </td>

                <td class="py-2 pr-4 text-right font-mono">
                  {order.ioRatio}
                </td>

                <td class="py-2 pr-4">
                  <span class="font-mono text-xs text-muted-foreground break-all">
                    {order.orderHash}
                  </span>
                </td>

                <td class="py-2 text-xs text-muted-foreground">
                  {formatTimestamp(order.createdAt)}
                </td>
              </tr>
            {/each}
          </tbody>
        </table>
      {/if}
    {:else}
      <div class="flex h-full items-center justify-center text-muted-foreground">
        No order data available.
      </div>
    {/if}
  </Card.Content>
</Card.Root>
