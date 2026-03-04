<script lang="ts">
  import { useQueryClient } from '@tanstack/svelte-query'
  import { onMount } from 'svelte'
  import HeaderBar from '$lib/components/header-bar.svelte'
  import KpiStrip from '$lib/components/kpi-strip.svelte'
  import SpreadsPanel from '$lib/components/spreads-panel.svelte'
  import TradeLogPanel from '$lib/components/trade-log-panel.svelte'
  import InventoryPanel from '$lib/components/inventory-panel.svelte'
  import RebalancingPanel from '$lib/components/rebalancing-panel.svelte'
  import { getWebSocketUrl } from '$lib/env'
  import { reactive } from '$lib/frp.svelte'
  import { createWebSocket, type WebSocketConnection } from '$lib/websocket.svelte'

  const queryClient = useQueryClient()
  const ws = reactive<WebSocketConnection | null>(null)

  onMount(() => {
    ws.update(() => createWebSocket(getWebSocketUrl(), queryClient))
    ws.current?.connect()

    return () => {
      ws.current?.disconnect()
    }
  })

  const connectionState = $derived(ws.current?.state ?? 'disconnected')
  const errorContext = $derived(ws.current?.error ?? null)
</script>

<div class="flex h-screen flex-col bg-background">
  <HeaderBar connectionStatus={connectionState === 'error' ? 'disconnected' : connectionState} />

  {#if errorContext}
    <div
      class="mx-2 mt-2 rounded-md border border-destructive bg-destructive/10
        px-4 py-2 text-sm text-destructive md:mx-4"
    >
      Connection error. Reconnecting in {Math.round(errorContext.nextRetryMs / 1000)}s (attempt {errorContext.attempts})...
    </div>
  {/if}

  <!-- TODO: visual polish before review
    - fix inconsistent spacing/padding between panels
    - center empty state text ("No spread data available", etc.)
    - fix inventory table spacing
    - match SPEC.md layout proportions more closely
    - add mobile viewport baseline screenshots for visual regression
  -->
  <main class="flex-1 overflow-auto p-2 md:overflow-hidden md:p-4">
    <div class="flex h-full flex-col gap-2 md:gap-4">
      <KpiStrip />

      <div class="grid min-h-0 flex-1 grid-cols-1 gap-2 md:grid-cols-[2fr_1fr] md:gap-4">
        <SpreadsPanel />
        <TradeLogPanel />
      </div>

      <div class="grid min-h-0 flex-1 grid-cols-1 gap-2 md:grid-cols-2 md:gap-4">
        <InventoryPanel />
        <RebalancingPanel />
      </div>
    </div>
  </main>
</div>
