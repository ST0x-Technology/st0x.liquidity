<script lang="ts">
  import { useQueryClient } from '@tanstack/svelte-query'
  import { onMount } from 'svelte'
  import HeaderBar from '$lib/components/header-bar.svelte'
  import SettingsBar from '$lib/components/settings-bar.svelte'
  import InventoryPanel from '$lib/components/inventory-panel.svelte'
  import PendingOrders from '$lib/components/pending-orders.svelte'
  import TradeHistoryPanel from '$lib/components/trade-history-panel.svelte'
  import TransferPanel from '$lib/components/transfer-panel.svelte'
  import LogPanel from '$lib/components/log-panel.svelte'
  import { getWebSocketUrl } from '$lib/env'
  import { reactive } from '$lib/frp.svelte'
  import { createWebSocket, type WebSocketConnection } from '$lib/websocket'

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

  let countdown = $state(0)

  $effect(() => {
    if (!errorContext) {
      countdown = 0
      return
    }

    countdown = Math.round(errorContext.nextRetryMs / 1000)
    const interval = setInterval(() => {
      countdown = Math.max(0, countdown - 1)
    }, 1000)

    return () => { clearInterval(interval); }
  })

  type Tab = 'dashboard' | 'logs'

  const activeTab = reactive<Tab>('dashboard')

  const tabClass = (active: boolean): string =>
    `relative px-4 py-2.5 text-sm font-medium transition-colors ${active ? 'text-foreground after:absolute after:bottom-0 after:left-0 after:right-0 after:h-0.5 after:bg-primary' : 'text-muted-foreground hover:text-foreground'}`
</script>

<div class="flex h-screen flex-col bg-background">
  <HeaderBar
    connectionStatus={connectionState === 'error' ? 'disconnected' : connectionState}
  />

  {#if errorContext}
    <div
      class="mx-2 mt-2 rounded-md border border-destructive bg-destructive/10
        px-4 py-2 text-sm text-destructive md:mx-4"
    >
      Connection error. Reconnecting in {countdown}s
      (attempt {errorContext.attempts})...
    </div>
  {/if}

  <nav class="flex shrink-0 gap-1 border-b bg-card/50 px-2 md:px-4">
    <button
      class={tabClass(activeTab.current === 'dashboard')}
      onclick={() => activeTab.update(() => 'dashboard')}
    >
      Dashboard
    </button>

    <button
      class={tabClass(activeTab.current === 'logs')}
      onclick={() => activeTab.update(() => 'logs')}
    >
      Logs
    </button>
  </nav>

  <SettingsBar />

  {#if activeTab.current === 'dashboard'}
    <main class="flex min-h-0 flex-1 flex-col gap-4 overflow-auto p-2 md:p-4">
      <div class="grid min-h-[280px] gap-4 md:h-[40%] md:min-h-0 md:grid-cols-[2fr_1fr]">
        <InventoryPanel />
        <PendingOrders />
      </div>

      <div class="grid min-h-[280px] flex-1 gap-4 md:min-h-0 md:grid-cols-2">
        <TradeHistoryPanel />
        <TransferPanel />
      </div>
    </main>
  {:else}
    <main class="flex-1 overflow-hidden p-2 md:p-4">
      <LogPanel />
    </main>
  {/if}
</div>
