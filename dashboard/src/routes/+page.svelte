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
  import OrdersPanel from '$lib/components/orders-panel.svelte'
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

  type Tab = 'dashboard' | 'orders' | 'logs'
  type MobilePanel = 'inventory' | 'pending' | 'trades' | 'transfers'

  const activeTab = reactive<Tab>('dashboard')
  const mobilePanel = reactive<MobilePanel>('inventory')

  const mobilePanelClass = (panel: MobilePanel): string =>
    `relative whitespace-nowrap px-3 py-2 text-sm font-medium transition-colors ${mobilePanel.current === panel ? 'text-foreground after:absolute after:bottom-0 after:left-0 after:right-0 after:h-0.5 after:bg-primary' : 'text-muted-foreground hover:text-foreground'}`

  const desktopTabClass = (active: boolean): string =>
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

  <!-- Mobile nav: panel tabs + logs -->
  <nav class="flex shrink-0 gap-1 overflow-x-auto border-b bg-card/50 px-2 md:hidden">
    {#if activeTab.current === 'dashboard'}
      <button class={mobilePanelClass('inventory')} onclick={() => mobilePanel.update(() => 'inventory')}>Inventory</button>
      <button class={mobilePanelClass('pending')} onclick={() => mobilePanel.update(() => 'pending')}>Pending</button>
      <button class={mobilePanelClass('trades')} onclick={() => mobilePanel.update(() => 'trades')}>Trades</button>
      <button class={mobilePanelClass('transfers')} onclick={() => mobilePanel.update(() => 'transfers')}>Transfers</button>
    {/if}
    <button
      class="relative whitespace-nowrap px-3 py-2 text-sm font-medium transition-colors {activeTab.current === 'orders' ? 'text-foreground after:absolute after:bottom-0 after:left-0 after:right-0 after:h-0.5 after:bg-primary' : 'text-muted-foreground hover:text-foreground'}"
      onclick={() => activeTab.update((tab) => tab === 'orders' ? 'dashboard' : 'orders')}
    >
      Orders
    </button>
    <button
      class="relative whitespace-nowrap px-3 py-2 text-sm font-medium transition-colors {activeTab.current === 'logs' ? 'text-foreground after:absolute after:bottom-0 after:left-0 after:right-0 after:h-0.5 after:bg-primary' : 'text-muted-foreground hover:text-foreground'}"
      onclick={() => activeTab.update((tab) => tab === 'logs' ? 'dashboard' : 'logs')}
    >
      Logs
    </button>
  </nav>

  <!-- Desktop nav: dashboard vs logs -->
  <nav class="hidden shrink-0 gap-1 border-b bg-card/50 px-4 md:flex">
    <button
      class={desktopTabClass(activeTab.current === 'dashboard')}
      onclick={() => activeTab.update(() => 'dashboard')}
    >
      Dashboard
    </button>

    <button
      class={desktopTabClass(activeTab.current === 'orders')}
      onclick={() => activeTab.update(() => 'orders')}
    >
      Orders
    </button>

    <button
      class={desktopTabClass(activeTab.current === 'logs')}
      onclick={() => activeTab.update(() => 'logs')}
    >
      Logs
    </button>
  </nav>

  <SettingsBar />

  {#if activeTab.current === 'dashboard'}
    <!-- Mobile: one panel at a time -->
    <main class="flex-1 overflow-hidden p-2 md:hidden">
      {#if mobilePanel.current === 'inventory'}
        <InventoryPanel />
      {:else if mobilePanel.current === 'pending'}
        <PendingOrders />
      {:else if mobilePanel.current === 'trades'}
        <TradeHistoryPanel />
      {:else}
        <TransferPanel />
      {/if}
    </main>

    <!-- Desktop: all panels, fixed proportions, internal scrolling -->
    <main class="hidden min-h-0 flex-1 flex-col gap-4 overflow-hidden p-4 md:flex">
      <div class="grid h-[40%] min-h-0 grid-cols-[2fr_1fr] gap-4">
        <InventoryPanel />
        <PendingOrders />
      </div>

      <div class="grid min-h-0 flex-1 grid-cols-2 gap-4">
        <TradeHistoryPanel />
        <TransferPanel />
      </div>
    </main>
  {:else if activeTab.current === 'orders'}
    <main class="flex-1 overflow-hidden p-2 md:p-4">
      <OrdersPanel />
    </main>
  {:else}
    <main class="flex-1 overflow-hidden p-2 md:p-4">
      <LogPanel />
    </main>
  {/if}
</div>
