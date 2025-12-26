<script lang="ts">
  import { useQueryClient } from '@tanstack/svelte-query'
  import { onMount } from 'svelte'
  import HeaderBar from '$lib/components/header-bar.svelte'
  import PlaceholderPanel from '$lib/components/placeholder-panel.svelte'
  import LiveEventsPanel from '$lib/components/live-events-panel.svelte'
  import { brokerStore } from '$lib/stores/broker.svelte'
  import { getWebSocketUrl, type Broker } from '$lib/env'
  import { reactive } from '$lib/frp.svelte'
  import { createWebSocket, type WebSocketConnection } from '$lib/websocket.svelte'

  const queryClient = useQueryClient()
  const ws = reactive<WebSocketConnection | null>(null)

  const handleBrokerChange = (broker: Broker) => {
    ws.current?.disconnect()
    queryClient.clear()
    brokerStore.set(broker)
    ws.update(() => createWebSocket(getWebSocketUrl(broker), queryClient))
    ws.current?.connect()
  }

  onMount(() => {
    ws.update(() => createWebSocket(getWebSocketUrl(brokerStore.value), queryClient))
    ws.current?.connect()

    return () => {
      ws.current?.disconnect()
    }
  })

  const connectionState = $derived(ws.current?.state ?? 'disconnected')
  const errorContext = $derived(ws.current?.error ?? null)
</script>

<div class="flex h-screen flex-col bg-background">
  <HeaderBar
    broker={brokerStore.value}
    onBrokerChange={handleBrokerChange}
    connectionStatus={connectionState === 'error' ? 'disconnected' : connectionState}
  />

  {#if errorContext}
    <div
      class="mx-2 mt-2 rounded-md border border-destructive bg-destructive/10
        px-4 py-2 text-sm text-destructive md:mx-4"
    >
      Connection error. Reconnecting in {Math.round(errorContext.nextRetryMs / 1000)}s
      (attempt {errorContext.attempts})...
    </div>
  {/if}

  <main class="flex-1 overflow-auto p-2 md:overflow-hidden md:p-4">
    <div
      class="grid h-full grid-cols-1 gap-2 md:grid-cols-2
        md:grid-rows-[1fr_1fr_1fr] md:gap-4 lg:grid-cols-3 lg:grid-rows-[1fr_1fr]"
    >
      <PlaceholderPanel title="Performance Metrics" />
      <PlaceholderPanel title="Trade Log" />
      <PlaceholderPanel title="Spreads" />
      <PlaceholderPanel title="Inventory" />
      <PlaceholderPanel title="Rebalancing" />
      <LiveEventsPanel />
    </div>
  </main>
</div>
