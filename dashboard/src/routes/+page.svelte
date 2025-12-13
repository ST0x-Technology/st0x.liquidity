<script lang="ts">
  import { useQueryClient } from '@tanstack/svelte-query'
  import HeaderBar from '$lib/components/header-bar.svelte'
  import PlaceholderPanel from '$lib/components/placeholder-panel.svelte'
  import LiveEventsPanel from '$lib/components/live-events-panel.svelte'
  import { brokerStore } from '$lib/stores/broker.svelte'
  import { getWebSocketUrl, type Broker } from '$lib/env'
  import { createWebSocket, type WebSocketConnection } from '$lib/websocket.svelte'
  import { onMount } from 'svelte'

  const queryClient = useQueryClient()

  let ws = $state<WebSocketConnection | null>(null)

  const handleBrokerChange = (broker: Broker) => {
    ws?.disconnect()
    queryClient.clear()
    brokerStore.set(broker)
    ws = createWebSocket(getWebSocketUrl(broker), queryClient)
    ws.connect()
  }

  onMount(() => {
    ws = createWebSocket(getWebSocketUrl(brokerStore.value), queryClient)
    ws.connect()
    return () => {
      ws?.disconnect()
    }
  })

  const connectionStatus = $derived(ws?.status ?? 'disconnected')
</script>

<div class="flex h-screen flex-col bg-background">
  <HeaderBar
    broker={brokerStore.value}
    onBrokerChange={handleBrokerChange}
    {connectionStatus}
  />

  <main class="flex-1 overflow-auto p-2 md:overflow-hidden md:p-4">
    <div class="grid h-full grid-cols-1 gap-2 md:grid-cols-2 md:grid-rows-[1fr_1fr_1fr] md:gap-4 lg:grid-cols-3 lg:grid-rows-[1fr_1fr]">
      <PlaceholderPanel title="Performance Metrics" />
      <PlaceholderPanel title="Trade Log" />
      <PlaceholderPanel title="Spreads" />
      <PlaceholderPanel title="Inventory" />
      <PlaceholderPanel title="Rebalancing" />
      <LiveEventsPanel />
    </div>
  </main>
</div>
