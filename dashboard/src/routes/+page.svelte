<script lang="ts">
  import { useQueryClient } from '@tanstack/svelte-query'
  import HeaderBar from '$lib/components/header-bar.svelte'
  import PlaceholderPanel from '$lib/components/placeholder-panel.svelte'
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

<div class="flex min-h-screen flex-col bg-background">
  <HeaderBar
    broker={brokerStore.value}
    onBrokerChange={handleBrokerChange}
    {connectionStatus}
  />

  <main class="flex-1 p-4">
    <div class="grid h-full grid-cols-3 grid-rows-2 gap-4">
      <PlaceholderPanel title="Performance Metrics" />
      <PlaceholderPanel title="Trade Log" />
      <PlaceholderPanel title="Spreads" />
      <PlaceholderPanel title="Inventory" />
      <PlaceholderPanel title="Rebalancing" />
      <PlaceholderPanel title="Live Events" />
    </div>
  </main>
</div>
