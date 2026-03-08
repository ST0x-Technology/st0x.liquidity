<script lang="ts">
  import { useQueryClient } from '@tanstack/svelte-query'
  import { onMount } from 'svelte'
  import HeaderBar from '$lib/components/header-bar.svelte'
  import InventoryPanel from '$lib/components/inventory-panel.svelte'
  import LiveEventsPanel from '$lib/components/live-events-panel.svelte'
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
  <HeaderBar
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
    <div class="grid h-full grid-cols-1 gap-2 md:grid-cols-[3fr_2fr] md:gap-4">
      <InventoryPanel />
      <LiveEventsPanel />
    </div>
  </main>
</div>
