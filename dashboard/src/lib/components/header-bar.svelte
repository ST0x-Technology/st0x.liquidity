<script lang="ts">
  import { Badge } from '$lib/components/ui/badge'
  import type { ConnectionState } from '$lib/websocket.svelte'

  type Props = {
    connectionStatus: ConnectionState
  }

  const { connectionStatus }: Props = $props()

  const statusVariant = $derived(
    connectionStatus === 'connected'
      ? 'default'
      : connectionStatus === 'connecting'
        ? 'secondary'
        : 'destructive'
  )

  const statusLabel = $derived(
    connectionStatus === 'connected'
      ? 'Live'
      : connectionStatus === 'connecting'
        ? 'Connecting...'
        : 'Disconnected'
  )
</script>

<header class="shrink-0 border-b bg-background px-2 py-2 md:px-4 md:py-3">
  <div class="flex items-center justify-between gap-2">
    <h1 class="text-sm font-semibold md:text-lg">st0x.liquidity</h1>

    <div class="flex items-center gap-2 md:gap-4">
      <!-- TODO: Circuit Breaker toggle — issue #183 -->

      <Badge variant={statusVariant}>
        {statusLabel}
      </Badge>
    </div>
  </div>
</header>
