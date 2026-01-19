<script lang="ts">
  import * as Select from '$lib/components/ui/select'
  import { Badge } from '$lib/components/ui/badge'
  import { Separator } from '$lib/components/ui/separator'
  import type { Broker } from '$lib/env'
  import type { ConnectionState } from '$lib/websocket.svelte'

  type Props = {
    broker: Broker
    onBrokerChange: (broker: Broker) => void
    connectionStatus: ConnectionState
  }

  const { broker, onBrokerChange, connectionStatus }: Props = $props()

  const statusVariant = $derived(
    connectionStatus === 'connected'
      ? 'default'
      : connectionStatus === 'connecting'
        ? 'secondary'
        : 'destructive'
  )

  const statusLabel = $derived(
    connectionStatus === 'connected'
      ? 'Connected'
      : connectionStatus === 'connecting'
        ? 'Connecting...'
        : 'Disconnected'
  )

  const handleBrokerChange = (value: string | undefined) => {
    if (value === 'schwab' || value === 'alpaca') {
      onBrokerChange(value)
    }
  }
</script>

<header class="shrink-0 border-b bg-background px-2 py-2 md:px-4 md:py-3">
  <div class="flex items-center justify-between gap-2">
    <div class="flex items-center gap-2 md:gap-4">
      <h1 class="text-sm font-semibold md:text-lg">st0x.liquidity</h1>

      <Separator orientation="vertical" class="hidden h-6 md:block" />

      <Select.Root type="single" value={broker} onValueChange={handleBrokerChange}>
        <Select.Trigger class="w-[90px]">
          {broker === 'schwab' ? 'Schwab' : 'Alpaca'}
        </Select.Trigger>
        <Select.Content>
          <Select.Item value="schwab">Schwab</Select.Item>
          <Select.Item value="alpaca">Alpaca</Select.Item>
        </Select.Content>
      </Select.Root>
    </div>

    <div class="flex items-center gap-2 md:gap-4">
      <!-- TODO: Circuit Breaker button - implement with issues #178-184 -->

      <Badge variant={statusVariant}>
        {statusLabel}
      </Badge>
    </div>
  </div>
</header>
