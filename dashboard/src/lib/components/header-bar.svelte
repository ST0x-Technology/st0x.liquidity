<script lang="ts">
  import * as Select from '$lib/components/ui/select'
  import { Badge } from '$lib/components/ui/badge'
  import { Button } from '$lib/components/ui/button'
  import { Separator } from '$lib/components/ui/separator'
  import type { Broker } from '$lib/env'
  import type { ConnectionStatus } from '$lib/websocket.svelte'

  type Props = {
    broker: Broker
    onBrokerChange: (broker: Broker) => void
    connectionStatus: ConnectionStatus
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

<header class="border-b bg-background px-4 py-3">
  <div class="flex items-center justify-between">
    <div class="flex items-center gap-4">
      <h1 class="text-lg font-semibold">st0x.liquidity</h1>

      <Separator orientation="vertical" class="h-6" />

      <Select.Root type="single" value={broker} onValueChange={handleBrokerChange}>
        <Select.Trigger class="w-32">
          {broker === 'schwab' ? 'Schwab' : 'Alpaca'}
        </Select.Trigger>
        <Select.Content>
          <Select.Item value="schwab">Schwab</Select.Item>
          <Select.Item value="alpaca">Alpaca</Select.Item>
        </Select.Content>
      </Select.Root>
    </div>

    <div class="flex items-center gap-4">
      <Button variant="outline" disabled>
        Circuit Breaker
      </Button>

      <Badge variant={statusVariant}>
        {statusLabel}
      </Badge>
    </div>
  </div>
</header>
