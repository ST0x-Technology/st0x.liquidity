<script lang="ts">
  import { onMount } from 'svelte'
  import { Badge } from '$lib/components/ui/badge'
  import type { ConnectionState } from '$lib/websocket'
  import { getApiBaseUrl } from '$lib/env'
  import { formatUtcClock, FETCH_TIMEOUT_MS } from '$lib/time'
  import { reactive } from '$lib/frp.svelte'

  type Props = {
    connectionStatus: ConnectionState
  }

  const { connectionStatus }: Props = $props()

  type HealthInfo = {
    gitCommit: string
    uptimeSeconds: number
  }

  const health = reactive<HealthInfo | null>(null)
  const botReachable = reactive<boolean | null>(null)
  const now = reactive(new Date())

  const fetchHealth = async () => {
    try {
      const baseUrl = getApiBaseUrl()
      const response = await fetch(`${baseUrl}/health`, {
        signal: AbortSignal.timeout(FETCH_TIMEOUT_MS)
      })

      if (!response.ok) {
        botReachable.update(() => false)
        return
      }

      const data = await response.json() as HealthInfo
      health.update(() => data)
      botReachable.update(() => true)
    } catch {
      botReachable.update(() => false)
    }
  }

  const formatUptime = (seconds: number): string => {
    const days = Math.floor(seconds / 86400)
    const hours = Math.floor((seconds % 86400) / 3600)
    const minutes = Math.floor((seconds % 3600) / 60)

    if (days > 0) return `${String(days)}d ${String(hours)}h`
    if (hours > 0) return `${String(hours)}h ${String(minutes)}m`
    return `${String(minutes)}m`
  }

  onMount(() => {
    void fetchHealth()
    const healthInterval = setInterval(() => { void fetchHealth() }, 30_000)
    const clockInterval = setInterval(() => { now.update(() => new Date()) }, 1000)
    return () => {
      clearInterval(healthInterval)
      clearInterval(clockInterval)
    }
  })

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
</script>

<header class="shrink-0 border-b bg-card px-2 py-2 shadow-sm md:px-4 md:py-3">
  <div class="flex items-center justify-between gap-2">
    <h1 class="text-sm font-semibold md:text-lg">st0x.liquidity</h1>

    <div class="flex items-center gap-2 md:gap-4">
      <span class="font-mono text-xs text-muted-foreground">
        {formatUtcClock(now.current)}
      </span>

      {#if health.current && botReachable.current}
        <span class="hidden text-xs text-muted-foreground md:inline-flex md:items-center md:gap-3">
          {#if health.current.gitCommit !== 'dev'}
            <span title="Deployed commit"><span class="font-mono">{health.current.gitCommit.slice(0, 7)}</span></span>
          {/if}
          <span title="Bot uptime">up {formatUptime(health.current.uptimeSeconds)}</span>
        </span>
      {:else if botReachable.current === false}
        <span class="text-xs text-destructive">Bot offline</span>
      {/if}

      <Badge variant={statusVariant}>
        {statusLabel}
      </Badge>
    </div>
  </div>
</header>
