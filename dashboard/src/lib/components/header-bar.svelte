<script lang="ts">
  import { onMount } from 'svelte'
  import { Badge } from '$lib/components/ui/badge'
  import RecoveryGuide from '$lib/components/recovery-guide.svelte'
  import type { ConnectionState } from '$lib/websocket'
  import {
    getApiBaseUrl,
    getSimulateRev,
    getSimulateBackendPort,
    getSimulateSourceId,
    isDashboardMockMode
  } from '$lib/env'
  import { formatUtcClock, FETCH_TIMEOUT_MS } from '$lib/time'
  import { reactive } from '$lib/frp.svelte'

  const simulateRev = getSimulateRev()
  const simulateBackendPort = getSimulateBackendPort()
  const simulateSourceId = getSimulateSourceId()

  const simulateLabel = simulateRev
    ? `simulate · ${simulateRev}${
        simulateBackendPort ? `:${simulateBackendPort}` : ''
      }${simulateSourceId ? ` · ${simulateSourceId}` : ''}`
    : null

  const pageTitle = simulateLabel ? `${simulateLabel} · st0x.liquidity` : 'st0x.liquidity'

  const hashString = (input: string): number => {
    let hash = 0
    for (let i = 0; i < input.length; i++) {
      hash = (hash * 31 + input.charCodeAt(i)) | 0
    }
    return hash
  }

  const simulateHue =
    simulateSourceId !== null ? Math.abs(hashString(simulateSourceId)) % 360 : null

  const simulateBackground = simulateHue !== null ? `hsl(${String(simulateHue)} 70% 35%)` : null

  type Props = {
    connectionStatus: ConnectionState
    skipHealth?: boolean
    connectionLabel?: string | null
  }

  const { connectionStatus, skipHealth = false, connectionLabel = null }: Props = $props()

  type HealthInfo = {
    gitCommit: string
    uptimeSeconds: number
  }

  const health = reactive<HealthInfo | null>(null)
  const botReachable = reactive<boolean | null>(null)
  const now = reactive(new Date())

  const fetchHealth = async () => {
    if (skipHealth) {
      health.update(() => null)
      botReachable.update(() => null)
      return
    }

    if (isDashboardMockMode()) {
      health.update(() => ({ gitCommit: 'mock', uptimeSeconds: 0 }))
      botReachable.update(() => true)
      return
    }

    try {
      const baseUrl = getApiBaseUrl()
      const response = await fetch(`${baseUrl}/health`, {
        signal: AbortSignal.timeout(FETCH_TIMEOUT_MS)
      })

      if (!response.ok) {
        botReachable.update(() => false)
        return
      }

      const data = (await response.json()) as HealthInfo
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
    const healthInterval = setInterval(() => {
      void fetchHealth()
    }, 30_000)
    const clockInterval = setInterval(() => {
      now.update(() => new Date())
    }, 1000)
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
    connectionLabel !== null
      ? connectionLabel
      : connectionStatus === 'connected'
      ? 'Connected'
      : connectionStatus === 'connecting'
        ? 'Connecting...'
        : 'Disconnected'
  )
</script>

<svelte:head>
  <title>{pageTitle}</title>
</svelte:head>

<header
  class="shrink-0 border-b bg-card px-2 py-2 shadow-sm md:px-4 md:py-3"
  style:background-color={simulateBackground}
  style:color={simulateBackground !== null ? 'white' : null}
>
  <div class="flex items-center justify-between gap-2">
    <h1 class="text-sm font-semibold md:text-lg">
      st0x.liquidity
      {#if simulateLabel}
        <Badge variant="outline" class="ml-2 font-mono text-xs">
          {simulateLabel}
        </Badge>
      {/if}
    </h1>

    <div class="flex items-center gap-2 md:gap-4">
      <RecoveryGuide />

      <span class="font-mono text-xs text-muted-foreground">
        {formatUtcClock(now.current)}
      </span>

      {#if health.current && botReachable.current}
        <span class="hidden text-xs text-muted-foreground md:inline-flex md:items-center md:gap-3">
          {#if health.current.gitCommit !== 'dev'}
            <span title="Deployed commit"
              ><span class="font-mono">{health.current.gitCommit.slice(0, 7)}</span></span
            >
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
