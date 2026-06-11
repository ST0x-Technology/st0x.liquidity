<script lang="ts">
  import { onMount } from 'svelte'
  import * as Card from '$lib/components/ui/card'
  import type { HedgeLatencies } from '$lib/api/HedgeLatencies'
  import type { ReliabilityReport } from '$lib/api/ReliabilityReport'
  import { reactive } from '$lib/frp.svelte'
  import { fetchHedgeLatencies, fetchReliabilityReport } from '$lib/performance/api'
  import {
    detectionCard,
    errorsCard,
    exposureCard,
    openExposureCard,
  } from '$lib/performance/cards'
  import { type SloStatus } from '$lib/performance/slo'

  const POLL_INTERVAL_MS = 30_000
  const CARD_WINDOW_HOURS = 24

  const latencies = reactive<HedgeLatencies | null>(null)
  const reliability = reactive<ReliabilityReport | null>(null)
  const error = reactive<string | null>(null)
  const lastRefreshed = reactive<Date | null>(null)

  const refresh = async () => {
    const to = new Date()
    const from = new Date(to.getTime() - CARD_WINDOW_HOURS * 3_600_000)

    try {
      const [latencyReport, reliabilityReport] = await Promise.all([
        fetchHedgeLatencies({ from, to }),
        fetchReliabilityReport({ from, to }),
      ])
      latencies.update(() => latencyReport)
      reliability.update(() => reliabilityReport)
      lastRefreshed.update(() => to)
      error.update(() => null)
    } catch (fetchError) {
      error.update(() =>
        fetchError instanceof Error ? fetchError.message : 'Unknown error',
      )
    }
  }

  onMount(() => {
    void refresh()
    const interval = setInterval(() => {
      void refresh()
    }, POLL_INTERVAL_MS)

    return () => {
      clearInterval(interval)
    }
  })

  const cards = $derived([
    detectionCard(latencies.current),
    exposureCard(latencies.current),
    errorsCard(reliability.current, CARD_WINDOW_HOURS),
    openExposureCard(latencies.current, lastRefreshed.current),
  ])

  const statusClasses = (status: SloStatus): string => {
    if (status === 'unknown') {
      return 'border-l-slate-500/70'
    }

    if (status === 'good') {
      return 'border-l-emerald-500/70'
    }

    if (status === 'warning') {
      return 'border-l-amber-500/70'
    }

    return 'border-l-red-500/70'
  }

  const statusDot = (status: SloStatus): string => {
    if (status === 'unknown') {
      return 'bg-slate-500'
    }

    if (status === 'good') {
      return 'bg-emerald-500'
    }

    if (status === 'warning') {
      return 'bg-amber-500'
    }

    return 'bg-red-500'
  }
</script>

<div class="flex h-full flex-col gap-4 overflow-y-auto">
  {#if error.current}
    <div
      class="rounded-md border border-destructive bg-destructive/10 px-4 py-2
        text-sm text-destructive"
    >
      Failed to load performance data: {error.current}
    </div>
  {/if}

  <section>
    <div class="grid grid-cols-1 gap-3 sm:grid-cols-2 xl:grid-cols-4">
      {#each cards as card (card.title)}
        <Card.Root class={`border-l-4 ${statusClasses(card.status)}`}>
          <Card.Header class="pb-1">
            <Card.Title
              class="flex items-center gap-2 text-sm font-medium
                text-muted-foreground"
            >
              <span class={`h-2 w-2 rounded-full ${statusDot(card.status)}`}></span>
              {card.title}
            </Card.Title>
          </Card.Header>
          <Card.Content>
            <div class="text-2xl font-semibold">{card.primary}</div>
            <div class="text-xs text-muted-foreground">{card.secondary}</div>
          </Card.Content>
        </Card.Root>
      {/each}
    </div>
    <p class="mt-2 text-xs text-muted-foreground">
      Last {CARD_WINDOW_HOURS}h ·
      {#if lastRefreshed.current}
        refreshed {lastRefreshed.current.toLocaleTimeString()}
      {:else}
        loading…
      {/if}
    </p>
  </section>
</div>
