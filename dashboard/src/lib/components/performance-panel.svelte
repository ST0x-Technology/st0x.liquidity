<script lang="ts">
  import { onMount } from 'svelte'
  import * as Card from '$lib/components/ui/card'
  import type { HedgeLatencies } from '$lib/api/HedgeLatencies'
  import type { InfraReport } from '$lib/api/InfraReport'
  import type { RebalanceStageName } from '$lib/api/RebalanceStageName'
  import type { RebalanceTimings } from '$lib/api/RebalanceTimings'
  import type { ReliabilityReport } from '$lib/api/ReliabilityReport'
  import type { StageLatencies } from '$lib/api/StageLatencies'
  import type { UsdcBridgeDirection } from '$lib/api/UsdcBridgeDirection'
  import { reactive } from '$lib/frp.svelte'
  import {
    fetchHedgeLatencies,
    fetchInfraReport,
    fetchRebalanceTimings,
    fetchReliabilityReport,
  } from '$lib/performance/api'
  import {
    blockLagCard,
    detectionCard,
    errorsCard,
    exposureCard,
    openExposureCard,
  } from '$lib/performance/cards'
  import {
    type WaterfallSegmentName,
    type WaterfallSort,
    layoutAttestationTrend,
    layoutBlockLagTrend,
    layoutPercentileSeries,
    layoutRebalanceBars,
    layoutWaterfall,
  } from '$lib/performance/charts'
  import { type SloStatus, formatDurationMs } from '$lib/performance/slo'

  const { onOpenLogs }: { onOpenLogs?: (target: string) => void } = $props()

  const POLL_INTERVAL_MS = 30_000
  const CARD_WINDOW_HOURS = 24

  const latencies = reactive<HedgeLatencies | null>(null)
  const reliability = reactive<ReliabilityReport | null>(null)
  const infra = reactive<InfraReport | null>(null)
  const error = reactive<string | null>(null)
  const lastRefreshed = reactive<Date | null>(null)

  // Discard out-of-date responses: a slow earlier fetch resolving after a
  // newer one must not overwrite fresher data (same pattern as pnl-panel).
  let cardFetchSeq = 0

  const refresh = async () => {
    const seq = ++cardFetchSeq
    const to = new Date()
    const from = new Date(to.getTime() - CARD_WINDOW_HOURS * 3_600_000)

    try {
      const [latencyReport, reliabilityReport, infraReport] = await Promise.all([
        fetchHedgeLatencies({ from, to }),
        fetchReliabilityReport({ from, to }),
        fetchInfraReport({ from, to }),
      ])
      if (seq !== cardFetchSeq) return
      latencies.update(() => latencyReport)
      reliability.update(() => reliabilityReport)
      infra.update(() => infraReport)
      lastRefreshed.update(() => to)
      error.update(() => null)
    } catch (fetchError) {
      if (seq !== cardFetchSeq) return
      error.update(() =>
        fetchError instanceof Error ? fetchError.message : 'Unknown error',
      )
    }
  }

  type RangePreset = '1W' | '1M' | 'YTD' | '1Y' | 'ALL'

  const RANGE_PRESETS: RangePreset[] = ['1W', '1M', 'YTD', '1Y', 'ALL']

  /** Pre-dates the system's first deployment, so ALL covers everything. */
  const ALL_TIME_START = new Date('2025-01-01T00:00:00Z')

  const rangeStart = (preset: RangePreset, now: Date): Date => {
    if (preset === '1W') {
      return new Date(now.getTime() - 7 * 86_400_000)
    }

    if (preset === '1M') {
      return new Date(now.getTime() - 31 * 86_400_000)
    }

    if (preset === 'YTD') {
      return new Date(Date.UTC(now.getUTCFullYear(), 0, 1))
    }

    if (preset === '1Y') {
      return new Date(now.getTime() - 365 * 86_400_000)
    }

    return ALL_TIME_START
  }

  const chartRange = reactive<RangePreset>('1W')
  const chartLatencies = reactive<HedgeLatencies | null>(null)
  const chartRebalances = reactive<RebalanceTimings | null>(null)
  const chartInfra = reactive<InfraReport | null>(null)
  const chartError = reactive<string | null>(null)

  let chartFetchSeq = 0

  const refreshCharts = async () => {
    const seq = ++chartFetchSeq
    const to = new Date()
    const range = {
      from: rangeStart(chartRange.current, to),
      to,
    }

    try {
      const [latencyReport, rebalanceReport, infraReport] = await Promise.all([
        fetchHedgeLatencies(range),
        fetchRebalanceTimings(range),
        fetchInfraReport(range),
      ])
      if (seq !== chartFetchSeq) return
      chartLatencies.update(() => latencyReport)
      chartRebalances.update(() => rebalanceReport)
      chartInfra.update(() => infraReport)
      chartError.update(() => null)
    } catch (fetchError) {
      if (seq !== chartFetchSeq) return
      chartError.update(() =>
        fetchError instanceof Error ? fetchError.message : 'Unknown error',
      )
    }
  }

  const selectRange = (preset: RangePreset) => {
    chartRange.update(() => preset)
    void refreshCharts()
  }

  onMount(() => {
    void refresh()
    void refreshCharts()
    const interval = setInterval(() => {
      void refresh()
      void refreshCharts()
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
    blockLagCard(infra.current, lastRefreshed.current),
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

  const WATERFALL_PLOT_WIDTH = 600
  const WATERFALL_MAX_ROWS = 20

  const waterfallSort = reactive<WaterfallSort>('slowest')

  const SEGMENT_COLORS: Record<WaterfallSegmentName, string> = {
    unhedged: '#64748b',
    submission: '#38bdf8',
    execution: '#34d399',
  }

  const SEGMENT_LABELS: Record<WaterfallSegmentName, string> = {
    unhedged: 'unhedged (fill -> placed)',
    submission: 'submission (placed -> accepted)',
    execution: 'execution (accepted -> filled)',
  }

  const segmentColor = (
    name: WaterfallSegmentName,
    status: string,
  ): string => (name === 'execution' && status === 'failed' ? '#f87171' : SEGMENT_COLORS[name])

  const waterfallRows = $derived(
    layoutWaterfall(chartLatencies.current?.cycles ?? [], {
      plotWidth: WATERFALL_PLOT_WIDTH,
      sort: waterfallSort.current,
      maxRows: WATERFALL_MAX_ROWS,
      now: lastRefreshed.current ?? new Date(),
    }),
  )

  type StageKey = keyof StageLatencies

  const STAGE_OPTIONS: { key: StageKey; label: string }[] = [
    { key: 'exposureWindow', label: 'Exposure window' },
    { key: 'detection', label: 'Detection' },
    { key: 'decision', label: 'Decision' },
    { key: 'submission', label: 'Submission' },
    { key: 'execution', label: 'Execution' },
  ]

  const SERIES_PLOT_WIDTH = 600
  const SERIES_PLOT_HEIGHT = 160

  const PERCENTILE_COLORS = {
    p50Ms: '#34d399',
    p90Ms: '#fbbf24',
    p99Ms: '#f87171',
  } as const

  const selectedStage = reactive<StageKey>('exposureWindow')

  const seriesLayout = $derived(
    layoutPercentileSeries(chartLatencies.current?.buckets ?? [], selectedStage.current, {
      plotWidth: SERIES_PLOT_WIDTH,
      plotHeight: SERIES_PLOT_HEIGHT,
      maxXLabels: 8,
    }),
  )

  const rangeButtonClass = (active: boolean): string =>
    `rounded px-2 py-1 text-xs font-medium transition-colors ${active ? 'bg-primary text-primary-foreground' : 'bg-muted text-muted-foreground hover:text-foreground'}`

  const REBALANCE_PLOT_WIDTH = 600
  const REBALANCE_MAX_ROWS = 15
  const TREND_PLOT_WIDTH = 600
  const TREND_PLOT_HEIGHT = 80

  const STAGE_COLORS: Record<RebalanceStageName, string> = {
    conversion: '#c084fc',
    withdrawal: '#38bdf8',
    burn: '#fb923c',
    attestation: '#fbbf24',
    mint: '#34d399',
    deposit: '#818cf8',
  }

  const rebalanceRows = $derived(
    layoutRebalanceBars(chartRebalances.current?.operations ?? [], {
      plotWidth: REBALANCE_PLOT_WIDTH,
      maxRows: REBALANCE_MAX_ROWS,
    }),
  )

  const attestationTrend = $derived(
    layoutAttestationTrend(chartRebalances.current?.attestationTrend ?? [], {
      plotWidth: TREND_PLOT_WIDTH,
      plotHeight: TREND_PLOT_HEIGHT,
    }),
  )

  const blockLagTrend = $derived(
    layoutBlockLagTrend(chartInfra.current?.monitor.blockLag ?? [], {
      plotWidth: TREND_PLOT_WIDTH,
      plotHeight: TREND_PLOT_HEIGHT,
    }),
  )

  const directionLabel = (direction: UsdcBridgeDirection | null): string => {
    if (direction === 'alpaca_to_base') {
      return 'Alpaca → Base'
    }

    if (direction === 'base_to_alpaca') {
      return 'Base → Alpaca'
    }

    return '—'
  }

  const sparklineMax = (sparkline: number[]): number =>
    Math.max(1, ...sparkline)
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
    <div class="grid grid-cols-1 gap-3 sm:grid-cols-2 xl:grid-cols-5">
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

  <section class="flex items-center gap-1">
    {#each RANGE_PRESETS as preset (preset)}
      <button
        class={rangeButtonClass(chartRange.current === preset)}
        onclick={() => {
          selectRange(preset)
        }}
      >
        {preset}
      </button>
    {/each}
    {#if chartError.current}
      <span class="ml-2 text-xs text-destructive">
        Failed to load charts: {chartError.current}
      </span>
    {/if}
  </section>

  <Card.Root>
    <Card.Header class="pb-2">
      <div class="flex items-center justify-between">
        <Card.Title class="text-sm font-medium">Hedge cycle waterfall</Card.Title>
        <div class="flex gap-1">
          <button
            class={rangeButtonClass(waterfallSort.current === 'slowest')}
            onclick={() => {
              waterfallSort.update(() => 'slowest')
            }}
          >
            Slowest
          </button>
          <button
            class={rangeButtonClass(waterfallSort.current === 'newest')}
            onclick={() => {
              waterfallSort.update(() => 'newest')
            }}
          >
            Newest
          </button>
        </div>
      </div>
      <div class="flex flex-wrap gap-3 text-xs text-muted-foreground">
        {#each Object.entries(SEGMENT_LABELS) as [name, label] (name)}
          <span class="flex items-center gap-1">
            <span
              class="inline-block h-2 w-2 rounded-sm"
              style:background={SEGMENT_COLORS[name as WaterfallSegmentName]}
            ></span>
            {label}
          </span>
        {/each}
      </div>
    </Card.Header>
    <Card.Content>
      {#if waterfallRows.length === 0}
        <p class="py-6 text-center text-sm text-muted-foreground">
          No hedge cycles in the selected range.
        </p>
      {:else}
        <div class="flex flex-col gap-1">
          {#each waterfallRows as row (row.id)}
            <div class="flex items-center gap-2 text-xs">
              <span class="w-14 shrink-0 font-medium">{row.symbol}</span>
              <svg
                viewBox={`0 0 ${String(WATERFALL_PLOT_WIDTH)} 14`}
                class="h-3.5 min-w-0 flex-1"
                preserveAspectRatio="none"
              >
                {#each row.segments as segment (segment.name)}
                  <rect
                    x={segment.x}
                    y="2"
                    width={Math.max(segment.width, segment.ms > 0 ? 1 : 0)}
                    height="10"
                    rx="1"
                    fill={segmentColor(segment.name, row.status)}
                  >
                    <title>
                      {SEGMENT_LABELS[segment.name]}: {formatDurationMs(segment.ms)}
                    </title>
                  </rect>
                {/each}
              </svg>
              <span class="w-16 shrink-0 text-right tabular-nums text-muted-foreground">
                {formatDurationMs(row.totalMs)}
              </span>
            </div>
          {/each}
        </div>
        {#if (chartLatencies.current?.totalCycles ?? 0) > waterfallRows.length}
          <p class="mt-2 text-xs text-muted-foreground">
            Showing {waterfallRows.length} of {chartLatencies.current?.totalCycles} cycles
            in range.
          </p>
        {/if}
      {/if}
    </Card.Content>
  </Card.Root>

  <Card.Root>
    <Card.Header class="pb-2">
      <div class="flex items-center justify-between">
        <Card.Title class="text-sm font-medium">Latency percentiles over time</Card.Title>
        <div class="flex gap-1">
          {#each STAGE_OPTIONS as option (option.key)}
            <button
              class={rangeButtonClass(selectedStage.current === option.key)}
              onclick={() => {
                selectedStage.update(() => option.key)
              }}
            >
              {option.label}
            </button>
          {/each}
        </div>
      </div>
      <div class="flex gap-3 text-xs text-muted-foreground">
        {#each Object.entries(PERCENTILE_COLORS) as [percentile, color] (percentile)}
          <span class="flex items-center gap-1">
            <span class="inline-block h-0.5 w-3" style:background={color}></span>
            {percentile.replace('Ms', '')}
          </span>
        {/each}
      </div>
    </Card.Header>
    <Card.Content>
      {#if (seriesLayout.lines[0]?.points.length ?? 0) === 0}
        <p class="py-6 text-center text-sm text-muted-foreground">
          No samples for this stage in the selected range.
        </p>
      {:else}
        <div class="flex items-start gap-2">
          <span class="shrink-0 text-xs tabular-nums text-muted-foreground">
            {formatDurationMs(seriesLayout.maxMs)}
          </span>
          <svg
            viewBox={`0 0 ${String(SERIES_PLOT_WIDTH)} ${String(SERIES_PLOT_HEIGHT + 18)}`}
            class="min-w-0 flex-1"
          >
            <line
              x1="0"
              y1={SERIES_PLOT_HEIGHT}
              x2={SERIES_PLOT_WIDTH}
              y2={SERIES_PLOT_HEIGHT}
              stroke="currentColor"
              stroke-opacity="0.15"
            />
            {#each seriesLayout.lines as line (line.percentile)}
              <polyline
                points={line.path}
                fill="none"
                stroke={PERCENTILE_COLORS[line.percentile]}
                stroke-width="1.5"
              />
              {#each line.points as point, pointIndex (pointIndex)}
                <circle
                  cx={point.x}
                  cy={point.y}
                  r="2"
                  fill={PERCENTILE_COLORS[line.percentile]}
                />
              {/each}
            {/each}
            {#each seriesLayout.xLabels as xLabel (xLabel.x)}
              <text
                x={xLabel.x}
                y={SERIES_PLOT_HEIGHT + 14}
                text-anchor="middle"
                class="fill-muted-foreground"
                font-size="10"
              >
                {xLabel.label}
              </text>
            {/each}
          </svg>
        </div>
      {/if}
    </Card.Content>
  </Card.Root>

  <Card.Root>
    <Card.Header class="pb-2">
      <Card.Title class="text-sm font-medium">
        Errors &amp; warnings by module (24h)
      </Card.Title>
    </Card.Header>
    <Card.Content>
      {#if (reliability.current?.logTargets.length ?? 0) === 0}
        <p class="py-6 text-center text-sm text-muted-foreground">
          No errors or warnings in the last {CARD_WINDOW_HOURS}h.
        </p>
      {:else}
        <div class="flex flex-col gap-1">
          {#each reliability.current?.logTargets ?? [] as row (`${row.target}:${row.level}`)}
            <div class="flex items-center gap-2 text-xs">
              <span
                class={`w-12 shrink-0 font-semibold ${row.level === 'ERROR' ? 'text-red-400' : 'text-amber-400'}`}
              >
                {row.level}
              </span>
              <span class="w-36 shrink-0 truncate font-mono">{row.target}</span>
              <svg
                viewBox={`0 0 ${String(row.sparkline.length * 6)} 14`}
                class="h-3.5 w-28 shrink-0"
                preserveAspectRatio="none"
              >
                {#each row.sparkline as bucketCount, bucketIndex (bucketIndex)}
                  <rect
                    x={bucketIndex * 6}
                    y={14 - (bucketCount / sparklineMax(row.sparkline)) * 12 - 1}
                    width="4"
                    height={(bucketCount / sparklineMax(row.sparkline)) * 12 + 1}
                    fill={row.level === 'ERROR' ? '#f87171' : '#fbbf24'}
                    opacity={bucketCount === 0 ? 0.15 : 0.9}
                  />
                {/each}
              </svg>
              <span class="w-10 shrink-0 text-right tabular-nums">{row.count}</span>
              <button
                class="text-muted-foreground underline-offset-2 hover:text-foreground
                  hover:underline"
                onclick={() => {
                  onOpenLogs?.(row.target)
                }}
              >
                logs →
              </button>
            </div>
          {/each}
        </div>
      {/if}

      {#if (reliability.current?.failureEvents.length ?? 0) > 0}
        <div class="mt-4 border-t pt-2">
          <p class="mb-1 text-xs font-medium text-red-400">
            Lifecycle failures (money-at-risk)
          </p>
          {#each reliability.current?.failureEvents ?? [] as failure (failure.eventType)}
            <div class="flex items-center gap-2 text-xs">
              <span class="w-72 shrink-0 truncate font-mono">{failure.eventType}</span>
              <span class="w-10 shrink-0 text-right tabular-nums">{failure.count}</span>
              <span class="text-muted-foreground">
                last {new Date(failure.lastAt).toLocaleString()}
              </span>
            </div>
          {/each}
        </div>
      {/if}
    </Card.Content>
  </Card.Root>

  <Card.Root>
    <Card.Header class="pb-2">
      <Card.Title class="text-sm font-medium">Rebalance stage breakdown</Card.Title>
      <div class="flex flex-wrap gap-3 text-xs text-muted-foreground">
        {#each Object.entries(STAGE_COLORS) as [stage, color] (stage)}
          <span class="flex items-center gap-1">
            <span class="inline-block h-2 w-2 rounded-sm" style:background={color}
            ></span>
            {stage}
          </span>
        {/each}
      </div>
    </Card.Header>
    <Card.Content>
      {#if rebalanceRows.length === 0}
        <p class="py-6 text-center text-sm text-muted-foreground">
          No rebalance operations in the selected range.
        </p>
      {:else}
        <div class="flex flex-col gap-1">
          {#each rebalanceRows as row (row.id)}
            <div class="flex items-center gap-2 text-xs">
              <span class="w-28 shrink-0">{directionLabel(row.direction)}</span>
              <span class="w-20 shrink-0 text-right tabular-nums text-muted-foreground">
                {row.amount ?? '—'} USDC
              </span>
              <svg
                viewBox={`0 0 ${String(REBALANCE_PLOT_WIDTH)} 14`}
                class="h-3.5 min-w-0 flex-1"
                preserveAspectRatio="none"
              >
                {#each row.segments as segment, segmentIndex (segmentIndex)}
                  <rect
                    x={segment.x}
                    y="2"
                    width={Math.max(segment.width, segment.ms > 0 ? 1 : 0)}
                    height="10"
                    rx="1"
                    fill={segment.failed ? '#f87171' : STAGE_COLORS[segment.stage]}
                  >
                    <title>{segment.stage}: {formatDurationMs(segment.ms)}</title>
                  </rect>
                {/each}
              </svg>
              <span class="w-16 shrink-0 text-right tabular-nums text-muted-foreground">
                {formatDurationMs(row.totalMs)}
              </span>
            </div>
          {/each}
        </div>
      {/if}

      {#if attestationTrend.points.length > 0}
        <div class="mt-4 border-t pt-2">
          <p class="mb-1 text-xs font-medium text-muted-foreground">
            CCTP attestation time trend (max {formatDurationMs(attestationTrend.maxMs)})
          </p>
          <svg
            viewBox={`0 0 ${String(TREND_PLOT_WIDTH)} ${String(TREND_PLOT_HEIGHT)}`}
            class="h-20 w-full"
            preserveAspectRatio="none"
          >
            <polyline
              points={attestationTrend.path}
              fill="none"
              stroke="#fbbf24"
              stroke-width="1.5"
            />
            {#each attestationTrend.points as point, pointIndex (pointIndex)}
              <circle cx={point.x} cy={point.y} r="2" fill="#fbbf24" />
            {/each}
          </svg>
        </div>
      {/if}
    </Card.Content>
  </Card.Root>

  <Card.Root>
    <Card.Header class="pb-2">
      <Card.Title class="text-sm font-medium">Ingestion health</Card.Title>
    </Card.Header>
    <Card.Content>
      {#if blockLagTrend.points.length === 0}
        <p class="py-6 text-center text-sm text-muted-foreground">
          No block-lag samples in the selected range.
        </p>
      {:else}
        <p class="mb-1 text-xs font-medium text-muted-foreground">
          Worst block lag per bucket (max {blockLagTrend.maxLagBlocks} blocks)
        </p>
        <svg
          viewBox={`0 0 ${String(TREND_PLOT_WIDTH)} ${String(TREND_PLOT_HEIGHT)}`}
          class="h-20 w-full"
          preserveAspectRatio="none"
        >
          <polyline
            points={blockLagTrend.path}
            fill="none"
            stroke="#38bdf8"
            stroke-width="1.5"
          />
          {#each blockLagTrend.points as point, pointIndex (pointIndex)}
            <circle cx={point.x} cy={point.y} r="2" fill="#38bdf8" />
          {/each}
        </svg>
      {/if}

      {#if chartInfra.current}
        {@const poll = chartInfra.current.monitor.poll}
        <div class="mt-4 flex flex-wrap gap-x-4 gap-y-1 border-t pt-2 text-xs text-muted-foreground">
          <span>{poll.cycles} poll cycles</span>
          <span class={poll.errors > 0 ? 'text-red-400' : ''}>
            {poll.errors} errors
          </span>
          <span class={poll.skippedTicks > 0 ? 'text-amber-400' : ''}>
            {poll.skippedTicks} skipped ticks
          </span>
          {#if poll.duration}
            <span>
              poll p50 {formatDurationMs(poll.duration.p50Ms)} · p95
              {formatDurationMs(poll.duration.p95Ms)} · max
              {formatDurationMs(poll.duration.maxMs)}
            </span>
          {/if}
        </div>
      {/if}
    </Card.Content>
  </Card.Root>
</div>
