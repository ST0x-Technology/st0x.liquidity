<script lang="ts">
  import { onMount } from 'svelte'
  import { BarChart, LineChart, type ChartState, type Tooltip } from 'layerchart'
  import * as Card from '$lib/components/ui/card'
  import * as Chart from '$lib/components/ui/chart'
  import type { EquityOperationKind } from '$lib/api/EquityOperationKind'
  import type { EquityStageName } from '$lib/api/EquityStageName'
  import type { EquityTimings } from '$lib/api/EquityTimings'
  import type { HedgeLatencies } from '$lib/api/HedgeLatencies'
  import type { InfraReport } from '$lib/api/InfraReport'
  import type { RebalanceStageName } from '$lib/api/RebalanceStageName'
  import type { RebalanceTimings } from '$lib/api/RebalanceTimings'
  import type { ReliabilityReport } from '$lib/api/ReliabilityReport'
  import type { StageLatencies } from '$lib/api/StageLatencies'
  import type { UsdcBridgeDirection } from '$lib/api/UsdcBridgeDirection'
  import { reactive } from '$lib/frp.svelte'
  import {
    fetchEquityTimings,
    fetchHedgeLatencies,
    fetchInfraReport,
    fetchRebalanceTimings,
    fetchReliabilityReport
  } from '$lib/performance/api'
  import {
    blockLagCard,
    detectionCard,
    errorsCard,
    exposureCard,
    openExposureCard
  } from '$lib/performance/cards'
  import {
    type EquityBarRow,
    type RebalanceBarRow,
    type WaterfallBarRow,
    type WaterfallSort,
    buildEquityRows,
    buildPercentileSeries,
    buildRebalanceRows,
    buildWaterfallRows,
    layoutDependencySparkline,
    thinTicks
  } from '$lib/performance/charts'
  import { type SloStatus, formatDurationMs } from '$lib/performance/slo'

  const { onOpenLogs }: { onOpenLogs?: (target: string) => void } = $props()

  // Explicit params type for every `Chart.Tooltip` `formatter` snippet below --
  // layerchart's generic component typing doesn't reliably flow through to
  // inline snippet destructuring, leaving `item` typed `any` without this.
  type TooltipFormatterParams = {
    value: unknown
    name: string
    item: Tooltip.TooltipSeries
  }

  const POLL_INTERVAL_MS = 30_000
  const CARD_WINDOW_HOURS = 24
  // Mirrors the backend telemetry RETENTION (14d): ingestion-health samples
  // (block lag, poll cycles) are pruned past this window, so a longer chart
  // range would just render a truncated view.
  const INGESTION_RETENTION_MS = 14 * 86_400_000

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

    // Infra (block lag) is supplementary, so fetch it on its own promise: a
    // transient /performance/infra failure must not block the latency and
    // reliability refresh. On failure we keep the last infra value, which the
    // block-lag card already degrades to STALE once its sample ages out.
    const infraRefresh = fetchInfraReport({ from, to })
      .then((infraReport) => {
        if (seq === cardFetchSeq) infra.update(() => infraReport)
      })
      .catch(() => {
        // Isolated: latency/reliability stay fresh even if infra is down.
      })

    try {
      const [latencyReport, reliabilityReport] = await Promise.all([
        fetchHedgeLatencies({ from, to }),
        fetchReliabilityReport({ from, to })
      ])
      if (seq !== cardFetchSeq) return

      const refreshedAt = new Date()

      latencies.update(() => latencyReport)
      reliability.update(() => reliabilityReport)
      lastRefreshed.update(() => refreshedAt)
      error.update(() => null)
    } catch (fetchError) {
      if (seq !== cardFetchSeq) return

      error.update(() => (fetchError instanceof Error ? fetchError.message : 'Unknown error'))
    }

    await infraRefresh
  }

  type RangePreset = '1W' | '2W' | '1M' | 'YTD' | '1Y' | 'ALL'

  const RANGE_PRESETS: RangePreset[] = ['1W', '2W', '1M', 'YTD', '1Y', 'ALL']

  /** Pre-dates the system's first deployment, so ALL covers everything. */
  const ALL_TIME_START = new Date('2025-01-01T00:00:00Z')

  const rangeStart = (preset: RangePreset, now: Date): Date => {
    if (preset === '1W') {
      return new Date(now.getTime() - 7 * 86_400_000)
    }

    if (preset === '2W') {
      // 15, not 14: stays under ReportRange::bucket_width's 31-day daily/
      // weekly threshold (so this still renders at daily granularity, same
      // as 1W) with a day of margin over a literal two weeks. Covers
      // `nix run .#simulate-14d`'s 14-day seeded fixture as long as the
      // dashboard is viewed within ~12h of the bot starting (the fixture's
      // oldest sample is a fixed timestamp set once at seed time, so a
      // longer-running session can eventually age it out of this trailing
      // window -- see `seed_simulated_hedge_latency_history`).
      return new Date(now.getTime() - 15 * 86_400_000)
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
  const chartEquityTimings = reactive<EquityTimings | null>(null)
  const chartInfra = reactive<InfraReport | null>(null)
  const ingestionTruncated = reactive<boolean>(false)
  const chartError = reactive<string | null>(null)
  // Timestamp captured at the moment the chart fetch was initiated, used as
  // `now` for in-progress cycle and operation elapsed-time calculations.
  const chartRefreshedAt = reactive<Date | null>(null)
  let chartFetchSeq = 0

  const refreshCharts = async () => {
    const seq = ++chartFetchSeq
    const to = new Date()
    const range = {
      from: rangeStart(chartRange.current, to),
      to
    }

    // Ingestion-health telemetry is pruned at the retention window, so clamp
    // the infra query to it and flag when the selected range was truncated --
    // a 1Y view must not be mistaken for "the system has only run 14 days".
    const infraFrom = new Date(
      Math.max(range.from.getTime(), to.getTime() - INGESTION_RETENTION_MS)
    )
    ingestionTruncated.update(() => infraFrom.getTime() > range.from.getTime())

    // Infra is supplementary here too: keep its failure from blocking the
    // latency and rebalance chart refresh (see refresh() for rationale).
    const infraRefresh = fetchInfraReport({ from: infraFrom, to })
      .then((infraReport) => {
        if (seq === chartFetchSeq) chartInfra.update(() => infraReport)
      })
      .catch(() => {
        // Isolated: latency/rebalance charts stay fresh even if infra is down.
      })

    // Equity timing is the newest endpoint, so keep its failure from blocking
    // the latency and rebalance chart refresh (same rationale as infra above).
    const equityRefresh = fetchEquityTimings(range)
      .then((equityReport) => {
        if (seq === chartFetchSeq) chartEquityTimings.update(() => equityReport)
      })
      .catch(() => {
        // Isolated: latency/rebalance charts stay fresh even if equity is down.
      })

    try {
      const [latencyReport, rebalanceReport] = await Promise.all([
        fetchHedgeLatencies(range),
        fetchRebalanceTimings(range)
      ])

      if (seq !== chartFetchSeq) return

      chartLatencies.update(() => latencyReport)
      chartRebalances.update(() => rebalanceReport)
      chartRefreshedAt.update(() => to)
      chartError.update(() => null)
    } catch (fetchError) {
      if (seq !== chartFetchSeq) return

      chartError.update(() => (fetchError instanceof Error ? fetchError.message : 'Unknown error'))
    }

    await infraRefresh
    await equityRefresh
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
    blockLagCard(infra.current, lastRefreshed.current)
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

  const WATERFALL_MAX_ROWS = 20
  const WATERFALL_ROW_HEIGHT_PX = 28

  const waterfallSort = reactive<WaterfallSort>('slowest')

  const WATERFALL_CHART_CONFIG = {
    unhedged: {
      label: 'unhedged (fill -> placed)',
      color: '#64748b'
    },
    submission: {
      label: 'submission (placed -> accepted)',
      color: '#38bdf8'
    },
    executionOk: {
      label: 'execution (accepted -> filled)',
      color: '#34d399'
    },
    executionFailed: {
      label: 'execution (failed)',
      color: '#f87171'
    }
  } satisfies Chart.ChartConfig

  const waterfallSeries = Object.entries(WATERFALL_CHART_CONFIG).map(([key, { label, color }]) => ({
    key,
    label,
    color,
    value: key
  }))

  const waterfallRows = $derived(
    buildWaterfallRows(chartLatencies.current?.cycles ?? [], {
      sort: waterfallSort.current,
      maxRows: WATERFALL_MAX_ROWS,
      now: chartRefreshedAt.current ?? new Date()
    })
  )

  const waterfallSymbolById = $derived(new Map(waterfallRows.map((row) => [row.id, row.symbol])))

  type StageKey = keyof StageLatencies

  const STAGE_OPTIONS: { key: StageKey; label: string }[] = [
    { key: 'exposureWindow', label: 'Exposure window' },
    { key: 'detection', label: 'Detection' },
    { key: 'decision', label: 'Decision' },
    { key: 'submission', label: 'Submission' },
    { key: 'execution', label: 'Execution' }
  ]

  const PERCENTILE_CHART_CONFIG = {
    p50Ms: {
      label: 'p50',
      color: '#34d399'
    },
    p90Ms: {
      label: 'p90',
      color: '#fbbf24'
    },
    p99Ms: {
      label: 'p99',
      color: '#f87171'
    }
  } satisfies Chart.ChartConfig

  const selectedStage = reactive<StageKey>('exposureWindow')

  const percentileSeriesData = $derived(
    buildPercentileSeries(chartLatencies.current?.buckets ?? [], selectedStage.current)
  )

  // A trend needs at least two sampled buckets; a single bucket would
  // otherwise render as a few floating points with no line.
  const percentileSampleCount = $derived(
    percentileSeriesData.filter((point) => point.p50Ms !== null).length
  )

  const MAX_PERCENTILE_X_TICKS = 8

  // Explicit tick values, thinned to a fixed cap: see thinTicks' doc comment
  // for why the default d3 time-scale tick generator can't be trusted here.
  const percentileXTicks = $derived(thinTicks(percentileSeriesData, MAX_PERCENTILE_X_TICKS))

  const rangeButtonClass = (active: boolean): string =>
    `rounded px-2 py-1 text-xs font-medium transition-colors ${active ? 'bg-primary text-primary-foreground' : 'bg-muted text-muted-foreground hover:text-foreground'}`

  const REBALANCE_MAX_ROWS = 15
  const REBALANCE_ROW_HEIGHT_PX = 28

  const ATTESTATION_CHART_CONFIG = {
    durationMs: {
      label: 'Attestation time',
      color: '#fbbf24'
    }
  } satisfies Chart.ChartConfig

  const BLOCK_LAG_CHART_CONFIG = {
    maxLagBlocks: {
      label: 'Block lag',
      color: '#38bdf8'
    }
  } satisfies Chart.ChartConfig

  const STAGE_COLORS: Record<RebalanceStageName, string> = {
    conversion: '#c084fc',
    withdrawal: '#38bdf8',
    burn: '#fb923c',
    attestation: '#fbbf24',
    mint: '#34d399',
    deposit: '#818cf8'
  }

  const REBALANCE_FAILED_COLOR = '#f87171'

  // Segment order is fixed (conversion -> withdrawal -> burn -> attestation ->
  // mint -> deposit) and matches AlpacaToBase's chronological pipeline, but
  // NOT BaseToAlpaca's -- conversion is that direction's last stage (see
  // src/performance/rebalance.rs), so its bar renders left-to-right out of
  // real chronological order. A single shared stacked chart can't be
  // chronological for both directions at once.
  const REBALANCE_CHART_CONFIG = {
    conversionOk: {
      label: 'conversion',
      color: STAGE_COLORS.conversion
    },
    conversionFailed: {
      label: 'conversion (failed)',
      color: REBALANCE_FAILED_COLOR
    },
    withdrawalOk: {
      label: 'withdrawal',
      color: STAGE_COLORS.withdrawal
    },
    withdrawalFailed: {
      label: 'withdrawal (failed)',
      color: REBALANCE_FAILED_COLOR
    },
    burnOk: {
      label: 'burn',
      color: STAGE_COLORS.burn
    },
    burnFailed: {
      label: 'burn (failed)',
      color: REBALANCE_FAILED_COLOR
    },
    attestationOk: {
      label: 'attestation',
      color: STAGE_COLORS.attestation
    },
    attestationFailed: {
      label: 'attestation (failed)',
      color: REBALANCE_FAILED_COLOR
    },
    mintOk: {
      label: 'mint',
      color: STAGE_COLORS.mint
    },
    mintFailed: {
      label: 'mint (failed)',
      color: REBALANCE_FAILED_COLOR
    },
    depositOk: {
      label: 'deposit',
      color: STAGE_COLORS.deposit
    },
    depositFailed: {
      label: 'deposit (failed)',
      color: REBALANCE_FAILED_COLOR
    },
    // Elapsed time not accounted for by any measured stage -- covers both an
    // in-progress operation's still-open stage (no durationMs yet) and a
    // completed-but-recovered operation whose stages don't fully cover its
    // measured round-trip. Without this, either case would render as an
    // invisible or shorter-than-real bar.
    unmeasuredMs: {
      label: 'unaccounted elapsed',
      color: '#64748b'
    }
  } satisfies Chart.ChartConfig

  const rebalanceSeries = Object.entries(REBALANCE_CHART_CONFIG).map(([key, { label, color }]) => ({
    key,
    label,
    color,
    value: key
  }))

  const rebalanceRows = $derived(
    buildRebalanceRows(chartRebalances.current?.operations ?? [], {
      maxRows: REBALANCE_MAX_ROWS,
      now: chartRefreshedAt.current ?? new Date()
    })
  )

  const attestationChartData = $derived(
    (chartRebalances.current?.attestationTrend ?? []).map((sample) => ({
      burnedAt: new Date(sample.burnedAt),
      durationMs: sample.durationMs
    }))
  )
  const attestationMaxMs = $derived(
    Math.max(0, ...attestationChartData.map((sample) => sample.durationMs))
  )

  const blockLagChartData = $derived(
    (chartInfra.current?.monitor.blockLag ?? []).map((point) => ({
      start: new Date(point.start),
      maxLagBlocks: point.maxLagBlocks
    }))
  )
  const blockLagMax = $derived(Math.max(0, ...blockLagChartData.map((point) => point.maxLagBlocks)))

  const DIRECTION_LABELS: Record<UsdcBridgeDirection, string> = {
    alpaca_to_base: 'Alpaca → Base',
    base_to_alpaca: 'Base → Alpaca'
  }

  const directionLabel = (direction: UsdcBridgeDirection | null): string =>
    direction === null ? '—' : DIRECTION_LABELS[direction]

  const EQUITY_MAX_ROWS = 15
  const EQUITY_ROW_HEIGHT_PX = 28

  const EQUITY_STAGE_COLORS: Record<EquityStageName, string> = {
    mint_acceptance: '#c084fc',
    mint_receipt: '#38bdf8',
    mint_wrap: '#fb923c',
    mint_deposit: '#34d399',
    redemption_withdraw: '#818cf8',
    redemption_unwrap: '#f472b6',
    redemption_send: '#facc15',
    redemption_detection: '#22d3ee',
    redemption_completion: '#a3e635'
  }

  const EQUITY_FAILED_COLOR = '#f87171'

  const EQUITY_CHART_CONFIG = {
    mintAcceptanceOk: {
      label: 'mint acceptance',
      color: EQUITY_STAGE_COLORS.mint_acceptance
    },
    mintAcceptanceFailed: {
      label: 'mint acceptance (failed)',
      color: EQUITY_FAILED_COLOR
    },
    mintReceiptOk: {
      label: 'mint receipt',
      color: EQUITY_STAGE_COLORS.mint_receipt
    },
    mintReceiptFailed: {
      label: 'mint receipt (failed)',
      color: EQUITY_FAILED_COLOR
    },
    mintWrapOk: {
      label: 'mint wrap',
      color: EQUITY_STAGE_COLORS.mint_wrap
    },
    mintWrapFailed: {
      label: 'mint wrap (failed)',
      color: EQUITY_FAILED_COLOR
    },
    mintDepositOk: {
      label: 'mint deposit',
      color: EQUITY_STAGE_COLORS.mint_deposit
    },
    mintDepositFailed: {
      label: 'mint deposit (failed)',
      color: EQUITY_FAILED_COLOR
    },
    redemptionWithdrawOk: {
      label: 'redemption withdraw',
      color: EQUITY_STAGE_COLORS.redemption_withdraw
    },
    redemptionWithdrawFailed: {
      label: 'redemption withdraw (failed)',
      color: EQUITY_FAILED_COLOR
    },
    redemptionUnwrapOk: {
      label: 'redemption unwrap',
      color: EQUITY_STAGE_COLORS.redemption_unwrap
    },
    redemptionUnwrapFailed: {
      label: 'redemption unwrap (failed)',
      color: EQUITY_FAILED_COLOR
    },
    redemptionSendOk: {
      label: 'redemption send',
      color: EQUITY_STAGE_COLORS.redemption_send
    },
    redemptionSendFailed: {
      label: 'redemption send (failed)',
      color: EQUITY_FAILED_COLOR
    },
    redemptionDetectionOk: {
      label: 'redemption detection',
      color: EQUITY_STAGE_COLORS.redemption_detection
    },
    redemptionDetectionFailed: {
      label: 'redemption detection (failed)',
      color: EQUITY_FAILED_COLOR
    },
    redemptionCompletionOk: {
      label: 'redemption completion',
      color: EQUITY_STAGE_COLORS.redemption_completion
    },
    redemptionCompletionFailed: {
      label: 'redemption completion (failed)',
      color: EQUITY_FAILED_COLOR
    },
    // Same elapsed-remainder rationale as REBALANCE_CHART_CONFIG.unmeasuredMs
    // -- covers both in-progress and completed-but-recovered operations.
    unmeasuredMs: {
      label: 'unaccounted elapsed',
      color: '#64748b'
    }
  } satisfies Chart.ChartConfig

  const equitySeries = Object.entries(EQUITY_CHART_CONFIG).map(([key, { label, color }]) => ({
    key,
    label,
    color,
    value: key
  }))

  const equityRows = $derived(
    buildEquityRows(chartEquityTimings.current?.operations ?? [], {
      maxRows: EQUITY_MAX_ROWS,
      now: chartRefreshedAt.current ?? new Date()
    })
  )

  const EQUITY_KIND_LABELS: Record<EquityOperationKind, string> = {
    mint: 'Mint',
    redeem: 'Redeem'
  }

  const equityKindLabel = (kind: EquityOperationKind): string => EQUITY_KIND_LABELS[kind]
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
        {#each Object.entries(WATERFALL_CHART_CONFIG) as [name, { label, color }] (name)}
          <span class="flex items-center gap-1">
            <span class="inline-block h-2 w-2 rounded-sm" style:background={color}></span>
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
        <Chart.Container
          config={WATERFALL_CHART_CONFIG}
          class="aspect-auto w-full"
          style="height: {waterfallRows.length * WATERFALL_ROW_HEIGHT_PX + 40}px"
        >
          <BarChart
            data={waterfallRows}
            x={Object.keys(WATERFALL_CHART_CONFIG)}
            y="id"
            orientation="horizontal"
            seriesLayout="stack"
            series={waterfallSeries}
            padding={{ left: 70 }}
            props={{
              xAxis: { format: (value: number) => formatDurationMs(value) },
              yAxis: { format: (id: string) => waterfallSymbolById.get(id) ?? id }
            }}
          >
            {#snippet tooltip({ context }: { context: ChartState<WaterfallBarRow> })}
              {@const row = context.tooltip.data as WaterfallBarRow | null}
              {@const label = row ? (waterfallSymbolById.get(row.id) ?? row.id) : ''}
              <Chart.Tooltip {label} excludeZero>
                {#snippet formatter({ value, name, item }: TooltipFormatterParams)}
                  {#if typeof value === 'number' && value > 0}
                    <div class="flex w-full flex-1 items-center justify-between gap-2">
                      <span class="flex items-center gap-1.5 text-muted-foreground">
                        <span
                          class="inline-block h-2.5 w-2.5 shrink-0 rounded-sm"
                          style:background={item.color}
                        ></span>
                        {name}
                      </span>
                      <span class="font-mono font-medium tabular-nums text-foreground">
                        {formatDurationMs(value)}
                      </span>
                    </div>
                  {/if}
                {/snippet}
              </Chart.Tooltip>
            {/snippet}
          </BarChart>
        </Chart.Container>
        {#if (chartLatencies.current?.totalCycles ?? 0) > waterfallRows.length}
          <p class="mt-2 text-xs text-muted-foreground">
            Showing {waterfallRows.length} of {chartLatencies.current?.totalCycles} cycles in range{waterfallSort.current ===
            'slowest'
              ? ' — slowest among the most recent cycles shown, not full-range'
              : ''}.
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
        {#each Object.entries(PERCENTILE_CHART_CONFIG) as [percentile, { color }] (percentile)}
          <span class="flex items-center gap-1">
            <span class="inline-block h-0.5 w-3" style:background={color}></span>
            {percentile.replace('Ms', '')}
          </span>
        {/each}
      </div>
    </Card.Header>
    <Card.Content>
      {#if percentileSampleCount === 0}
        <p class="py-6 text-center text-sm text-muted-foreground">
          No samples for this stage in the selected range.
        </p>
      {:else if percentileSampleCount === 1}
        <p class="py-6 text-center text-sm text-muted-foreground">
          Only one time bucket has samples in this range, so there's no trend to plot yet. Widen the
          range or wait for more data to accumulate.
        </p>
      {:else}
        <Chart.Container config={PERCENTILE_CHART_CONFIG} class="aspect-auto h-40 w-full">
          <!--
            xPadding reserves 12px on each side of the x (time) scale's domain --
            without it the scale maps the first/last sample straight to the plot's
            literal edges, clipping the endpoint point markers and x-axis labels.
          -->
          <LineChart
            data={percentileSeriesData}
            x="start"
            grid={{ x: false, y: true }}
            series={Object.entries(PERCENTILE_CHART_CONFIG).map(([key, { label, color }]) => ({
              key,
              label,
              color
            }))}
            points={true}
            xPadding={[12, 12]}
            props={{
              xAxis: {
                ticks: percentileXTicks,
                format: (value: Date) =>
                  value.toLocaleDateString(undefined, { month: 'short', day: 'numeric' })
              },
              yAxis: { format: (value: number) => formatDurationMs(value) }
            }}
          >
            {#snippet tooltip()}
              <Chart.Tooltip
                labelFormatter={(value) => (value as Date).toLocaleString()}
                indicator="line"
              >
                {#snippet formatter({ value, name, item }: TooltipFormatterParams)}
                  {#if typeof value === 'number'}
                    <div class="flex w-full flex-1 items-center justify-between gap-2">
                      <span class="flex items-center gap-1.5 text-muted-foreground">
                        <span
                          class="inline-block h-2.5 w-2.5 shrink-0 rounded-sm"
                          style:background={item.color}
                        ></span>
                        {name}
                      </span>
                      <span class="font-mono font-medium tabular-nums text-foreground">
                        {formatDurationMs(value)}
                      </span>
                    </div>
                  {/if}
                {/snippet}
              </Chart.Tooltip>
            {/snippet}
          </LineChart>
        </Chart.Container>
      {/if}
    </Card.Content>
  </Card.Root>

  <Card.Root>
    <Card.Header class="pb-2">
      <Card.Title class="text-sm font-medium">Errors &amp; warnings by module (24h)</Card.Title>
    </Card.Header>
    <Card.Content>
      {#if reliability.current !== null && reliability.current.logTargets.length === 0}
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
                    y={14 - (bucketCount / Math.max(1, ...row.sparkline)) * 12 - 1}
                    width="4"
                    height={(bucketCount / Math.max(1, ...row.sparkline)) * 12 + 1}
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

      {#if reliability.current !== null && reliability.current.failureEvents.length > 0}
        <div class="mt-4 border-t pt-2">
          <p class="mb-1 text-xs font-medium text-red-400">Lifecycle failures (money-at-risk)</p>
          {#each reliability.current.failureEvents as failure (failure.eventType)}
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
      <Card.Title class="text-sm font-medium">USDC rebalance stage breakdown</Card.Title>
      <div class="flex flex-wrap gap-3 text-xs text-muted-foreground">
        {#each Object.entries(STAGE_COLORS) as [stage, color] (stage)}
          <span class="flex items-center gap-1">
            <span class="inline-block h-2 w-2 rounded-sm" style:background={color}></span>
            {stage}
          </span>
        {/each}
      </div>
    </Card.Header>
    <Card.Content>
      {#if (chartRebalances.current?.skippedOperations ?? 0) > 0}
        <p class="mb-2 text-xs text-amber-400">
          {chartRebalances.current?.skippedOperations} operations excluded (unparseable timing).
        </p>
      {/if}
      {#if rebalanceRows.length === 0}
        <p class="py-6 text-center text-sm text-muted-foreground">
          No rebalance operations in the selected range.
        </p>
      {:else}
        <Chart.Container
          config={REBALANCE_CHART_CONFIG}
          class="aspect-auto w-full"
          style="height: {rebalanceRows.length * REBALANCE_ROW_HEIGHT_PX + 40}px"
        >
          <BarChart
            data={rebalanceRows}
            x={Object.keys(REBALANCE_CHART_CONFIG)}
            y="id"
            orientation="horizontal"
            seriesLayout="stack"
            series={rebalanceSeries}
            padding={{ left: 110 }}
            props={{
              xAxis: { format: (value: number) => formatDurationMs(value) },
              yAxis: {
                format: (id: string) => {
                  const row = rebalanceRows.find((entry) => entry.id === id)
                  return row ? directionLabel(row.direction) : id
                }
              }
            }}
          >
            {#snippet tooltip({ context }: { context: ChartState<RebalanceBarRow> })}
              {@const row = context.tooltip.data as RebalanceBarRow | null}
              {@const label = row
                ? `${directionLabel(row.direction)} · ${row.amount ?? '—'} USDC · total ${row.totalMs !== null ? formatDurationMs(row.totalMs) : 'unmeasured'}`
                : ''}
              <Chart.Tooltip {label} excludeZero>
                {#snippet formatter({ value, name, item }: TooltipFormatterParams)}
                  {#if typeof value === 'number' && value > 0}
                    <div class="flex w-full flex-1 items-center justify-between gap-2">
                      <span class="flex items-center gap-1.5 text-muted-foreground">
                        <span
                          class="inline-block h-2.5 w-2.5 shrink-0 rounded-sm"
                          style:background={item.color}
                        ></span>
                        {name}
                      </span>
                      <span class="font-mono font-medium tabular-nums text-foreground">
                        {formatDurationMs(value)}
                      </span>
                    </div>
                  {/if}
                {/snippet}
              </Chart.Tooltip>
            {/snippet}
          </BarChart>
        </Chart.Container>
        {#if (chartRebalances.current?.totalOperations ?? 0) > rebalanceRows.length}
          <p class="mt-2 text-xs text-muted-foreground">
            Showing {rebalanceRows.length} of {chartRebalances.current?.totalOperations} operations
            in range.
          </p>
        {/if}
      {/if}

      {#if attestationChartData.length > 0}
        <div class="mt-4 border-t pt-2">
          <p class="mb-1 text-xs font-medium text-muted-foreground">
            CCTP attestation time trend (max {formatDurationMs(attestationMaxMs)})
          </p>
          <Chart.Container config={ATTESTATION_CHART_CONFIG} class="aspect-auto h-20 w-full">
            <LineChart
              data={attestationChartData}
              x="burnedAt"
              series={Object.entries(ATTESTATION_CHART_CONFIG).map(([key, { label, color }]) => ({
                key,
                label,
                color
              }))}
              points={true}
              props={{
                yAxis: { format: (value: number) => formatDurationMs(value) }
              }}
            >
              {#snippet tooltip()}
                <Chart.Tooltip
                  labelFormatter={(value) => (value as Date).toLocaleString()}
                  indicator="line"
                >
                  {#snippet formatter({ value, name, item }: TooltipFormatterParams)}
                    {#if typeof value === 'number'}
                      <div class="flex w-full flex-1 items-center justify-between gap-2">
                        <span class="flex items-center gap-1.5 text-muted-foreground">
                          <span
                            class="inline-block h-2.5 w-2.5 shrink-0 rounded-sm"
                            style:background={item.color}
                          ></span>
                          {name}
                        </span>
                        <span class="font-mono font-medium tabular-nums text-foreground">
                          {formatDurationMs(value)}
                        </span>
                      </div>
                    {/if}
                  {/snippet}
                </Chart.Tooltip>
              {/snippet}
            </LineChart>
          </Chart.Container>
        </div>
      {/if}
    </Card.Content>
  </Card.Root>

  <Card.Root>
    <Card.Header class="pb-2">
      <Card.Title class="text-sm font-medium">Equity rebalance stage breakdown</Card.Title>
      <div class="flex flex-wrap gap-3 text-xs text-muted-foreground">
        {#each Object.entries(EQUITY_STAGE_COLORS) as [stage, color] (stage)}
          <span class="flex items-center gap-1">
            <span class="inline-block h-2 w-2 rounded-sm" style:background={color}></span>
            {stage.replace('_', ' ')}
          </span>
        {/each}
      </div>
    </Card.Header>
    <Card.Content>
      {#if (chartEquityTimings.current?.skippedOperations ?? 0) > 0}
        <p class="mb-2 text-xs text-amber-400">
          {chartEquityTimings.current?.skippedOperations} operations excluded (unparseable timing).
        </p>
      {/if}
      {#if equityRows.length === 0}
        <p class="py-6 text-center text-sm text-muted-foreground">
          No equity mint/redemption operations in the selected range.
        </p>
      {:else}
        <Chart.Container
          config={EQUITY_CHART_CONFIG}
          class="aspect-auto w-full"
          style="height: {equityRows.length * EQUITY_ROW_HEIGHT_PX + 40}px"
        >
          <BarChart
            data={equityRows}
            x={Object.keys(EQUITY_CHART_CONFIG)}
            y="id"
            orientation="horizontal"
            seriesLayout="stack"
            series={equitySeries}
            padding={{ left: 110 }}
            props={{
              xAxis: { format: (value: number) => formatDurationMs(value) },
              yAxis: {
                format: (id: string) => {
                  const row = equityRows.find((entry) => entry.id === id)
                  return row ? equityKindLabel(row.kind) : id
                }
              }
            }}
          >
            {#snippet tooltip({ context }: { context: ChartState<EquityBarRow> })}
              {@const row = context.tooltip.data as EquityBarRow | null}
              {@const label = row
                ? `${equityKindLabel(row.kind)} · ${row.symbol ?? '—'} · total ${row.totalMs !== null ? formatDurationMs(row.totalMs) : 'unmeasured'}`
                : ''}
              <Chart.Tooltip {label} excludeZero>
                {#snippet formatter({ value, name, item }: TooltipFormatterParams)}
                  {#if typeof value === 'number' && value > 0}
                    <div class="flex w-full flex-1 items-center justify-between gap-2">
                      <span class="flex items-center gap-1.5 text-muted-foreground">
                        <span
                          class="inline-block h-2.5 w-2.5 shrink-0 rounded-sm"
                          style:background={item.color}
                        ></span>
                        {name}
                      </span>
                      <span class="font-mono font-medium tabular-nums text-foreground">
                        {formatDurationMs(value)}
                      </span>
                    </div>
                  {/if}
                {/snippet}
              </Chart.Tooltip>
            {/snippet}
          </BarChart>
        </Chart.Container>
        {#if (chartEquityTimings.current?.totalOperations ?? 0) > equityRows.length}
          <p class="mt-2 text-xs text-muted-foreground">
            Showing {equityRows.length} of {chartEquityTimings.current?.totalOperations} operations
            in range.
          </p>
        {/if}
      {/if}
    </Card.Content>
  </Card.Root>

  <Card.Root>
    <Card.Header class="pb-2">
      <Card.Title class="text-sm font-medium">Ingestion health</Card.Title>
      {#if ingestionTruncated.current}
        <p class="text-xs text-muted-foreground">
          Showing the last 14 days · older samples are pruned by the telemetry retention window.
        </p>
      {/if}
    </Card.Header>
    <Card.Content>
      {#if blockLagChartData.length === 0}
        <p class="py-6 text-center text-sm text-muted-foreground">
          No block-lag samples in the selected range.
        </p>
      {:else}
        <p class="mb-1 text-xs font-medium text-muted-foreground">
          Worst block lag per bucket (max {blockLagMax} blocks)
        </p>
        <Chart.Container config={BLOCK_LAG_CHART_CONFIG} class="aspect-auto h-20 w-full">
          <LineChart
            data={blockLagChartData}
            x="start"
            series={Object.entries(BLOCK_LAG_CHART_CONFIG).map(([key, { label, color }]) => ({
              key,
              label,
              color
            }))}
            points={true}
          >
            {#snippet tooltip()}
              <Chart.Tooltip
                labelFormatter={(value) => (value as Date).toLocaleString()}
                indicator="line"
              />
            {/snippet}
          </LineChart>
        </Chart.Container>
      {/if}

      {#if chartInfra.current}
        {@const poll = chartInfra.current.monitor.poll}
        <div
          class="mt-4 flex flex-wrap gap-x-4 gap-y-1 border-t pt-2 text-xs text-muted-foreground"
        >
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

  <Card.Root>
    <Card.Header class="pb-2">
      <Card.Title class="text-sm font-medium">Dependency health</Card.Title>
      <p class="text-xs text-muted-foreground">
        Per-operation latency and errors for the RPC provider and broker API. Sparkline bars are
        per-bucket median latency; red bars contain errors.
      </p>
    </Card.Header>
    <Card.Content>
      {#if (chartInfra.current?.dependencies.length ?? 0) === 0}
        <p class="py-6 text-center text-sm text-muted-foreground">
          No dependency calls in the selected range.
        </p>
      {:else}
        <div class="flex flex-col gap-1">
          {#each chartInfra.current?.dependencies ?? [] as row (`${row.dependency}:${row.operation}`)}
            {@const bars = layoutDependencySparkline(row.buckets, { plotHeight: 12 })}
            <div class="flex items-center gap-2 text-xs">
              <span class="w-14 shrink-0 font-semibold uppercase text-muted-foreground">
                {row.dependency}
              </span>
              <span class="w-44 shrink-0 truncate font-mono">{row.operation}</span>
              <svg
                viewBox={`0 0 ${String(bars.length * 6)} 14`}
                class="h-3.5 w-28 shrink-0"
                preserveAspectRatio="none"
              >
                {#each bars as bar, barIndex (barIndex)}
                  <rect
                    x={barIndex * 6}
                    y={14 - bar.height - 1}
                    width="4"
                    height={bar.height + 1}
                    fill={bar.hasErrors ? '#f87171' : '#38bdf8'}
                    opacity="0.9"
                  />
                {/each}
              </svg>
              <span class="w-20 shrink-0 text-right tabular-nums text-muted-foreground">
                {row.calls} calls
              </span>
              <span
                class={`w-16 shrink-0 text-right tabular-nums ${row.errors > 0 ? 'text-red-400' : 'text-muted-foreground'}`}
              >
                {row.errors} err
              </span>
              <span class="min-w-0 flex-1 truncate text-right tabular-nums text-muted-foreground">
                {#if row.latency}
                  p50 {formatDurationMs(row.latency.p50Ms)} · p95
                  {formatDurationMs(row.latency.p95Ms)} · max
                  {formatDurationMs(row.latency.maxMs)}
                {:else}
                  —
                {/if}
              </span>
            </div>
          {/each}
        </div>
      {/if}
    </Card.Content>
  </Card.Root>
</div>
