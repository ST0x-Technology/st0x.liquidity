<script lang="ts">
  import { onMount } from 'svelte'
  import * as Card from '$lib/components/ui/card'
  import * as Table from '$lib/components/ui/table'
  import MultiSelect from '$lib/components/multi-select.svelte'
  import { decimalCompare, formatDecimal } from '$lib/decimal'
  import { getPnlSqlApiUrl, isDashboardMockMode } from '$lib/env'
  import { reactive } from '$lib/frp.svelte'
  import { formatUtc } from '$lib/time'
  import { fetchPnlReport } from '$lib/pnl/api'
  import { STREAM_KEYS, syntheticPnlDashboard } from '$lib/pnl/synthetic'
  import type {
    PnlEntry,
    PnlResponse,
    PnlStreamKey,
    PnlSummary,
    PnlSymbolSummary,
    SyntheticPnlWindow,
    SyntheticPnlWindowSymbol
  } from '$lib/pnl/types'

  type RangePreset = '1w' | '1m' | 'ytd' | '1y' | 'all' | 'custom'

  const PNL_ENTRY_LIMIT = Number.MAX_SAFE_INTEGER
  const POLL_INTERVAL_MS = 10_000
  const MS_PER_DAY = 24 * 60 * 60 * 1_000
  const mockMode = isDashboardMockMode()
  const hasSqlPnlSource = getPnlSqlApiUrl() !== null

  const dateFromIso = (value: string): Date => new Date(`${value}T00:00:00.000Z`)

  const isoDate = (date: Date): string => date.toISOString().slice(0, 10)

  const shiftDate = (value: string, days: number): string => {
    const date = dateFromIso(value)
    date.setUTCDate(date.getUTCDate() + days)
    return isoDate(date)
  }

  const syntheticAvailableStartDate = syntheticPnlDashboard.windows[0]?.startAt.slice(0, 10) ?? ''
  const syntheticAvailableEndDate = syntheticPnlDashboard.windows.at(-1)?.startAt.slice(0, 10) ?? ''
  const initialAvailableEndDate =
    !mockMode && hasSqlPnlSource ? new Date().toISOString().slice(0, 10) : syntheticAvailableEndDate
  const initialAvailableStartDate =
    !mockMode && hasSqlPnlSource
      ? shiftDate(initialAvailableEndDate, -6)
      : syntheticAvailableStartDate
  const defaultChartSymbols = syntheticPnlDashboard.report.symbols.map((row) => row.symbol).sort()

  const clampDateForRange = (value: string, start: string, end: string): string => {
    if (!start || !end) return value
    if (value < start) return start
    if (value > end) return end
    return value
  }

  const startOfYear = (value: string): string => `${value.slice(0, 4)}-01-01`
  const presetStartForRange = (preset: RangePreset, start: string, end: string): string => {
    switch (preset) {
      case '1w':
        return clampDateForRange(shiftDate(end, -6), start, end)
      case '1m':
        return clampDateForRange(shiftDate(end, -29), start, end)
      case 'ytd':
        return clampDateForRange(startOfYear(end), start, end)
      case '1y':
        return clampDateForRange(shiftDate(end, -364), start, end)
      case 'all':
        return start
      case 'custom':
        return clampDateForRange(shiftDate(end, -6), start, end)
    }
  }

  const report = reactive<PnlResponse | null>(null)
  const entries = reactive<PnlEntry[]>([])
  const loading = reactive(false)
  const error = reactive<string | null>(null)
  const total = reactive(0)
  const selectedSymbols = reactive<Set<string>>(new Set())
  const selectedAssetChartSymbols = reactive<Set<string>>(new Set(defaultChartSymbols))
  const selectedAssetChartStreams = reactive<Set<PnlStreamKey>>(new Set(STREAM_KEYS))
  const selectedMethodChartSymbols = reactive<Set<string>>(new Set(defaultChartSymbols))
  const selectedMethodChartStreams = reactive<Set<PnlStreamKey>>(new Set(STREAM_KEYS))
  const dayFilter = reactive<'all' | 'weekday' | 'weekend'>('all')
  const dataMode = reactive<'synthetic' | 'backend'>(
    !mockMode && hasSqlPnlSource ? 'backend' : 'synthetic'
  )
  const availableStartDate = $derived.by(() => {
    if (dataMode.current === 'synthetic') return syntheticAvailableStartDate
    return (
      report.current?.sampleStats?.firstAt?.slice(0, 10) ??
      report.current?.windows?.[0]?.startAt.slice(0, 10) ??
      initialAvailableStartDate
    )
  })
  const availableEndDate = $derived.by(() => {
    if (dataMode.current === 'synthetic') return syntheticAvailableEndDate
    return (
      report.current?.sampleStats?.lastAt?.slice(0, 10) ??
      report.current?.windows?.at(-1)?.startAt.slice(0, 10) ??
      initialAvailableEndDate
    )
  })
  const clampDate = (value: string): string =>
    clampDateForRange(value, availableStartDate, availableEndDate)
  const presetStart = (preset: RangePreset): string =>
    presetStartForRange(preset, availableStartDate, availableEndDate)
  const allSymbols = reactive<string[]>([])
  const selectedPreset = reactive<RangePreset>('1w')
  const fromDate = reactive(
    presetStartForRange('1w', initialAvailableStartDate, initialAvailableEndDate)
  )
  const toDate = reactive(initialAvailableEndDate)
  const dateRangeKey = (from: string, to: string): string => `${from}:${to}`
  let lastRequestedDateRangeKey: string | null = null
  let fetchSeq = 0

  const streamLabels: Record<PnlStreamKey, string> = {
    counterTradePnlUsd: 'Counter-trade',
    onchainNettingPnlUsd: 'On-chain netting',
    directionalInventoryBaselinePnlUsd: 'Baseline drift',
    directionalImbalanceExcessPnlUsd: 'Directional excess'
  }
  const streamColors: Record<PnlStreamKey, string> = {
    counterTradePnlUsd: '#38bdf8',
    onchainNettingPnlUsd: '#22c55e',
    directionalInventoryBaselinePnlUsd: '#f59e0b',
    directionalImbalanceExcessPnlUsd: '#f43f5e'
  }
  const assetColors = ['#38bdf8', '#f59e0b', '#22c55e', '#f43f5e', '#a78bfa']
  const formulaRows = [
    {
      label: 'Counter-trade PnL',
      formula: 'sum((broker_price - onchain_price) * matched_qty * side_sign)',
      note: 'Only broker fills inside the counter-trade threshold are attributed here. side_sign is +1 for long inventory opened by the on-chain fill and -1 for short inventory.'
    },
    {
      label: 'On-chain netting PnL',
      formula: 'sum((closing_onchain_price - opening_onchain_price) * matched_qty * side_sign)',
      note: 'Used when a later opposite on-chain fill closes inventory before or instead of a broker counter-trade.'
    },
    {
      label: 'Baseline drift PnL',
      formula: 'sum(frozen_start_qty * (bucket_end_mark - bucket_start_mark))',
      note: 'This isolates the mark-to-market move of inventory that already existed at the start of the displayed bucket.'
    },
    {
      label: 'Directional excess PnL',
      formula: 'sum(excess_open_qty * (bucket_end_mark - exposure_open_mark) * side_sign)',
      note: 'Used for dangling or late-countered exposure beyond the normal balanced inventory threshold.'
    },
    {
      label: 'Directional total PnL',
      formula: 'baseline_drift_pnl + directional_excess_pnl',
      note: 'This is the full directional component before adding realized counter-trade and on-chain netting PnL.'
    },
    {
      label: 'Total PnL',
      formula:
        'counter_trade_pnl + onchain_netting_pnl + baseline_drift_pnl + directional_excess_pnl',
      note: 'Bars are non-cumulative. Weekly and monthly bars sum the day-level buckets inside that displayed period.'
    }
  ]

  const streamOptions = STREAM_KEYS.map((stream) => ({
    value: stream,
    label: streamLabels[stream]
  }))

  const symbolOptions = $derived(
    allSymbols.current.map((symbol) => ({
      value: symbol,
      label: symbol
    }))
  )

  const isPnlStreamKey = (value: string): value is PnlStreamKey =>
    (STREAM_KEYS as readonly string[]).includes(value)

  const isWeekendTimestamp = (timestamp: string): boolean =>
    [0, 6].includes(new Date(timestamp).getUTCDay())

  const matchesDayFilter = (isWeekend: boolean): boolean => {
    if (dayFilter.current === 'weekday') return !isWeekend
    if (dayFilter.current === 'weekend') return isWeekend
    return true
  }

  const dateKey = (timestamp: string): string => timestamp.slice(0, 10)

  const matchesDateRange = (date: string): boolean => {
    const start = fromDate.current <= toDate.current ? fromDate.current : toDate.current
    const end = fromDate.current <= toDate.current ? toDate.current : fromDate.current

    return (!start || date >= start) && (!end || date <= end)
  }

  const emptySymbolSummary = (symbol: string): PnlSymbolSummary => ({
    symbol,
    counterTradePnlUsd: '0',
    onchainNettingPnlUsd: '0',
    directionalInventoryBaselinePnlUsd: '0',
    directionalImbalanceExcessPnlUsd: '0',
    directionalExposurePnlUsd: '0',
    totalPnlUsd: '0',
    realizedPnlUsd: '0',
    matchedShares: '0',
    inventoryDriftShares: '0',
    inventoryDriftUsd: '0',
    openLongShares: '0',
    openShortShares: '0',
    unmatchedOffchainShares: '0',
    matchedLotCount: 0,
    onchainFillCount: 0,
    offchainFillCount: 0,
    unmatchedOffchainFillCount: 0
  })

  const addWindowSymbol = (target: PnlSymbolSummary, row: SyntheticPnlWindowSymbol): void => {
    const counter = Number(target.counterTradePnlUsd) + Number(row.counterTradePnlUsd)
    const onchain = Number(target.onchainNettingPnlUsd) + Number(row.onchainNettingPnlUsd)
    const baseline =
      Number(target.directionalInventoryBaselinePnlUsd) +
      Number(row.directionalInventoryBaselinePnlUsd)
    const excess =
      Number(target.directionalImbalanceExcessPnlUsd) + Number(row.directionalImbalanceExcessPnlUsd)
    const directional = baseline + excess
    const realized = counter + onchain

    target.counterTradePnlUsd = moneyText(counter)
    target.onchainNettingPnlUsd = moneyText(onchain)
    target.directionalInventoryBaselinePnlUsd = moneyText(baseline)
    target.directionalImbalanceExcessPnlUsd = moneyText(excess)
    target.directionalExposurePnlUsd = moneyText(directional)
    target.realizedPnlUsd = moneyText(realized)
    target.totalPnlUsd = moneyText(realized + directional)
    target.inventoryDriftUsd = target.directionalInventoryBaselinePnlUsd
  }

  const syntheticWindowsForCurrentFilter = (): SyntheticPnlWindow[] =>
    syntheticPnlDashboard.windows.filter(
      (window) => matchesDayFilter(window.isWeekend) && matchesDateRange(dateKey(window.startAt))
    )

  const syntheticEntriesForCurrentFilter = (): PnlEntry[] => {
    const selected = selectedSymbols.current

    return syntheticPnlDashboard.report.entries.filter((entry) => {
      if (selected.size > 0 && !selected.has(entry.symbol)) return false
      if (!matchesDateRange(dateKey(entry.closedAt))) return false
      return matchesDayFilter(isWeekendTimestamp(entry.closedAt))
    })
  }

  const syntheticSymbolRowsForCurrentFilter = (
    syntheticEntries: PnlEntry[]
  ): PnlSymbolSummary[] => {
    const selected = selectedSymbols.current
    const windows = syntheticWindowsForCurrentFilter()
    const symbols = new Map<string, PnlSymbolSummary>()

    for (const row of syntheticPnlDashboard.report.symbols) {
      if (selected.size > 0 && !selected.has(row.symbol)) continue
      symbols.set(row.symbol, emptySymbolSummary(row.symbol))
    }

    for (const window of windows) {
      for (const row of window.symbols) {
        if (selected.size > 0 && !selected.has(row.symbol)) continue
        const target = symbols.get(row.symbol)
        if (target) addWindowSymbol(target, row)
      }
    }

    for (const row of symbols.values()) {
      const symbolEntries = syntheticEntries.filter((entry) => entry.symbol === row.symbol)
      row.matchedLotCount = symbolEntries.length
      row.onchainFillCount = windows.length * 28
      row.offchainFillCount = windows.length * 20
      row.unmatchedOffchainFillCount = symbolEntries.filter(
        (entry) => entry.delayedCounterTrade
      ).length
      row.matchedShares = moneyText(
        symbolEntries.reduce((totalValue, entry) => totalValue + Number(entry.shares), 0)
      )
      row.unmatchedOffchainShares = moneyText(
        symbolEntries
          .filter((entry) => entry.delayedCounterTrade)
          .reduce((totalValue, entry) => totalValue + Number(entry.shares) * 0.08, 0)
      )
    }

    return [...symbols.values()].sort((left, right) => left.symbol.localeCompare(right.symbol))
  }

  const isCompleteSelection = (selected: Set<string>, symbols: string[]): boolean =>
    symbols.length > 0 && symbols.every((symbol) => selected.has(symbol))

  const reconcileSymbolSelection = (
    selected: Set<string>,
    previousSymbols: string[],
    nextSymbols: string[],
    replace = false
  ): Set<string> => {
    if (nextSymbols.length === 0) return new Set()

    const shouldSelectAll =
      replace ||
      previousSymbols.length === 0 ||
      selected.size === 0 ||
      isCompleteSelection(selected, previousSymbols)

    if (shouldSelectAll) return new Set(nextSymbols)

    const nextSymbolSet = new Set(nextSymbols)
    const filtered = [...selected].filter((symbol) => nextSymbolSet.has(symbol))
    return new Set(filtered.length > 0 ? filtered : nextSymbols)
  }

  const syncAvailableSymbols = (symbols: string[], replace = false) => {
    const previousSymbols = allSymbols.current
    const source = replace ? symbols : [...allSymbols.current, ...symbols]
    const sorted = [...new Set(source)].sort()
    allSymbols.update(() => sorted)

    selectedSymbols.update((selected) =>
      reconcileSymbolSelection(selected, previousSymbols, sorted, replace)
    )
    selectedAssetChartSymbols.update((selected) =>
      reconcileSymbolSelection(selected, previousSymbols, sorted, replace)
    )
    selectedMethodChartSymbols.update((selected) =>
      reconcileSymbolSelection(selected, previousSymbols, sorted, replace)
    )
  }

  const aggregateSummary = (symbols: PnlSymbolSummary[]): PnlSummary => {
    const base = syntheticPnlDashboard.report.summary
    const sums = symbols.reduce(
      (acc, row) => ({
        counterTradePnlUsd: acc.counterTradePnlUsd + Number(row.counterTradePnlUsd),
        onchainNettingPnlUsd: acc.onchainNettingPnlUsd + Number(row.onchainNettingPnlUsd),
        directionalInventoryBaselinePnlUsd:
          acc.directionalInventoryBaselinePnlUsd + Number(row.directionalInventoryBaselinePnlUsd),
        directionalImbalanceExcessPnlUsd:
          acc.directionalImbalanceExcessPnlUsd + Number(row.directionalImbalanceExcessPnlUsd),
        matchedLotCount: acc.matchedLotCount + row.matchedLotCount,
        onchainFillCount: acc.onchainFillCount + row.onchainFillCount,
        offchainFillCount: acc.offchainFillCount + row.offchainFillCount,
        unmatchedOffchainFillCount: acc.unmatchedOffchainFillCount + row.unmatchedOffchainFillCount,
        matchedShares: acc.matchedShares + Number(row.matchedShares),
        unmatchedOffchainShares: acc.unmatchedOffchainShares + Number(row.unmatchedOffchainShares)
      }),
      {
        counterTradePnlUsd: 0,
        onchainNettingPnlUsd: 0,
        directionalInventoryBaselinePnlUsd: 0,
        directionalImbalanceExcessPnlUsd: 0,
        matchedLotCount: 0,
        onchainFillCount: 0,
        offchainFillCount: 0,
        unmatchedOffchainFillCount: 0,
        matchedShares: 0,
        unmatchedOffchainShares: 0
      }
    )
    const directional =
      sums.directionalInventoryBaselinePnlUsd + sums.directionalImbalanceExcessPnlUsd
    const realized = sums.counterTradePnlUsd + sums.onchainNettingPnlUsd

    return {
      ...base,
      counterTradePnlUsd: moneyText(sums.counterTradePnlUsd),
      onchainNettingPnlUsd: moneyText(sums.onchainNettingPnlUsd),
      directionalInventoryBaselinePnlUsd: moneyText(sums.directionalInventoryBaselinePnlUsd),
      directionalImbalanceExcessPnlUsd: moneyText(sums.directionalImbalanceExcessPnlUsd),
      directionalExposurePnlUsd: moneyText(directional),
      realizedPnlUsd: moneyText(realized),
      totalPnlUsd: moneyText(realized + directional),
      inventoryDriftUsd: moneyText(sums.directionalInventoryBaselinePnlUsd),
      matchedLotCount: sums.matchedLotCount,
      onchainFillCount: sums.onchainFillCount,
      offchainFillCount: sums.offchainFillCount,
      unmatchedOffchainFillCount: sums.unmatchedOffchainFillCount,
      matchedShares: moneyText(sums.matchedShares),
      onchainNotionalUsd: moneyText(sums.matchedShares * 42),
      offchainNotionalUsd: moneyText(sums.matchedShares * 41.9),
      unmatchedOffchainShares: moneyText(sums.unmatchedOffchainShares),
      unmatchedOffchainNotionalUsd: moneyText(sums.unmatchedOffchainShares * 55)
    }
  }

  const loadSyntheticPnl = () => {
    const selectedEntries = syntheticEntriesForCurrentFilter()
    const selectedSymbolRows = syntheticSymbolRowsForCurrentFilter(selectedEntries)

    report.update(() => ({
      ...syntheticPnlDashboard.report,
      summary: aggregateSummary(selectedSymbolRows),
      symbols: selectedSymbolRows,
      entries: selectedEntries,
      total: selectedEntries.length,
      hasMore: false
    }))
    total.update(() => selectedEntries.length)
    entries.update(() => selectedEntries)
    syncAvailableSymbols(
      syntheticPnlDashboard.report.symbols.map((row) => row.symbol),
      true
    )
    error.update(() => null)
  }

  const fetchPnl = async () => {
    const seq = ++fetchSeq

    if (dataMode.current === 'synthetic') {
      loadSyntheticPnl()
      return
    }

    loading.update(() => true)
    error.update(() => null)
    const requestFromDate = fromDate.current
    const requestToDate = toDate.current
    lastRequestedDateRangeKey = dateRangeKey(requestFromDate, requestToDate)

    try {
      const data = await fetchPnlReport({
        limit: PNL_ENTRY_LIMIT,
        offset: 0,
        symbols: selectedSymbols.current,
        fromDate: requestFromDate,
        toDate: requestToDate,
        dayFilter: dayFilter.current
      })

      if (seq !== fetchSeq) return

      report.update(() => data)
      total.update(() => data.total)
      entries.update(() => data.entries)

      syncAvailableSymbols(data.symbolUniverse ?? data.symbols.map((summary) => summary.symbol))
    } catch (fetchError) {
      if (seq !== fetchSeq) return

      error.update(() => (fetchError instanceof Error ? fetchError.message : 'Unknown error'))
    } finally {
      if (seq === fetchSeq) loading.update(() => false)
    }
  }

  const refresh = () => {
    void fetchPnl()
  }

  const handleSymbolChange = (selected: Set<string>) => {
    selectedSymbols.update(() => (selected.size === 0 ? new Set(allSymbols.current) : selected))
    void fetchPnl()
  }

  const clearFilters = () => {
    selectedSymbols.update(() => new Set(allSymbols.current))
    void fetchPnl()
  }

  const handleAssetChartSymbolChange = (selected: Set<string>) => {
    selectedAssetChartSymbols.update(() => selected)
  }

  const handleAssetChartStreamChange = (selected: Set<string>) => {
    selectedAssetChartStreams.update(() => new Set([...selected].filter(isPnlStreamKey)))
  }

  const handleMethodChartSymbolChange = (selected: Set<string>) => {
    selectedMethodChartSymbols.update(() => selected)
  }

  const handleMethodChartStreamChange = (selected: Set<string>) => {
    selectedMethodChartStreams.update(() => new Set([...selected].filter(isPnlStreamKey)))
  }

  const setDayFilter = (filter: 'all' | 'weekday' | 'weekend') => {
    dayFilter.update(() => filter)
    refresh()
  }

  const setDateRange = (side: 'from' | 'to', value: string) => {
    const nextDate = clampDate(value)

    selectedPreset.update(() => 'custom')
    if (side === 'from') {
      fromDate.update(() => nextDate)
      if (nextDate > toDate.current) {
        toDate.update(() => nextDate)
      }
    } else {
      toDate.update(() => nextDate)
      if (nextDate < fromDate.current) {
        fromDate.update(() => nextDate)
      }
    }
    refresh()
  }

  const openDatePicker = (input: HTMLInputElement) => {
    try {
      input.showPicker()
    } catch {
      // Some browsers only allow showPicker from direct pointer activation.
    }
  }

  const applyRangePreset = (preset: RangePreset) => {
    selectedPreset.update(() => preset)
    fromDate.update(() => presetStart(preset))
    toDate.update(() => availableEndDate)
    refresh()
  }

  const presetButtons: { value: Exclude<RangePreset, 'custom'>; label: string }[] = [
    { value: '1w', label: '1W' },
    { value: '1m', label: '1M' },
    { value: 'ytd', label: 'YTD' },
    { value: '1y', label: '1Y' },
    { value: 'all', label: 'All' }
  ]

  const hasFilters = $derived(
    allSymbols.current.length > 0 &&
      !isCompleteSelection(selectedSymbols.current, allSymbols.current)
  )

  $effect(() => {
    if (!availableStartDate || !availableEndDate) return

    const nextFrom =
      selectedPreset.current === 'custom'
        ? clampDate(fromDate.current)
        : presetStart(selectedPreset.current)
    const nextTo =
      selectedPreset.current === 'custom' ? clampDate(toDate.current) : availableEndDate

    const nextRangeKey = dateRangeKey(nextFrom, nextTo)
    if (nextRangeKey === dateRangeKey(fromDate.current, toDate.current)) return

    fromDate.update(() => nextFrom)
    toDate.update(() => nextTo)
    if (nextRangeKey !== lastRequestedDateRangeKey) void fetchPnl()
  })

  onMount(() => {
    void fetchPnl()
    const interval = setInterval(() => {
      void fetchPnl()
    }, POLL_INTERVAL_MS)

    return () => {
      clearInterval(interval)
    }
  })

  const pnlColor = (value: string): string => {
    const comparison = decimalCompare(value, '0')

    if (comparison > 0) {
      return 'text-green-500'
    }

    if (comparison < 0) {
      return 'text-red-500'
    }

    return 'text-muted-foreground'
  }

  const directionColor = (direction: string): string =>
    direction.toLowerCase() === 'buy' ? 'text-green-500' : 'text-red-500'

  const fmtUsd = (value: string): string => `$${formatDecimal(value, 2)}`
  const fmtSignedUsd = (value: string): string => {
    const prefix = decimalCompare(value, '0') > 0 ? '+' : ''
    return `${prefix}${fmtUsd(value)}`
  }
  const fmtShares = (value: string): string => formatDecimal(value, 4)
  const moneyText = (value: number): string => value.toFixed(2).replace(/\.?0+$/u, '')
  const fmtDuration = (seconds: number): string => {
    if (seconds < 60) return `${String(seconds)}s`
    const minutes = Math.floor(seconds / 60)
    const remainingSeconds = seconds % 60
    return remainingSeconds === 0
      ? `${String(minutes)}m`
      : `${String(minutes)}m ${String(remainingSeconds)}s`
  }
  const fmtCount = (value: number): string => new Intl.NumberFormat('en-US').format(value)
  const fmtMaybeUtc = (value: string | null): string => (value === null ? 'n/a' : formatUtc(value))

  const shortId = (id: string): string => {
    if (id.length <= 18) return id
    return `${id.slice(0, 8)}...${id.slice(-6)}`
  }

  const chartSymbolUniverse = $derived(
    allSymbols.current.length > 0 ? allSymbols.current : defaultChartSymbols
  )
  const chartSymbolOptions = $derived(
    chartSymbolUniverse.map((symbol) => ({
      value: symbol,
      label: symbol
    }))
  )
  const selectedAssetChartSymbolValues = $derived(
    new Set(chartSymbolUniverse.filter((symbol) => selectedAssetChartSymbols.current.has(symbol)))
  )
  const assetChartSymbols = $derived(
    chartSymbolUniverse.filter((symbol) => selectedAssetChartSymbols.current.has(symbol))
  )
  const selectedAssetChartStreamValues = $derived(
    new Set<string>(STREAM_KEYS.filter((stream) => selectedAssetChartStreams.current.has(stream)))
  )
  const assetChartStreams = $derived(
    STREAM_KEYS.filter((stream) => selectedAssetChartStreams.current.has(stream))
  )
  const selectedMethodChartSymbolValues = $derived(
    new Set(chartSymbolUniverse.filter((symbol) => selectedMethodChartSymbols.current.has(symbol)))
  )
  const methodChartSymbols = $derived(
    chartSymbolUniverse.filter((symbol) => selectedMethodChartSymbols.current.has(symbol))
  )
  const selectedMethodChartStreamValues = $derived(
    new Set<string>(STREAM_KEYS.filter((stream) => selectedMethodChartStreams.current.has(stream)))
  )
  const methodChartStreams = $derived(
    STREAM_KEYS.filter((stream) => selectedMethodChartStreams.current.has(stream))
  )

  const chartWindowSource = $derived(report.current?.windows ?? syntheticPnlDashboard.windows)
  const chartWindows = $derived(
    chartWindowSource.filter(
      (window) => matchesDayFilter(window.isWeekend) && matchesDateRange(dateKey(window.startAt))
    )
  )

  type BucketCadence = 'day' | 'week' | 'month'

  type BucketWindow = {
    key: string
    label: string
    windows: SyntheticPnlWindow[]
  }

  type RawBarSegment = {
    key: string
    label: string
    value: number
    color: string
  }

  type BarSegment = RawBarSegment & {
    x: number
    y: number
    width: number
    height: number
  }

  type BarRow = {
    key: string
    label: string
    labelX: number
    total: number
    segments: BarSegment[]
  }

  type AreaPoint = {
    x: number
    y0: number
    y1: number
  }

  type AreaLayer = {
    key: string
    label: string
    color: string
    path: string
    points: AreaPoint[]
  }

  type AreaLabel = {
    key: string
    label: string
    x: number
  }

  type AreaLayout = {
    layers: AreaLayer[]
    labels: AreaLabel[]
    zeroY: number
    min: number
    max: number
  }

  const barChartWidth = 860
  const barChartHeight = 330
  const barChartViewBox = ['0', '0', String(barChartWidth), String(barChartHeight)].join(' ')
  const barPadX = 92
  const barPadTop = 24
  const barPadBottom = 50
  const labelY = barChartHeight - 18
  const rotateLabel = (x: number): string => `rotate(-45 ${String(x)} ${String(labelY)})`

  const assetColor = (index: number): string => assetColors[index % assetColors.length] ?? '#94a3b8'
  const assetColorForSymbol = (symbol: string): string =>
    assetColor(Math.max(0, chartSymbolUniverse.indexOf(symbol)))

  const selectedRangeDays = $derived(
    Math.floor(
      Math.abs(dateFromIso(toDate.current).getTime() - dateFromIso(fromDate.current).getTime()) /
        MS_PER_DAY
    ) + 1
  )

  const bucketCadence = $derived<BucketCadence>(
    selectedRangeDays > 183 ? 'month' : selectedRangeDays > 31 ? 'week' : 'day'
  )

  const startOfWeek = (value: string): string => {
    const date = dateFromIso(value)
    const dayOffset = (date.getUTCDay() + 6) % 7
    date.setUTCDate(date.getUTCDate() - dayOffset)
    return isoDate(date)
  }

  const bucketKeyFor = (window: SyntheticPnlWindow, cadence: BucketCadence): string => {
    const key = dateKey(window.startAt)
    if (cadence === 'month') return key.slice(0, 7)
    if (cadence === 'week') return startOfWeek(key)
    return key
  }

  const monthLabelFor = (key: string): string => {
    const date = new Date(`${key}-01T00:00:00.000Z`)
    return date.toLocaleDateString('en-US', { month: 'short', year: '2-digit', timeZone: 'UTC' })
  }

  const weekLabelFor = (key: string): string => {
    const weekStart = dateFromIso(key)
    const weekEnd = dateFromIso(key)
    weekEnd.setUTCDate(weekStart.getUTCDate() + 6)

    return `${key.slice(5)}-${isoDate(weekEnd).slice(5)}`
  }

  const bucketLabelFor = (key: string, cadence: BucketCadence): string => {
    if (cadence === 'month') return monthLabelFor(key)
    if (cadence === 'week') return weekLabelFor(key)
    return key.slice(5)
  }

  const bucketWindows = (windows: SyntheticPnlWindow[], cadence: BucketCadence): BucketWindow[] => {
    const buckets = new Map<string, BucketWindow>()

    for (const window of windows) {
      const key = bucketKeyFor(window, cadence)
      const bucket = buckets.get(key)

      if (bucket) {
        bucket.windows.push(window)
      } else {
        buckets.set(key, {
          key,
          label: bucketLabelFor(key, cadence),
          windows: [window]
        })
      }
    }

    return [...buckets.values()]
  }

  const windowValueForSymbol = (
    window: SyntheticPnlWindow,
    symbol: string,
    streams: PnlStreamKey[]
  ): number => {
    const row = window.symbols.find((candidate) => candidate.symbol === symbol)
    if (!row) return 0

    return streams.reduce((totalValue, stream) => totalValue + Number(row[stream]), 0)
  }

  const layoutVerticalStackBars = (
    rows: { key: string; label: string; segments: RawBarSegment[] }[]
  ): BarRow[] => {
    const maxAbs = Math.max(
      1,
      ...rows.flatMap((row) => {
        const positive = row.segments
          .filter((segment) => segment.value > 0)
          .reduce((totalValue, segment) => totalValue + segment.value, 0)
        const negative = Math.abs(
          row.segments
            .filter((segment) => segment.value < 0)
            .reduce((totalValue, segment) => totalValue + segment.value, 0)
        )

        return [positive, negative]
      })
    )
    const plotWidth = barChartWidth - barPadX * 2
    const plotHeight = barChartHeight - barPadTop - barPadBottom
    const slotWidth = rows.length > 0 ? plotWidth / rows.length : plotWidth
    const barWidth = Math.min(34, Math.max(1, slotWidth * 0.68))
    const zeroY = barPadTop + plotHeight / 2

    return rows.map((row, rowIndex) => {
      let positiveCursor = 0
      let negativeCursor = 0
      const x = barPadX + rowIndex * slotWidth + (slotWidth - barWidth) / 2
      const segments = row.segments.map((segment) => {
        const height = (Math.abs(segment.value) / maxAbs) * (plotHeight / 2)

        if (segment.value >= 0) {
          const y = zeroY - positiveCursor - height
          positiveCursor += height
          return { ...segment, x, y, width: barWidth, height }
        }

        const y = zeroY + negativeCursor
        negativeCursor += height
        return { ...segment, x, y, width: barWidth, height }
      })

      return {
        key: row.key,
        label: row.label,
        labelX: x + barWidth / 2,
        total: row.segments.reduce((totalValue, segment) => totalValue + segment.value, 0),
        segments
      }
    })
  }

  const layoutCumulativeArea = (
    rows: { key: string; label: string; segments: RawBarSegment[] }[]
  ): AreaLayout => {
    const cumulativeByKey = new Map<string, number>()
    const cumulativeRows = rows.map((row) => ({
      key: row.key,
      label: row.label,
      segments: row.segments.map((segment) => {
        const cumulative = (cumulativeByKey.get(segment.key) ?? 0) + segment.value
        cumulativeByKey.set(segment.key, cumulative)
        return { ...segment, cumulative }
      })
    }))
    const allValues = cumulativeRows.flatMap((row) =>
      row.segments.map((segment) => segment.cumulative)
    )
    const min = Math.min(0, ...allValues)
    const max = Math.max(0, ...allValues)
    const range = max - min || 1
    const plotWidth = barChartWidth - barPadX * 2
    const plotHeight = barChartHeight - barPadTop - barPadBottom
    const xDenominator = Math.max(1, rows.length - 1)
    const yFor = (value: number): number => barPadTop + ((max - value) / range) * plotHeight
    const xFor = (index: number): number =>
      rows.length <= 1 ? barPadX + plotWidth / 2 : barPadX + (index / xDenominator) * plotWidth
    const keys = [...new Set(rows.flatMap((row) => row.segments.map((segment) => segment.key)))]
    const layers = keys.map((key) => {
      const points = cumulativeRows.map((row, rowIndex) => {
        const segment = row.segments.find((candidate) => candidate.key === key)
        const cumulative = segment?.cumulative ?? 0
        return {
          x: xFor(rowIndex),
          y0: yFor(0),
          y1: yFor(cumulative)
        }
      })
      const top = points.map((point) => `${point.x.toFixed(1)},${point.y1.toFixed(1)}`)
      const bottom = [...points]
        .reverse()
        .map((point) => `${point.x.toFixed(1)},${point.y0.toFixed(1)}`)
      const firstSegment = rows
        .flatMap((row) => row.segments)
        .find((segment) => segment.key === key)

      return {
        key,
        label: firstSegment?.label ?? key,
        color: firstSegment?.color ?? '#94a3b8',
        path: [...top, ...bottom].join(' '),
        points
      }
    })

    return {
      min,
      max,
      zeroY: yFor(0),
      labels: rows.map((row, rowIndex) => ({
        key: row.key,
        label: row.label,
        x: xFor(rowIndex)
      })),
      layers
    }
  }

  const stackAxisLimit = (rows: BarRow[]): number =>
    Math.max(
      1,
      ...rows.map((row) => {
        const positive = row.segments
          .filter((segment) => segment.value > 0)
          .reduce((totalValue, segment) => totalValue + segment.value, 0)
        const negative = Math.abs(
          row.segments
            .filter((segment) => segment.value < 0)
            .reduce((totalValue, segment) => totalValue + segment.value, 0)
        )

        return Math.max(positive, negative)
      })
    )

  const timeBuckets = $derived.by(() => bucketWindows(chartWindows, bucketCadence))
  const barZeroY = $derived(barPadTop + (barChartHeight - barPadTop - barPadBottom) / 2)
  const cadenceLabel = $derived(
    bucketCadence === 'month' ? 'monthly' : bucketCadence === 'week' ? 'weekly' : 'daily'
  )
  const bucketSemantics = $derived(
    bucketCadence === 'day'
      ? 'Each bar is one calendar day of non-cumulative PnL.'
      : `Each bar sums non-cumulative daily PnL into one ${bucketCadence}.`
  )

  const byAssetBarRows = $derived.by(() =>
    layoutVerticalStackBars(
      timeBuckets.map((bucket) => ({
        key: bucket.key,
        label: bucket.label,
        segments: assetChartSymbols.map((symbol) => ({
          key: symbol,
          label: symbol,
          value: bucket.windows.reduce(
            (totalValue, window) =>
              totalValue + windowValueForSymbol(window, symbol, assetChartStreams),
            0
          ),
          color: assetColorForSymbol(symbol)
        }))
      }))
    )
  )
  const byAssetBarAxisLimit = $derived(stackAxisLimit(byAssetBarRows))

  const byMethodBarRows = $derived.by(() =>
    layoutVerticalStackBars(
      timeBuckets.map((bucket) => ({
        key: bucket.key,
        label: bucket.label,
        segments: methodChartStreams.map((stream) => ({
          key: stream,
          label: streamLabels[stream],
          value: bucket.windows.reduce(
            (bucketTotal, window) =>
              bucketTotal +
              methodChartSymbols.reduce(
                (symbolTotal, symbol) =>
                  symbolTotal + windowValueForSymbol(window, symbol, [stream]),
                0
              ),
            0
          ),
          color: streamColors[stream]
        }))
      }))
    )
  )
  const byMethodBarAxisLimit = $derived(stackAxisLimit(byMethodBarRows))
  const byAssetArea = $derived.by(() =>
    layoutCumulativeArea(
      timeBuckets.map((bucket) => ({
        key: bucket.key,
        label: bucket.label,
        segments: assetChartSymbols.map((symbol) => ({
          key: symbol,
          label: symbol,
          value: bucket.windows.reduce(
            (totalValue, window) =>
              totalValue + windowValueForSymbol(window, symbol, assetChartStreams),
            0
          ),
          color: assetColorForSymbol(symbol)
        }))
      }))
    )
  )

  const byMethodArea = $derived.by(() =>
    layoutCumulativeArea(
      timeBuckets.map((bucket) => ({
        key: bucket.key,
        label: bucket.label,
        segments: methodChartStreams.map((stream) => ({
          key: stream,
          label: streamLabels[stream],
          value: bucket.windows.reduce(
            (bucketTotal, window) =>
              bucketTotal +
              methodChartSymbols.reduce(
                (symbolTotal, symbol) =>
                  symbolTotal + windowValueForSymbol(window, symbol, [stream]),
                0
              ),
            0
          ),
          color: streamColors[stream]
        }))
      }))
    )
  )

  const axisUsd = (value: number): string => {
    const prefix = value > 0 ? '+' : ''
    return `${prefix}$${value.toFixed(0)}`
  }
</script>

<Card.Root class="flex h-full flex-col overflow-hidden border-l-4 border-l-amber-500/60">
  <Card.Header class="shrink-0 space-y-3 pb-0">
    <Card.Title class="flex flex-wrap items-center justify-between gap-2">
      <span>PnL</span>

      <div class="flex items-center gap-2">
        {#if !mockMode && hasFilters}
          <button
            class="rounded border bg-background px-2 py-1 text-xs hover:bg-accent"
            onclick={clearFilters}>All assets</button
          >
        {/if}
        <button
          class="rounded border bg-background px-2 py-1 text-xs hover:bg-accent"
          onclick={refresh}
          disabled={loading.current}
        >
          {loading.current ? 'Loading...' : 'Refresh'}
        </button>
      </div>
    </Card.Title>

    <div class="flex flex-wrap items-center gap-2 rounded-md bg-muted/30 px-2 py-1.5 text-xs">
      {#if !mockMode && symbolOptions.length > 0}
        <MultiSelect
          label="Assets"
          options={symbolOptions}
          selected={selectedSymbols.current}
          onchange={handleSymbolChange}
        />
      {/if}
      <div class="inline-flex overflow-hidden rounded border text-xs">
        {#each presetButtons as preset (preset.value)}
          <button
            class="px-2 py-1 hover:bg-accent {selectedPreset.current === preset.value
              ? 'bg-amber-500/20 text-amber-200'
              : 'bg-background'}"
            onclick={() => {
              applyRangePreset(preset.value)
            }}
          >
            {preset.label}
          </button>
        {/each}
      </div>
      <label class="inline-flex items-center gap-2 rounded border bg-background px-2 py-1">
        <span class="text-muted-foreground">From</span>
        <input
          class="w-32 cursor-pointer rounded bg-muted/40 px-1 py-0.5 font-mono text-xs text-foreground outline-none hover:bg-muted"
          type="date"
          min={availableStartDate}
          max={toDate.current || availableEndDate}
          value={fromDate.current}
          onchange={(event) => {
            setDateRange('from', event.currentTarget.value)
          }}
          onfocus={(event) => {
            openDatePicker(event.currentTarget)
          }}
          onclick={(event) => {
            openDatePicker(event.currentTarget)
          }}
        />
      </label>
      <label class="inline-flex items-center gap-2 rounded border bg-background px-2 py-1">
        <span class="text-muted-foreground">To</span>
        <input
          class="w-32 cursor-pointer rounded bg-muted/40 px-1 py-0.5 font-mono text-xs text-foreground outline-none hover:bg-muted"
          type="date"
          min={fromDate.current || availableStartDate}
          max={availableEndDate}
          value={toDate.current}
          onchange={(event) => {
            setDateRange('to', event.currentTarget.value)
          }}
          onfocus={(event) => {
            openDatePicker(event.currentTarget)
          }}
          onclick={(event) => {
            openDatePicker(event.currentTarget)
          }}
        />
      </label>
      <div class="inline-flex overflow-hidden rounded border text-xs">
        <button
          class="px-2 py-1 hover:bg-accent {dayFilter.current === 'all'
            ? 'bg-amber-500/20 text-amber-200'
            : 'bg-background'}"
          onclick={() => {
            setDayFilter('all')
          }}>All days</button
        >
        <button
          class="border-l px-2 py-1 hover:bg-accent {dayFilter.current === 'weekday'
            ? 'bg-amber-500/20 text-amber-200'
            : 'bg-background'}"
          onclick={() => {
            setDayFilter('weekday')
          }}>Weekdays</button
        >
        <button
          class="border-l px-2 py-1 hover:bg-accent {dayFilter.current === 'weekend'
            ? 'bg-amber-500/20 text-amber-200'
            : 'bg-background'}"
          onclick={() => {
            setDayFilter('weekend')
          }}>Weekends</button
        >
      </div>
      <span class="text-muted-foreground">
        Showing {fromDate.current} to {toDate.current} as {timeBuckets.length}
        {cadenceLabel} non-cumulative buckets. Cash values treat USD and USDC 1:1; portfolio percentage
        return is not shown because historical NAV is not persisted.
      </span>
    </div>
  </Card.Header>

  <Card.Content class="relative min-h-0 flex-1 overflow-auto px-6 pt-3">
    {#if error.current}
      <div class="flex h-full items-center justify-center text-destructive">
        Failed to load PnL: {error.current}
      </div>
    {:else if !report.current && loading.current}
      <div class="flex h-full items-center justify-center text-muted-foreground">
        Loading PnL...
      </div>
    {:else if report.current}
      {@const summary = report.current.summary}

      {#if report.current.sampleStats}
        {@const sample = report.current.sampleStats}
        <section class="mb-3 rounded-xl border bg-card/50 p-3">
          <div class="flex flex-wrap items-start justify-between gap-3">
            <div>
              <div class="text-sm font-semibold">Database Trade Sample</div>
              <div class="mt-1 text-xs text-muted-foreground">
                Full unfiltered Position fill history available through the SQL JSON endpoint. Use
                this range when choosing dashboard dates.
              </div>
            </div>
            <div class="grid gap-2 text-xs sm:grid-cols-2 lg:grid-cols-5">
              <div class="rounded border bg-background/50 px-3 py-2">
                <div class="text-muted-foreground">First fill</div>
                <div class="mt-1 font-mono">{fmtMaybeUtc(sample.firstAt)}</div>
              </div>
              <div class="rounded border bg-background/50 px-3 py-2">
                <div class="text-muted-foreground">Last fill</div>
                <div class="mt-1 font-mono">{fmtMaybeUtc(sample.lastAt)}</div>
              </div>
              <div class="rounded border bg-background/50 px-3 py-2">
                <div class="text-muted-foreground">Total fills</div>
                <div class="mt-1 font-mono">{fmtCount(sample.totalFillCount)}</div>
              </div>
              <div class="rounded border bg-background/50 px-3 py-2">
                <div class="text-muted-foreground">On / off-chain</div>
                <div class="mt-1 font-mono">
                  {fmtCount(sample.onchainFillCount)} / {fmtCount(sample.offchainFillCount)}
                </div>
              </div>
              <div class="rounded border bg-background/50 px-3 py-2">
                <div class="text-muted-foreground">Assets</div>
                <div class="mt-1 font-mono">{fmtCount(sample.symbolCount)}</div>
              </div>
            </div>
          </div>

          <div class="mt-3 flex gap-2 overflow-x-auto pb-1 text-xs">
            {#each sample.symbols as row (row.symbol)}
              <div class="min-w-44 rounded border bg-background/40 px-3 py-2">
                <div class="flex items-center justify-between gap-2">
                  <span class="font-mono font-semibold">{row.symbol}</span>
                  <span class="font-mono text-muted-foreground"
                    >{fmtCount(row.totalFillCount)} fills</span
                  >
                </div>
                <div class="mt-1 font-mono text-[11px] text-muted-foreground">
                  {fmtCount(row.onchainFillCount)} on / {fmtCount(row.offchainFillCount)} off
                </div>
                <div class="mt-1 text-[11px] text-muted-foreground">
                  {row.firstAt?.slice(0, 10) ?? 'n/a'} to {row.lastAt?.slice(0, 10) ?? 'n/a'}
                </div>
              </div>
            {/each}
          </div>
        </section>
      {/if}

      <div class="grid gap-3 md:grid-cols-2 xl:grid-cols-6">
        <div class="rounded-lg border bg-card/60 p-3">
          <div class="text-xs text-muted-foreground">Total PnL</div>
          <div class="mt-1 font-mono text-xl font-semibold {pnlColor(summary.totalPnlUsd)}">
            {fmtSignedUsd(summary.totalPnlUsd)}
          </div>
        </div>

        <div class="rounded-lg border bg-card/60 p-3">
          <div class="text-xs text-muted-foreground">Counter-Trade PnL</div>
          <div class="mt-1 font-mono text-xl font-semibold {pnlColor(summary.counterTradePnlUsd)}">
            {fmtSignedUsd(summary.counterTradePnlUsd)}
          </div>
          <div class="mt-1 text-xs text-muted-foreground">Broker hedge stream</div>
        </div>

        <div class="rounded-lg border bg-card/60 p-3">
          <div class="text-xs text-muted-foreground">On-Chain Netting PnL</div>
          <div
            class="mt-1 font-mono text-xl font-semibold {pnlColor(summary.onchainNettingPnlUsd)}"
          >
            {fmtSignedUsd(summary.onchainNettingPnlUsd)}
          </div>
          <div class="mt-1 text-xs text-muted-foreground">
            Users passively crossed open inventory
          </div>
        </div>

        <div class="rounded-lg border bg-card/60 p-3">
          <div class="text-xs text-muted-foreground">Directional Excess PnL</div>
          <div
            class="mt-1 font-mono text-xl font-semibold {pnlColor(
              summary.directionalImbalanceExcessPnlUsd
            )}"
          >
            {fmtSignedUsd(summary.directionalImbalanceExcessPnlUsd)}
          </div>
          <div class="mt-1 text-xs text-muted-foreground">
            Uncountered or delayed hedge exposure
          </div>
        </div>

        <div class="rounded-lg border bg-card/60 p-3">
          <div class="text-xs text-muted-foreground">Baseline Drift PnL</div>
          <div
            class="mt-1 font-mono text-xl font-semibold {pnlColor(
              summary.directionalInventoryBaselinePnlUsd
            )}"
          >
            {fmtSignedUsd(summary.directionalInventoryBaselinePnlUsd)}
          </div>
          <div class="mt-1 text-xs text-muted-foreground">Start-of-window inventory move</div>
        </div>

        <div class="rounded-lg border bg-card/60 p-3">
          <div class="text-xs text-muted-foreground">Directional Total PnL</div>
          <div
            class="mt-1 font-mono text-xl font-semibold {pnlColor(
              summary.directionalExposurePnlUsd
            )}"
          >
            {fmtSignedUsd(summary.directionalExposurePnlUsd)}
          </div>
          <div class="mt-1 text-xs text-muted-foreground">
            Open long {fmtShares(summary.openLongShares)} / short {fmtShares(
              summary.openShortShares
            )}
          </div>
        </div>

        <div class="rounded-lg border bg-card/60 p-3">
          <div class="text-xs text-muted-foreground">Unmatched Counter Trades</div>
          <div
            class="mt-1 font-mono text-xl font-semibold {summary.unmatchedOffchainFillCount > 0
              ? 'text-amber-300'
              : 'text-muted-foreground'}"
          >
            {fmtShares(summary.unmatchedOffchainShares)}
          </div>
          <div class="mt-1 font-mono text-xs text-muted-foreground">
            {fmtUsd(summary.unmatchedOffchainNotionalUsd)}
          </div>
        </div>
      </div>

      {#if report.current.warnings.length > 0}
        <details
          class="mt-3 rounded-xl border border-amber-500/30 bg-amber-500/10 p-3 text-sm text-amber-100"
        >
          <summary class="cursor-pointer select-none font-semibold">
            Debug ({report.current.warnings.length})
          </summary>
          <div class="mt-3 space-y-1 font-mono text-xs leading-5">
            {#each report.current.warnings as warning (warning)}
              <div class="break-words">{warning}</div>
            {/each}
          </div>
        </details>
      {/if}

      <details class="mt-3 rounded-xl border bg-card/40 p-3 text-sm">
        <summary class="cursor-pointer select-none font-semibold">
          Math / formula reference
        </summary>
        <div class="mt-3 grid gap-3 lg:grid-cols-2">
          {#each formulaRows as formula (formula.label)}
            <div class="rounded-lg border bg-background/50 p-3">
              <div class="text-xs font-semibold uppercase tracking-wide text-muted-foreground">
                {formula.label}
              </div>
              <code class="mt-2 block rounded bg-muted/50 px-2 py-1 font-mono text-xs">
                {formula.formula}
              </code>
              <p class="mt-2 text-xs leading-5 text-muted-foreground">
                {formula.note}
              </p>
            </div>
          {/each}
        </div>
      </details>

      <div class="mt-4 grid gap-4 xl:grid-cols-2">
        <section class="overflow-hidden rounded-xl border bg-card/40">
          <div class="flex flex-wrap items-center justify-between gap-2 border-b px-3 py-2">
            <div>
              <div class="text-sm font-semibold">PnL by Asset per Time Unit</div>
              <div class="text-xs text-muted-foreground">
                {bucketSemantics} Assets are stacked using selected streams.
              </div>
            </div>
            <div class="flex flex-wrap items-center gap-2 text-xs">
              <MultiSelect
                label="Assets"
                options={chartSymbolOptions}
                selected={selectedAssetChartSymbolValues}
                onchange={handleAssetChartSymbolChange}
              />
              <MultiSelect
                label="Streams"
                options={streamOptions}
                selected={selectedAssetChartStreamValues}
                onchange={handleAssetChartStreamChange}
              />
              {#each assetChartSymbols as symbol (symbol)}
                <span class="inline-flex items-center gap-1 rounded-full border px-2 py-0.5">
                  <span class="h-2 w-2 rounded-full" style:background={assetColorForSymbol(symbol)}
                  ></span>
                  {symbol}
                </span>
              {/each}
            </div>
          </div>

          <div class="overflow-x-auto p-3">
            <svg viewBox={barChartViewBox} class="h-80 min-w-[860px] w-full">
              <rect
                x="0"
                y="0"
                width={barChartWidth}
                height={barChartHeight}
                rx="14"
                class="fill-background/30"
              />
              <line
                x1={barPadX}
                x2={barChartWidth - barPadX}
                y1={barZeroY}
                y2={barZeroY}
                class="stroke-border"
              />
              <line
                x1={barPadX}
                x2={barPadX}
                y1={barPadTop}
                y2={barChartHeight - barPadBottom}
                class="stroke-border"
              />
              <text x="8" y={barPadTop + 4} class="fill-muted-foreground text-[11px]"
                >{axisUsd(byAssetBarAxisLimit)}</text
              >
              <text x="8" y={barZeroY + 4} class="fill-muted-foreground text-[11px]">$0</text>
              <text
                x="8"
                y={barChartHeight - barPadBottom}
                class="fill-muted-foreground text-[11px]">{axisUsd(-byAssetBarAxisLimit)}</text
              >

              {#each byAssetBarRows as row (row.key)}
                {#each row.segments as segment (segment.key)}
                  {#if segment.height > 0.1}
                    <rect
                      x={segment.x}
                      y={segment.y}
                      width={segment.width}
                      height={segment.height}
                      rx="3"
                      fill={segment.color}
                      opacity="0.82"
                    />
                  {/if}
                {/each}
                <text
                  x={row.labelX}
                  y={labelY}
                  text-anchor="middle"
                  transform={rotateLabel(row.labelX)}
                  class="fill-muted-foreground text-[10px]"
                >
                  {row.label}
                </text>
              {/each}
            </svg>
          </div>
        </section>

        <section class="overflow-hidden rounded-xl border bg-card/40">
          <div class="flex flex-wrap items-center justify-between gap-2 border-b px-3 py-2">
            <div>
              <div class="text-sm font-semibold">PnL by PnL-Stream per Time Unit</div>
              <div class="text-xs text-muted-foreground">
                {bucketSemantics} PnL methods are stacked for selected assets.
              </div>
            </div>
            <div class="flex flex-wrap items-center gap-2 text-xs">
              <MultiSelect
                label="Assets"
                options={chartSymbolOptions}
                selected={selectedMethodChartSymbolValues}
                onchange={handleMethodChartSymbolChange}
              />
              <MultiSelect
                label="Streams"
                options={streamOptions}
                selected={selectedMethodChartStreamValues}
                onchange={handleMethodChartStreamChange}
              />
              {#each methodChartStreams as stream (stream)}
                <span class="inline-flex items-center gap-1 rounded-full border px-2 py-0.5">
                  <span class="h-2 w-2 rounded-full" style:background={streamColors[stream]}></span>
                  {streamLabels[stream]}
                </span>
              {/each}
            </div>
          </div>

          <div class="overflow-x-auto p-3">
            <svg viewBox={barChartViewBox} class="h-80 min-w-[860px] w-full">
              <rect
                x="0"
                y="0"
                width={barChartWidth}
                height={barChartHeight}
                rx="14"
                class="fill-background/30"
              />
              <line
                x1={barPadX}
                x2={barChartWidth - barPadX}
                y1={barZeroY}
                y2={barZeroY}
                class="stroke-border"
              />
              <line
                x1={barPadX}
                x2={barPadX}
                y1={barPadTop}
                y2={barChartHeight - barPadBottom}
                class="stroke-border"
              />
              <text x="8" y={barPadTop + 4} class="fill-muted-foreground text-[11px]"
                >{axisUsd(byMethodBarAxisLimit)}</text
              >
              <text x="8" y={barZeroY + 4} class="fill-muted-foreground text-[11px]">$0</text>
              <text
                x="8"
                y={barChartHeight - barPadBottom}
                class="fill-muted-foreground text-[11px]">{axisUsd(-byMethodBarAxisLimit)}</text
              >

              {#each byMethodBarRows as row (row.key)}
                {#each row.segments as segment (segment.key)}
                  {#if segment.height > 0.1}
                    <rect
                      x={segment.x}
                      y={segment.y}
                      width={segment.width}
                      height={segment.height}
                      rx="3"
                      fill={segment.color}
                      opacity="0.82"
                    />
                  {/if}
                {/each}
                <text
                  x={row.labelX}
                  y={labelY}
                  text-anchor="middle"
                  transform={rotateLabel(row.labelX)}
                  class="fill-muted-foreground text-[10px]"
                >
                  {row.label}
                </text>
              {/each}
            </svg>
          </div>
        </section>
      </div>

      <div class="mt-4 grid gap-4 xl:grid-cols-2">
        <section class="overflow-hidden rounded-xl border bg-card/40">
          <div class="flex flex-wrap items-center justify-between gap-2 border-b px-3 py-2">
            <div>
              <div class="text-sm font-semibold">Cumulative PnL by Asset</div>
              <div class="text-xs text-muted-foreground">
                Cumulative PnL on the y-axis using the same assets and streams as the asset bar
                chart.
              </div>
            </div>
            <div class="flex flex-wrap items-center gap-2 text-xs">
              <MultiSelect
                label="Assets"
                options={chartSymbolOptions}
                selected={selectedAssetChartSymbolValues}
                onchange={handleAssetChartSymbolChange}
              />
              <MultiSelect
                label="Streams"
                options={streamOptions}
                selected={selectedAssetChartStreamValues}
                onchange={handleAssetChartStreamChange}
              />
            </div>
          </div>

          <div class="overflow-x-auto p-3">
            <svg viewBox={barChartViewBox} class="h-80 min-w-[860px] w-full">
              <rect
                x="0"
                y="0"
                width={barChartWidth}
                height={barChartHeight}
                rx="14"
                class="fill-background/30"
              />
              <line
                x1={barPadX}
                x2={barChartWidth - barPadX}
                y1={byAssetArea.zeroY}
                y2={byAssetArea.zeroY}
                class="stroke-border"
              />
              <line
                x1={barPadX}
                x2={barPadX}
                y1={barPadTop}
                y2={barChartHeight - barPadBottom}
                class="stroke-border"
              />
              <text x="8" y={barPadTop + 4} class="fill-muted-foreground text-[11px]"
                >{axisUsd(byAssetArea.max)}</text
              >
              <text x="8" y={byAssetArea.zeroY + 4} class="fill-muted-foreground text-[11px]"
                >$0</text
              >
              <text
                x="8"
                y={barChartHeight - barPadBottom}
                class="fill-muted-foreground text-[11px]">{axisUsd(byAssetArea.min)}</text
              >

              {#each byAssetArea.layers as layer (layer.key)}
                <polygon points={layer.path} fill={layer.color} opacity="0.3" />
                <polyline
                  points={layer.points
                    .map((point) => `${point.x.toFixed(1)},${point.y1.toFixed(1)}`)
                    .join(' ')}
                  fill="none"
                  stroke={layer.color}
                  stroke-width="1.6"
                  opacity="0.9"
                />
              {/each}
              {#each byAssetArea.labels as label (label.key)}
                <text
                  x={label.x}
                  y={labelY}
                  text-anchor="middle"
                  transform={rotateLabel(label.x)}
                  class="fill-muted-foreground text-[10px]"
                >
                  {label.label}
                </text>
              {/each}
            </svg>
          </div>
        </section>

        <section class="overflow-hidden rounded-xl border bg-card/40">
          <div class="flex flex-wrap items-center justify-between gap-2 border-b px-3 py-2">
            <div>
              <div class="text-sm font-semibold">Cumulative PnL by PnL-Stream</div>
              <div class="text-xs text-muted-foreground">
                Cumulative PnL on the y-axis using the same assets and streams as the stream bar
                chart.
              </div>
            </div>
            <div class="flex flex-wrap items-center gap-2 text-xs">
              <MultiSelect
                label="Assets"
                options={chartSymbolOptions}
                selected={selectedMethodChartSymbolValues}
                onchange={handleMethodChartSymbolChange}
              />
              <MultiSelect
                label="Streams"
                options={streamOptions}
                selected={selectedMethodChartStreamValues}
                onchange={handleMethodChartStreamChange}
              />
            </div>
          </div>

          <div class="overflow-x-auto p-3">
            <svg viewBox={barChartViewBox} class="h-80 min-w-[860px] w-full">
              <rect
                x="0"
                y="0"
                width={barChartWidth}
                height={barChartHeight}
                rx="14"
                class="fill-background/30"
              />
              <line
                x1={barPadX}
                x2={barChartWidth - barPadX}
                y1={byMethodArea.zeroY}
                y2={byMethodArea.zeroY}
                class="stroke-border"
              />
              <line
                x1={barPadX}
                x2={barPadX}
                y1={barPadTop}
                y2={barChartHeight - barPadBottom}
                class="stroke-border"
              />
              <text x="8" y={barPadTop + 4} class="fill-muted-foreground text-[11px]"
                >{axisUsd(byMethodArea.max)}</text
              >
              <text x="8" y={byMethodArea.zeroY + 4} class="fill-muted-foreground text-[11px]"
                >$0</text
              >
              <text
                x="8"
                y={barChartHeight - barPadBottom}
                class="fill-muted-foreground text-[11px]">{axisUsd(byMethodArea.min)}</text
              >

              {#each byMethodArea.layers as layer (layer.key)}
                <polygon points={layer.path} fill={layer.color} opacity="0.3" />
                <polyline
                  points={layer.points
                    .map((point) => `${point.x.toFixed(1)},${point.y1.toFixed(1)}`)
                    .join(' ')}
                  fill="none"
                  stroke={layer.color}
                  stroke-width="1.6"
                  opacity="0.9"
                />
              {/each}
              {#each byMethodArea.labels as label (label.key)}
                <text
                  x={label.x}
                  y={labelY}
                  text-anchor="middle"
                  transform={rotateLabel(label.x)}
                  class="fill-muted-foreground text-[10px]"
                >
                  {label.label}
                </text>
              {/each}
            </svg>
          </div>
        </section>
      </div>

      {#if timeBuckets.length === 0}
        <div class="mt-4 rounded-xl border bg-card/40 p-4 text-sm text-muted-foreground">
          No PnL chart buckets for the selected asset/date filters.
        </div>
      {/if}

      <div class="mt-4 grid gap-4 lg:grid-cols-[0.9fr_1.4fr]">
        <section class="min-h-0 overflow-hidden rounded-lg border">
          <div class="border-b px-3 py-2 text-sm font-semibold">By Asset</div>
          <div class="max-h-80 overflow-auto">
            <Table.Root>
              <Table.Header>
                <Table.Row>
                  <Table.Head>Asset</Table.Head>
                  <Table.Head class="text-right">Total</Table.Head>
                  <Table.Head class="text-right">Counter</Table.Head>
                  <Table.Head class="text-right">On-chain</Table.Head>
                  <Table.Head class="text-right">Excess</Table.Head>
                  <Table.Head class="text-right">Baseline Drift</Table.Head>
                  <Table.Head class="text-right">Lots</Table.Head>
                </Table.Row>
              </Table.Header>
              <Table.Body>
                {#each report.current.symbols as row (row.symbol)}
                  <Table.Row>
                    <Table.Cell class="font-mono text-xs font-medium">{row.symbol}</Table.Cell>
                    <Table.Cell class="text-right font-mono text-xs {pnlColor(row.totalPnlUsd)}">
                      {fmtSignedUsd(row.totalPnlUsd)}
                    </Table.Cell>
                    <Table.Cell
                      class="text-right font-mono text-xs {pnlColor(row.counterTradePnlUsd)}"
                    >
                      {fmtSignedUsd(row.counterTradePnlUsd)}
                    </Table.Cell>
                    <Table.Cell
                      class="text-right font-mono text-xs {pnlColor(row.onchainNettingPnlUsd)}"
                    >
                      {fmtSignedUsd(row.onchainNettingPnlUsd)}
                    </Table.Cell>
                    <Table.Cell
                      class="text-right font-mono text-xs {pnlColor(
                        row.directionalImbalanceExcessPnlUsd
                      )}"
                    >
                      {fmtSignedUsd(row.directionalImbalanceExcessPnlUsd)}
                    </Table.Cell>
                    <Table.Cell
                      class="text-right font-mono text-xs {pnlColor(
                        row.directionalInventoryBaselinePnlUsd
                      )}"
                    >
                      {fmtSignedUsd(row.directionalInventoryBaselinePnlUsd)}
                    </Table.Cell>
                    <Table.Cell class="text-right font-mono text-xs text-muted-foreground">
                      {row.matchedLotCount}
                    </Table.Cell>
                  </Table.Row>
                {/each}
              </Table.Body>
            </Table.Root>
          </div>
        </section>

        <section class="min-h-0 overflow-hidden rounded-lg border">
          <div class="flex items-center justify-between gap-2 border-b px-3 py-2">
            <span class="text-sm font-semibold">Linked Lots</span>
            <span class="font-mono text-xs text-muted-foreground">
              {fmtCount(total.current)} matched lots
            </span>
          </div>
          <div class="max-h-80 overflow-auto">
            {#if entries.current.length === 0 && !loading.current}
              <div class="flex h-32 items-center justify-center text-sm text-muted-foreground">
                No matched PnL lots yet
              </div>
            {:else}
              <Table.Root>
                <Table.Header>
                  <Table.Row>
                    <Table.Head class="text-right">Closed</Table.Head>
                    <Table.Head>Asset</Table.Head>
                    <Table.Head class="text-right">Bucket</Table.Head>
                    <Table.Head class="text-right">Flow</Table.Head>
                    <Table.Head class="text-right">Lag</Table.Head>
                    <Table.Head class="text-right">Shares</Table.Head>
                    <Table.Head class="text-right">Spread</Table.Head>
                    <Table.Head class="text-right">PnL</Table.Head>
                    <Table.Head class="text-right">IDs</Table.Head>
                  </Table.Row>
                </Table.Header>
                <Table.Body>
                  {#each entries.current as entry, idx (`${entry.openingFillId}-${entry.closingFillId}-${String(idx)}`)}
                    <Table.Row class={idx % 2 === 0 ? 'bg-muted/40' : ''}>
                      <Table.Cell class="text-right font-mono text-xs text-muted-foreground">
                        {formatUtc(entry.closedAt)}
                      </Table.Cell>
                      <Table.Cell class="font-mono text-xs font-medium">{entry.symbol}</Table.Cell>
                      <Table.Cell class="text-right font-mono text-[11px] text-muted-foreground">
                        {entry.pnlBucket}
                      </Table.Cell>
                      <Table.Cell class="text-right text-xs">
                        <span class={directionColor(entry.openingDirection)}
                          >{entry.openingDirection}</span
                        >
                        <span class="text-muted-foreground"> / </span>
                        <span class={directionColor(entry.closingDirection)}
                          >{entry.closingDirection}</span
                        >
                      </Table.Cell>
                      <Table.Cell
                        class="text-right font-mono text-[11px] {entry.delayedCounterTrade
                          ? 'text-amber-300'
                          : 'text-muted-foreground'}"
                      >
                        {fmtDuration(entry.elapsedSeconds)}
                      </Table.Cell>
                      <Table.Cell class="text-right font-mono text-xs"
                        >{fmtShares(entry.shares)}</Table.Cell
                      >
                      <Table.Cell class="text-right font-mono text-xs {pnlColor(entry.spreadUsd)}">
                        {fmtSignedUsd(entry.spreadUsd)}
                      </Table.Cell>
                      <Table.Cell
                        class="text-right font-mono text-xs {pnlColor(entry.realizedPnlUsd)}"
                      >
                        {fmtSignedUsd(entry.realizedPnlUsd)}
                      </Table.Cell>
                      <Table.Cell class="text-right font-mono text-[11px] text-muted-foreground">
                        <div>{entry.openingVenue}: {shortId(entry.openingFillId)}</div>
                        <div>{entry.closingVenue}: {shortId(entry.closingFillId)}</div>
                      </Table.Cell>
                    </Table.Row>
                  {/each}
                </Table.Body>
              </Table.Root>
            {/if}
          </div>
        </section>
      </div>
    {/if}
  </Card.Content>
</Card.Root>
