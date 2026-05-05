<script lang="ts">
  import { onMount } from 'svelte'
  import * as Card from '$lib/components/ui/card'
  import * as Table from '$lib/components/ui/table'
  import MultiSelect from '$lib/components/multi-select.svelte'
  import { reactive } from '$lib/frp.svelte'
  import { getApiBaseUrl } from '$lib/env'
  import { formatUtc, toDatetimeLocal, TIME_PRESETS, FETCH_TIMEOUT_MS, toRfc3339 } from '$lib/time'
  import { formatDecimal } from '$lib/decimal'
  import { getExplorerTxUrl } from '$lib/env'

  type TradeEvent = {
    step: string
    sequence: number
    payload: Record<string, unknown>
  }

  type TradeEntry = {
    id: string
    filledAt: string
    venue: string
    direction: string
    symbol: string
    shares: string
  }

  type TradeResponse = {
    entries: TradeEntry[]
    total: number
    hasMore: boolean
  }

  const PAGE_SIZE = 100
  const POLL_INTERVAL_MS = 10_000
  const ALL_VENUES = ['raindex', 'alpaca', 'dry_run'] as const

  const entries = reactive<TradeEntry[]>([])
  const loading = reactive(false)
  const loadingMore = reactive(false)
  const error = reactive<string | null>(null)
  const offset = reactive(0)
  const total = reactive(0)
  const hasMore = reactive(false)
  const selectedVenues = reactive<Set<string>>(new Set(ALL_VENUES))
  const selectedSymbols = reactive<Set<string>>(new Set())
  const allSymbols = reactive<string[]>([])
  const since = reactive('')
  const until = reactive('')

  let sinceInput: HTMLInputElement | undefined
  let untilInput: HTMLInputElement | undefined

  const venueLabel = (venue: string): string => {
    switch (venue) {
      case 'raindex': return 'Raindex'
      case 'alpaca': return 'Alpaca'
      case 'dry_run': return 'DryRun'
      default: return venue
    }
  }

  const venueColor = (venue: string): string => {
    switch (venue) {
      case 'raindex': return 'text-blue-400'
      case 'alpaca': return 'text-amber-400'
      case 'dry_run': return 'text-muted-foreground'
      default: return ''
    }
  }

  const venueOptions = ALL_VENUES.map((venue) => ({ value: venue, label: venueLabel(venue) }))
  const symbolOptions = $derived(allSymbols.current.map((sym) => ({ value: sym, label: sym })))

  const buildParams = (): URLSearchParams => {
    const params = new URLSearchParams({
      limit: String(PAGE_SIZE),
      offset: String(offset.current),
    })

    if (selectedVenues.current.size > 0 && selectedVenues.current.size < ALL_VENUES.length) {
      params.set('venue', [...selectedVenues.current].join(','))
    }

    if (selectedSymbols.current.size > 0) {
      params.set('symbol', [...selectedSymbols.current].join(','))
    }

    if (since.current) params.set('since', toRfc3339(since.current))
    if (until.current) params.set('until', toRfc3339(until.current))

    return params
  }

  const fetchTrades = async (mode: 'replace' | 'append') => {
    if (selectedVenues.current.size === 0) {
      entries.update(() => [])
      hasMore.update(() => false)
      return
    }

    const isLoadMore = mode === 'append'
    if (isLoadMore) {
      loadingMore.update(() => true)
    } else {
      loading.update(() => true)
    }
    error.update(() => null)

    try {
      const baseUrl = getApiBaseUrl()
      const response = await fetch(
        `${baseUrl}/trades?${buildParams().toString()}`,
        { signal: AbortSignal.timeout(FETCH_TIMEOUT_MS) }
      )

      if (!response.ok) {
        error.update(() => `HTTP ${String(response.status)}`)
        return
      }

      const data: TradeResponse = await response.json() as TradeResponse
      total.update(() => data.total)
      hasMore.update(() => data.hasMore)

      if (isLoadMore) {
        entries.update((prev) => [...prev, ...data.entries])
      } else {
        entries.update(() => data.entries)
      }

      // Accumulate symbols across fetches so the dropdown doesn't shrink
      const newSyms = new Set(data.entries.map((trade) => trade.symbol))
      allSymbols.update((prev) => {
        const merged = new Set([...prev, ...newSyms])
        return [...merged].sort()
      })
    } catch (fetchError) {
      error.update(() => fetchError instanceof Error ? fetchError.message : 'Unknown error')
    } finally {
      loading.update(() => false)
      loadingMore.update(() => false)
    }
  }

  const refresh = () => {
    offset.update(() => 0)
    void fetchTrades('replace')
  }

  const loadMore = () => {
    offset.update((current) => current + PAGE_SIZE)
    void fetchTrades('append')
  }

  const applyPreset = (minutes: number) => () => {
    const sinceDate = new Date(Date.now() - minutes * 60_000)
    since.update(() => toDatetimeLocal(sinceDate))
    until.update(() => '')
    if (sinceInput) sinceInput.value = toDatetimeLocal(sinceDate)
    if (untilInput) untilInput.value = ''
    offset.update(() => 0)
    void fetchTrades('replace')
  }

  const jumpToLatest = () => {
    since.update(() => '')
    until.update(() => '')
    selectedVenues.update(() => new Set(ALL_VENUES))
    selectedSymbols.update(() => new Set())
    offset.update(() => 0)
    if (sinceInput) sinceInput.value = ''
    if (untilInput) untilInput.value = ''
    void fetchTrades('replace')
  }

  const hasFilters = $derived(
    since.current !== '' ||
    until.current !== '' ||
    selectedVenues.current.size < ALL_VENUES.length ||
    selectedSymbols.current.size > 0
  )

  onMount(() => {
    void fetchTrades('replace')
    const interval = setInterval(() => {
      if (offset.current === 0) void fetchTrades('replace')
    }, POLL_INTERVAL_MS)
    return () => { clearInterval(interval) }
  })

  const handleVenueChange = (selected: Set<string>) => {
    selectedVenues.update(() => selected)
    offset.update(() => 0)
    void fetchTrades('replace')
  }

  const handleSymbolChange = (selected: Set<string>) => {
    selectedSymbols.update(() => selected)
    offset.update(() => 0)
    void fetchTrades('replace')
  }

  const handleSinceChange = (event: Event) => {
    since.update(() => (event.target as HTMLInputElement).value)
    offset.update(() => 0)
    void fetchTrades('replace')
  }

  const handleUntilChange = (event: Event) => {
    until.update(() => (event.target as HTMLInputElement).value)
    offset.update(() => 0)
    void fetchTrades('replace')
  }

  const directionColor = (direction: string): string =>
    direction === 'Buy' ? 'text-green-500' : 'text-red-500'

  const fmtSize = (value: string): string => {
    const num = parseFloat(value)
    if (num === 0) return '0'
    if (Math.abs(num) < 0.01) return num.toPrecision(2)
    return num.toFixed(2)
  }

  // -- Detail modal state --

  let detailDialogEl: HTMLDialogElement | undefined = $state()
  const detailEvents = reactive<TradeEvent[]>([])
  const detailTrade = reactive<TradeEntry | null>(null)
  const detailLoading = reactive(false)
  const detailError = reactive<string | null>(null)

  const openDetail = async (trade: TradeEntry) => {
    detailTrade.update(() => trade)
    detailLoading.update(() => true)
    detailError.update(() => null)
    detailEvents.update(() => [])
    detailDialogEl?.showModal()

    try {
      const baseUrl = getApiBaseUrl()
      const response = await fetch(
        `${baseUrl}/trades/${trade.venue}/${trade.id}/events`,
        { signal: AbortSignal.timeout(FETCH_TIMEOUT_MS) },
      )

      if (!response.ok) {
        detailError.update(() => `HTTP ${String(response.status)}`)
        return
      }

      const data = (await response.json()) as { events: TradeEvent[] }
      detailEvents.update(() => data.events)
    } catch (err) {
      detailError.update(() => err instanceof Error ? err.message : 'Unknown error')
    } finally {
      detailLoading.update(() => false)
    }
  }

  // -- Detail rendering helpers --

  const humanizeStep = (step: string): string =>
    step.replace(/([A-Z])/g, ' $1').trim()

  const isTxHash = (value: unknown): value is string =>
    typeof value === 'string' && /^0x[0-9a-fA-F]{64}$/.test(value)

  const isTimestampField = (key: string): boolean => key.endsWith('_at')

  const formatFieldName = (key: string): string =>
    key.replace(/_/g, ' ').replace(/\b\w/g, (char) => char.toUpperCase())

  const extractTimestamp = (payload: Record<string, unknown>): string | null => {
    for (const [key, value] of Object.entries(payload)) {
      if (isTimestampField(key) && typeof value === 'string') return value
    }
    return null
  }

  const stepStyle = (step: string): string => {
    const lower = step.toLowerCase()

    if (lower.includes('filled') || lower.includes('enriched')) {
      return 'text-green-500'
    }

    if (lower.includes('failed')) {
      return 'text-destructive'
    }

    return 'text-muted-foreground'
  }

  const stepDot = (step: string): string => {
    const lower = step.toLowerCase()

    if (lower.includes('filled') || lower.includes('enriched')) {
      return 'bg-green-500'
    }

    if (lower.includes('failed')) {
      return 'bg-destructive'
    }

    return 'bg-muted-foreground'
  }
</script>

<Card.Root class="flex h-full flex-col overflow-hidden border-l-4 border-l-green-500/50">
  <Card.Header class="shrink-0 space-y-2 pb-0">
    <Card.Title class="flex items-center justify-between">
      <span class="flex items-center gap-1.5">
        Trade History
        <span class="group relative cursor-help text-muted-foreground">
          <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 20 20" fill="currentColor" class="h-3.5 w-3.5"><path fill-rule="evenodd" d="M18 10a8 8 0 1 1-16 0 8 8 0 0 1 16 0Zm-7-4a1 1 0 1 1-2 0 1 1 0 0 1 2 0ZM9 9a.75.75 0 0 0 0 1.5h.253a.25.25 0 0 1 .244.304l-.459 2.066A1.75 1.75 0 0 0 10.747 15H11a.75.75 0 0 0 0-1.5h-.253a.25.25 0 0 1-.244-.304l.459-2.066A1.75 1.75 0 0 0 9.253 9H9Z" clip-rule="evenodd" /></svg>
          <span class="pointer-events-none absolute left-0 top-full z-50 mt-1 hidden w-56 rounded bg-popover px-3 py-2 text-xs font-normal text-popover-foreground shadow-lg group-hover:block">
            Onchain fills from Raindex orders and the corresponding Alpaca hedge trades placed to offset exposure.
          </span>
        </span>
      </span>
      <div class="flex items-center gap-2">
        {#if hasFilters}
          <button class="rounded border bg-background px-2 py-1 text-xs hover:bg-accent" onclick={jumpToLatest}>Latest</button>
        {/if}
        <button class="rounded border bg-background px-2 py-1 text-xs hover:bg-accent" onclick={refresh} disabled={loading.current}>
          {loading.current ? 'Loading...' : 'Refresh'}
        </button>
        <span class="text-sm font-normal text-muted-foreground">
          {entries.current.length} of {total.current}
        </span>
      </div>
    </Card.Title>

    <div class="flex flex-wrap items-center gap-2 rounded-md bg-muted/30 px-2 py-1.5 text-xs">
      <MultiSelect label="Venues" options={venueOptions} selected={selectedVenues.current} onchange={handleVenueChange} />

      {#if symbolOptions.length > 0}
        <MultiSelect label="Assets" options={symbolOptions} selected={selectedSymbols.current} onchange={handleSymbolChange} />
      {/if}

      <div class="flex items-center gap-1">
        {#each TIME_PRESETS as preset (preset.label)}
          <button class="rounded border bg-background px-1.5 py-0.5 text-xs hover:bg-accent" onclick={applyPreset(preset.minutes)}>
            {preset.label}
          </button>
        {/each}
      </div>

      <div class="flex items-center gap-1 text-muted-foreground">
        <span>From (UTC)</span>
        <input bind:this={sinceInput} type="datetime-local" class="rounded border bg-background px-1.5 py-0.5 text-xs text-foreground" style="color-scheme: dark" onchange={handleSinceChange} step="1" />
        <span>to (UTC)</span>
        <input bind:this={untilInput} type="datetime-local" class="rounded border bg-background px-1.5 py-0.5 text-xs text-foreground" style="color-scheme: dark" onchange={handleUntilChange} step="1" />
      </div>
    </div>
  </Card.Header>

  <Card.Content class="relative min-h-0 flex-1 overflow-auto px-6 pt-2">
    {#if error.current}
      <div class="flex h-full items-center justify-center text-destructive">
        Failed to load trades: {error.current}
      </div>
    {:else if entries.current.length === 0 && !loading.current}
      <div class="flex h-full items-center justify-center text-muted-foreground">
        No trades yet
      </div>
    {:else}
      <Table.Root>
        <Table.Header>
          <Table.Row>
            <Table.Head class="w-8"></Table.Head>
            <Table.Head class="text-right">Time (UTC)</Table.Head>
            <Table.Head>Asset</Table.Head>
            <Table.Head class="text-right">Venue</Table.Head>
            <Table.Head class="text-right">Side</Table.Head>
            <Table.Head class="text-center">Size</Table.Head>
          </Table.Row>
        </Table.Header>
        <Table.Body>
          {#each entries.current as trade, idx (trade.id + String(idx))}
            <Table.Row class={idx % 2 === 0 ? 'bg-muted/40' : ''}>
              <Table.Cell class="w-8 px-1">
                <button
                  class="inline-flex items-center text-muted-foreground hover:text-foreground"
                  title="View trade details"
                  onclick={() => openDetail(trade)}
                >
                  <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 20 20" fill="currentColor" class="h-3.5 w-3.5">
                    <path fill-rule="evenodd" d="M18 10a8 8 0 1 1-16 0 8 8 0 0 1 16 0Zm-7-4a1 1 0 1 1-2 0 1 1 0 0 1 2 0ZM9 9a.75.75 0 0 0 0 1.5h.253a.25.25 0 0 1 .244.304l-.459 2.066A1.75 1.75 0 0 0 10.747 15H11a.75.75 0 0 0 0-1.5h-.253a.25.25 0 0 1-.244-.304l.459-2.066A1.75 1.75 0 0 0 9.253 9H9Z" clip-rule="evenodd" />
                  </svg>
                </button>
              </Table.Cell>
              <Table.Cell class="text-right font-mono text-xs text-muted-foreground">
                {formatUtc(trade.filledAt)}
              </Table.Cell>
              <Table.Cell class="font-mono text-xs font-medium">{trade.symbol}</Table.Cell>
              <Table.Cell class="text-right text-xs font-medium {venueColor(trade.venue)}">{venueLabel(trade.venue)}</Table.Cell>
              <Table.Cell class="text-right text-xs font-medium {directionColor(trade.direction)}">
                {trade.direction}
              </Table.Cell>
              <Table.Cell class="text-center font-mono text-xs">{fmtSize(trade.shares)}</Table.Cell>
            </Table.Row>
          {/each}
        </Table.Body>
      </Table.Root>

      {#if hasMore.current}
        <div class="flex justify-center py-2">
          <button class="rounded border bg-background px-3 py-1 text-xs hover:bg-accent" onclick={loadMore} disabled={loadingMore.current}>
            {loadingMore.current ? 'Loading...' : 'Load older trades'}
          </button>
        </div>
      {/if}

      <div class="pointer-events-none sticky bottom-0 h-8 bg-gradient-to-t from-card to-transparent"></div>
    {/if}
  </Card.Content>
</Card.Root>

<dialog
  bind:this={detailDialogEl}
  class="w-full max-w-lg rounded-lg border bg-card p-0 text-foreground shadow-lg backdrop:bg-black/50"
  onclick={(event) => { if (event.target === detailDialogEl) detailDialogEl.close() }}
>
  {#if detailTrade.current}
    {@const trade = detailTrade.current}
    <div class="flex items-center justify-between border-b px-5 py-3">
      <div class="flex items-center gap-2 text-sm font-semibold">
        <span class={directionColor(trade.direction)}>{trade.direction}</span>
        <span class="font-mono text-xs font-normal">{trade.symbol}</span>
        <span class="font-mono text-xs font-normal text-muted-foreground">{fmtSize(trade.shares)} shares</span>
        <span class="text-xs font-normal {venueColor(trade.venue)}">{venueLabel(trade.venue)}</span>
      </div>
      <button
        class="text-lg leading-none text-muted-foreground hover:text-foreground"
        onclick={() => detailDialogEl?.close()}
      >
        &times;
      </button>
    </div>

    <div class="max-h-[60vh] overflow-y-auto px-5 py-4">
      {#if detailLoading.current}
        <div class="flex items-center justify-center py-8 text-sm text-muted-foreground">
          Loading events...
        </div>
      {:else if detailError.current}
        <div class="flex items-center justify-center py-8 text-sm text-destructive">
          Failed to load events: {detailError.current}
        </div>
      {:else}
        <!-- Summary fields -->
        <div class="mb-4 space-y-1.5 text-xs">
          <div class="flex items-center gap-2 font-mono">
            <span class="shrink-0 text-muted-foreground">ID</span>
            {#if trade.venue === 'raindex'}
              {@const txHash = trade.id.slice(0, trade.id.lastIndexOf(':'))}
              {@const logIndex = trade.id.slice(trade.id.lastIndexOf(':') + 1)}
              <a
                href={getExplorerTxUrl(txHash)}
                target="_blank"
                rel="noopener noreferrer"
                class="inline-flex items-center gap-1 text-blue-400 hover:text-blue-300"
              >
                {txHash.slice(0, 10)}...{txHash.slice(-8)}
                <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 16 16" fill="currentColor" class="h-3 w-3">
                  <path d="M6.22 8.72a.75.75 0 0 0 1.06 1.06l5.22-5.22v1.69a.75.75 0 0 0 1.5 0v-3.5a.75.75 0 0 0-.75-.75h-3.5a.75.75 0 0 0 0 1.5h1.69L6.22 8.72Z" />
                  <path d="M3.5 6.75c0-.69.56-1.25 1.25-1.25H7A.75.75 0 0 0 7 4H4.75A2.75 2.75 0 0 0 2 6.75v4.5A2.75 2.75 0 0 0 4.75 14h4.5A2.75 2.75 0 0 0 12 11.25V9a.75.75 0 0 0-1.5 0v2.25c0 .69-.56 1.25-1.25 1.25h-4.5c-.69 0-1.25-.56-1.25-1.25v-4.5Z" />
                </svg>
              </a>
              <span class="text-muted-foreground">(log {logIndex})</span>
            {:else}
              <span class="text-muted-foreground">{trade.id}</span>
            {/if}
          </div>

          <div class="flex items-center gap-2 font-mono">
            <span class="shrink-0 text-muted-foreground">Filled At</span>
            <span>{formatUtc(trade.filledAt)}</span>
          </div>
        </div>

        <!-- Event timeline -->
        <div class="relative space-y-0 border-l-2 border-muted pl-4">
          {#each detailEvents.current as event (event.sequence)}
            {@const timestamp = extractTimestamp(event.payload)}
            {@const fields = Object.entries(event.payload).filter(([key]) => !isTimestampField(key))}
            <div class="relative pb-4">
              <!-- Timeline dot -->
              <div class="absolute -left-[calc(0.25rem+1px+1rem)] top-0.5 h-2 w-2 rounded-full {stepDot(event.step)}"></div>

              <div class="text-xs font-medium {stepStyle(event.step)}">
                {humanizeStep(event.step)}
              </div>

              {#if timestamp}
                <div class="mt-0.5 font-mono text-xs text-muted-foreground">
                  {formatUtc(timestamp)}
                </div>
              {/if}

              {#if fields.length > 0}
                <div class="mt-1.5 space-y-1">
                  {#each fields as [key, value] (key)}
                    <div class="flex items-start gap-2 font-mono text-xs">
                      <span class="shrink-0 text-muted-foreground">{formatFieldName(key)}</span>
                      <span class="min-w-0 break-all">
                        {#if key === 'error'}
                          <span class="text-destructive">{value}</span>
                        {:else if isTxHash(value)}
                          <a
                            href={getExplorerTxUrl(value)}
                            target="_blank"
                            rel="noopener noreferrer"
                            class="inline-flex items-center gap-1 text-blue-400 hover:text-blue-300"
                          >
                            {value.slice(0, 10)}...{value.slice(-8)}
                            <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 16 16" fill="currentColor" class="h-3 w-3">
                              <path d="M6.22 8.72a.75.75 0 0 0 1.06 1.06l5.22-5.22v1.69a.75.75 0 0 0 1.5 0v-3.5a.75.75 0 0 0-.75-.75h-3.5a.75.75 0 0 0 0 1.5h1.69L6.22 8.72Z" />
                              <path d="M3.5 6.75c0-.69.56-1.25 1.25-1.25H7A.75.75 0 0 0 7 4H4.75A2.75 2.75 0 0 0 2 6.75v4.5A2.75 2.75 0 0 0 4.75 14h4.5A2.75 2.75 0 0 0 12 11.25V9a.75.75 0 0 0-1.5 0v2.25c0 .69-.56 1.25-1.25 1.25h-4.5c-.69 0-1.25-.56-1.25-1.25v-4.5Z" />
                            </svg>
                          </a>
                        {:else if key === 'executor_order_id' && typeof value === 'object' && value !== null}
                          <span class="text-muted-foreground">{JSON.stringify(value)}</span>
                        {:else if key === 'pyth_price' && typeof value === 'object' && value !== null}
                          {@const pyth = value as Record<string, unknown>}
                          <span>${formatDecimal(String(pyth['value']), 2)} (conf: {pyth['conf']}, expo: {pyth['expo']})</span>
                        {:else if typeof value === 'object' && value !== null}
                          {JSON.stringify(value)}
                        {:else}
                          {String(value)}
                        {/if}
                      </span>
                    </div>
                  {/each}
                </div>
              {/if}
            </div>
          {/each}
        </div>
      {/if}
    </div>
  {/if}
</dialog>
