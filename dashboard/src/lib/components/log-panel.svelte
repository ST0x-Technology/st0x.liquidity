<script lang="ts">
  import { onMount } from 'svelte'
  import * as Card from '$lib/components/ui/card'
  import MultiSelect from '$lib/components/multi-select.svelte'
  import { reactive } from '$lib/frp.svelte'
  import { getApiBaseUrl } from '$lib/env'
  import { resolveLogFilter, takeRequestedLogTarget } from '$lib/log-filter-request.svelte'
  import { formatUtcMs, toDatetimeLocal, TIME_PRESETS, toRfc3339 } from '$lib/time'

  /** Log queries scan files on disk and can be slow at trace level. */
  const LOGS_TIMEOUT_MS = 15_000

  type LogEntry = {
    timestamp: string
    level: string
    fields: Record<string, string> & { message?: string }
    target: string
    span?: { name: string }
    spans?: Array<{ name: string }>
  }

  type LogResponse = {
    entries: LogEntry[]
    total: number
    hasMore: boolean
  }

  const PAGE_SIZE = 100
  const DEBOUNCE_MS = 300

  const ALL_LEVELS = ['ERROR', 'WARN', 'INFO', 'DEBUG', 'TRACE'] as const

  const ALL_CATEGORIES = [
    'bridge',
    'broker',
    'cqrs',
    'dashboard',
    'hedge',
    'inventory',
    'orderbook',
    'rebalance',
    'startup',
    'tokenization',
    'wallet',
  ] as const

  const ALL_CRATES = [
    'st0x_hedge',
    'st0x_bridge',
    'st0x_event_sorcery',
    'st0x_evm',
    'st0x_execution',
    'apalis',
    'rocket',
    'sqlx',
  ] as const

  const categoryOptions = ALL_CATEGORIES.map((cat) => ({
    value: cat,
    label: cat,
  }))

  const crateOptions = ALL_CRATES.map((crt) => ({
    value: crt,
    label: crt.replace('st0x_', ''),
  }))

  const entries = reactive<LogEntry[]>([])
  const loading = reactive(false)
  const loadingMore = reactive(false)
  const error = reactive<string | null>(null)
  const search = reactive('')
  const offset = reactive(0)
  const total = reactive(0)
  const hasMore = reactive(false)
  const DEFAULT_LEVELS = ['ERROR', 'WARN', 'INFO'] as const
  const OUR_CRATES = ALL_CRATES.filter((crt) => crt.startsWith('st0x_'))

  const selectedLevels = reactive<Set<string>>(new Set(DEFAULT_LEVELS))
  const selectedCategories = reactive<Set<string>>(new Set(ALL_CATEGORIES))
  const selectedCrates = reactive<Set<string>>(new Set(OUR_CRATES))
  const since = reactive('')
  const until = reactive('')

  const buildParams = (): URLSearchParams => {
    const params = new URLSearchParams({
      limit: String(PAGE_SIZE),
      offset: String(offset.current),
    })

    const query = search.current.trim()
    if (query) params.set('search', query)

    if (selectedLevels.current.size > 0) {
      params.set('level', [...selectedLevels.current].join(','))
    }

    const targetFilters = [
      ...selectedCategories.current,
      ...selectedCrates.current,
    ]
    if (targetFilters.length > 0) {
      params.set('target', targetFilters.join(','))
    }

    // datetime-local values are treated as UTC directly
    if (since.current) params.set('since', toRfc3339(since.current))
    if (until.current) params.set('until', toRfc3339(until.current))

    return params
  }

  const fetchLogs = async (mode: 'replace' | 'append') => {
    const noFilters =
      selectedLevels.current.size === 0 ||
      (selectedCategories.current.size === 0 && selectedCrates.current.size === 0)

    if (noFilters) {
      error.update(() => null)
      loading.update(() => false)
      loadingMore.update(() => false)
      entries.update(() => [])
      hasMore.update(() => false)
      total.update(() => 0)
      error.update(() => null)
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
      const params = buildParams()

      const response = await fetch(
        `${baseUrl}/logs?${params.toString()}`,
        { signal: AbortSignal.timeout(LOGS_TIMEOUT_MS) }
      )

      if (!response.ok) {
        error.update(() => `HTTP ${String(response.status)}`)
        return
      }

      const data: LogResponse = await response.json() as LogResponse
      total.update(() => data.total)
      hasMore.update(() => data.hasMore)

      if (isLoadMore) {
        entries.update((prev) => [...prev, ...data.entries])
      } else {
        entries.update(() => data.entries)
      }
    } catch (fetchError) {
      error.update(() => fetchError instanceof Error ? fetchError.message : 'Unknown error')
    } finally {
      loading.update(() => false)
      loadingMore.update(() => false)
    }
  }

  const refresh = () => {
    offset.update(() => 0)
    void fetchLogs('replace')
  }

  const loadMore = () => {
    offset.update((current) => current + PAGE_SIZE)
    void fetchLogs('append')
  }

  let sinceInput: HTMLInputElement | undefined
  let untilInput: HTMLInputElement | undefined

  const jumpToLatest = () => {
    since.update(() => '')
    until.update(() => '')
    search.update(() => '')
    selectedLevels.update(() => new Set(DEFAULT_LEVELS))
    selectedCategories.update(() => new Set(ALL_CATEGORIES))
    selectedCrates.update(() => new Set(OUR_CRATES))
    offset.update(() => 0)
    if (sinceInput) sinceInput.value = ''
    if (untilInput) untilInput.value = ''
    void fetchLogs('replace')
  }

  const setsEqual = (set_a: Set<string>, set_b: ReadonlyArray<string>) =>
    set_a.size === set_b.length && set_b.every((item) => set_a.has(item))

  const hasFilters = $derived(
    since.current !== '' ||
    until.current !== '' ||
    search.current !== '' ||
    !setsEqual(selectedLevels.current, DEFAULT_LEVELS) ||
    !setsEqual(selectedCategories.current, ALL_CATEGORIES) ||
    !setsEqual(selectedCrates.current, OUR_CRATES)
  )

  /**
   * Seed filters from a Performance-tab click-through before the first
   * fetch: known categories/crates narrow their checkbox filters, anything
   * else lands in the free-text search so the click never silently no-ops.
   */
  const applyRequestedTarget = () => {
    const requested = takeRequestedLogTarget()
    if (requested === null) return

    const resolution = resolveLogFilter(requested, ALL_CATEGORIES, ALL_CRATES)

    if (resolution.kind === 'category') {
      selectedCategories.update(() => new Set([resolution.target]))
      selectedCrates.update(() => new Set<string>())
      return
    }

    if (resolution.kind === 'crate') {
      selectedCategories.update(() => new Set<string>())
      selectedCrates.update(() => new Set([resolution.target]))
      return
    }

    search.update(() => resolution.target)
  }

  onMount(() => {
    applyRequestedTarget()
    void fetchLogs('replace')
  })

  // Debounced search
  let debounceTimer: ReturnType<typeof setTimeout> | undefined

  const handleSearchInput = (event: Event) => {
    const target = event.target as HTMLInputElement
    const value = target.value

    clearTimeout(debounceTimer)
    debounceTimer = setTimeout(() => {
      search.update(() => value)
      offset.update(() => 0)
      void fetchLogs('replace')
    }, DEBOUNCE_MS)
  }

  const handleCategoryChange = (selected: Set<string>) => {
    selectedCategories.update(() => selected)
    offset.update(() => 0)
    void fetchLogs('replace')
  }

  const handleCrateChange = (selected: Set<string>) => {
    selectedCrates.update(() => selected)
    offset.update(() => 0)
    void fetchLogs('replace')
  }

  const toggleLevel = (level: string) => () => {
    selectedLevels.update((current) => {
      const next = new Set(current)
      if (next.has(level)) {
        next.delete(level)
      } else {
        next.add(level)
      }
      return next
    })
    offset.update(() => 0)
    void fetchLogs('replace')
  }

  const applyPreset = (minutes: number) => () => {
    const sinceDate = new Date(Date.now() - minutes * 60_000)
    since.update(() => toDatetimeLocal(sinceDate))
    until.update(() => '')
    if (sinceInput) sinceInput.value = toDatetimeLocal(sinceDate)
    if (untilInput) untilInput.value = ''
    offset.update(() => 0)
    void fetchLogs('replace')
  }

  const downloading = reactive(false)
  const downloadProgress = reactive('')

  const buildDownloadParams = (batchOffset: number, batchSize: number, minutes: number | null): URLSearchParams => {
    const params = new URLSearchParams({
      limit: String(batchSize),
      offset: String(batchOffset),
    })

    if (selectedLevels.current.size > 0) {
      params.set('level', [...selectedLevels.current].join(','))
    }

    const targetFilters = [
      ...selectedCategories.current,
      ...selectedCrates.current,
    ]
    if (targetFilters.length > 0) {
      params.set('target', targetFilters.join(','))
    }

    if (minutes !== null) {
      const sinceDate = new Date(Date.now() - minutes * 60_000)
      params.set('since', sinceDate.toISOString())
    }

    return params
  }

  const downloadLogs = (minutes: number | null) => async () => {
    downloading.update(() => true)
    downloadProgress.update(() => 'Fetching...')
    const baseUrl = getApiBaseUrl()

    try {
      const allEntries: LogEntry[] = []
      const batchSize = 5000
      let currentOffset = 0
      let hasMorePages = true

      while (hasMorePages) {
        const params = buildDownloadParams(currentOffset, batchSize, minutes)

        downloadProgress.update(() =>
          allEntries.length === 0
            ? 'Fetching...'
            : `${String(allEntries.length)} entries...`,
        )

        const response = await fetch(
          `${baseUrl}/logs?${params.toString()}`,
          { signal: AbortSignal.timeout(60_000) },
        )

        if (!response.ok) return

        const data: LogResponse = await response.json() as LogResponse
        allEntries.push(...data.entries)
        hasMorePages = data.hasMore
        currentOffset += batchSize
      }

      downloadProgress.update(() => `Writing ${String(allEntries.length)} entries...`)

      const blob = new Blob(
        [allEntries.map((entry) => JSON.stringify(entry)).join('\n')],
        { type: 'application/jsonl+json' },
      )

      const suffix = minutes === null
        ? 'all'
        : TIME_PRESETS.find((preset) => preset.minutes === minutes)?.label ?? `${String(minutes)}m`
      const anchor = document.createElement('a')
      anchor.href = URL.createObjectURL(blob)
      anchor.download = `logs-${suffix}-${new Date().toISOString().slice(0, 19).replace(/:/g, '')}.jsonl`
      anchor.click()
      URL.revokeObjectURL(anchor.href)
    } catch {
      // Download failed — user can retry
    } finally {
      downloading.update(() => false)
      downloadProgress.update(() => '')
      showDownloadMenu = false
    }
  }

  let showDownloadMenu = $state(false)

  const handleDownloadClickOutside = (event: MouseEvent) => {
    const target = event.target as HTMLElement
    if (!target.closest('.download-menu-root')) {
      showDownloadMenu = false
    }
  }

  $effect(() => {
    if (!showDownloadMenu) return

    document.addEventListener('click', handleDownloadClickOutside, true)
    return () => { document.removeEventListener('click', handleDownloadClickOutside, true) }
  })

  const handleSinceChange = (event: Event) => {
    const target = event.target as HTMLInputElement
    since.update(() => target.value)
    offset.update(() => 0)
    void fetchLogs('replace')
  }

  const handleUntilChange = (event: Event) => {
    const target = event.target as HTMLInputElement
    until.update(() => target.value)
    offset.update(() => 0)
    void fetchLogs('replace')
  }

  const levelColor = (level: string): string => {
    switch (level.toUpperCase()) {
      case 'ERROR': return 'text-red-500'
      case 'WARN': return 'text-yellow-500'
      case 'INFO': return 'text-blue-400'
      case 'DEBUG': return 'text-cyan-400'
      case 'TRACE': return 'text-purple-400'
      default: return ''
    }
  }

  const levelBtnColor = (level: string, active: boolean): string => {
    if (!active) return 'opacity-30'
    return levelColor(level)
  }

  const getContextFields = (entry: LogEntry): string =>
    Object.entries(entry.fields)
      .filter(([key]) => key !== 'message')
      .map(([key, value]) => `${key}=${value}`)
      .join(' ')

  const getTarget = (entry: LogEntry): string => {
    const spans = entry.spans?.map(span => span.name).join(' > ')
    if (spans) return `${entry.target}::${spans}`
    if (entry.span) return `${entry.target}::${entry.span.name}`
    return entry.target
  }
</script>

<Card.Root class="flex h-full flex-col overflow-hidden border-l-4 border-l-orange-500/50">
  <Card.Header class="shrink-0 space-y-2 pb-2">
    <Card.Title class="flex items-center justify-between">
      <span>Log History</span>

      <div class="flex items-center gap-2">
        <input
          type="text"
          placeholder="Search logs..."
          class="rounded border bg-background px-2 py-1 text-xs w-48"
          oninput={handleSearchInput}
        />

        {#if hasFilters}
          <button
            class="rounded border bg-background px-2 py-1 text-xs hover:bg-accent"
            onclick={jumpToLatest}
            title="Clear all filters and jump to latest logs"
          >
            Latest
          </button>
        {/if}

        <div class="download-menu-root relative">
          <button
            class="inline-flex items-center gap-1.5 rounded border bg-background px-2 py-1 text-xs hover:bg-accent"
            onclick={() => { if (!downloading.current) showDownloadMenu = !showDownloadMenu }}
            disabled={downloading.current}
          >
            {#if downloading.current}
              <svg class="h-3 w-3 animate-spin" viewBox="0 0 24 24" fill="none">
                <circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle>
                <path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z"></path>
              </svg>
              <span>{downloadProgress.current}</span>
            {:else}
              Download
            {/if}
          </button>

          {#if showDownloadMenu}
            <div
              class="absolute right-0 top-full z-10 mt-1 flex flex-col rounded border bg-background shadow-md"
              role="menu"
            >
              <button class="px-3 py-1.5 text-left text-xs hover:bg-accent" onclick={downloadLogs(null)} role="menuitem">All</button>
              {#each TIME_PRESETS as preset (preset.label)}
                <button class="px-3 py-1.5 text-left text-xs hover:bg-accent" onclick={downloadLogs(preset.minutes)} role="menuitem">Last {preset.label}</button>
              {/each}
            </div>
          {/if}
        </div>

        <button
          class="rounded border bg-background px-2 py-1 text-xs hover:bg-accent"
          onclick={refresh}
          disabled={loading.current}
        >
          {loading.current ? 'Loading...' : 'Refresh'}
        </button>

        <span class="text-xs text-muted-foreground">
          {entries.current.length} of {total.current}
        </span>
      </div>
    </Card.Title>

    <div class="flex flex-wrap items-center gap-x-4 gap-y-1 text-xs">
      <div class="flex items-center gap-1">
        {#each ALL_LEVELS as level (level)}
          {@const active = selectedLevels.current.has(level)}
          <button
            class="rounded border px-1.5 py-0.5 font-mono font-semibold transition-opacity {levelBtnColor(level, active)}"
            onclick={toggleLevel(level)}
            title={active ? `Hide ${level}` : `Show ${level}`}
          >
            {level}
          </button>
        {/each}
      </div>

      <MultiSelect
        label="Category"
        options={categoryOptions}
        selected={selectedCategories.current}
        onchange={handleCategoryChange}
      />

      <MultiSelect
        label="Crate"
        options={crateOptions}
        selected={selectedCrates.current}
        onchange={handleCrateChange}
      />

      <div class="flex items-center gap-1">
        {#each TIME_PRESETS as preset (preset.label)}
          <button
            class="rounded border bg-background px-1.5 py-0.5 text-xs hover:bg-accent"
            onclick={applyPreset(preset.minutes)}
          >
            {preset.label}
          </button>
        {/each}
      </div>

      <div class="flex items-center gap-1 text-muted-foreground">
        <span>From (UTC)</span>
        <input
          bind:this={sinceInput}
          type="datetime-local"
          class="rounded border bg-background px-1.5 py-0.5 text-xs text-foreground"
          style="color-scheme: dark"
          onchange={handleSinceChange}
          step="1"
        />

        <span>to (UTC)</span>
        <input
          bind:this={untilInput}
          type="datetime-local"
          class="rounded border bg-background px-1.5 py-0.5 text-xs text-foreground"
          style="color-scheme: dark"
          onchange={handleUntilChange}
          step="1"
        />
      </div>
    </div>
  </Card.Header>

  <Card.Content class="relative min-h-0 flex-1 overflow-auto px-4 pt-0">
    {#if error.current}
      <div class="flex h-full items-center justify-center text-destructive">
        Failed to load logs: {error.current}
      </div>
    {:else if entries.current.length === 0 && !loading.current}
      <div class="flex h-full items-center justify-center text-muted-foreground">
        {search.current ? 'No matching log entries' : 'No log entries found. Configure log_dir in config.toml to enable.'}
      </div>
    {:else}
      <div class="font-mono text-xs leading-relaxed">
        {#each entries.current as entry, idx (idx)}
          <div class="flex gap-2 border-b border-border/30 py-0.5 hover:bg-accent/30 {idx % 2 === 0 ? 'bg-muted/40' : ''}">
            <span class="shrink-0 text-muted-foreground" title={entry.timestamp}>
              {formatUtcMs(entry.timestamp)}
            </span>

            <span class="w-12 shrink-0 text-right font-semibold {levelColor(entry.level)}">
              {entry.level}
            </span>

            <span class="shrink-0 text-nowrap text-muted-foreground">
              {getTarget(entry)}
            </span>

            <span class="flex-1 break-all">
              {entry.fields.message ?? ''}
              {#if getContextFields(entry)}
                <span class="text-muted-foreground"> {getContextFields(entry)}</span>
              {/if}
            </span>
          </div>
        {/each}

        {#if hasMore.current}
          <div class="flex justify-center py-2">
            <button
              class="rounded border bg-background px-3 py-1 text-xs hover:bg-accent"
              onclick={loadMore}
              disabled={loadingMore.current}
            >
              {loadingMore.current ? 'Loading...' : 'Load older entries'}
            </button>
          </div>
        {/if}
      </div>
    {/if}
  </Card.Content>
</Card.Root>
