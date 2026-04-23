<script lang="ts">
  import { onMount } from 'svelte'
  import * as Card from '$lib/components/ui/card'
  import * as Table from '$lib/components/ui/table'
  import MultiSelect from '$lib/components/multi-select.svelte'
  import { reactive } from '$lib/frp.svelte'
  import { getApiBaseUrl } from '$lib/env'
  import { formatUtc, toDatetimeLocal, TIME_PRESETS, FETCH_TIMEOUT_MS, toRfc3339 } from '$lib/time'

  type TransferEntry = {
    kind: string
    id: string
    symbol?: string
    quantity?: string
    amount?: string
    direction?: string
    status: { status: string }
    startedAt: string
    updatedAt: string
  }

  type TransferResponse = {
    entries: TransferEntry[]
    total: number
    hasMore: boolean
  }

  const PAGE_SIZE = 100
  const POLL_INTERVAL_MS = 10_000
  const ALL_KINDS = ['equity_mint', 'equity_redemption', 'usdc_bridge'] as const

  const entries = reactive<TransferEntry[]>([])
  const loading = reactive(false)
  const loadingMore = reactive(false)
  const error = reactive<string | null>(null)
  const offset = reactive(0)
  const total = reactive(0)
  const hasMore = reactive(false)
  const selectedKinds = reactive<Set<string>>(new Set(ALL_KINDS))
  const since = reactive('')
  const until = reactive('')

  let sinceInput: HTMLInputElement | undefined
  let untilInput: HTMLInputElement | undefined

  const kindLabel = (kind: string): string => {
    switch (kind) {
      case 'equity_mint': return 'Mint'
      case 'equity_redemption': return 'Redeem'
      case 'usdc_bridge': return 'USDC Bridge'
      default: return kind
    }
  }

  const kindOptions = ALL_KINDS.map((kind) => ({ value: kind, label: kindLabel(kind) }))

  const buildParams = (): URLSearchParams => {
    const params = new URLSearchParams({
      limit: String(PAGE_SIZE),
      offset: String(offset.current),
    })

    if (selectedKinds.current.size > 0 && selectedKinds.current.size < ALL_KINDS.length) {
      params.set('kind', [...selectedKinds.current].join(','))
    }

    if (since.current) params.set('since', toRfc3339(since.current))
    if (until.current) params.set('until', toRfc3339(until.current))

    return params
  }

  const fetchTransfers = async (mode: 'replace' | 'append') => {
    if (selectedKinds.current.size === 0) {
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
        `${baseUrl}/transfers?${buildParams().toString()}`,
        { signal: AbortSignal.timeout(FETCH_TIMEOUT_MS) }
      )

      if (!response.ok) {
        error.update(() => `HTTP ${String(response.status)}`)
        return
      }

      const data: TransferResponse = await response.json() as TransferResponse
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
    void fetchTransfers('replace')
  }

  const loadMore = () => {
    offset.update((current) => current + PAGE_SIZE)
    void fetchTransfers('append')
  }

  const applyPreset = (minutes: number) => () => {
    const sinceDate = new Date(Date.now() - minutes * 60_000)
    since.update(() => toDatetimeLocal(sinceDate))
    until.update(() => '')
    if (sinceInput) sinceInput.value = toDatetimeLocal(sinceDate)
    if (untilInput) untilInput.value = ''
    offset.update(() => 0)
    void fetchTransfers('replace')
  }

  const jumpToLatest = () => {
    since.update(() => '')
    until.update(() => '')
    selectedKinds.update(() => new Set(ALL_KINDS))
    offset.update(() => 0)
    if (sinceInput) sinceInput.value = ''
    if (untilInput) untilInput.value = ''
    void fetchTransfers('replace')
  }

  const hasFilters = $derived(
    since.current !== '' ||
    until.current !== '' ||
    selectedKinds.current.size < ALL_KINDS.length
  )

  onMount(() => {
    void fetchTransfers('replace')
    const interval = setInterval(() => {
      if (offset.current === 0) void fetchTransfers('replace')
    }, POLL_INTERVAL_MS)
    return () => { clearInterval(interval) }
  })

  const handleKindChange = (selected: Set<string>) => {
    selectedKinds.update(() => selected)
    offset.update(() => 0)
    void fetchTransfers('replace')
  }

  const handleSinceChange = (event: Event) => {
    since.update(() => (event.target as HTMLInputElement).value)
    offset.update(() => 0)
    void fetchTransfers('replace')
  }

  const handleUntilChange = (event: Event) => {
    until.update(() => (event.target as HTMLInputElement).value)
    offset.update(() => 0)
    void fetchTransfers('replace')
  }

  type StatusStyle = { text: string; dot: string }

  const statusStyle = (status: string): StatusStyle => {
    const lower = status.toLowerCase()

    if (lower.includes('completed') || lower.includes('deposited') || lower.includes('confirmed')) {
      return { text: 'text-green-500', dot: 'bg-green-500' }
    }

    if (lower.includes('failed')) {
      return { text: 'text-destructive', dot: 'bg-destructive' }
    }

    return { text: 'text-muted-foreground', dot: 'bg-muted-foreground' }
  }
</script>

<Card.Root class="flex h-full flex-col overflow-hidden border-l-4 border-l-purple-500/50">
  <Card.Header class="shrink-0 space-y-2 pb-0">
    <Card.Title class="flex items-center justify-between">
      <span class="flex items-center gap-1.5">
        Cross-venue Transfers
        <span class="group relative cursor-help text-muted-foreground">
          <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 20 20" fill="currentColor" class="h-3.5 w-3.5"><path fill-rule="evenodd" d="M18 10a8 8 0 1 1-16 0 8 8 0 0 1 16 0Zm-7-4a1 1 0 1 1-2 0 1 1 0 0 1 2 0ZM9 9a.75.75 0 0 0 0 1.5h.253a.25.25 0 0 1 .244.304l-.459 2.066A1.75 1.75 0 0 0 10.747 15H11a.75.75 0 0 0 0-1.5h-.253a.25.25 0 0 1-.244-.304l.459-2.066A1.75 1.75 0 0 0 9.253 9H9Z" clip-rule="evenodd" /></svg>
          <span class="pointer-events-none absolute left-0 top-full z-50 mt-1 hidden w-64 rounded bg-popover px-3 py-2 text-xs font-normal text-popover-foreground shadow-lg group-hover:block">
            Asset movements between venues to rebalance inventory: equity mints (Alpaca to onchain), redemptions (onchain to Alpaca), and USDC bridges (Base/Ethereum via CCTP).
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
      <MultiSelect label="Type" options={kindOptions} selected={selectedKinds.current} onchange={handleKindChange} />

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
        Failed to load transfers: {error.current}
      </div>
    {:else if entries.current.length === 0 && !loading.current}
      <div class="flex h-full items-center justify-center text-muted-foreground">
        No transfers
      </div>
    {:else}
      <Table.Root>
        <Table.Header>
          <Table.Row>
            <Table.Head class="text-right">Started (UTC)</Table.Head>
            <Table.Head>Type</Table.Head>
            <Table.Head>Detail</Table.Head>
            <Table.Head class="text-right">Status</Table.Head>
            <Table.Head class="text-right">Updated (UTC)</Table.Head>
          </Table.Row>
        </Table.Header>
        <Table.Body>
          {#each entries.current as transfer, idx (transfer.id + transfer.kind)}
            {@const statusText = transfer.status.status}
            {@const style = statusStyle(statusText)}
            <Table.Row class={idx % 2 === 0 ? 'bg-muted/40' : ''}>
              <Table.Cell class="text-right font-mono text-xs text-muted-foreground">
                {formatUtc(transfer.startedAt)}
              </Table.Cell>
              <Table.Cell class="font-mono text-xs font-medium">
                {kindLabel(transfer.kind)}
              </Table.Cell>
              <Table.Cell class="font-mono text-xs">
                {transfer.symbol ?? ''}{#if transfer.quantity} {transfer.quantity}{/if}{#if transfer.amount} {transfer.amount}{/if}
              </Table.Cell>
              <Table.Cell class="text-right text-xs {style.text}">
                <span class="inline-flex items-center gap-1.5">
                  <span class="inline-block h-1.5 w-1.5 rounded-full {style.dot}"></span>
                  {statusText}
                </span>
              </Table.Cell>
              <Table.Cell class="text-right font-mono text-xs text-muted-foreground">
                {formatUtc(transfer.updatedAt)}
              </Table.Cell>
            </Table.Row>
          {/each}
        </Table.Body>
      </Table.Root>

      {#if hasMore.current}
        <div class="flex justify-center py-2">
          <button class="rounded border bg-background px-3 py-1 text-xs hover:bg-accent" onclick={loadMore} disabled={loadingMore.current}>
            {loadingMore.current ? 'Loading...' : 'Load older transfers'}
          </button>
        </div>
      {/if}

      <div class="pointer-events-none sticky bottom-0 h-8 bg-gradient-to-t from-card to-transparent"></div>
    {/if}
  </Card.Content>
</Card.Root>
