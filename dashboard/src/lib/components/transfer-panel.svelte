<script lang="ts">
  import { onMount } from 'svelte'
  import * as Card from '$lib/components/ui/card'
  import * as Table from '$lib/components/ui/table'
  import MultiSelect from '$lib/components/multi-select.svelte'
  import { reactive } from '$lib/frp.svelte'
  import { getApiBaseUrl, getExplorerTxUrl } from '$lib/env'
  import { formatUtc, toDatetimeLocal, TIME_PRESETS, FETCH_TIMEOUT_MS, toRfc3339 } from '$lib/time'
  import { formatDecimal } from '$lib/decimal'
  import {
    kindLabel,
    statusStyle,
    humanizeStatus,
    humanizeStep,
    isTxHash,
    isTransferRef,
    formatFieldName,
    extractTimestamp,
    detailFields,
  } from '$lib/transfer'

  const isNumeric = (value: unknown): boolean =>
    typeof value === 'string' && value !== '' && !Number.isNaN(Number(value))

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

  type TransferEvent = {
    step: string
    sequence: number
    payload: Record<string, unknown>
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

  let statusInfoOpen = $state(false)

  // -- Detail modal state --

  let detailDialogEl: HTMLDialogElement | undefined = $state()
  const detailEvents = reactive<TransferEvent[]>([])
  const detailTransfer = reactive<TransferEntry | null>(null)
  const detailLoading = reactive(false)
  const detailError = reactive<string | null>(null)
  let detailAbortController: AbortController | undefined

  const openDetail = async (transfer: TransferEntry) => {
    detailAbortController?.abort()
    const controller = new AbortController()
    detailAbortController = controller
    const isAborted = () => controller.signal.aborted

    detailTransfer.update(() => transfer)
    detailLoading.update(() => true)
    detailError.update(() => null)
    detailEvents.update(() => [])
    detailDialogEl?.showModal()

    try {
      const baseUrl = getApiBaseUrl()
      const response = await fetch(
        `${baseUrl}/transfers/${transfer.kind}/${transfer.id}/events`,
        {
          signal: AbortSignal.any([
            controller.signal,
            AbortSignal.timeout(FETCH_TIMEOUT_MS),
          ]),
        },
      )

      if (isAborted()) return

      if (!response.ok) {
        detailError.update(() => `HTTP ${String(response.status)}`)
        return
      }

      const data = (await response.json()) as { events: TransferEvent[] }

      if (isAborted()) return

      detailEvents.update(() => data.events)
    } catch (err) {
      if (isAborted()) return
      detailError.update(() => err instanceof Error ? err.message : 'Unknown error')
    } finally {
      if (!isAborted()) {
        detailLoading.update(() => false)
      }
    }
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
            <Table.Head class="w-8"></Table.Head>
            <Table.Head class="text-right">Started (UTC)</Table.Head>
            <Table.Head>Type</Table.Head>
            <Table.Head>Asset</Table.Head>
            <Table.Head class="text-right">Amount</Table.Head>
            <Table.Head class="text-right">
              <span class="inline-flex items-center justify-end gap-1">
                Status
                <button
                  class="text-muted-foreground hover:text-foreground"
                  aria-label="Show status legend"
                  onclick={() => { statusInfoOpen = !statusInfoOpen }}
                >
                  <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 20 20" fill="currentColor" class="h-3.5 w-3.5"><path fill-rule="evenodd" d="M18 10a8 8 0 1 1-16 0 8 8 0 0 1 16 0Zm-7-4a1 1 0 1 1-2 0 1 1 0 0 1 2 0ZM9 9a.75.75 0 0 0 0 1.5h.253a.25.25 0 0 1 .244.304l-.459 2.066A1.75 1.75 0 0 0 10.747 15H11a.75.75 0 0 0 0-1.5h-.253a.25.25 0 0 1-.244-.304l.459-2.066A1.75 1.75 0 0 0 9.253 9H9Z" clip-rule="evenodd" /></svg>
                </button>
              </span>
            </Table.Head>
            <Table.Head class="text-right">Updated (UTC)</Table.Head>
          </Table.Row>
        </Table.Header>
        <Table.Body>
          {#each entries.current as transfer, idx (transfer.id + transfer.kind)}
            {@const statusText = transfer.status.status}
            {@const style = statusStyle(statusText)}
            <Table.Row class={idx % 2 === 0 ? 'bg-muted/40' : ''}>
              <Table.Cell class="w-8 px-1">
                <button
                  class="inline-flex items-center text-muted-foreground hover:text-foreground"
                  title="View event history"
                  onclick={() => openDetail(transfer)}
                >
                  <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 20 20" fill="currentColor" class="h-3.5 w-3.5">
                    <path fill-rule="evenodd" d="M18 10a8 8 0 1 1-16 0 8 8 0 0 1 16 0Zm-7-4a1 1 0 1 1-2 0 1 1 0 0 1 2 0ZM9 9a.75.75 0 0 0 0 1.5h.253a.25.25 0 0 1 .244.304l-.459 2.066A1.75 1.75 0 0 0 10.747 15H11a.75.75 0 0 0 0-1.5h-.253a.25.25 0 0 1-.244-.304l.459-2.066A1.75 1.75 0 0 0 9.253 9H9Z" clip-rule="evenodd" />
                  </svg>
                </button>
              </Table.Cell>
              <Table.Cell class="text-right font-mono text-xs text-muted-foreground">
                {formatUtc(transfer.startedAt)}
              </Table.Cell>
              <Table.Cell class="font-mono text-xs font-medium">
                {kindLabel(transfer.kind)}
              </Table.Cell>
              <Table.Cell class="font-mono text-xs">
                {transfer.kind === 'usdc_bridge' ? 'USDC' : transfer.symbol ?? ''}
              </Table.Cell>
              <Table.Cell class="text-right font-mono text-xs">
                {#if transfer.quantity}{formatDecimal(transfer.quantity, 3)}{:else if transfer.amount}{formatDecimal(transfer.amount, 3)}{/if}
              </Table.Cell>
              <Table.Cell class="text-right text-xs {style.text}">
                <span class="inline-flex items-center gap-1.5">
                  <span class="inline-block h-1.5 w-1.5 rounded-full {style.dot}"></span>
                  {humanizeStatus(statusText)}
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

{#if statusInfoOpen}
  <!-- svelte-ignore a11y_no_static_element_interactions -->
  <div
    class="fixed inset-0 z-40"
    onclick={() => { statusInfoOpen = false }}
    onkeydown={(event) => { if (event.key === 'Escape') statusInfoOpen = false }}
  ></div>
  <div class="fixed right-8 top-1/3 z-50 w-64 rounded-lg border bg-popover px-4 py-3 text-xs text-popover-foreground shadow-lg">
    <div class="mb-2 font-semibold">Status flows by type</div>

    <div class="mb-1.5">
      <div class="mb-0.5 font-medium text-muted-foreground">Mint</div>
      <div>Minting → Wrapping → Depositing → <span class="text-green-500">Completed</span></div>
    </div>

    <div class="mb-1.5">
      <div class="mb-0.5 font-medium text-muted-foreground">Redeem</div>
      <div>Withdrawing → Unwrapping → Sending → Pending Confirmation → <span class="text-green-500">Completed</span></div>
    </div>

    <div class="mb-1.5">
      <div class="mb-0.5 font-medium text-muted-foreground">USDC Bridge</div>
      <div>Converting → Withdrawing → Bridging → Depositing → <span class="text-green-500">Completed</span></div>
    </div>

    <div class="text-muted-foreground">Any step can transition to <span class="text-destructive">Failed</span></div>
  </div>
{/if}

<dialog
  bind:this={detailDialogEl}
  class="w-full max-w-lg rounded-lg border bg-card p-0 text-foreground shadow-lg backdrop:bg-black/50"
  onclick={(event) => { if (event.target === detailDialogEl) detailDialogEl.close() }}
>
  {#if detailTransfer.current}
    {@const transfer = detailTransfer.current}
    <div class="flex items-center justify-between border-b px-5 py-3">
      <div class="flex items-center gap-2 text-sm font-semibold">
        <span>{kindLabel(transfer.kind)}</span>
        <span class="font-mono text-xs font-normal text-muted-foreground">
          {transfer.kind === 'usdc_bridge' ? 'USDC' : transfer.symbol ?? ''}
        </span>
        {#if transfer.quantity}
          <span class="font-mono text-xs font-normal text-muted-foreground">
            {formatDecimal(transfer.quantity, 3)}
          </span>
        {:else if transfer.amount}
          <span class="font-mono text-xs font-normal text-muted-foreground">
            {formatDecimal(transfer.amount, 3)}
          </span>
        {/if}
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
        <div class="relative space-y-0 border-l-2 border-muted pl-4">
          {#each detailEvents.current as event (event.sequence)}
            {@const stepStyle = statusStyle(event.step)}
            {@const timestamp = extractTimestamp(event.payload)}
            {@const fields = detailFields(event.payload)}
            <div class="relative pb-4">
              <!-- Timeline dot -->
              <div class="absolute -left-[calc(0.25rem+1px+1rem)] top-0.5 h-2 w-2 rounded-full {stepStyle.dot}"></div>

              <div class="text-xs font-medium {stepStyle.text}">
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
                        {#if key === 'reason'}
                          <span class="text-destructive">{value}</span>
                        {:else if key === 'failure'}
                          <span class="text-destructive">
                            {#if typeof value === 'object' && value !== null}
                              {#if 'ApiError' in value}
                                API error{@const apiErr = value as Record<string, unknown>}{#if apiErr['status_code']} (status {apiErr['status_code']}){/if}
                              {:else if 'Timeout' in value}
                                Timeout
                              {:else}
                                {JSON.stringify(value)}
                              {/if}
                            {:else if isNumeric(value)}
                              {formatDecimal(String(value), 3)}
                            {:else}
                              {String(value)}
                            {/if}
                          </span>
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
                        {:else if isTransferRef(value)}
                          {#if 'OnchainTx' in value}
                            {@const txHash = value['OnchainTx']}
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
                          {:else if 'AlpacaId' in value}
                            <span class="text-muted-foreground">Alpaca: {value['AlpacaId']}</span>
                          {:else}
                            {JSON.stringify(value)}
                          {/if}
                        {:else if typeof value === 'object' && value !== null}
                          {JSON.stringify(value)}
                        {:else if isNumeric(value)}
                          {formatDecimal(String(value), 3)}
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
