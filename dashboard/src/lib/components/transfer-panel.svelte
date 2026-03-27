<script lang="ts">
  import { browser } from '$app/environment'
  import { createQuery } from '@tanstack/svelte-query'
  import * as Card from '$lib/components/ui/card'
  import * as Table from '$lib/components/ui/table'
  import type { TransferOperation } from '$lib/api/TransferOperation'
  import { matcher } from '$lib/fp'
  import { decimalCompare, formatDecimal } from '$lib/decimal'
  import { reactive } from '$lib/frp.svelte'

  const activeQuery = createQuery<TransferOperation[]>(() => ({
    queryKey: ['transfers', 'active'],
    enabled: false
  }))

  const recentQuery = createQuery<TransferOperation[]>(() => ({
    queryKey: ['transfers', 'recent'],
    enabled: false
  }))

  const activeTransfers = $derived(activeQuery.data ?? [])
  const recentTransfers = $derived(recentQuery.data ?? [])

  const allTransfers = $derived.by(() => {
    const byId = new Map(activeTransfers.map((transfer) => [transfer.id, transfer]))
    for (const transfer of recentTransfers) {
      if (!byId.has(transfer.id)) byId.set(transfer.id, transfer)
    }
    return [...byId.values()]
  })

  const stripPrefix = (symbol: string): string =>
    symbol.startsWith('t') ? symbol.slice(1) : symbol

  type Formatted = { display: string; full: string; truncated: boolean }

  const fmtTransferAmount = (value: string): Formatted => {
    const formatted = formatDecimal(value, 2).replace(/0+$/, '').replace(/\.$/, '')
    return { display: formatted, full: value, truncated: false }
  }

  const matchKind = matcher<TransferOperation>()('kind')

  const transferDestination = (transfer: TransferOperation): string =>
    matchKind(transfer, {
      equity_mint: () => 'Raindex',
      equity_redemption: () => 'Alpaca',
      usdc_bridge: ({ direction }) =>
        direction === 'alpaca_to_base' ? 'Raindex' : 'Alpaca'
    })

  const transferUnderlying = (transfer: TransferOperation): string =>
    matchKind(transfer, {
      equity_mint: ({ symbol }) => stripPrefix(symbol),
      equity_redemption: ({ symbol }) => stripPrefix(symbol),
      usdc_bridge: () => 'USD'
    })

  const transferAmountRaw = (transfer: TransferOperation): string =>
    matchKind(transfer, {
      equity_mint: ({ quantity }) => quantity,
      equity_redemption: ({ quantity }) => quantity,
      usdc_bridge: ({ amount }) => amount
    })

  const transferAmount = (transfer: TransferOperation): Formatted =>
    matchKind(transfer, {
      equity_mint: ({ quantity }) => fmtTransferAmount(quantity),
      equity_redemption: ({ quantity }) => fmtTransferAmount(quantity),
      usdc_bridge: ({ amount }) => fmtTransferAmount(amount),
    })

  const formatTime = (timestamp: string): string => {
    const date = new Date(timestamp)
    const locale = browser ? navigator.language : 'en-US'
    return date.toLocaleTimeString(locale, {
      hour12: false,
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit'
    })
  }

  type StatusStyle = { text: string; dot: string }

  const statusStyle = (status: string): StatusStyle => {
    if (status === 'completed')
      return {
        text: 'text-muted-foreground',
        dot: 'bg-muted-foreground'
      }

    if (status === 'failed')
      return {
        text: 'text-destructive',
        dot: 'bg-destructive'
      }

    return { text: '', dot: 'bg-green-500' }
  }

  type SortDir = 'asc' | 'desc'
  type SortState<Col extends string> = { column: Col; dir: SortDir } | null

  const toggleSort = <Col extends string>(
    current: SortState<Col>,
    column: Col
  ): SortState<Col> => {
    if (current?.column === column) {
      return current.dir === 'asc' ? { column, dir: 'desc' } : null
    }
    return { column, dir: 'asc' }
  }

  const sortIndicator = <Col extends string>(
    state: SortState<Col>,
    column: Col
  ): string => {
    if (state?.column !== column) return ''
    return state.dir === 'asc' ? ' \u25B2' : ' \u25BC'
  }

  const sortBtnClass = [
    'w-full',
    'cursor-pointer',
    'select-none',
    'focus-visible:outline-none',
    'focus-visible:ring-1',
    'focus-visible:ring-ring'
  ].join(' ')

  const ariaSort = <Col extends string>(
    state: SortState<Col>,
    column: Col
  ): 'ascending' | 'descending' | 'none' => {
    if (state?.column !== column) return 'none'
    return state.dir === 'asc' ? 'ascending' : 'descending'
  }

  type TransferCol = 'time' | 'destination' | 'amount' | 'underlying' | 'status'
  const transferSort = reactive<SortState<TransferCol>>(null)
  const sortTransfer = (col: TransferCol) => () => { transferSort.update((current) => toggleSort(current, col)) }

  const transferComparators: Record<TransferCol, (lhs: TransferOperation, rhs: TransferOperation) => number> = {
    time: (lhs, rhs) =>
      lhs.startedAt.localeCompare(rhs.startedAt),
    destination: (lhs, rhs) =>
      transferDestination(lhs).localeCompare(transferDestination(rhs)),
    amount: (lhs, rhs) =>
      decimalCompare(transferAmountRaw(lhs), transferAmountRaw(rhs)),
    underlying: (lhs, rhs) =>
      transferUnderlying(lhs).localeCompare(transferUnderlying(rhs)),
    status: (lhs, rhs) =>
      lhs.status.status.localeCompare(rhs.status.status)
  }

  const sortedTransfers = $derived.by(() => {
    if (!transferSort.current) return allTransfers
    const { column, dir } = transferSort.current
    const cmp = transferComparators[column]
    return [...allTransfers].sort((lhs, rhs) =>
      dir === 'desc' ? -cmp(lhs, rhs) : cmp(lhs, rhs)
    )
  })
</script>

<Card.Root class="flex h-full min-h-56 flex-col overflow-hidden">
  <Card.Header class="shrink-0 pb-0">
    <Card.Title class="flex items-center justify-between">
      <span>Cross-venue Transfers</span>
      <span class="text-sm font-normal text-muted-foreground">
        {#if activeTransfers.length > 0}
          [{activeTransfers.length}/{allTransfers.length}] in-flight
        {:else}
          {allTransfers.length} transfers
        {/if}
      </span>
    </Card.Title>
  </Card.Header>
  <Card.Content class="relative min-h-0 flex-1 overflow-auto px-6 pt-0">
    {#if allTransfers.length === 0}
      <div class="flex h-full items-center justify-center text-muted-foreground">
        No transfers
      </div>
    {:else}
      <Table.Root>
        <Table.Header>
          <Table.Row>
            <Table.Head aria-sort={ariaSort(transferSort.current, 'time')}>
              <button class="{sortBtnClass} text-left" onclick={sortTransfer('time')}>
                Time{sortIndicator(transferSort.current, 'time')}
              </button>
            </Table.Head>

            <Table.Head aria-sort={ariaSort(transferSort.current, 'destination')}>
              <button class="{sortBtnClass} text-left" onclick={sortTransfer('destination')}>
                Destination{sortIndicator(transferSort.current, 'destination')}
              </button>
            </Table.Head>

            <Table.Head class="text-right pr-6" aria-sort={ariaSort(transferSort.current, 'amount')}>
              <button class="{sortBtnClass} text-right" onclick={sortTransfer('amount')}>
                Amount{sortIndicator(transferSort.current, 'amount')}
              </button>
            </Table.Head>

            <Table.Head aria-sort={ariaSort(transferSort.current, 'underlying')}>
              <button class="{sortBtnClass} text-left" onclick={sortTransfer('underlying')}>
                Underlying{sortIndicator(transferSort.current, 'underlying')}
              </button>
            </Table.Head>

            <Table.Head aria-sort={ariaSort(transferSort.current, 'status')}>
              <button class="{sortBtnClass} text-left" onclick={sortTransfer('status')}>
                Status{sortIndicator(transferSort.current, 'status')}
              </button>
            </Table.Head>
          </Table.Row>
        </Table.Header>
        <Table.Body>
          {#each sortedTransfers as transfer (transfer.id)}
            {@const style = statusStyle(transfer.status.status)}
            <Table.Row>
              <Table.Cell class="font-mono text-muted-foreground">
                {formatTime(transfer.startedAt)}
              </Table.Cell>
              <Table.Cell>
                {transferDestination(transfer)}
              </Table.Cell>
              {@const amt = transferAmount(transfer)}
              <Table.Cell class="text-right font-mono pr-6" title={amt.truncated ? amt.full : undefined}>
                {amt.display}
              </Table.Cell>
              <Table.Cell class="font-mono font-medium">
                {transferUnderlying(transfer)}
              </Table.Cell>
              <Table.Cell class={style.text}>
                <span class="inline-flex items-center gap-1.5">
                  <span class="inline-block h-1.5 w-1.5 rounded-full {style.dot}"></span>
                  {transfer.status.status}
                </span>
              </Table.Cell>
            </Table.Row>
          {/each}
        </Table.Body>
      </Table.Root>
      <div
        class="pointer-events-none sticky bottom-0 h-8 bg-gradient-to-t from-card to-transparent"
      ></div>
    {/if}
  </Card.Content>
</Card.Root>
