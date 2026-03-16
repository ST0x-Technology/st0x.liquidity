<script lang="ts">
  import { browser } from '$app/environment'
  import { createQuery } from '@tanstack/svelte-query'
  import * as Card from '$lib/components/ui/card'
  import * as Table from '$lib/components/ui/table'
  import * as Separator from '$lib/components/ui/separator'
  import type { Inventory } from '$lib/api/Inventory'
  import type { TransferOperation } from '$lib/api/TransferOperation'
  import { matcher } from '$lib/fp'
  import { decimalAdd, decimalCompare, decimalIsZero, formatDecimal } from '$lib/decimal'
  import { reactive } from '$lib/frp.svelte'

  const inventoryQuery = createQuery<Inventory>(() => ({
    queryKey: ['inventory'],
    enabled: false
  }))

  const activeQuery = createQuery<TransferOperation[]>(() => ({
    queryKey: ['transfers', 'active'],
    enabled: false
  }))

  const recentQuery = createQuery<TransferOperation[]>(() => ({
    queryKey: ['transfers', 'recent'],
    enabled: false
  }))

  const inventory = $derived(inventoryQuery.data)
  const symbols = $derived(inventory?.perSymbol ?? [])
  const usdc = $derived(inventory?.usdc)

  const activeTransfers = $derived(activeQuery.data ?? [])
  const recentTransfers = $derived(recentQuery.data ?? [])
  const allTransfers = $derived.by(() => {
    const byId = new Map(activeTransfers.map((transfer) => [transfer.id, transfer]))
    for (const transfer of recentTransfers) {
      if (!byId.has(transfer.id)) byId.set(transfer.id, transfer)
    }
    return [...byId.values()]
  })

  const decimalPlaces = (value: string): number => {
    const dotIdx = value.indexOf('.')
    if (dotIdx === -1) return 0
    return Math.max(2, value.length - dotIdx - 1)
  }

  const fmt = (value: string): string => {
    if (decimalIsZero(value)) return '-'
    return formatDecimal(value, decimalPlaces(value))
  }

  const fmtNum = (value: string): string =>
    formatDecimal(value, decimalPlaces(value))

  type CashRow = { label: string; value: string; decimals: number }

  const cashRows = $derived.by((): CashRow[] => {
    if (!usdc) return []
    return [
      { label: 'Total', value: decimalAdd(usdc.onchainAvailable, usdc.offchainAvailable), decimals: 6 },
      { label: 'USDC', value: usdc.onchainAvailable, decimals: 6 },
      { label: 'USD', value: usdc.offchainAvailable, decimals: 2 }
    ]
  })

  const maxDecimals = 6

  const fmtCashAligned = (value: string, decimals: number): string => {
    if (decimalIsZero(value)) return '-'
    const formatted = formatDecimal(value, decimals)
    const trailingPad = maxDecimals - decimals
    return formatted + '\u00A0'.repeat(trailingPad)
  }

  const stripPrefix = (symbol: string): string =>
    symbol.startsWith('t') ? symbol.slice(1) : symbol

  const matchKind = matcher<TransferOperation>()('kind')

  const transferPurpose = (transfer: TransferOperation): string =>
    matchKind(transfer, {
      equity_mint: () => 'Providing Liquidity',
      equity_redemption: () => 'Hedging Risk',
      usdc_bridge: ({ direction }) =>
        direction === 'alpaca_to_base' ? 'Providing Liquidity' : 'Hedging Risk'
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

  const transferAmount = (transfer: TransferOperation): string =>
    matchKind(transfer, {
      equity_mint: ({ quantity }) => fmtNum(quantity),
      equity_redemption: ({ quantity }) => fmtNum(quantity),
      usdc_bridge: ({ amount }) => fmtNum(amount)
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

  type EquityCol = 'underlying' | 'raindex' | 'alpaca' | 'total'
  const equitySort = reactive<SortState<EquityCol>>(null)
  const sortEquity = (col: EquityCol) => () => { equitySort.update((current) => toggleSort(current, col)) }

  type SymbolInventory = Inventory['perSymbol'][number]

  const equityComparators: Record<EquityCol, (lhs: SymbolInventory, rhs: SymbolInventory) => number> = {
    underlying: (lhs, rhs) =>
      stripPrefix(lhs.symbol).localeCompare(stripPrefix(rhs.symbol)),
    raindex: (lhs, rhs) =>
      decimalCompare(lhs.onchainAvailable, rhs.onchainAvailable),
    alpaca: (lhs, rhs) =>
      decimalCompare(lhs.offchainAvailable, rhs.offchainAvailable),
    total: (lhs, rhs) =>
      decimalCompare(
        decimalAdd(lhs.onchainAvailable, lhs.offchainAvailable),
        decimalAdd(rhs.onchainAvailable, rhs.offchainAvailable)
      )
  }

  const sortedSymbols = $derived.by(() => {
    if (!equitySort.current) return symbols
    const { column, dir } = equitySort.current
    const cmp = equityComparators[column]
    return [...symbols].sort((lhs, rhs) =>
      dir === 'desc' ? -cmp(lhs, rhs) : cmp(lhs, rhs)
    )
  })

  type TransferCol = 'time' | 'purpose' | 'amount' | 'underlying' | 'status'
  const transferSort = reactive<SortState<TransferCol>>(null)
  const sortTransfer = (col: TransferCol) => () => { transferSort.update((current) => toggleSort(current, col)) }

  const transferComparators: Record<TransferCol, (lhs: TransferOperation, rhs: TransferOperation) => number> = {
    time: (lhs, rhs) =>
      lhs.startedAt.localeCompare(rhs.startedAt),
    purpose: (lhs, rhs) =>
      transferPurpose(lhs).localeCompare(transferPurpose(rhs)),
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
  <Card.Header class="shrink-0 pb-3">
    <Card.Title>Available Inventory</Card.Title>
  </Card.Header>
  <Card.Content class="relative min-h-0 flex-1 overflow-auto px-6 pt-0">
    {#if !inventory}
      <div class="flex h-full items-center justify-center text-muted-foreground">
        Waiting for inventory data…
      </div>
    {:else}
      <div class="flex gap-0">
        <div class="flex-[2] pr-6">
          <Table.Root>
            <Table.Header>
              <Table.Row>
                <Table.Head aria-sort={ariaSort(equitySort.current, 'underlying')}>
                  <button class="{sortBtnClass} text-left" onclick={sortEquity('underlying')}>
                    Equity{sortIndicator(equitySort.current, 'underlying')}
                  </button>
                </Table.Head>

                <Table.Head class="text-right" aria-sort={ariaSort(equitySort.current, 'raindex')}>
                  <button class="{sortBtnClass} text-right" onclick={sortEquity('raindex')}>
                    Raindex{sortIndicator(equitySort.current, 'raindex')}
                  </button>
                </Table.Head>

                <Table.Head class="text-right" aria-sort={ariaSort(equitySort.current, 'alpaca')}>
                  <button class="{sortBtnClass} text-right" onclick={sortEquity('alpaca')}>
                    Alpaca{sortIndicator(equitySort.current, 'alpaca')}
                  </button>
                </Table.Head>

                <Table.Head class="text-right" aria-sort={ariaSort(equitySort.current, 'total')}>
                  <button class="{sortBtnClass} text-right" onclick={sortEquity('total')}>
                    Total{sortIndicator(equitySort.current, 'total')}
                  </button>
                </Table.Head>
              </Table.Row>
            </Table.Header>
            <Table.Body>
              {#each sortedSymbols as item (item.symbol)}
                <Table.Row>
                  <Table.Cell class="font-mono font-medium">
                    {stripPrefix(item.symbol)}
                  </Table.Cell>
                  <Table.Cell class="text-right font-mono opacity-90">
                    {fmt(item.onchainAvailable)}
                  </Table.Cell>
                  <Table.Cell class="text-right font-mono opacity-90">
                    {fmt(item.offchainAvailable)}
                  </Table.Cell>
                  <Table.Cell class="text-right font-mono font-semibold">
                    {fmtNum(decimalAdd(item.onchainAvailable, item.offchainAvailable))}
                  </Table.Cell>
                </Table.Row>
              {/each}
            </Table.Body>
          </Table.Root>
        </div>

        {#if usdc}
          <div class="flex-1 border-l pl-6">
            <Table.Root>
              <Table.Header>
                <Table.Row>
                  <Table.Head>Currency</Table.Head>
                  <Table.Head>Balance</Table.Head>
                </Table.Row>
              </Table.Header>
              <Table.Body>
                {#each cashRows as row (row.label)}
                  <Table.Row>
                    <Table.Cell
                      class="font-mono {row.label === 'Total' ? 'font-semibold' : 'font-medium'}"
                    >
                      {row.label}
                    </Table.Cell>
                    <Table.Cell
                      class="font-mono whitespace-pre {row.label === 'Total' ? 'font-semibold' : ''}"
                    >
                      {fmtCashAligned(row.value, row.decimals)}
                    </Table.Cell>
                  </Table.Row>
                {/each}
              </Table.Body>
            </Table.Root>
          </div>
        {/if}
      </div>

      {#if allTransfers.length > 0}
        <Separator.Root class="my-6" />

        <div class="pb-3 text-lg font-semibold leading-none tracking-tight">Inventory Transfers</div>

        <Table.Root>
          <Table.Header>
            <Table.Row>
              <Table.Head aria-sort={ariaSort(transferSort.current, 'time')}>
                <button class="{sortBtnClass} text-left" onclick={sortTransfer('time')}>
                  Time{sortIndicator(transferSort.current, 'time')}
                </button>
              </Table.Head>

              <Table.Head aria-sort={ariaSort(transferSort.current, 'purpose')}>
                <button class="{sortBtnClass} text-left" onclick={sortTransfer('purpose')}>
                  Purpose{sortIndicator(transferSort.current, 'purpose')}
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
                  {transferPurpose(transfer)}
                </Table.Cell>
                <Table.Cell class="text-right font-mono pr-6">
                  {transferAmount(transfer)}
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
      {/if}

      <div
        class="pointer-events-none sticky bottom-0 h-8 bg-gradient-to-t from-card to-transparent"
      ></div>
    {/if}
  </Card.Content>
</Card.Root>
