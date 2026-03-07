<script lang="ts">
  import { browser } from '$app/environment'
  import { createQuery } from '@tanstack/svelte-query'
  import * as Card from '$lib/components/ui/card'
  import * as Table from '$lib/components/ui/table'
  import * as Separator from '$lib/components/ui/separator'
  import type { Inventory } from '$lib/api/Inventory'
  import type { TransferOperation } from '$lib/api/TransferOperation'
  import { matcher } from '$lib/fp'

  const inventoryQuery = createQuery<Inventory>(() => ({
    queryKey: ['inventory'],
    queryFn: () =>
      Promise.resolve({
        perSymbol: [],
        usdc: {
          onchainAvailable: '0',
          onchainInflight: '0',
          offchainAvailable: '0',
          offchainInflight: '0'
        },
        snapshotAt: null
      }),
    staleTime: Infinity
  }))

  const activeQuery = createQuery<TransferOperation[]>(() => ({
    queryKey: ['transfers', 'active'],
    queryFn: () => Promise.resolve([]),
    staleTime: Infinity
  }))

  const recentQuery = createQuery<TransferOperation[]>(() => ({
    queryKey: ['transfers', 'recent'],
    queryFn: () => Promise.resolve([]),
    staleTime: Infinity
  }))

  const inventory = $derived(inventoryQuery.data)
  const symbols = $derived(inventory?.perSymbol ?? [])
  const usdc = $derived(inventory?.usdc)

  const activeTransfers = $derived(activeQuery.data ?? [])
  const recentTransfers = $derived(recentQuery.data ?? [])
  const allTransfers = $derived([...activeTransfers, ...recentTransfers])

  const decimalPlaces = (value: string): number => {
    const dotIdx = value.indexOf('.')
    if (dotIdx === -1) return 0
    return Math.max(2, value.length - dotIdx - 1)
  }

  const fmt = (value: string): string => {
    const num = parseFloat(value)
    if (num === 0) return '-'
    const dp = decimalPlaces(value)
    return num.toLocaleString('en-US', { minimumFractionDigits: dp, maximumFractionDigits: dp })
  }

  const fmtNum = (value: string): string => {
    const num = parseFloat(value)
    const dp = decimalPlaces(value)
    return num.toLocaleString('en-US', { minimumFractionDigits: dp, maximumFractionDigits: dp })
  }

  type CashRow = { label: string; value: string; decimals: number }

  const cashRows = $derived.by((): CashRow[] => {
    if (!usdc) return []
    const usdcVal = parseFloat(usdc.onchainAvailable)
    const usdVal = parseFloat(usdc.offchainAvailable)
    const totalVal = usdcVal + usdVal
    return [
      { label: 'Total', value: String(totalVal), decimals: 6 },
      { label: 'USDC', value: String(usdcVal), decimals: 6 },
      { label: 'USD', value: String(usdVal), decimals: 2 }
    ]
  })

  const maxDecimals = 6

  const fmtCashAligned = (value: string, decimals: number): string => {
    const num = parseFloat(value)
    if (num === 0) return '-'
    const formatted = num.toLocaleString('en-US', {
      minimumFractionDigits: decimals,
      maximumFractionDigits: decimals
    })
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
      usdc_bridge: (op) => (op.direction === 'alpaca_to_base' ? 'Providing Liquidity' : 'Hedging Risk')
    })

  const transferUnderlying = (transfer: TransferOperation): string =>
    matchKind(transfer, {
      equity_mint: (op) => stripPrefix(op.symbol),
      equity_redemption: (op) => stripPrefix(op.symbol),
      usdc_bridge: () => 'USD'
    })

  const transferAmount = (transfer: TransferOperation): string =>
    matchKind(transfer, {
      equity_mint: (op) => fmtNum(op.quantity),
      equity_redemption: (op) => fmtNum(op.quantity),
      usdc_bridge: (op) => fmtNum(op.amount)
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
    if (status === 'completed') return { text: 'text-muted-foreground', dot: 'bg-muted-foreground' }
    if (status === 'failed') return { text: 'text-destructive', dot: 'bg-destructive' }
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

  type EquityCol = 'underlying' | 'raindex' | 'alpaca' | 'total'
  let equitySort = $state<SortState<EquityCol>>(null)

  const sortedSymbols = $derived.by(() => {
    if (!equitySort) return symbols
    const sorted = [...symbols]
    const { column, dir } = equitySort
    sorted.sort((left, right) => {
      let cmp = 0
      if (column === 'underlying') cmp = stripPrefix(left.symbol).localeCompare(stripPrefix(right.symbol))
      else if (column === 'raindex') cmp = parseFloat(left.onchainAvailable) - parseFloat(right.onchainAvailable)
      else if (column === 'alpaca') cmp = parseFloat(left.offchainAvailable) - parseFloat(right.offchainAvailable)
      else cmp = (parseFloat(left.onchainAvailable) + parseFloat(left.offchainAvailable)) - (parseFloat(right.onchainAvailable) + parseFloat(right.offchainAvailable))
      return dir === 'desc' ? -cmp : cmp
    })
    return sorted
  })

  type TransferCol = 'time' | 'purpose' | 'amount' | 'underlying' | 'status'
  let transferSort = $state<SortState<TransferCol>>(null)

  const sortedTransfers = $derived.by(() => {
    if (!transferSort) return allTransfers
    const sorted = [...allTransfers]
    const { column, dir } = transferSort
    sorted.sort((left, right) => {
      let cmp = 0
      if (column === 'time') cmp = left.startedAt.localeCompare(right.startedAt)
      else if (column === 'purpose') cmp = transferPurpose(left).localeCompare(transferPurpose(right))
      else if (column === 'amount') cmp = parseFloat(transferAmount(left).replace(/,/g, '')) - parseFloat(transferAmount(right).replace(/,/g, ''))
      else if (column === 'underlying') cmp = transferUnderlying(left).localeCompare(transferUnderlying(right))
      else cmp = left.status.status.localeCompare(right.status.status)
      return dir === 'desc' ? -cmp : cmp
    })
    return sorted
  })
</script>

<Card.Root class="flex h-full min-h-56 flex-col overflow-hidden">
  <Card.Header class="shrink-0 pb-3">
    <Card.Title>Available Inventory</Card.Title>
  </Card.Header>
  <Card.Content class="relative min-h-0 flex-1 overflow-auto px-6 pt-0">
    {#if symbols.length === 0 && !usdc}
      <div class="flex h-full items-center justify-center text-muted-foreground">
        No inventory data
      </div>
    {:else}
      <div class="flex gap-0">
        <div class="flex-[2] pr-6">
          <Table.Root>
            <Table.Header>
              <Table.Row>
                <Table.Head class="cursor-pointer select-none" onclick={() => equitySort = toggleSort(equitySort, 'underlying')}>Equity{sortIndicator(equitySort, 'underlying')}</Table.Head>
                <Table.Head class="cursor-pointer select-none text-right" onclick={() => equitySort = toggleSort(equitySort, 'raindex')}>Raindex{sortIndicator(equitySort, 'raindex')}</Table.Head>
                <Table.Head class="cursor-pointer select-none text-right" onclick={() => equitySort = toggleSort(equitySort, 'alpaca')}>Alpaca{sortIndicator(equitySort, 'alpaca')}</Table.Head>
                <Table.Head class="cursor-pointer select-none text-right" onclick={() => equitySort = toggleSort(equitySort, 'total')}>Total{sortIndicator(equitySort, 'total')}</Table.Head>
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
                    {fmtNum(String(parseFloat(item.onchainAvailable) + parseFloat(item.offchainAvailable)))}
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
                    <Table.Cell class="font-mono {row.label === 'Total' ? 'font-semibold' : 'font-medium'}">{row.label}</Table.Cell>
                    <Table.Cell class="font-mono whitespace-pre {row.label === 'Total' ? 'font-semibold' : ''}">
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
              <Table.Head class="cursor-pointer select-none" onclick={() => transferSort = toggleSort(transferSort, 'time')}>Time{sortIndicator(transferSort, 'time')}</Table.Head>
              <Table.Head class="cursor-pointer select-none" onclick={() => transferSort = toggleSort(transferSort, 'purpose')}>Purpose{sortIndicator(transferSort, 'purpose')}</Table.Head>
              <Table.Head class="cursor-pointer select-none text-right pr-6" onclick={() => transferSort = toggleSort(transferSort, 'amount')}>Amount{sortIndicator(transferSort, 'amount')}</Table.Head>
              <Table.Head class="cursor-pointer select-none" onclick={() => transferSort = toggleSort(transferSort, 'underlying')}>Underlying{sortIndicator(transferSort, 'underlying')}</Table.Head>
              <Table.Head class="cursor-pointer select-none" onclick={() => transferSort = toggleSort(transferSort, 'status')}>Status{sortIndicator(transferSort, 'status')}</Table.Head>
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
