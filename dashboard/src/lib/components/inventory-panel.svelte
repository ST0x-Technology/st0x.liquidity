<script lang="ts">
  import { browser } from '$app/environment'
  import { createQuery } from '@tanstack/svelte-query'
  import * as Card from '$lib/components/ui/card'
  import * as Table from '$lib/components/ui/table'
  import * as Separator from '$lib/components/ui/separator'
  import * as Tooltip from '$lib/components/ui/tooltip'
  import type { Inventory } from '$lib/api/Inventory'
  import type { RebalancingTargets } from '$lib/api/RebalancingTargets'
  import type { TransferOperation } from '$lib/api/TransferOperation'
  import { matcher } from '$lib/fp'
  import { decimalCompare } from '$lib/decimal'
  import {
    fmt, fmtNum, fmtPct, fmtDeviation,
    computeTotal, computeRatio, isWithinThreshold,
    stripPrefix
  } from './inventory-panel'

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

  const rebalancingQuery = createQuery<RebalancingTargets>(() => ({
    queryKey: ['rebalancing'],
    queryFn: () =>
      Promise.resolve({
        equityOnchainRatio: '0',
        equityTriggerThreshold: '0',
        cashOnchainRatio: null,
        cashTriggerThreshold: null
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
  const rebalancing = $derived(rebalancingQuery.data)

  const activeTransfers = $derived(activeQuery.data ?? [])
  const recentTransfers = $derived(recentQuery.data ?? [])
  const allTransfers = $derived.by(() => {
    const byId = new Map(recentTransfers.map((transfer) => [transfer.id, transfer]))
    for (const transfer of activeTransfers) byId.set(transfer.id, transfer)
    return [...byId.values()]
  })

  const cashRatio = $derived(
    usdc ? computeRatio(usdc.onchainAvailable, usdc.offchainAvailable) : null
  )

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

  const transferAmountRaw = (transfer: TransferOperation): string =>
    matchKind(transfer, {
      equity_mint: (op) => op.quantity,
      equity_redemption: (op) => op.quantity,
      usdc_bridge: (op) => op.amount
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

  type EquityCol = 'underlying' | 'offchain' | 'onchain' | 'total' | 'ratio'
  let equitySort = $state<SortState<EquityCol>>(null)

  const sortedSymbols = $derived.by(() => {
    if (!equitySort) return symbols
    const sorted = [...symbols]
    const { column, dir } = equitySort
    sorted.sort((left, right) => {
      let cmp = 0
      if (column === 'underlying') cmp = stripPrefix(left.symbol).localeCompare(stripPrefix(right.symbol))
      else if (column === 'offchain') cmp = decimalCompare(left.offchainAvailable, right.offchainAvailable)
      else if (column === 'onchain') cmp = decimalCompare(left.onchainAvailable, right.onchainAvailable)
      else if (column === 'ratio') cmp = (computeRatio(left.onchainAvailable, left.offchainAvailable) ?? 0) - (computeRatio(right.onchainAvailable, right.offchainAvailable) ?? 0)
      else cmp = decimalCompare(computeTotal(left.onchainAvailable, left.offchainAvailable), computeTotal(right.onchainAvailable, right.offchainAvailable))
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
      else if (column === 'amount') cmp = decimalCompare(transferAmountRaw(left), transferAmountRaw(right))
      else if (column === 'underlying') cmp = transferUnderlying(left).localeCompare(transferUnderlying(right))
      else cmp = left.status.status.localeCompare(right.status.status)
      return dir === 'desc' ? -cmp : cmp
    })
    return sorted
  })
</script>

<Card.Root>
  <Card.Header class="pb-3">
    <Card.Title>Inventory Available</Card.Title>
  </Card.Header>
  <Card.Content class="px-6 pt-0">
    {#if symbols.length === 0 && !usdc}
      <div class="flex h-full items-center justify-center text-muted-foreground">
        No inventory data
      </div>
    {:else}
      <Table.Root>
        <Table.Body>
          {#if usdc}
            {@const cashTarget = parseFloat(rebalancing?.cashOnchainRatio ?? '0')}
            {@const cashThreshold = parseFloat(rebalancing?.cashTriggerThreshold ?? '0')}
            {@const within = isWithinThreshold(cashRatio, cashTarget, cashThreshold)}
            <Table.Row class="border-b">
              <Table.Head>Cash</Table.Head>
              <Table.Head class="text-right">USD</Table.Head>
              <Table.Head class="text-right">USDC</Table.Head>
              <Table.Head class="text-right">Total</Table.Head>
              <Table.Head class="text-right">
                <Tooltip.Root>
                  <Tooltip.Trigger class="cursor-help underline decoration-dotted underline-offset-4">Ratio</Tooltip.Trigger>
                  <Tooltip.Content>Onchain value as a percentage of total (onchain + offchain)</Tooltip.Content>
                </Tooltip.Root>
              </Table.Head>
              <Table.Head class="text-right">
                <Tooltip.Root>
                  <Tooltip.Trigger class="cursor-help underline decoration-dotted underline-offset-4">Dev.</Tooltip.Trigger>
                  <Tooltip.Content>Deviation from the configured onchain ratio target, in percentage points</Tooltip.Content>
                </Tooltip.Root>
              </Table.Head>
            </Table.Row>
            <Table.Row>
              <Table.Cell class="font-mono font-semibold"></Table.Cell>
              <Table.Cell class="text-right font-mono opacity-90">
                {fmt(usdc.offchainAvailable)}
              </Table.Cell>
              <Table.Cell class="text-right font-mono opacity-90">
                {fmt(usdc.onchainAvailable)}
              </Table.Cell>
              <Table.Cell class="text-right font-mono font-semibold">
                {fmtNum(computeTotal(usdc.onchainAvailable, usdc.offchainAvailable))}
              </Table.Cell>
              <Table.Cell class="text-right font-mono {within ? 'text-muted-foreground' : 'text-destructive'}">
                {cashRatio !== null ? fmtPct(cashRatio) : '-'}
              </Table.Cell>
              <Table.Cell class="text-right font-mono {within ? 'text-muted-foreground' : 'text-destructive'}">
                {fmtDeviation(cashRatio, cashTarget)}
              </Table.Cell>
            </Table.Row>

            <Table.Row>
              <Table.Cell colspan={6} class="h-4"></Table.Cell>
            </Table.Row>
          {/if}

          <Table.Row class="border-b">
            <Table.Head class="cursor-pointer select-none" onclick={() => equitySort = toggleSort(equitySort, 'underlying')}>Equity{sortIndicator(equitySort, 'underlying')}</Table.Head>
            <Table.Head class="cursor-pointer select-none text-right" onclick={() => equitySort = toggleSort(equitySort, 'offchain')}>Alpaca{sortIndicator(equitySort, 'offchain')}</Table.Head>
            <Table.Head class="cursor-pointer select-none text-right" onclick={() => equitySort = toggleSort(equitySort, 'onchain')}>Raindex{sortIndicator(equitySort, 'onchain')}</Table.Head>
            <Table.Head class="cursor-pointer select-none text-right" onclick={() => equitySort = toggleSort(equitySort, 'total')}>Total{sortIndicator(equitySort, 'total')}</Table.Head>
            <Table.Head class="cursor-pointer select-none text-right" onclick={() => equitySort = toggleSort(equitySort, 'ratio')}>
              <Tooltip.Root>
                <Tooltip.Trigger class="cursor-help underline decoration-dotted underline-offset-4">Ratio{sortIndicator(equitySort, 'ratio')}</Tooltip.Trigger>
                <Tooltip.Content>Onchain value as a percentage of total (onchain + offchain)</Tooltip.Content>
              </Tooltip.Root>
            </Table.Head>
            <Table.Head class="text-right">
              <Tooltip.Root>
                <Tooltip.Trigger class="cursor-help underline decoration-dotted underline-offset-4">Dev.</Tooltip.Trigger>
                <Tooltip.Content>Deviation from the configured onchain ratio target, in percentage points</Tooltip.Content>
              </Tooltip.Root>
            </Table.Head>
          </Table.Row>
          {#each sortedSymbols as item (item.symbol)}
            {@const ratio = computeRatio(item.onchainAvailable, item.offchainAvailable)}
            {@const equityTarget = parseFloat(rebalancing?.equityOnchainRatio ?? '0')}
            {@const equityThreshold = parseFloat(rebalancing?.equityTriggerThreshold ?? '0')}
            {@const within = isWithinThreshold(ratio, equityTarget, equityThreshold)}
            <Table.Row>
              <Table.Cell class="font-mono font-medium">
                {stripPrefix(item.symbol)}
              </Table.Cell>
              <Table.Cell class="text-right font-mono opacity-90">
                {fmt(item.offchainAvailable)}
              </Table.Cell>
              <Table.Cell class="text-right font-mono opacity-90">
                {fmt(item.onchainAvailable)}
              </Table.Cell>
              <Table.Cell class="text-right font-mono font-semibold">
                {fmtNum(computeTotal(item.onchainAvailable, item.offchainAvailable))}
              </Table.Cell>
              <Table.Cell class="text-right font-mono {within ? 'text-muted-foreground' : 'text-destructive'}">
                {ratio !== null ? fmtPct(ratio) : '-'}
              </Table.Cell>
              <Table.Cell class="text-right font-mono {within ? 'text-muted-foreground' : 'text-destructive'}">
                {fmtDeviation(ratio, equityTarget)}
              </Table.Cell>
            </Table.Row>
          {/each}
        </Table.Body>
      </Table.Root>

      {#if allTransfers.length > 0}
        <Separator.Root class="my-6" />

        <div class="pb-3 text-lg font-semibold leading-none tracking-tight">Inventory Transfers</div>

        <Table.Root>
          <Table.Header>
            <Table.Row>
              <Table.Head class="cursor-pointer select-none" onclick={() => transferSort = toggleSort(transferSort, 'time')}>Time{sortIndicator(transferSort, 'time')}</Table.Head>
              <Table.Head class="cursor-pointer select-none" onclick={() => transferSort = toggleSort(transferSort, 'purpose')}>
                <Tooltip.Root>
                  <Tooltip.Trigger class="cursor-help underline decoration-dotted underline-offset-4">Purpose{sortIndicator(transferSort, 'purpose')}</Tooltip.Trigger>
                  <Tooltip.Content class="max-w-xs">Providing Liquidity: onchain ratio fell below threshold, transferring from hedging venue. Hedging Risk: onchain ratio exceeded threshold, transferring to hedging venue.</Tooltip.Content>
                </Tooltip.Root>
              </Table.Head>
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

    {/if}
  </Card.Content>
</Card.Root>
