<script lang="ts">
  import * as Table from '$lib/components/ui/table'
  import type { SymbolInventory } from '$lib/api/SymbolInventory'
  import type { UsdcInventory } from '$lib/api/UsdcInventory'
  import { decimalAdd, decimalCompare, decimalIsZero, formatDecimal } from '$lib/decimal'
  import { reactive } from '$lib/frp.svelte'

  interface Props {
    symbols: SymbolInventory[]
    usdc: UsdcInventory | undefined
  }

  let { symbols, usdc }: Props = $props()

  const MAX_DECIMAL_PLACES = 6

  const actualDecimalPlaces = (value: string): number => {
    const dotIdx = value.indexOf('.')
    if (dotIdx === -1) return 0
    return value.length - dotIdx - 1
  }

  const trimTrailingZeros = (formatted: string): string => {
    if (!formatted.includes('.')) return formatted
    return formatted.replace(/0+$/, '').replace(/\.$/, '')
  }

  type Formatted = { display: string; full: string; truncated: boolean }

  const fmtValue = (value: string): Formatted => {
    const actual = actualDecimalPlaces(value)
    const display = Math.min(MAX_DECIMAL_PLACES, Math.max(2, actual))
    const truncated = actual > display
    const formatted = truncated
      ? `${formatDecimal(value, 0)}.*`
      : trimTrailingZeros(formatDecimal(value, display))
    return {
      display: truncated ? `~${formatted}` : formatted,
      full: value,
      truncated,
    }
  }

  const fmt = (value: string): Formatted => {
    if (decimalIsZero(value)) return { display: '0', full: value, truncated: false }
    return fmtValue(value)
  }

  const stripPrefix = (symbol: string): string =>
    symbol.startsWith('t') ? symbol.slice(1) : symbol

  type SortDir = 'asc' | 'desc'
  type Col = 'asset' | 'alpaca' | 'inflight' | 'raindex' | 'total'
  type SortState = { column: Col; dir: SortDir } | null

  const sort = reactive<SortState>(null)

  const toggleSort = (column: Col) => () => {
    sort.update((current) => {
      if (current?.column === column) {
        return current.dir === 'asc' ? { column, dir: 'desc' } : null
      }
      return { column, dir: 'asc' }
    })
  }

  const ariaSort = (state: SortState, col: Col): 'ascending' | 'descending' | 'none' => {
    if (state?.column !== col) return 'none'
    return state.dir === 'asc' ? 'ascending' : 'descending'
  }

  const sortIndicator = (state: SortState, col: Col): string => {
    if (state?.column !== col) return ''
    return state.dir === 'asc' ? ' ↑' : ' ↓'
  }

  const sortBtnClass = 'w-full cursor-pointer select-none text-nowrap'

  type Row = {
    asset: string
    alpaca: Formatted
    inflight: Formatted
    raindex: Formatted
    total: Formatted
    isCash: boolean
  }

  const equityRows = $derived<Row[]>(
    symbols.map((item) => {
      const inflight = decimalAdd(item.onchainInflight, item.offchainInflight)
      const totalVal = decimalAdd(
        decimalAdd(item.onchainAvailable, item.offchainAvailable),
        inflight
      )
      return {
        asset: stripPrefix(item.symbol),
        alpaca: fmt(item.offchainAvailable),
        inflight: fmt(inflight),
        raindex: fmt(item.onchainAvailable),
        total: fmtValue(totalVal),
        isCash: false,
      }
    })
  )

  const cashRow = $derived.by<Row | null>(() => {
    if (!usdc) return null

    const inflight = decimalAdd(usdc.onchainInflight, usdc.offchainInflight)
    const total = decimalAdd(
      decimalAdd(usdc.onchainAvailable, usdc.offchainAvailable),
      inflight
    )

    return {
      asset: 'Cash',
      alpaca: fmt(usdc.offchainAvailable),
      inflight: fmt(inflight),
      raindex: fmt(usdc.onchainAvailable),
      total: fmtValue(total),
      isCash: true,
    }
  })

  const allRows = $derived<Row[]>(cashRow ? [...equityRows, cashRow] : [...equityRows])

  const comparators: Record<Col, (lhs: Row, rhs: Row) => number> = {
    asset: (lhs, rhs) => lhs.asset.localeCompare(rhs.asset),
    alpaca: (lhs, rhs) => decimalCompare(lhs.alpaca.full, rhs.alpaca.full),
    inflight: (lhs, rhs) => decimalCompare(lhs.inflight.full, rhs.inflight.full),
    raindex: (lhs, rhs) => decimalCompare(lhs.raindex.full, rhs.raindex.full),
    total: (lhs, rhs) => decimalCompare(lhs.total.full, rhs.total.full),
  }

  const sortedEquities = $derived.by(() => {
    const rows = [...equityRows]

    if (sort.current) {
      const { column, dir } = sort.current
      const cmp = comparators[column]
      const direction = dir === 'desc' ? -1 : 1
      rows.sort((lhs, rhs) => direction * cmp(lhs, rhs))
    }

    return rows
  })
  const approxClass = (val: Formatted): string =>
    val.truncated ? 'cursor-help opacity-80 hover:underline hover:decoration-dotted hover:decoration-muted-foreground hover:underline-offset-4' : ''
</script>

<Table.Root class="table-fixed">
  <Table.Header>
    <Table.Row>
      <Table.Head class="w-[16%]" aria-sort={ariaSort(sort.current, 'asset')}>
        <button class="{sortBtnClass} text-left" onclick={toggleSort('asset')}>
          Asset{sortIndicator(sort.current, 'asset')}
        </button>
      </Table.Head>

      <Table.Head class="text-right w-[21%]" aria-sort={ariaSort(sort.current, 'alpaca')}>
        <button class="{sortBtnClass} text-right" onclick={toggleSort('alpaca')}>
          Alpaca{sortIndicator(sort.current, 'alpaca')}
        </button>
      </Table.Head>

      <Table.Head class="text-right w-[21%]" aria-sort={ariaSort(sort.current, 'inflight')}>
        <button class="{sortBtnClass} text-right" onclick={toggleSort('inflight')}>
          Inflight{sortIndicator(sort.current, 'inflight')}
        </button>
      </Table.Head>

      <Table.Head class="text-right w-[21%]" aria-sort={ariaSort(sort.current, 'raindex')}>
        <button class="{sortBtnClass} text-right" onclick={toggleSort('raindex')}>
          Raindex{sortIndicator(sort.current, 'raindex')}
        </button>
      </Table.Head>

      <Table.Head class="text-right w-[21%]" aria-sort={ariaSort(sort.current, 'total')}>
        <button class="{sortBtnClass} text-right" onclick={toggleSort('total')}>
          Total{sortIndicator(sort.current, 'total')}
        </button>
      </Table.Head>
    </Table.Row>
  </Table.Header>
  {#if cashRow}
    <Table.Body>
      <Table.Row>
        <Table.Cell class="font-mono font-medium">Cash</Table.Cell>
        <Table.Cell class="text-right font-mono opacity-90">
          <span class={approxClass(cashRow.alpaca)} title={cashRow.alpaca.truncated ? cashRow.alpaca.full : undefined}>{cashRow.alpaca.display}</span>
        </Table.Cell>
        <Table.Cell class="text-right font-mono opacity-50">
          <span class={approxClass(cashRow.inflight)} title={cashRow.inflight.truncated ? cashRow.inflight.full : undefined}>{cashRow.inflight.display}</span>
        </Table.Cell>
        <Table.Cell class="text-right font-mono opacity-90">
          <span class={approxClass(cashRow.raindex)} title={cashRow.raindex.truncated ? cashRow.raindex.full : undefined}>{cashRow.raindex.display}</span>
        </Table.Cell>
        <Table.Cell class="text-right font-mono font-semibold">
          <span class={approxClass(cashRow.total)} title={cashRow.total.truncated ? cashRow.total.full : undefined}>{cashRow.total.display}</span>
        </Table.Cell>
      </Table.Row>
      <Table.Row>
        <Table.Cell colspan={5} class="py-3 px-0">
          <div class="h-px bg-border"></div>
        </Table.Cell>
      </Table.Row>
    </Table.Body>
  {/if}

  <Table.Body>
    {#each sortedEquities as row (row.asset)}
      <Table.Row>
        <Table.Cell class="font-mono font-medium">{row.asset}</Table.Cell>
        <Table.Cell class="text-right font-mono opacity-90">
          <span class={approxClass(row.alpaca)} title={row.alpaca.truncated ? row.alpaca.full : undefined}>{row.alpaca.display}</span>
        </Table.Cell>
        <Table.Cell class="text-right font-mono opacity-50">
          <span class={approxClass(row.inflight)} title={row.inflight.truncated ? row.inflight.full : undefined}>{row.inflight.display}</span>
        </Table.Cell>
        <Table.Cell class="text-right font-mono opacity-90">
          <span class={approxClass(row.raindex)} title={row.raindex.truncated ? row.raindex.full : undefined}>{row.raindex.display}</span>
        </Table.Cell>
        <Table.Cell class="text-right font-mono font-semibold">
          <span class={approxClass(row.total)} title={row.total.truncated ? row.total.full : undefined}>{row.total.display}</span>
        </Table.Cell>
      </Table.Row>
    {/each}
  </Table.Body>
</Table.Root>
