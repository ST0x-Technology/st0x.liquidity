<script lang="ts">
  import * as Table from '$lib/components/ui/table'
  import type { SymbolInventory } from '$lib/api/SymbolInventory'
  import type { UsdcInventory } from '$lib/api/UsdcInventory'
  import type { Position } from '$lib/api/Position'
  import type { Settings } from '$lib/api/Settings'
  import { decimalAdd, decimalCompare, decimalIsZero, formatDecimal } from '$lib/decimal'
  import { reactive } from '$lib/frp.svelte'

  interface Props {
    symbols: SymbolInventory[]
    usdc: UsdcInventory | undefined
    positions: Position[]
    settings: Settings | undefined
  }

  let { symbols, usdc, positions, settings }: Props = $props()

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
      ? trimTrailingZeros(formatDecimal(value, MAX_DECIMAL_PLACES))
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

  const stripPrefix = (symbol: string): string => {
    if (symbol.startsWith('wt')) return symbol.slice(2)
    if (symbol.startsWith('t')) return symbol.slice(1)
    return symbol
  }

  type SortDir = 'asc' | 'desc'
  type Col = 'asset' | 'alpaca' | 'inflight' | 'raindex' | 'total' | 'ratio' | 'dislocation'
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

  const computeRatio = (raindex: string, alpaca: string): number => {
    const onchain = parseFloat(raindex)
    const offchain = parseFloat(alpaca)
    const total = onchain + offchain
    if (total === 0) return 0
    return onchain / total
  }

  const formatRatio = (ratio: number): string =>
    `${(ratio * 100).toFixed(1)}%`

  const positionMap = $derived(
    new Map(positions.map((position) => [position.symbol, parseFloat(position.net)]))
  )

  type Row = {
    asset: string
    alpaca: Formatted
    inflight: Formatted
    raindex: Formatted
    total: Formatted
    ratio: number
    dislocation: number
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
        ratio: computeRatio(item.onchainAvailable, item.offchainAvailable),
        dislocation: positionMap.get(stripPrefix(item.symbol)) ?? 0,
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
      ratio: computeRatio(usdc.onchainAvailable, usdc.offchainAvailable),
      dislocation: 0,
      isCash: true,
    }
  })

  const comparators: Record<Col, (lhs: Row, rhs: Row) => number> = {
    asset: (lhs, rhs) => lhs.asset.localeCompare(rhs.asset),
    alpaca: (lhs, rhs) => decimalCompare(lhs.alpaca.full, rhs.alpaca.full),
    inflight: (lhs, rhs) => decimalCompare(lhs.inflight.full, rhs.inflight.full),
    raindex: (lhs, rhs) => decimalCompare(lhs.raindex.full, rhs.raindex.full),
    total: (lhs, rhs) => decimalCompare(lhs.total.full, rhs.total.full),
    ratio: (lhs, rhs) => lhs.ratio - rhs.ratio,
    dislocation: (lhs, rhs) => lhs.dislocation - rhs.dislocation,
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

  type DeviationStyle = 'normal' | 'high' | 'low'

  type Deviation = { text: string; style: DeviationStyle }

  const ratioDeviation = (ratio: number, isCash: boolean): Deviation | null => {
    if (!settings) return null

    const target = isCash ? (settings.usdcTarget ?? settings.equityTarget) : settings.equityTarget
    const deviation = isCash ? (settings.usdcDeviation ?? settings.equityDeviation) : settings.equityDeviation
    const diff = ratio - target
    const sign = diff >= 0 ? '+' : ''
    const text = `${sign}${(diff * 100).toFixed(1)}%`

    if (diff > deviation) return { text, style: 'high' }
    if (diff < -deviation) return { text, style: 'low' }
    return { text, style: 'normal' }
  }

  const deviationColor = (style: DeviationStyle): string => {
    if (style === 'high') return 'text-green-500'
    if (style === 'low') return 'text-red-500'
    return 'text-muted-foreground'
  }

  const fmtDislocation = (value: number): string => {
    if (value === 0) return '0'
    const sign = value > 0 ? '+' : '-'
    const abs = Math.abs(value)
    const formatted = abs >= 1 ? abs.toFixed(2) : abs.toPrecision(4)
    return `${sign}${formatted}`
  }

  const fmtPct = (value: number): string => `${(value * 100).toFixed(0)}%`

  const equityBounds = $derived(settings ? `${fmtPct(settings.equityTarget - settings.equityDeviation)}–${fmtPct(settings.equityTarget + settings.equityDeviation)}` : '')

  const usdcBounds = $derived(
    settings?.usdcTarget != null && settings.usdcDeviation != null
      ? `${fmtPct(settings.usdcTarget - settings.usdcDeviation)}–${fmtPct(settings.usdcTarget + settings.usdcDeviation)}`
      : ''
  )
</script>

{#if settings}
  <div class="mb-4 flex flex-wrap gap-x-6 gap-y-1 text-xs text-muted-foreground">
    <span>Equity ratio: <span class="font-mono">{fmtPct(settings.equityTarget)}</span> (allowed: <span class="font-mono">{equityBounds}</span>)</span>

    {#if settings.usdcTarget != null}
      <span>USDC ratio: <span class="font-mono">{fmtPct(settings.usdcTarget)}</span> (allowed: <span class="font-mono">{usdcBounds}</span>)</span>
    {/if}

    <span>Execution trigger: <span class="font-mono">{settings.executionThreshold}</span></span>

    {#if settings.assets.length > 0}
      <span>
        Assets:
        {#each settings.assets as asset, idx (asset.symbol)}
          <span class="font-mono {asset.trading ? '' : 'line-through opacity-50'}">{asset.symbol}</span>{#if idx < settings.assets.length - 1},
          {/if}
        {/each}
      </span>
    {/if}
  </div>
{/if}

<Table.Root>
  <Table.Header>
    <Table.Row>
      <Table.Head aria-sort={ariaSort(sort.current, 'asset')}>
        <button class="{sortBtnClass} text-left" onclick={toggleSort('asset')}>
          Asset{sortIndicator(sort.current, 'asset')}
        </button>
      </Table.Head>

      <Table.Head class="text-right" aria-sort={ariaSort(sort.current, 'alpaca')}>
        <button class="{sortBtnClass} text-right" onclick={toggleSort('alpaca')}>
          Alpaca{sortIndicator(sort.current, 'alpaca')}
        </button>
      </Table.Head>

      <Table.Head class="text-right" aria-sort={ariaSort(sort.current, 'inflight')}>
        <button class="{sortBtnClass} text-right" onclick={toggleSort('inflight')}>
          Inflight{sortIndicator(sort.current, 'inflight')}
        </button>
      </Table.Head>

      <Table.Head class="text-right" aria-sort={ariaSort(sort.current, 'raindex')}>
        <button class="{sortBtnClass} text-right" onclick={toggleSort('raindex')}>
          Raindex{sortIndicator(sort.current, 'raindex')}
        </button>
      </Table.Head>

      <Table.Head class="text-right" aria-sort={ariaSort(sort.current, 'total')}>
        <button class="{sortBtnClass} text-right" onclick={toggleSort('total')}>
          Total{sortIndicator(sort.current, 'total')}
        </button>
      </Table.Head>

      <Table.Head aria-sort={ariaSort(sort.current, 'ratio')}>
        <button
          class="{sortBtnClass} text-left"
          onclick={toggleSort('ratio')}
          title="Proportion of total holdings on Raindex (onchain / total)"
        >
          Ratio{sortIndicator(sort.current, 'ratio')}
        </button>
      </Table.Head>

      <Table.Head aria-sort={ariaSort(sort.current, 'dislocation')}>
        <button
          class="{sortBtnClass} text-left"
          onclick={toggleSort('dislocation')}
          title="Net directional exposure from counterparty fills"
        >
          Dislocation{sortIndicator(sort.current, 'dislocation')}
        </button>
      </Table.Head>
    </Table.Row>
  </Table.Header>
  {#if cashRow}
    {@const dev = ratioDeviation(cashRow.ratio, true)}
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
        <Table.Cell class="font-mono">
          {formatRatio(cashRow.ratio)}
          {#if dev}
            <span class={deviationColor(dev.style)}>
              ({dev.text})
            </span>
          {/if}
        </Table.Cell>
        <Table.Cell class="font-mono text-muted-foreground">—</Table.Cell>
      </Table.Row>
      <Table.Row>
        <Table.Cell colspan={7} class="py-3 px-0">
          <div class="h-px bg-border"></div>
        </Table.Cell>
      </Table.Row>
    </Table.Body>
  {/if}

  <Table.Body>
    {#each sortedEquities as row (row.asset)}
      {@const dev = ratioDeviation(row.ratio, false)}
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
        <Table.Cell class="font-mono">
          {formatRatio(row.ratio)}
          {#if dev}
            <span class={deviationColor(dev.style)}>
              ({dev.text})
            </span>
          {/if}
        </Table.Cell>
        <Table.Cell class="font-mono {row.dislocation === 0 ? 'text-muted-foreground' : row.dislocation > 0 ? 'text-green-500' : 'text-red-500'}">
          {fmtDislocation(row.dislocation)}
        </Table.Cell>
      </Table.Row>
    {/each}
  </Table.Body>
</Table.Root>
