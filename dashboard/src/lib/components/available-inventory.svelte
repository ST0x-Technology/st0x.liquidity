<script lang="ts">
  import * as Table from '$lib/components/ui/table'
  import type { SymbolInventory } from '$lib/api/SymbolInventory'
  import type { UsdcInventory } from '$lib/api/UsdcInventory'
  import type { Position } from '$lib/api/Position'
  import type { Settings } from '$lib/api/Settings'
  import InventoryHoverValue from '$lib/components/inventory-hover-value.svelte'
  import { decimalAdd, decimalCompare, decimalIsZero, formatDecimal } from '$lib/decimal'
  import { reactive } from '$lib/frp.svelte'
  import { cashInventoryAmounts } from '$lib/inventory-cash'
  import { cashUsdTooltip, equityUsdTooltip, positionSharesTooltip } from '$lib/inventory-value'

  interface Props {
    symbols: SymbolInventory[]
    usdc: UsdcInventory | undefined
    positions: Position[]
    settings: Settings | undefined
  }

  let { symbols, usdc, positions, settings }: Props = $props()

  const trimTrailingZeros = (formatted: string): string => {
    if (!formatted.includes('.')) return formatted
    return formatted.replace(/0+$/, '').replace(/\.$/, '')
  }

  type Formatted = { display: string; full: string; truncated: boolean }

  const fmtValue = (value: string): Formatted => {
    const display = trimTrailingZeros(formatDecimal(value, 2))
    const lossless = trimTrailingZeros(formatDecimal(value, 18))
    const truncated = display !== lossless
    return {
      display,
      full: value,
      truncated
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
  type EquityCol =
    | 'asset'
    | 'alpaca'
    | 'inflight'
    | 'unwrapped'
    | 'wrapped'
    | 'raindex'
    | 'total'
    | 'ratio'
    | 'exposure'
  type EquitySortState = { column: EquityCol; dir: SortDir } | null

  const sort = reactive<EquitySortState>(null)

  const toggleSort = (column: EquityCol) => () => {
    sort.update((current) => {
      if (current?.column === column) {
        return current.dir === 'asc' ? { column, dir: 'desc' } : null
      }
      return { column, dir: 'asc' }
    })
  }

  const ariaSort = (
    state: EquitySortState,
    col: EquityCol
  ): 'ascending' | 'descending' | 'none' => {
    if (state?.column !== col) return 'none'
    return state.dir === 'asc' ? 'ascending' : 'descending'
  }

  const sortIndicator = (state: EquitySortState, col: EquityCol): string => {
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

  const formatRatio = (ratio: number): string => `${(ratio * 100).toFixed(1)}%`

  type PositionInfo = { net: string; priceUsdc: string | null }
  type AssetFlags = {
    counterTrading: boolean
    rebalancing: boolean
    extendedHours: boolean
  }

  const positionMap = $derived(
    new Map(
      positions.map((position) => [
        position.symbol,
        {
          net: position.net,
          priceUsdc: position.last_price_usdc
        } satisfies PositionInfo
      ])
    )
  )

  const assetFlagsMap = $derived(
    new Map(
      settings?.assets.map((asset) => {
        const counterTrading = asset.counterTrading
        const flags: AssetFlags =
          counterTrading.status === 'enabled'
            ? {
                counterTrading: true,
                rebalancing: asset.rebalancing,
                extendedHours: counterTrading.extendedHours
              }
            : {
                counterTrading: false,
                rebalancing: asset.rebalancing,
                extendedHours: false
              }

        return [asset.symbol, flags] as const
      }) ?? []
    )
  )

  type EquityRow = {
    asset: string
    alpaca: Formatted
    inflight: Formatted
    unwrapped: Formatted
    wrapped: Formatted
    raindex: Formatted
    total: Formatted
    ratio: number
    exposure: number
    netShares: string
    priceUsdc: string | null
    // null when the symbol has no settings entry (unconfigured): the status
    // lights render as unknown rather than falsely showing all-disabled.
    flags: AssetFlags | null
  }

  const equityRows = $derived<EquityRow[]>(
    symbols.map((item) => {
      const inflight = decimalAdd(item.onchainInflight, item.offchainInflight)
      const totalVal = decimalAdd(
        decimalAdd(item.onchainAvailable, item.offchainAvailable),
        inflight
      )
      const stripped = stripPrefix(item.symbol)
      const pos = positionMap.get(stripped)
      const flags = assetFlagsMap.get(stripped) ?? null

      return {
        asset: stripped,
        alpaca: fmt(item.offchainAvailable),
        inflight: fmt(inflight),
        unwrapped: fmt(item.inflightEquity.baseWalletUnwrapped),
        wrapped: fmt(item.inflightEquity.baseWalletWrapped),
        raindex: fmt(item.onchainAvailable),
        total: fmtValue(totalVal),
        ratio: computeRatio(item.onchainAvailable, item.offchainAvailable),
        exposure: pos?.priceUsdc ? parseFloat(pos.net) * parseFloat(pos.priceUsdc) : 0,
        netShares: pos?.net ?? '0',
        priceUsdc: pos?.priceUsdc ?? null,
        flags
      }
    })
  )

  type CashCells = {
    alpacaUsd: Formatted
    alpacaUsdc: Formatted | null
    counterTradeUsd: Formatted
    rebalanceableUsd: Formatted | null
    inflight: Formatted
    ethWallet: Formatted | null
    baseWallet: Formatted | null
    raindex: Formatted
    total: Formatted
    ratio: number
  }

  const cashCells = $derived.by<CashCells | null>(() => {
    if (!usdc) return null

    const inflight = decimalAdd(usdc.onchainInflight, usdc.offchainInflight)
    const amounts = cashInventoryAmounts(usdc, settings?.cashReserved ?? null)

    return {
      alpacaUsd: fmt(amounts.alpacaUsd),
      alpacaUsdc: amounts.alpacaUsdc === null ? null : fmt(amounts.alpacaUsdc),
      counterTradeUsd: fmt(amounts.counterTradeUsd),
      rebalanceableUsd:
        amounts.rebalanceableUsd === null ? null : fmt(amounts.rebalanceableUsd),
      inflight: fmt(inflight),
      ethWallet:
        usdc.inflightCash.ethereumWallet === null ? null : fmt(usdc.inflightCash.ethereumWallet),
      baseWallet: usdc.inflightCash.baseWallet === null ? null : fmt(usdc.inflightCash.baseWallet),
      raindex: fmt(usdc.onchainAvailable),
      total: fmtValue(amounts.trackedTotal),
      ratio: amounts.venueRatio
    }
  })

  const equityComparators: Record<EquityCol, (lhs: EquityRow, rhs: EquityRow) => number> = {
    asset: (lhs, rhs) => lhs.asset.localeCompare(rhs.asset),
    alpaca: (lhs, rhs) => decimalCompare(lhs.alpaca.full, rhs.alpaca.full),
    inflight: (lhs, rhs) => decimalCompare(lhs.inflight.full, rhs.inflight.full),
    unwrapped: (lhs, rhs) => decimalCompare(lhs.unwrapped.full, rhs.unwrapped.full),
    wrapped: (lhs, rhs) => decimalCompare(lhs.wrapped.full, rhs.wrapped.full),
    raindex: (lhs, rhs) => decimalCompare(lhs.raindex.full, rhs.raindex.full),
    total: (lhs, rhs) => decimalCompare(lhs.total.full, rhs.total.full),
    ratio: (lhs, rhs) => lhs.ratio - rhs.ratio,
    exposure: (lhs, rhs) => lhs.exposure - rhs.exposure
  }

  const sortedEquities = $derived.by(() => {
    const rows = [...equityRows]

    rows.sort((lhs, rhs) => {
      // Counter-trading assets always come first
      const lhsActive = lhs.flags?.counterTrading ?? false
      const rhsActive = rhs.flags?.counterTrading ?? false
      if (lhsActive !== rhsActive) return lhsActive ? -1 : 1

      if (sort.current) {
        const { column, dir } = sort.current
        const cmp = equityComparators[column]
        const direction = dir === 'desc' ? -1 : 1
        return direction * cmp(lhs, rhs)
      }

      return 0
    })

    return rows
  })

  const approxClass = (val: Formatted): string =>
    val.truncated
      ? 'cursor-help opacity-80 hover:underline hover:decoration-dotted hover:decoration-muted-foreground hover:underline-offset-4'
      : ''

  const valueClass = (val: Formatted): string =>
    val.truncated
      ? approxClass(val)
      : 'cursor-help hover:underline hover:decoration-dotted hover:decoration-muted-foreground hover:underline-offset-4'

  const dimClass = (base: string, active = true): string => (active ? base : `${base} opacity-40`)

  type LightState = 'enabled' | 'disabled' | 'unknown'

  // A light reflects only its own flag. When the asset has no settings entry
  // (unconfigured) the state is unknown -- rendered distinctly, never as a false
  // "disabled". Counter-trading state does not dim sibling lights: each of CT /
  // Rebal / Ext stands on its own so an enabled mode never reads as off.
  const lightState = (flags: AssetFlags | null, key: keyof AssetFlags): LightState =>
    flags === null ? 'unknown' : flags[key] ? 'enabled' : 'disabled'

  const statusLightClass = (state: LightState): string => {
    const color =
      state === 'enabled'
        ? 'border-green-500/70 bg-green-500 shadow-[0_0_0_3px_rgba(34,197,94,0.12)]'
        : state === 'disabled'
          ? 'border-red-500/70 bg-red-500 shadow-[0_0_0_3px_rgba(239,68,68,0.10)]'
          : 'border-muted-foreground/40 bg-muted-foreground/30'

    return `mx-auto block h-2.5 w-2.5 rounded-full border ${color}`
  }

  const statusGroupHeadClass = 'px-4 text-center font-normal normal-case tracking-normal'
  const statusGroupClass =
    'mx-auto grid w-max grid-cols-[1.25rem_2.5rem_1.25rem] items-center justify-items-center gap-1'
  const statusCellClass = 'px-4 text-center'

  const statusLabel = (label: string, state: LightState): string =>
    `${label} ${state === 'unknown' ? 'unconfigured' : state}`

  type DeviationStyle = 'normal' | 'high' | 'low'

  type Deviation = { style: DeviationStyle }

  const ratioDeviation = (ratio: number, isCash: boolean): Deviation | null => {
    if (!settings) return null

    const target = isCash ? (settings.usdcTarget ?? settings.equityTarget) : settings.equityTarget
    const deviation = isCash
      ? (settings.usdcDeviation ?? settings.equityDeviation)
      : settings.equityDeviation
    const diff = ratio - target

    if (diff > deviation) return { style: 'high' }
    if (diff < -deviation) return { style: 'low' }
    return { style: 'normal' }
  }

  const isNegligible = (value: number): boolean => Math.abs(value) < 0.01

  const fmtExposure = (value: number): string => {
    if (value === 0 || isNegligible(value)) return '$0'
    const sign = value > 0 ? '+' : '-'
    const abs = Math.abs(value)
    return `${sign}$${abs.toFixed(2)}`
  }

  const showRebalanceable = $derived(
    cashCells?.rebalanceableUsd !== null && cashCells?.rebalanceableUsd !== undefined
  )
  const showAlpacaUsdc = $derived(
    cashCells?.alpacaUsdc !== null && cashCells?.alpacaUsdc !== undefined
  )
  const showEthWallet = $derived(
    cashCells?.ethWallet !== null && cashCells?.ethWallet !== undefined
  )
  const showBaseWallet = $derived(
    cashCells?.baseWallet !== null && cashCells?.baseWallet !== undefined
  )

  // Visual separator before wallet-observed / info columns to signal they're
  // out-of-band and not part of imbalance math.
  const infoSepClass = 'border-l border-border pl-6'
  const groupSepClass = 'border-l border-border'
  const cashCoreHeadClass = 'h-8 align-bottom pb-1 text-left'
  const cashGroupHeadClass = 'h-4 pb-0 pt-1 text-center align-bottom text-[10px]'
  const cashSubHeadClass = 'h-6 pt-0 text-left'
  const inventoryNumberClass = 'text-[13px]'
  type CashInfoCol = 'alpacaUsdc' | 'rebalanceable' | 'counterTrade' | 'eth' | 'base'

  const alpacaCashColCount = $derived(
    (showAlpacaUsdc ? 1 : 0) + (showRebalanceable ? 1 : 0) + (cashCells ? 1 : 0)
  )
  const walletCashColCount = $derived((showEthWallet ? 1 : 0) + (showBaseWallet ? 1 : 0))
  const cashFirstAlpacaCol = $derived<CashInfoCol | null>(
    showAlpacaUsdc
      ? 'alpacaUsdc'
      : showRebalanceable
        ? 'rebalanceable'
        : cashCells
          ? 'counterTrade'
          : null
  )
  const cashFirstWalletCol = $derived<CashInfoCol | null>(
    showEthWallet ? 'eth' : showBaseWallet ? 'base' : null
  )
  const cashSectionBoundary = (col: CashInfoCol): string =>
    col === cashFirstAlpacaCol || col === cashFirstWalletCol ? infoSepClass : ''
</script>

{#if cashCells}
  {@const dev = ratioDeviation(cashCells.ratio, true)}
  <div class="cash-table">
    <Table.Root>
      <Table.Header class="[&_tr:first-child]:border-b-0">
        <Table.Row>
          <Table.Head class={cashCoreHeadClass} rowspan={2}>Asset</Table.Head>
          <Table.Head
            class={cashCoreHeadClass}
            rowspan={2}
            title="USDC available in Raindex vaults to settle takers."
            >Raindex</Table.Head
          >
          <Table.Head
            class={cashCoreHeadClass}
            rowspan={2}
            title="USDC the books track as in motion between venues (CCTP transfers, pending settlements). Part of imbalance math."
            >Inflight</Table.Head
          >
          <Table.Head
            class={cashCoreHeadClass}
            rowspan={2}
            title="Total USD sitting at Alpaca. If no reserve is configured, this equals the reserve-adjusted balance."
            >Alpaca Total</Table.Head
          >
          <Table.Head class={cashCoreHeadClass} rowspan={2}>Total</Table.Head>
          <Table.Head
            class={cashCoreHeadClass}
            rowspan={2}
            title="Venue split for cash allocation: Raindex / (Raindex + Alpaca Total). Tracked Inflight is shown in Total but excluded from this venue ratio."
            >Ratio</Table.Head
          >
          <Table.Head class="h-8 w-full" rowspan={2} aria-hidden="true"></Table.Head>
          <Table.Head class="info-col {cashGroupHeadClass} {groupSepClass}" colspan={alpacaCashColCount}
            >Alpaca</Table.Head
          >
          {#if walletCashColCount > 0}
            <Table.Head class="info-col {cashGroupHeadClass} {groupSepClass}" colspan={walletCashColCount}
              >Wallets</Table.Head
            >
          {/if}
        </Table.Row>

        <Table.Row>
          {#if showAlpacaUsdc}
            <Table.Head
              class="info-col {cashSubHeadClass} {cashSectionBoundary('alpacaUsdc')}"
              title="USDC token balance held in the Alpaca account. This is separate from Alpaca USD cash and does not count toward the cash reserve."
              >USDC</Table.Head
            >
          {/if}
          {#if showRebalanceable}
            <Table.Head
              class="info-col {cashSubHeadClass} {cashSectionBoundary('rebalanceable')}"
              title="Settled Alpaca cash that can move to Raindex after preserving the configured reserve."
              >Rebalanceable</Table.Head
            >
          {/if}
          {#if cashCells}
            <Table.Head
              class="info-col {cashSubHeadClass} {cashSectionBoundary('counterTrade')}"
              title="USD available for buy-side equity hedges. The configured reserve is not subtracted from counter-trade preflight."
              >Counter-tradeable</Table.Head
            >
          {/if}
          {#if showEthWallet}
            <Table.Head
              class="info-col {cashSubHeadClass} {cashSectionBoundary('eth')}"
              title="Wallet-observed USDC on the Ethereum wallet between Alpaca and CCTP. Not part of imbalance math."
              >Eth</Table.Head
            >
          {/if}
          {#if showBaseWallet}
            <Table.Head
              class="info-col {cashSubHeadClass} {cashSectionBoundary('base')}"
              title="Wallet-observed USDC on the Base wallet between CCTP and Raindex vaults. Not part of imbalance math."
              >Base</Table.Head
            >
          {/if}
        </Table.Row>
      </Table.Header>

      <Table.Body>
        <Table.Row>
          <Table.Cell class="font-mono font-medium">Cash</Table.Cell>

          <Table.Cell class="text-left font-mono {inventoryNumberClass}">
            <InventoryHoverValue
              display={cashCells.raindex.display}
              tooltip={cashUsdTooltip(cashCells.raindex.full)}
              class={dimClass(valueClass(cashCells.raindex), true)}
            />
          </Table.Cell>

          <Table.Cell class="text-left font-mono {inventoryNumberClass}">
            <InventoryHoverValue
              display={cashCells.inflight.display}
              tooltip={cashUsdTooltip(cashCells.inflight.full)}
              class={dimClass(`${valueClass(cashCells.inflight)} opacity-50`, true)}
            />
          </Table.Cell>

          <Table.Cell class="text-left font-mono {inventoryNumberClass}">
            <InventoryHoverValue
              display={cashCells.alpacaUsd.display}
              tooltip={cashUsdTooltip(cashCells.alpacaUsd.full)}
              class={valueClass(cashCells.alpacaUsd)}
            />
          </Table.Cell>

          <Table.Cell class="text-left font-mono {inventoryNumberClass} font-semibold">
            <InventoryHoverValue
              display={cashCells.total.display}
              tooltip={cashUsdTooltip(cashCells.total.full)}
              class={valueClass(cashCells.total)}
            />
          </Table.Cell>

          <Table.Cell>
            <div class="flex items-center gap-2">
              <div class="h-1.5 w-16 overflow-hidden rounded-full bg-muted">
                <div
                  class="h-full rounded-full {dev?.style === 'high'
                    ? 'bg-green-500'
                    : dev?.style === 'low'
                      ? 'bg-red-500'
                      : 'bg-blue-400'}"
                  style="width: {String(Math.min(cashCells.ratio * 100, 100))}%"
                ></div>
              </div>
              <span class="font-mono text-[11px]">{formatRatio(cashCells.ratio)}</span>
            </div>
          </Table.Cell>

          <Table.Cell class="w-full" aria-hidden="true"></Table.Cell>

          {#if showAlpacaUsdc && cashCells.alpacaUsdc}
            <Table.Cell class="info-col text-left font-mono {inventoryNumberClass} {cashSectionBoundary('alpacaUsdc')}">
              <InventoryHoverValue
                display={cashCells.alpacaUsdc.display}
                tooltip={cashUsdTooltip(cashCells.alpacaUsdc.full)}
                class={valueClass(cashCells.alpacaUsdc)}
              />
            </Table.Cell>
          {/if}

          {#if showRebalanceable && cashCells.rebalanceableUsd}
            <Table.Cell class="info-col text-left font-mono {inventoryNumberClass} {cashSectionBoundary('rebalanceable')}">
              <InventoryHoverValue
                display={cashCells.rebalanceableUsd.display}
                tooltip={cashUsdTooltip(cashCells.rebalanceableUsd.full)}
                class={valueClass(cashCells.rebalanceableUsd)}
              />
            </Table.Cell>
          {/if}

          <Table.Cell class="info-col text-left font-mono {inventoryNumberClass} {cashSectionBoundary('counterTrade')}">
            <InventoryHoverValue
              display={cashCells.counterTradeUsd.display}
              tooltip={cashUsdTooltip(cashCells.counterTradeUsd.full)}
              class={valueClass(cashCells.counterTradeUsd)}
            />
          </Table.Cell>

          {#if showEthWallet && cashCells.ethWallet}
            <Table.Cell class="info-col text-left font-mono {inventoryNumberClass} {cashSectionBoundary('eth')}">
              <InventoryHoverValue
                display={cashCells.ethWallet.display}
                tooltip={cashUsdTooltip(cashCells.ethWallet.full)}
                class={`${valueClass(cashCells.ethWallet)} opacity-50`}
              />
            </Table.Cell>
          {/if}

          {#if showBaseWallet && cashCells.baseWallet}
            <Table.Cell class="info-col text-left font-mono {inventoryNumberClass} {cashSectionBoundary('base')}">
              <InventoryHoverValue
                display={cashCells.baseWallet.display}
                tooltip={cashUsdTooltip(cashCells.baseWallet.full)}
                class={`${valueClass(cashCells.baseWallet)} opacity-50`}
              />
            </Table.Cell>
          {/if}
        </Table.Row>
      </Table.Body>
    </Table.Root>
  </div>

  <div class="my-4 h-px bg-border"></div>
{/if}

<div class="equity-table">
  <Table.Root>
    <Table.Header>
      <Table.Row>
        <Table.Head class="text-left" aria-sort={ariaSort(sort.current, 'asset')}>
          <button class="{sortBtnClass} text-left" onclick={toggleSort('asset')}>
            Asset{sortIndicator(sort.current, 'asset')}
          </button>
        </Table.Head>

        <Table.Head class={statusGroupHeadClass}>
          <div class={statusGroupClass}>
            <span title="Counter-trading / hedging for this asset: green enabled, red disabled, grey not configured."
              >CT</span
            >

            <span title="Automatic equity rebalancing for this asset: green enabled, red disabled, grey not configured."
              >Rebal</span
            >

            <span title="Extended-hours counter-trading (only active while counter-trading is enabled): green enabled, red disabled, grey not configured."
              >Ext</span
            >
          </div>
        </Table.Head>

        <Table.Head class="text-left" aria-sort={ariaSort(sort.current, 'raindex')}>
          <button
            class="{sortBtnClass} text-left"
            onclick={toggleSort('raindex')}
            title="Tokens in Raindex vaults."
          >
            Raindex{sortIndicator(sort.current, 'raindex')}
          </button>
        </Table.Head>

        <Table.Head class="text-left" aria-sort={ariaSort(sort.current, 'inflight')}>
          <button
            class="{sortBtnClass} text-left"
            onclick={toggleSort('inflight')}
            title="Shares the books track as in motion between venues (mints, redeems). Part of imbalance math."
          >
            Inflight{sortIndicator(sort.current, 'inflight')}
          </button>
        </Table.Head>

        <Table.Head class="text-left" aria-sort={ariaSort(sort.current, 'alpaca')}>
          <button
            class="{sortBtnClass} text-left"
            onclick={toggleSort('alpaca')}
            title="Shares held at Alpaca."
          >
            Alpaca{sortIndicator(sort.current, 'alpaca')}
          </button>
        </Table.Head>

        <Table.Head class="text-left" aria-sort={ariaSort(sort.current, 'total')}>
          <button class="{sortBtnClass} text-left" onclick={toggleSort('total')}>
            Total{sortIndicator(sort.current, 'total')}
          </button>
        </Table.Head>

        <Table.Head class="text-left" aria-sort={ariaSort(sort.current, 'ratio')}>
          <button
            class="{sortBtnClass} text-left"
            onclick={toggleSort('ratio')}
            title="Proportion of total holdings on Raindex (onchain / total)."
          >
            Ratio{sortIndicator(sort.current, 'ratio')}
          </button>
        </Table.Head>

        <Table.Head class="text-left" aria-sort={ariaSort(sort.current, 'exposure')}>
          <button
            class="{sortBtnClass} text-left"
            onclick={toggleSort('exposure')}
            title="Net directional exposure from counterparty fills."
          >
            Exposure{sortIndicator(sort.current, 'exposure')}
          </button>
        </Table.Head>

        <Table.Head class="w-full" aria-hidden="true"></Table.Head>

        <Table.Head
          class="info-col text-left {infoSepClass}"
          aria-sort={ariaSort(sort.current, 'unwrapped')}
        >
          <button
            class="{sortBtnClass} text-left"
            onclick={toggleSort('unwrapped')}
            title="Unwrapped tokenized equity (tSTOCK) parked on the Base wallet between venues. Wallet-observed, not part of imbalance math."
          >
            Unwrapped{sortIndicator(sort.current, 'unwrapped')}
          </button>
        </Table.Head>

        <Table.Head class="info-col text-left" aria-sort={ariaSort(sort.current, 'wrapped')}>
          <button
            class="{sortBtnClass} text-left"
            onclick={toggleSort('wrapped')}
            title="Wrapped equity vault shares (wtSTOCK) parked on the Base wallet between venues. Wallet-observed, not part of imbalance math."
          >
            Wrapped{sortIndicator(sort.current, 'wrapped')}
          </button>
        </Table.Head>
      </Table.Row>
    </Table.Header>

    <Table.Body>
      {#each sortedEquities as row, idx (row.asset)}
        {@const dev = ratioDeviation(row.ratio, false)}
        {@const ctActive = row.flags?.counterTrading ?? false}
        {@const ctState = lightState(row.flags, 'counterTrading')}
        {@const rebalState = lightState(row.flags, 'rebalancing')}
        {@const extState = lightState(row.flags, 'extendedHours')}
        <Table.Row class={idx % 2 === 0 ? 'bg-muted/40' : ''}>
          <Table.Cell class="font-mono font-medium {ctActive ? '' : 'opacity-40'}"
            >{row.asset}</Table.Cell
          >

          <Table.Cell class={statusCellClass}>
            <div class={statusGroupClass}>
              <span
                class={statusLightClass(ctState)}
                role="img"
                title={statusLabel('Counter-trading', ctState)}
                aria-label={statusLabel('Counter-trading', ctState)}
              ></span>

              <span
                class={statusLightClass(rebalState)}
                role="img"
                title={statusLabel('Rebalancing', rebalState)}
                aria-label={statusLabel('Rebalancing', rebalState)}
              ></span>

              <span
                class={statusLightClass(extState)}
                role="img"
                title={statusLabel('Extended hours', extState)}
                aria-label={statusLabel('Extended hours', extState)}
              ></span>
            </div>
          </Table.Cell>

          <Table.Cell class="text-left font-mono {inventoryNumberClass}">
            <InventoryHoverValue
              display={row.raindex.display}
              tooltip={equityUsdTooltip(row.raindex.full, row.priceUsdc)}
              class={dimClass(valueClass(row.raindex), ctActive)}
            />
          </Table.Cell>

          <Table.Cell class="text-left font-mono {inventoryNumberClass}">
            <InventoryHoverValue
              display={row.inflight.display}
              tooltip={equityUsdTooltip(row.inflight.full, row.priceUsdc)}
              class={dimClass(`${valueClass(row.inflight)} opacity-50`, ctActive)}
            />
          </Table.Cell>

          <Table.Cell class="text-left font-mono {inventoryNumberClass}">
            <InventoryHoverValue
              display={row.alpaca.display}
              tooltip={equityUsdTooltip(row.alpaca.full, row.priceUsdc)}
              class={dimClass(valueClass(row.alpaca), ctActive)}
            />
          </Table.Cell>

          <Table.Cell class="text-left font-mono {inventoryNumberClass} font-semibold">
            <InventoryHoverValue
              display={row.total.display}
              tooltip={equityUsdTooltip(row.total.full, row.priceUsdc)}
              class={dimClass(valueClass(row.total), ctActive)}
            />
          </Table.Cell>

          <Table.Cell>
            <div class="flex items-center gap-2">
              <div class="h-1.5 w-16 overflow-hidden rounded-full bg-muted">
                <div
                  class="h-full rounded-full {dev?.style === 'high'
                    ? 'bg-green-500'
                    : dev?.style === 'low'
                      ? 'bg-red-500'
                      : 'bg-blue-400'}"
                  style="width: {String(Math.min(row.ratio * 100, 100))}%"
                ></div>
              </div>
              <span class="font-mono text-[11px] {ctActive ? '' : 'opacity-40'}"
                >{formatRatio(row.ratio)}</span
              >
            </div>
          </Table.Cell>

          <Table.Cell>
            <div class="flex items-center gap-1.5 font-mono text-[11px]">
              {#if !isNegligible(row.exposure) && row.exposure !== 0}
                <span
                  class="text-base leading-none {row.exposure > 0
                    ? 'text-green-500'
                    : 'text-red-500'}">{row.exposure > 0 ? '▲' : '▼'}</span
                >
              {/if}
              <InventoryHoverValue
                display={fmtExposure(row.exposure)}
                tooltip={positionSharesTooltip(row.netShares)}
                class={dimClass(
                  `cursor-help hover:underline hover:decoration-dotted hover:decoration-muted-foreground hover:underline-offset-4 ${row.exposure === 0 || isNegligible(row.exposure) ? 'text-muted-foreground' : row.exposure > 0 ? 'text-green-500' : 'text-red-500'}`,
                  ctActive
                )}
              />
            </div>
          </Table.Cell>

          <Table.Cell class="w-full" aria-hidden="true"></Table.Cell>

          <Table.Cell class="info-col text-left font-mono {inventoryNumberClass} {infoSepClass}">
            <InventoryHoverValue
              display={row.unwrapped.display}
              tooltip={equityUsdTooltip(row.unwrapped.full, row.priceUsdc)}
              class={dimClass(`${valueClass(row.unwrapped)} opacity-50`, ctActive)}
            />
          </Table.Cell>

          <Table.Cell class="info-col text-left font-mono {inventoryNumberClass}">
            <InventoryHoverValue
              display={row.wrapped.display}
              tooltip={equityUsdTooltip(row.wrapped.full, row.priceUsdc)}
              class={dimClass(`${valueClass(row.wrapped)} opacity-50`, ctActive)}
            />
          </Table.Cell>
        </Table.Row>
      {/each}
    </Table.Body>
  </Table.Root>
</div>

<style>
  /* Cash table: keep headers in normal case, not the default uppercase. */
  .cash-table :global([data-slot='table-head']) {
    text-transform: none;
    letter-spacing: normal;
    font-size: 0.8rem;
  }

  /* Widen column padding so cells breathe more and the spacer column between
     core math and info columns isn't asked to absorb so much slack. Applied
     to both tables for visual consistency. */
  .equity-table :global([data-slot='table-head']),
  .equity-table :global([data-slot='table-cell']),
  .cash-table :global([data-slot='table-head']),
  .cash-table :global([data-slot='table-cell']) {
    padding-left: 1rem;
    padding-right: 1rem;
  }

  /* Info columns (right of the divider) get the default tighter padding since
     the info data is already compact and the wider padding wasted space. */
  .cash-table :global(.info-col),
  .equity-table :global(.info-col) {
    padding-left: 0.5rem;
    padding-right: 0.5rem;
  }
</style>
