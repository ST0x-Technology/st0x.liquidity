import Decimal from 'decimal.js'
import { FETCH_TIMEOUT_MS } from '$lib/time'
import type {
  PnlEntry,
  PnlResponse,
  PnlSampleStats,
  PnlSampleSymbolStats,
  PnlStreamKey,
  PnlSummary,
  PnlSymbolSummary,
  SyntheticPnlWindow,
  SyntheticPnlWindowSymbol
} from './types'

export type SqlPnlQuery = {
  limit: number
  offset: number
  symbols: Set<string>
  fromDate?: string
  toDate?: string
  dayFilter?: 'all' | 'weekday' | 'weekend'
}

export type SqlPositionEventRow = {
  rowid: number
  symbol: string
  event_type: string
  payload: unknown
}

export type SqlPositionViewRow = {
  symbol: string
  net_position: string | null
  last_updated: string | null
  last_price_usdc: string | null
}

export type SqlSampleStatsRow = {
  symbol: string
  onchain_fill_count: number
  offchain_fill_count: number
  first_at: string | null
  last_at: string | null
}

type Direction = 'Buy' | 'Sell'
type LotSide = 'long' | 'short'
type PnlBucket = 'counter_trade' | 'onchain_netting' | 'directional_exposure'

type Fill = {
  rowid: number
  id: string
  symbol: string
  shares: Decimal
  direction: Direction
  price: Decimal
  executedAt: string
}

type Lot = {
  tradeId: string
  side: LotSide
  remainingShares: Decimal
  price: Decimal
  openedAt: string
  openedRowid: number
}

type SummaryAcc = {
  counterTradePnlUsd: Decimal
  onchainNettingPnlUsd: Decimal
  directionalInventoryBaselinePnlUsd: Decimal
  directionalImbalanceExcessPnlUsd: Decimal
  directionalExposurePnlUsd: Decimal
  realizedPnlUsd: Decimal
  matchedShares: Decimal
  onchainNotionalUsd: Decimal
  offchainNotionalUsd: Decimal
  openLongShares: Decimal
  openShortShares: Decimal
  openLongNotionalUsd: Decimal
  openShortNotionalUsd: Decimal
  unmatchedOffchainBuyShares: Decimal
  unmatchedOffchainSellShares: Decimal
  unmatchedOffchainBuyNotionalUsd: Decimal
  unmatchedOffchainSellNotionalUsd: Decimal
  onchainFillCount: number
  offchainFillCount: number
  matchedLotCount: number
  openLotCount: number
  unmatchedOffchainFillCount: number
}

type SymbolBook = {
  longLots: Lot[]
  shortLots: Lot[]
  unmatchedOffchainLongLots: Lot[]
  unmatchedOffchainShortLots: Lot[]
  seenOnchainFillIds: Set<string>
  seenOffchainPlacementIds: Set<string>
  seenOffchainFillIds: Set<string>
  originalOnchainShares: Map<string, Decimal>
  matchedOnchainShares: Map<string, Decimal>
  summary: SummaryAcc
}

type UnmatchedOffchainAllocation = {
  symbol: string
  fillId: string
  shares: Decimal
}

type PositionReplayDelta = {
  symbol: string
  replayNet: Decimal
  positionNet: Decimal
}

const ATTRIBUTION_METHOD = 'direct_sql_position_fill_replay_fifo'
const COUNTER_TRADE_THRESHOLD_SECONDS = 300
const ZERO = new Decimal(0)

const ATTRIBUTION_WARNING =
  'PnL source: rows are read from the deployed SQL JSON endpoint. Offchain fills are allocated to open opposite-side onchain lots by deterministic FIFO replay because explicit offchain_order_id -> onchain_trade_ids parentage is not currently persisted.'
const BASELINE_WARNING =
  'Baseline drift is reported as zero in the deployed view because historical portfolio state and price/NAV snapshots are not currently persisted. Realized replay buckets are computed from persisted fills.'

const emptySummary = (): SummaryAcc => ({
  counterTradePnlUsd: new Decimal(0),
  onchainNettingPnlUsd: new Decimal(0),
  directionalInventoryBaselinePnlUsd: new Decimal(0),
  directionalImbalanceExcessPnlUsd: new Decimal(0),
  directionalExposurePnlUsd: new Decimal(0),
  realizedPnlUsd: new Decimal(0),
  matchedShares: new Decimal(0),
  onchainNotionalUsd: new Decimal(0),
  offchainNotionalUsd: new Decimal(0),
  openLongShares: new Decimal(0),
  openShortShares: new Decimal(0),
  openLongNotionalUsd: new Decimal(0),
  openShortNotionalUsd: new Decimal(0),
  unmatchedOffchainBuyShares: new Decimal(0),
  unmatchedOffchainSellShares: new Decimal(0),
  unmatchedOffchainBuyNotionalUsd: new Decimal(0),
  unmatchedOffchainSellNotionalUsd: new Decimal(0),
  onchainFillCount: 0,
  offchainFillCount: 0,
  matchedLotCount: 0,
  openLotCount: 0,
  unmatchedOffchainFillCount: 0
})

const emptyBook = (): SymbolBook => ({
  longLots: [],
  shortLots: [],
  unmatchedOffchainLongLots: [],
  unmatchedOffchainShortLots: [],
  seenOnchainFillIds: new Set(),
  seenOffchainPlacementIds: new Set(),
  seenOffchainFillIds: new Set(),
  originalOnchainShares: new Map(),
  matchedOnchainShares: new Map(),
  summary: emptySummary()
})

const sqlString = (value: string): string => `'${value.replaceAll("'", "''")}'`

const symbolPredicate = (symbols: Set<string>, column: string): string => {
  if (symbols.size === 0) return ''
  return ` AND ${column} IN (${[...symbols].map(sqlString).join(',')})`
}

const globalOrigin = (): string => {
  const maybeLocation = (globalThis as Record<string, unknown>)['location']
  if (
    typeof maybeLocation === 'object' &&
    maybeLocation !== null &&
    'origin' in maybeLocation &&
    typeof maybeLocation.origin === 'string'
  ) {
    return maybeLocation.origin
  }

  return 'http://localhost'
}

export const buildSqlApiUrl = (baseUrl: string, sql: string): string => {
  const url = new URL(baseUrl, globalOrigin())
  url.searchParams.set('sql', sql)
  url.searchParams.set('_shape', 'array')
  url.searchParams.set('_size', 'max')
  return url.toString()
}

const fetchSqlRows = async <Row>(baseUrl: string, sql: string): Promise<Row[]> => {
  const response = await fetch(buildSqlApiUrl(baseUrl, sql), {
    signal: AbortSignal.timeout(FETCH_TIMEOUT_MS)
  })

  if (!response.ok) {
    throw new Error(`SQL endpoint HTTP ${String(response.status)}`)
  }

  const rows = (await response.json()) as unknown
  if (!Array.isArray(rows)) {
    throw new Error('SQL endpoint returned a non-array response')
  }

  return rows as Row[]
}

const positionEventsSql = (symbols: Set<string>): string => `
SELECT rowid, aggregate_id AS symbol, event_type, payload
FROM events NOT INDEXED
WHERE aggregate_type = 'Position'
  AND event_type IN (
    'PositionEvent::OnChainOrderFilled',
    'PositionEvent::OffChainOrderPlaced',
    'PositionEvent::OffChainOrderFilled'
  )
  ${symbolPredicate(symbols, 'aggregate_id')}
ORDER BY rowid ASC
`

const positionViewSql = (symbols: Set<string>): string => `
SELECT
  symbol,
  net_position,
  last_updated,
  json_extract(payload, '$.Live.last_price_usdc') AS last_price_usdc
FROM position_view
WHERE symbol IS NOT NULL
  ${symbolPredicate(symbols, 'symbol')}
ORDER BY symbol ASC
`

const sampleStatsSql = (): string => `
SELECT
  aggregate_id AS symbol,
  SUM(CASE WHEN event_type = 'PositionEvent::OnChainOrderFilled' THEN 1 ELSE 0 END) AS onchain_fill_count,
  SUM(CASE WHEN event_type = 'PositionEvent::OffChainOrderFilled' THEN 1 ELSE 0 END) AS offchain_fill_count,
  MIN(COALESCE(
    json_extract(payload, '$.OnChainOrderFilled.block_timestamp'),
    json_extract(payload, '$.OffChainOrderFilled.broker_timestamp')
  )) AS first_at,
  MAX(COALESCE(
    json_extract(payload, '$.OnChainOrderFilled.block_timestamp'),
    json_extract(payload, '$.OffChainOrderFilled.broker_timestamp')
  )) AS last_at
FROM events NOT INDEXED
WHERE aggregate_type = 'Position'
  AND event_type IN (
    'PositionEvent::OnChainOrderFilled',
    'PositionEvent::OffChainOrderFilled'
  )
GROUP BY aggregate_id
ORDER BY aggregate_id ASC
`

const parsePayload = (payload: unknown): Record<string, unknown> | null => {
  if (payload !== null && typeof payload === 'object' && !Array.isArray(payload)) {
    return payload as Record<string, unknown>
  }

  if (typeof payload !== 'string') return null

  try {
    const parsed = JSON.parse(payload) as unknown
    if (parsed !== null && typeof parsed === 'object' && !Array.isArray(parsed)) {
      return parsed as Record<string, unknown>
    }
  } catch {
    return null
  }

  return null
}

const nestedRecord = (
  payload: Record<string, unknown>,
  key: string
): Record<string, unknown> | null => {
  const value = payload[key]
  return value !== null && typeof value === 'object' && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : null
}

const textField = (payload: Record<string, unknown>, key: string): string | null => {
  const value = payload[key]
  return typeof value === 'string' ? value : null
}

const numberTextField = (payload: Record<string, unknown>, key: string): string | null => {
  const value = payload[key]
  if (typeof value === 'string' || typeof value === 'number') return String(value)
  return null
}

const decimalField = (payload: Record<string, unknown>, key: string): Decimal | null => {
  const value = numberTextField(payload, key)
  if (value === null) return null

  try {
    return new Decimal(value)
  } catch {
    return null
  }
}

const directionField = (payload: Record<string, unknown>, key: string): Direction | null => {
  const value = textField(payload, key)
  if (value === 'Buy' || value === 'Sell') return value
  if (value === 'buy') return 'Buy'
  if (value === 'sell') return 'Sell'
  return null
}

const dateKey = (iso: string): string => iso.slice(0, 10)

const isWeekendIso = (iso: string): boolean => {
  const day = new Date(iso).getUTCDay()
  return day === 0 || day === 6
}

const matchesDateFilter = (entry: PnlEntry, query: SqlPnlQuery): boolean => {
  const day = dateKey(entry.closedAt)
  if (query.fromDate !== undefined && day < query.fromDate) return false
  if (query.toDate !== undefined && day > query.toDate) return false
  if (query.dayFilter === 'weekday' && isWeekendIso(entry.closedAt)) return false
  if (query.dayFilter === 'weekend' && !isWeekendIso(entry.closedAt)) return false
  return true
}

const secondsBetween = (startIso: string, endIso: string): number =>
  Math.max(0, Math.floor((new Date(endIso).getTime() - new Date(startIso).getTime()) / 1000))

const fmtDecimal = (value: Decimal): string => {
  const fixed = value.toFixed(9)
  const trimmed = fixed.replace(/\.?0+$/u, '')
  return trimmed === '-0' || trimmed === '' ? '0' : trimmed
}

const allocationSummaryText = (allocations: UnmatchedOffchainAllocation[]): string | null => {
  if (allocations.length === 0) return null

  const bySymbol = new Map<string, { fillIds: Set<string>; shares: Decimal }>()
  for (const allocation of allocations) {
    const current = bySymbol.get(allocation.symbol) ?? { fillIds: new Set<string>(), shares: ZERO }
    current.fillIds.add(allocation.fillId)
    current.shares = current.shares.plus(allocation.shares)
    bySymbol.set(allocation.symbol, current)
  }

  const symbolDetails = [...bySymbol.entries()]
    .sort(([left], [right]) => left.localeCompare(right))
    .map(
      ([symbol, value]) =>
        `${symbol}: ${fmtDecimal(value.shares)} shares across ${String(value.fillIds.size)} fills`
    )
    .join('; ')

  return `Allocation note: ${String(allocations.length)} offchain fills are outside the matched onchain replay (${symbolDetails}). Those shares are carried as unmatched offchain exposure in the deployed view.`
}

const positionReplayDeltaText = (deltas: PositionReplayDelta[]): string | null => {
  if (deltas.length === 0) return null

  const details = [...deltas]
    .sort((left, right) => left.symbol.localeCompare(right.symbol))
    .map(
      (delta) =>
        `${delta.symbol}: replay ${fmtDecimal(delta.replayNet)}, position_view ${fmtDecimal(delta.positionNet)}`
    )
    .join('; ')

  return `Reconciliation note: replayed open lots differ from position_view for ${String(deltas.length)} symbols (${details}). This means the persisted Position fill events available to the dashboard do not fully reconstruct the current projected position for those symbols.`
}

const appendReplayDiagnostics = (
  warnings: string[],
  unmatchedOffchainAllocations: UnmatchedOffchainAllocation[],
  positionReplayDeltas: PositionReplayDelta[]
): void => {
  const allocationText = allocationSummaryText(unmatchedOffchainAllocations)
  if (allocationText !== null) warnings.push(allocationText)

  const deltaText = positionReplayDeltaText(positionReplayDeltas)
  if (deltaText !== null) warnings.push(deltaText)
}

const directionLabel = (direction: Direction): string => (direction === 'Buy' ? 'buy' : 'sell')
const lotSideToOnchainDirection = (side: LotSide): string => (side === 'long' ? 'buy' : 'sell')
const closingVenue = (bucket: PnlBucket): string =>
  bucket === 'onchain_netting' ? 'onchain' : 'offchain'

const addRealizedPnl = (summary: SummaryAcc, bucket: PnlBucket, value: Decimal): void => {
  if (bucket === 'counter_trade') {
    summary.counterTradePnlUsd = summary.counterTradePnlUsd.plus(value)
    summary.realizedPnlUsd = summary.realizedPnlUsd.plus(value)
    return
  }

  if (bucket === 'onchain_netting') {
    summary.onchainNettingPnlUsd = summary.onchainNettingPnlUsd.plus(value)
    summary.realizedPnlUsd = summary.realizedPnlUsd.plus(value)
    return
  }

  summary.directionalImbalanceExcessPnlUsd = summary.directionalImbalanceExcessPnlUsd.plus(value)
  summary.directionalExposurePnlUsd = summary.directionalExposurePnlUsd.plus(value)
}

const parseOnchainFill = (row: SqlPositionEventRow, warnings: string[]): Fill | null => {
  const payload = parsePayload(row.payload)
  const filled = payload === null ? null : nestedRecord(payload, 'OnChainOrderFilled')
  if (filled === null) {
    warnings.push(
      `Skipped malformed position onchain fill ${row.symbol}: missing OnChainOrderFilled`
    )
    return null
  }

  const amount = decimalField(filled, 'amount')
  const direction = directionField(filled, 'direction')
  const price = decimalField(filled, 'price_usdc')
  const executedAt = textField(filled, 'block_timestamp')
  const tradeId = nestedRecord(filled, 'trade_id')
  const txHash = tradeId === null ? null : textField(tradeId, 'tx_hash')
  const logIndex = tradeId === null ? null : numberTextField(tradeId, 'log_index')

  if (
    amount === null ||
    direction === null ||
    price === null ||
    executedAt === null ||
    txHash === null ||
    logIndex === null
  ) {
    warnings.push(`Skipped malformed position onchain fill ${row.symbol}: incomplete payload`)
    return null
  }

  return {
    rowid: row.rowid,
    id: `${txHash}:${logIndex}`,
    symbol: row.symbol,
    shares: amount,
    direction,
    price,
    executedAt
  }
}

const parseOffchainFill = (row: SqlPositionEventRow, warnings: string[]): Fill | null => {
  const payload = parsePayload(row.payload)
  const filled = payload === null ? null : nestedRecord(payload, 'OffChainOrderFilled')
  if (filled === null) {
    warnings.push(
      `Skipped malformed position offchain fill ${row.symbol}: missing OffChainOrderFilled`
    )
    return null
  }

  const orderId = textField(filled, 'offchain_order_id')
  const shares = decimalField(filled, 'shares_filled')
  const direction = directionField(filled, 'direction')
  const price = decimalField(filled, 'price')
  const executedAt = textField(filled, 'broker_timestamp')

  if (
    orderId === null ||
    shares === null ||
    direction === null ||
    price === null ||
    executedAt === null
  ) {
    warnings.push(`Skipped malformed position offchain fill ${row.symbol}: incomplete payload`)
    return null
  }

  return {
    rowid: row.rowid,
    id: orderId,
    symbol: row.symbol,
    shares,
    direction,
    price,
    executedAt
  }
}

const parseOffchainPlacementId = (row: SqlPositionEventRow, warnings: string[]): string | null => {
  const payload = parsePayload(row.payload)
  const placed = payload === null ? null : nestedRecord(payload, 'OffChainOrderPlaced')
  const orderId = placed === null ? null : textField(placed, 'offchain_order_id')

  if (orderId === null) {
    warnings.push(`Skipped malformed position offchain placement ${row.symbol}: incomplete payload`)
  }

  return orderId
}

const getBook = (books: Map<string, SymbolBook>, symbol: string): SymbolBook => {
  const existing = books.get(symbol)
  if (existing !== undefined) return existing
  const book = emptyBook()
  books.set(symbol, book)
  return book
}

const applyOnchainFill = (
  book: SymbolBook,
  fill: Fill,
  entries: PnlEntry[],
  warnings: string[]
): void => {
  if (book.seenOnchainFillIds.has(fill.id)) {
    warnings.push(
      `PnL audit error: duplicate onchain trade_id ${fill.id} for ${fill.symbol} was skipped`
    )
    return
  }

  book.seenOnchainFillIds.add(fill.id)
  book.summary.onchainFillCount += 1

  const sourceLots = fill.direction === 'Buy' ? book.shortLots : book.longLots
  const remaining = matchFillAgainstLots(
    fill,
    sourceLots,
    book.summary,
    book.matchedOnchainShares,
    entries,
    'onchain_netting'
  )
  if (remaining.isZero()) return

  const original = book.originalOnchainShares.get(fill.id) ?? ZERO
  book.originalOnchainShares.set(fill.id, original.plus(remaining))

  const side: LotSide = fill.direction === 'Buy' ? 'long' : 'short'
  const lot: Lot = {
    tradeId: fill.id,
    side,
    remainingShares: remaining,
    price: fill.price,
    openedAt: fill.executedAt,
    openedRowid: fill.rowid
  }

  if (side === 'long') {
    book.longLots.push(lot)
  } else {
    book.shortLots.push(lot)
  }
}

const applyOffchainPlacement = (
  book: SymbolBook,
  row: SqlPositionEventRow,
  warnings: string[]
): void => {
  const orderId = parseOffchainPlacementId(row, warnings)
  if (orderId === null) return

  if (book.seenOffchainPlacementIds.has(orderId)) {
    warnings.push(
      `PnL audit error: duplicate offchain placement ${orderId} for ${row.symbol} was skipped`
    )
    return
  }

  book.seenOffchainPlacementIds.add(orderId)
}

const applyOffchainFill = (
  book: SymbolBook,
  fill: Fill,
  entries: PnlEntry[],
  warnings: string[],
  unmatchedOffchainAllocations: UnmatchedOffchainAllocation[]
): void => {
  book.summary.offchainFillCount += 1

  if (book.seenOffchainFillIds.has(fill.id)) {
    warnings.push(
      `PnL audit error: duplicate offchain fill ${fill.id} for ${fill.symbol} was skipped`
    )
    return
  }

  book.seenOffchainFillIds.add(fill.id)
  const sourceLots = fill.direction === 'Buy' ? book.shortLots : book.longLots
  const remaining = matchFillAgainstLots(
    fill,
    sourceLots,
    book.summary,
    book.matchedOnchainShares,
    entries,
    'counter_trade'
  )

  if (!remaining.isZero()) {
    unmatchedOffchainAllocations.push({
      symbol: fill.symbol,
      fillId: fill.id,
      shares: remaining
    })
    recordUnmatchedOffchain(book, fill, remaining)
  }
}

const matchFillAgainstLots = (
  fill: Fill,
  sourceLots: Lot[],
  summary: SummaryAcc,
  matchedOnchainShares: Map<string, Decimal>,
  entries: PnlEntry[],
  bucket: PnlBucket
): Decimal => {
  let remaining = fill.shares

  while (!remaining.isZero() && sourceLots.length > 0) {
    const frontLot = sourceLots[0]
    if (frontLot === undefined) break

    const matchedShares = Decimal.min(remaining, frontLot.remainingShares)
    if (matchedShares.isZero()) {
      sourceLots.shift()
      continue
    }

    const elapsedSeconds = secondsBetween(frontLot.openedAt, fill.executedAt)
    const effectiveBucket: PnlBucket =
      bucket === 'counter_trade' && elapsedSeconds > COUNTER_TRADE_THRESHOLD_SECONDS
        ? 'directional_exposure'
        : bucket
    const spread =
      frontLot.side === 'long' ? fill.price.minus(frontLot.price) : frontLot.price.minus(fill.price)
    const realizedPnl = matchedShares.mul(spread)
    const openingNotional = matchedShares.mul(frontLot.price)
    const closingNotional = matchedShares.mul(fill.price)

    frontLot.remainingShares = frontLot.remainingShares.minus(matchedShares)
    if (frontLot.remainingShares.isZero()) {
      sourceLots.shift()
    }

    addRealizedPnl(summary, effectiveBucket, realizedPnl)
    summary.matchedShares = summary.matchedShares.plus(matchedShares)
    summary.onchainNotionalUsd = summary.onchainNotionalUsd.plus(openingNotional)
    if (effectiveBucket === 'onchain_netting') {
      summary.onchainNotionalUsd = summary.onchainNotionalUsd.plus(closingNotional)
    } else {
      summary.offchainNotionalUsd = summary.offchainNotionalUsd.plus(closingNotional)
    }
    summary.matchedLotCount += 1

    const matched = matchedOnchainShares.get(frontLot.tradeId) ?? ZERO
    matchedOnchainShares.set(frontLot.tradeId, matched.plus(matchedShares))

    const openingDirection = lotSideToOnchainDirection(frontLot.side)
    const closingDirection = directionLabel(fill.direction)
    const openingPriceText = fmtDecimal(frontLot.price)
    const closingPriceText = fmtDecimal(fill.price)

    entries.push({
      symbol: fill.symbol,
      pnlBucket: effectiveBucket,
      matchedAt: fill.executedAt,
      openedAt: frontLot.openedAt,
      closedAt: fill.executedAt,
      openingFillId: frontLot.tradeId,
      closingFillId: fill.id,
      openingRowid: frontLot.openedRowid,
      closingRowid: fill.rowid,
      openingVenue: 'onchain',
      closingVenue: closingVenue(effectiveBucket),
      openingDirection,
      closingDirection,
      openingPriceUsd: openingPriceText,
      closingPriceUsd: closingPriceText,
      onchainTradeId: frontLot.tradeId,
      offchainOrderId: fill.id,
      onchainDirection: openingDirection,
      offchainDirection: closingDirection,
      shares: fmtDecimal(matchedShares),
      onchainPriceUsdc: openingPriceText,
      offchainPriceUsd: closingPriceText,
      spreadUsd: fmtDecimal(spread),
      realizedPnlUsd: fmtDecimal(realizedPnl),
      elapsedSeconds,
      counterTradeThresholdSeconds: COUNTER_TRADE_THRESHOLD_SECONDS,
      delayedCounterTrade: effectiveBucket === 'directional_exposure',
      attributionMethod: ATTRIBUTION_METHOD
    })

    remaining = remaining.minus(matchedShares)
  }

  return remaining
}

const recordUnmatchedOffchain = (book: SymbolBook, fill: Fill, shares: Decimal): void => {
  const notional = shares.mul(fill.price)
  book.summary.unmatchedOffchainFillCount += 1

  if (fill.direction === 'Buy') {
    book.summary.unmatchedOffchainBuyShares = book.summary.unmatchedOffchainBuyShares.plus(shares)
    book.summary.unmatchedOffchainBuyNotionalUsd =
      book.summary.unmatchedOffchainBuyNotionalUsd.plus(notional)
    book.unmatchedOffchainLongLots.push({
      tradeId: fill.id,
      side: 'long',
      remainingShares: shares,
      price: fill.price,
      openedAt: fill.executedAt,
      openedRowid: fill.rowid
    })
    return
  }

  book.summary.unmatchedOffchainSellShares = book.summary.unmatchedOffchainSellShares.plus(shares)
  book.summary.unmatchedOffchainSellNotionalUsd =
    book.summary.unmatchedOffchainSellNotionalUsd.plus(notional)
  book.unmatchedOffchainShortLots.push({
    tradeId: fill.id,
    side: 'short',
    remainingShares: shares,
    price: fill.price,
    openedAt: fill.executedAt,
    openedRowid: fill.rowid
  })
}

const finalizeLots = (summary: SummaryAcc, lots: Lot[], markPrice: Decimal | undefined): void => {
  for (const lot of lots) {
    const notional = lot.remainingShares.mul(lot.price)

    if (lot.side === 'long') {
      summary.openLongShares = summary.openLongShares.plus(lot.remainingShares)
      summary.openLongNotionalUsd = summary.openLongNotionalUsd.plus(notional)
    } else {
      summary.openShortShares = summary.openShortShares.plus(lot.remainingShares)
      summary.openShortNotionalUsd = summary.openShortNotionalUsd.plus(notional)
    }

    if (markPrice !== undefined) {
      const spread = lot.side === 'long' ? markPrice.minus(lot.price) : lot.price.minus(markPrice)
      const directionalPnl = lot.remainingShares.mul(spread)
      summary.directionalImbalanceExcessPnlUsd =
        summary.directionalImbalanceExcessPnlUsd.plus(directionalPnl)
      summary.directionalExposurePnlUsd = summary.directionalExposurePnlUsd.plus(directionalPnl)
    }

    summary.openLotCount += 1
  }
}

const finalizeBook = (
  symbol: string,
  book: SymbolBook,
  markPrices: Map<string, Decimal>,
  positionNets: Map<string, Decimal>,
  warnings: string[],
  positionReplayDeltas: PositionReplayDelta[]
): void => {
  const markPrice = markPrices.get(symbol)
  finalizeLots(book.summary, book.longLots, markPrice)
  finalizeLots(book.summary, book.shortLots, markPrice)
  finalizeLots(book.summary, book.unmatchedOffchainLongLots, markPrice)
  finalizeLots(book.summary, book.unmatchedOffchainShortLots, markPrice)

  for (const [tradeId, matchedShares] of book.matchedOnchainShares) {
    const originalShares = book.originalOnchainShares.get(tradeId)
    if (originalShares !== undefined && matchedShares.gt(originalShares.plus('0.000001'))) {
      warnings.push(
        `PnL audit error: onchain lot ${tradeId} for ${symbol} matched ${fmtDecimal(matchedShares)} shares above original ${fmtDecimal(originalShares)}`
      )
    }
  }

  const positionNet = positionNets.get(symbol)
  if (positionNet !== undefined) {
    const replayNet = book.summary.openLongShares.minus(book.summary.openShortShares)
    if (replayNet.minus(positionNet).abs().gt('0.000001')) {
      positionReplayDeltas.push({
        symbol,
        replayNet,
        positionNet
      })
    }
  }
}

const addSummary = (target: SummaryAcc, source: SummaryAcc): void => {
  target.counterTradePnlUsd = target.counterTradePnlUsd.plus(source.counterTradePnlUsd)
  target.onchainNettingPnlUsd = target.onchainNettingPnlUsd.plus(source.onchainNettingPnlUsd)
  target.directionalInventoryBaselinePnlUsd = target.directionalInventoryBaselinePnlUsd.plus(
    source.directionalInventoryBaselinePnlUsd
  )
  target.directionalImbalanceExcessPnlUsd = target.directionalImbalanceExcessPnlUsd.plus(
    source.directionalImbalanceExcessPnlUsd
  )
  target.directionalExposurePnlUsd = target.directionalExposurePnlUsd.plus(
    source.directionalExposurePnlUsd
  )
  target.realizedPnlUsd = target.realizedPnlUsd.plus(source.realizedPnlUsd)
  target.matchedShares = target.matchedShares.plus(source.matchedShares)
  target.onchainNotionalUsd = target.onchainNotionalUsd.plus(source.onchainNotionalUsd)
  target.offchainNotionalUsd = target.offchainNotionalUsd.plus(source.offchainNotionalUsd)
  target.openLongShares = target.openLongShares.plus(source.openLongShares)
  target.openShortShares = target.openShortShares.plus(source.openShortShares)
  target.openLongNotionalUsd = target.openLongNotionalUsd.plus(source.openLongNotionalUsd)
  target.openShortNotionalUsd = target.openShortNotionalUsd.plus(source.openShortNotionalUsd)
  target.unmatchedOffchainBuyShares = target.unmatchedOffchainBuyShares.plus(
    source.unmatchedOffchainBuyShares
  )
  target.unmatchedOffchainSellShares = target.unmatchedOffchainSellShares.plus(
    source.unmatchedOffchainSellShares
  )
  target.unmatchedOffchainBuyNotionalUsd = target.unmatchedOffchainBuyNotionalUsd.plus(
    source.unmatchedOffchainBuyNotionalUsd
  )
  target.unmatchedOffchainSellNotionalUsd = target.unmatchedOffchainSellNotionalUsd.plus(
    source.unmatchedOffchainSellNotionalUsd
  )
  target.onchainFillCount += source.onchainFillCount
  target.offchainFillCount += source.offchainFillCount
  target.matchedLotCount += source.matchedLotCount
  target.openLotCount += source.openLotCount
  target.unmatchedOffchainFillCount += source.unmatchedOffchainFillCount
}

const summaryToDto = (summary: SummaryAcc): PnlSummary => {
  const directionalExposurePnl = summary.directionalInventoryBaselinePnlUsd.plus(
    summary.directionalImbalanceExcessPnlUsd
  )
  const totalPnl = summary.counterTradePnlUsd
    .plus(summary.onchainNettingPnlUsd)
    .plus(summary.directionalInventoryBaselinePnlUsd)
    .plus(summary.directionalImbalanceExcessPnlUsd)

  return {
    counterTradePnlUsd: fmtDecimal(summary.counterTradePnlUsd),
    onchainNettingPnlUsd: fmtDecimal(summary.onchainNettingPnlUsd),
    directionalInventoryBaselinePnlUsd: fmtDecimal(summary.directionalInventoryBaselinePnlUsd),
    directionalImbalanceExcessPnlUsd: fmtDecimal(summary.directionalImbalanceExcessPnlUsd),
    directionalExposurePnlUsd: fmtDecimal(directionalExposurePnl),
    totalPnlUsd: fmtDecimal(totalPnl),
    realizedPnlUsd: fmtDecimal(summary.realizedPnlUsd),
    matchedShares: fmtDecimal(summary.matchedShares),
    onchainNotionalUsd: fmtDecimal(summary.onchainNotionalUsd),
    offchainNotionalUsd: fmtDecimal(summary.offchainNotionalUsd),
    inventoryDriftShares: fmtDecimal(summary.openLongShares.minus(summary.openShortShares)),
    inventoryDriftUsd: fmtDecimal(summary.openLongNotionalUsd.minus(summary.openShortNotionalUsd)),
    openLongShares: fmtDecimal(summary.openLongShares),
    openShortShares: fmtDecimal(summary.openShortShares),
    unmatchedOffchainShares: fmtDecimal(
      summary.unmatchedOffchainBuyShares.plus(summary.unmatchedOffchainSellShares)
    ),
    unmatchedOffchainNotionalUsd: fmtDecimal(
      summary.unmatchedOffchainBuyNotionalUsd.plus(summary.unmatchedOffchainSellNotionalUsd)
    ),
    onchainFillCount: summary.onchainFillCount,
    offchainFillCount: summary.offchainFillCount,
    matchedLotCount: summary.matchedLotCount,
    openLotCount: summary.openLotCount,
    unmatchedOffchainFillCount: summary.unmatchedOffchainFillCount
  }
}

const symbolSummaryToDto = (symbol: string, summary: SummaryAcc): PnlSymbolSummary => {
  const dto = summaryToDto(summary)
  return {
    symbol,
    counterTradePnlUsd: dto.counterTradePnlUsd,
    onchainNettingPnlUsd: dto.onchainNettingPnlUsd,
    directionalInventoryBaselinePnlUsd: dto.directionalInventoryBaselinePnlUsd,
    directionalImbalanceExcessPnlUsd: dto.directionalImbalanceExcessPnlUsd,
    directionalExposurePnlUsd: dto.directionalExposurePnlUsd,
    totalPnlUsd: dto.totalPnlUsd,
    realizedPnlUsd: dto.realizedPnlUsd,
    matchedShares: dto.matchedShares,
    inventoryDriftShares: dto.inventoryDriftShares,
    inventoryDriftUsd: dto.inventoryDriftUsd,
    openLongShares: dto.openLongShares,
    openShortShares: dto.openShortShares,
    unmatchedOffchainShares: dto.unmatchedOffchainShares,
    matchedLotCount: dto.matchedLotCount,
    onchainFillCount: dto.onchainFillCount,
    offchainFillCount: dto.offchainFillCount,
    unmatchedOffchainFillCount: dto.unmatchedOffchainFillCount
  }
}

const entryBucketToStream = (bucket: string): PnlStreamKey | null => {
  if (bucket === 'counter_trade') return 'counterTradePnlUsd'
  if (bucket === 'onchain_netting') return 'onchainNettingPnlUsd'
  if (bucket === 'directional_exposure') return 'directionalImbalanceExcessPnlUsd'
  return null
}

const summaryFromEntries = (
  entries: PnlEntry[]
): { summary: PnlSummary; symbols: PnlSymbolSummary[] } => {
  const total = emptySummary()
  const perSymbol = new Map<string, SummaryAcc>()

  for (const entry of entries) {
    const summary = perSymbol.get(entry.symbol) ?? emptySummary()
    perSymbol.set(entry.symbol, summary)
    const shares = new Decimal(entry.shares)
    const openingNotional = shares.mul(new Decimal(entry.openingPriceUsd))
    const closingNotional = shares.mul(new Decimal(entry.closingPriceUsd))
    const pnl = new Decimal(entry.realizedPnlUsd)

    summary.matchedShares = summary.matchedShares.plus(shares)
    summary.onchainNotionalUsd = summary.onchainNotionalUsd.plus(openingNotional)
    summary.matchedLotCount += 1

    if (entry.pnlBucket === 'counter_trade') {
      summary.counterTradePnlUsd = summary.counterTradePnlUsd.plus(pnl)
      summary.realizedPnlUsd = summary.realizedPnlUsd.plus(pnl)
      summary.offchainNotionalUsd = summary.offchainNotionalUsd.plus(closingNotional)
    } else if (entry.pnlBucket === 'onchain_netting') {
      summary.onchainNettingPnlUsd = summary.onchainNettingPnlUsd.plus(pnl)
      summary.realizedPnlUsd = summary.realizedPnlUsd.plus(pnl)
      summary.onchainNotionalUsd = summary.onchainNotionalUsd.plus(closingNotional)
    } else if (entry.pnlBucket === 'directional_exposure') {
      summary.directionalImbalanceExcessPnlUsd = summary.directionalImbalanceExcessPnlUsd.plus(pnl)
      summary.directionalExposurePnlUsd = summary.directionalExposurePnlUsd.plus(pnl)
      summary.offchainNotionalUsd = summary.offchainNotionalUsd.plus(closingNotional)
    }
  }

  const symbols = [...perSymbol.entries()]
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([symbol, summary]) => {
      addSummary(total, summary)
      return symbolSummaryToDto(symbol, summary)
    })

  return { summary: summaryToDto(total), symbols }
}

const buildWindows = (entries: PnlEntry[], symbols: string[]): SyntheticPnlWindow[] => {
  const byDate = new Map<string, PnlEntry[]>()

  for (const entry of entries) {
    const key = dateKey(entry.closedAt)
    byDate.set(key, [...(byDate.get(key) ?? []), entry])
  }

  return [...byDate.entries()]
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([date, dayEntries]) => {
      const rows: SyntheticPnlWindowSymbol[] = symbols.map((symbol) => {
        const values: Record<PnlStreamKey, Decimal> = {
          counterTradePnlUsd: new Decimal(0),
          onchainNettingPnlUsd: new Decimal(0),
          directionalInventoryBaselinePnlUsd: new Decimal(0),
          directionalImbalanceExcessPnlUsd: new Decimal(0)
        }

        for (const entry of dayEntries) {
          if (entry.symbol !== symbol) continue
          const stream = entryBucketToStream(entry.pnlBucket)
          if (stream === null) continue
          values[stream] = values[stream].plus(new Decimal(entry.realizedPnlUsd))
        }

        const directionalExposure = values.directionalInventoryBaselinePnlUsd.plus(
          values.directionalImbalanceExcessPnlUsd
        )
        const totalPnl = values.counterTradePnlUsd
          .plus(values.onchainNettingPnlUsd)
          .plus(values.directionalInventoryBaselinePnlUsd)
          .plus(values.directionalImbalanceExcessPnlUsd)

        return {
          symbol,
          counterTradePnlUsd: fmtDecimal(values.counterTradePnlUsd),
          onchainNettingPnlUsd: fmtDecimal(values.onchainNettingPnlUsd),
          directionalInventoryBaselinePnlUsd: fmtDecimal(values.directionalInventoryBaselinePnlUsd),
          directionalImbalanceExcessPnlUsd: fmtDecimal(values.directionalImbalanceExcessPnlUsd),
          directionalExposurePnlUsd: fmtDecimal(directionalExposure),
          totalPnlUsd: fmtDecimal(totalPnl)
        }
      })

      return {
        windowId: date,
        startAt: `${date}T00:00:00.000Z`,
        endAt: `${date}T23:59:59.999Z`,
        label: date,
        isWeekend: isWeekendIso(`${date}T00:00:00.000Z`),
        granularity: 'day',
        symbols: rows
      }
    })
}

const parsePositionView = (
  rows: SqlPositionViewRow[],
  warnings: string[]
): { markPrices: Map<string, Decimal>; positionNets: Map<string, Decimal>; symbols: string[] } => {
  const markPrices = new Map<string, Decimal>()
  const positionNets = new Map<string, Decimal>()
  const symbols = new Set<string>()

  for (const row of rows) {
    symbols.add(row.symbol)

    if (row.last_price_usdc !== null) {
      try {
        markPrices.set(row.symbol, new Decimal(row.last_price_usdc))
      } catch {
        warnings.push(`Skipped malformed mark price for ${row.symbol}: ${row.last_price_usdc}`)
      }
    }

    if (row.net_position !== null) {
      try {
        positionNets.set(row.symbol, new Decimal(row.net_position))
      } catch {
        warnings.push(`Skipped malformed position net for ${row.symbol}: ${row.net_position}`)
      }
    }
  }

  return { markPrices, positionNets, symbols: [...symbols].sort() }
}

const parseCount = (value: number | string | null | undefined): number => {
  const parsed = Number(value ?? 0)
  return Number.isFinite(parsed) ? parsed : 0
}

const buildSampleStats = (rows: SqlSampleStatsRow[]): PnlSampleStats => {
  const symbols: PnlSampleSymbolStats[] = rows.map((row) => {
    const onchainFillCount = parseCount(row.onchain_fill_count)
    const offchainFillCount = parseCount(row.offchain_fill_count)

    return {
      symbol: row.symbol,
      firstAt: row.first_at,
      lastAt: row.last_at,
      onchainFillCount,
      offchainFillCount,
      totalFillCount: onchainFillCount + offchainFillCount
    }
  })

  const firstAt =
    symbols
      .map((row) => row.firstAt)
      .filter((value): value is string => value !== null)
      .sort((left, right) => left.localeCompare(right))[0] ?? null
  const lastAt =
    symbols
      .map((row) => row.lastAt)
      .filter((value): value is string => value !== null)
      .sort((left, right) => right.localeCompare(left))[0] ?? null

  return {
    firstAt,
    lastAt,
    symbolCount: symbols.length,
    onchainFillCount: symbols.reduce((total, row) => total + row.onchainFillCount, 0),
    offchainFillCount: symbols.reduce((total, row) => total + row.offchainFillCount, 0),
    totalFillCount: symbols.reduce((total, row) => total + row.totalFillCount, 0),
    symbols
  }
}

export const buildPnlResponseFromSqlRows = (
  eventRows: SqlPositionEventRow[],
  positionRows: SqlPositionViewRow[],
  sampleRows: SqlSampleStatsRow[],
  query: SqlPnlQuery
): PnlResponse => {
  const warnings = [ATTRIBUTION_WARNING, BASELINE_WARNING]
  const {
    markPrices,
    positionNets,
    symbols: positionSymbols
  } = parsePositionView(positionRows, warnings)
  const sampleStats = buildSampleStats(sampleRows)
  const books = new Map<string, SymbolBook>()
  const entries: PnlEntry[] = []
  const unmatchedOffchainAllocations: UnmatchedOffchainAllocation[] = []
  const positionReplayDeltas: PositionReplayDelta[] = []

  for (const row of [...eventRows].sort((left, right) => left.rowid - right.rowid)) {
    const book = getBook(books, row.symbol)

    if (row.event_type === 'PositionEvent::OnChainOrderFilled') {
      const fill = parseOnchainFill(row, warnings)
      if (fill !== null) applyOnchainFill(book, fill, entries, warnings)
    } else if (row.event_type === 'PositionEvent::OffChainOrderPlaced') {
      applyOffchainPlacement(book, row, warnings)
    } else if (row.event_type === 'PositionEvent::OffChainOrderFilled') {
      const fill = parseOffchainFill(row, warnings)
      if (fill !== null)
        applyOffchainFill(book, fill, entries, warnings, unmatchedOffchainAllocations)
    }
  }

  const fullTotal = emptySummary()
  for (const [symbol, book] of books) {
    finalizeBook(symbol, book, markPrices, positionNets, warnings, positionReplayDeltas)
    addSummary(fullTotal, book.summary)
  }
  appendReplayDiagnostics(warnings, unmatchedOffchainAllocations, positionReplayDeltas)

  const filteredEntries = entries
    .filter((entry) => matchesDateFilter(entry, query))
    .sort((left, right) => right.matchedAt.localeCompare(left.matchedAt))
  const total = filteredEntries.length
  const start = Math.min(query.offset, total)
  const end = Math.min(start + query.limit, total)
  const pageEntries = filteredEntries.slice(start, end)
  const filtered = summaryFromEntries(filteredEntries)
  const symbolUniverse = [
    ...new Set([...positionSymbols, ...books.keys(), ...filtered.symbols.map((row) => row.symbol)])
  ].sort()

  return {
    attributionMethod: ATTRIBUTION_METHOD,
    warnings,
    sampleStats,
    summary: filtered.summary,
    symbols: filtered.symbols,
    symbolUniverse,
    entries: pageEntries,
    total,
    hasMore: end < total,
    windows: buildWindows(filteredEntries, symbolUniverse)
  }
}

export const fetchPnlReportFromSql = async (
  baseUrl: string,
  query: SqlPnlQuery
): Promise<PnlResponse> => {
  const [eventRows, positionRows, sampleRows] = await Promise.all([
    fetchSqlRows<SqlPositionEventRow>(baseUrl, positionEventsSql(query.symbols)),
    fetchSqlRows<SqlPositionViewRow>(baseUrl, positionViewSql(new Set())),
    fetchSqlRows<SqlSampleStatsRow>(baseUrl, sampleStatsSql())
  ])

  return buildPnlResponseFromSqlRows(eventRows, positionRows, sampleRows, query)
}
