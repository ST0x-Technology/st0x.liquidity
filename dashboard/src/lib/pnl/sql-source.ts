import Decimal from 'decimal.js'
import type {
  PnlAccountingBucket,
  PnlAccountingEffect,
  PnlCostCategory,
  PnlCostEntry,
  PnlCostSummary,
  PnlEntry,
  PnlCounterTradingFilter,
  PnlMarketSessionFilter,
  PnlResponse,
  PnlSampleStats,
  PnlStreamKey,
  PnlSummary,
  PnlSymbolSummary,
  PnlWindow,
  PnlWindowCounterTradingSession,
  PnlWindowMarketSession,
  PnlWindowSymbol
} from './report'

export type SqlPnlQuery = {
  limit: number
  offset: number
  symbols: Set<string>
  fromDate?: string
  toDate?: string
  marketSessionFilter?: PnlMarketSessionFilter
  counterTradingFilter?: PnlCounterTradingFilter
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

export type SqlCostEventRow = {
  rowid: number
  aggregate_type: string
  aggregate_id: string
  event_type: string
  payload: unknown
}

type Direction = 'Buy' | 'Sell'
type LotSide = 'long' | 'short'
type PnlBucket = 'counter_trade' | 'onchain_netting' | 'directional_exposure'
type SqlSymbolColumn = 'aggregate_id' | 'symbol'
type Venue = 'onchain' | 'offchain'

type Fill = {
  rowid: number
  id: string
  symbol: string
  shares: Decimal
  direction: Direction
  price: Decimal
  executedAt: string
  venue: Venue
}

type Lot = {
  tradeId: string
  side: LotSide
  remainingShares: Decimal
  price: Decimal
  openedAt: string
  openedRowid: number
  openedVenue: Venue
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

type CostSummaryAcc = {
  counterTradeCostsUsd: Decimal
  onchainNettingCostsUsd: Decimal
  directionalExposureCostsUsd: Decimal
  genericCostsUsd: Decimal
  genericRevenueUsd: Decimal
  dividendRevenueUsd: Decimal
  offchainExecutionFeesUsd: Decimal
  tokenizationFeesUsd: Decimal
  cctpFeesUsd: Decimal
  conversionSlippageUsd: Decimal
  oracleWriteCostUsd: Decimal
  brokerFeesUsd: Decimal
  regulatoryFeesUsd: Decimal
  marginInterestUsd: Decimal
  botGasUsd: Decimal
  walletTransferFeesUsd: Decimal
  unclassifiedCostsUsd: Decimal
  missingCostObservationCount: number
}

const ATTRIBUTION_METHOD = 'direct_sql_position_fill_replay_fifo'
const COUNTER_TRADE_THRESHOLD_SECONDS = 300
const DATASSETTE_DEFAULT_MAX_RETURNED_ROWS = 1000
const DATASSETTE_SQL_PAGE_SIZE = 900
const SQL_FETCH_TIMEOUT_MS = 30000
const SAFE_SQL_SYMBOL = /^[A-Za-z0-9._-]+$/u
const ZERO = new Decimal(0)

const ATTRIBUTION_WARNING =
  'PnL source: realized gross replay from the deployed SQL JSON endpoint. Fills are ordered by execution timestamp and replayed through per-symbol FIFO inventory lots for accounting and attribution; explicit offchain_order_id -> onchain_trade_ids parentage is not currently persisted.'
const BASELINE_WARNING =
  'Displayed PnL is realized by lot close date from persisted fills. Baseline drift, percentage return, and true period/NAV PnL require a persisted portfolio state vector, price vector, and cash-flow events; those are not currently persisted, so baseline drift and percentage return are not reported.'
const COST_WARNING =
  'Tracked costs and revenues are built bottom-up by economic bucket. On-chain netting and raw directional drift have no direct bot-paid execution cost. USD and USDC are treated as equivalent reporting currency, so USD/USDC conversion basis is not modeled as PnL; only explicit persisted fees are deducted. Persisted SQLite costs currently include tokenization fees and CCTP fees; Alpaca account fees, margin interest, and dividends are included when the Alpaca account-activity API endpoint is configured. Oracle write cost is zero for the current setup. Wallet transfer fees and bot gas require additional ledger/receipt ingestion before they can be included.'

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

const emptyCostSummaryAcc = (): CostSummaryAcc => ({
  counterTradeCostsUsd: new Decimal(0),
  onchainNettingCostsUsd: new Decimal(0),
  directionalExposureCostsUsd: new Decimal(0),
  genericCostsUsd: new Decimal(0),
  genericRevenueUsd: new Decimal(0),
  dividendRevenueUsd: new Decimal(0),
  offchainExecutionFeesUsd: new Decimal(0),
  tokenizationFeesUsd: new Decimal(0),
  cctpFeesUsd: new Decimal(0),
  conversionSlippageUsd: new Decimal(0),
  oracleWriteCostUsd: new Decimal(0),
  brokerFeesUsd: new Decimal(0),
  regulatoryFeesUsd: new Decimal(0),
  marginInterestUsd: new Decimal(0),
  botGasUsd: new Decimal(0),
  walletTransferFeesUsd: new Decimal(0),
  unclassifiedCostsUsd: new Decimal(0),
  missingCostObservationCount: 0
})

const emptyBook = (): SymbolBook => ({
  longLots: [],
  shortLots: [],
  seenOnchainFillIds: new Set(),
  seenOffchainPlacementIds: new Set(),
  seenOffchainFillIds: new Set(),
  originalOnchainShares: new Map(),
  matchedOnchainShares: new Map(),
  summary: emptySummary()
})

const isSafeSqlSymbol = (symbol: string): boolean => SAFE_SQL_SYMBOL.test(symbol)

const sqlString = (value: string): string => `'${value.replaceAll("'", "''")}'`

const symbolPredicate = (symbols: Set<string>, column: SqlSymbolColumn): string => {
  if (symbols.size === 0) return ''

  const safeSymbols = [...symbols].filter(isSafeSqlSymbol)
  if (safeSymbols.length === 0) return ' AND 1 = 0'

  return ` AND ${column} IN (${safeSymbols.map(sqlString).join(',')})`
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

const stripSqlTerminator = (sql: string): string => sql.trim().replace(/;+\s*$/u, '')

const pagedSql = (sql: string, offset: number): string =>
  `SELECT * FROM (${stripSqlTerminator(sql)}) LIMIT ${String(DATASSETTE_SQL_PAGE_SIZE)} OFFSET ${String(offset)}`

export const buildSqlApiUrl = (baseUrl: string, sql: string, offset = 0): string => {
  const url = new URL(baseUrl, globalOrigin())
  url.searchParams.set('sql', pagedSql(sql, offset))
  url.searchParams.set('_shape', 'objects')
  url.searchParams.set('_size', 'max')
  return url.toString()
}

type DatasetteRowsResponse = {
  rows?: unknown
  truncated?: boolean
}

const fetchSqlPage = async <Row>(baseUrl: string, sql: string, offset: number): Promise<Row[]> => {
  const response = await fetch(buildSqlApiUrl(baseUrl, sql, offset), {
    signal: AbortSignal.timeout(SQL_FETCH_TIMEOUT_MS)
  })

  if (!response.ok) {
    throw new Error(`SQL endpoint HTTP ${String(response.status)}`)
  }

  const body = (await response.json()) as DatasetteRowsResponse
  if (!Array.isArray(body.rows)) {
    throw new Error('SQL endpoint returned a response without rows')
  }

  if (body.truncated === true) {
    throw new Error('SQL endpoint truncated a paged result set; refusing to render partial PnL')
  }

  if (body.truncated === undefined && body.rows.length >= DATASSETTE_DEFAULT_MAX_RETURNED_ROWS) {
    throw new Error('SQL endpoint may have truncated the result set; refusing to render partial PnL')
  }

  return body.rows as Row[]
}

const fetchSqlRows = async <Row>(baseUrl: string, sql: string): Promise<Row[]> => {
  const rows: Row[] = []
  let offset = 0

  for (;;) {
    const page = await fetchSqlPage<Row>(baseUrl, sql, offset)
    rows.push(...page)

    if (page.length < DATASSETTE_SQL_PAGE_SIZE) return rows
    offset += DATASSETTE_SQL_PAGE_SIZE
  }
}

const positionEventsSql = (symbols: Set<string>): string => `
SELECT rowid, aggregate_id AS symbol, event_type, payload
FROM events
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
  last_updated
FROM position_view
WHERE symbol IS NOT NULL
  ${symbolPredicate(symbols, 'symbol')}
ORDER BY symbol ASC
`

const costEventsSql = (): string => `
SELECT rowid, aggregate_type, aggregate_id, event_type, payload
FROM events
WHERE (
    aggregate_type = 'TokenizedEquityMint'
    AND event_type IN (
      'TokenizedEquityMintEvent::MintRequested',
      'TokenizedEquityMintEvent::TokensReceived',
      'TokenizedEquityMintEvent::ProviderCompletionRecovered'
    )
  )
  OR (
    aggregate_type = 'UsdcRebalance'
    AND event_type IN (
      'UsdcRebalanceEvent::Bridged',
      'UsdcRebalanceEvent::BridgingCompletionRecovered'
    )
  )
ORDER BY rowid ASC
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

const positionEventReplayTimestamp = (row: SqlPositionEventRow): string | null => {
  const payload = parsePayload(row.payload)
  if (payload === null) return null

  if (row.event_type === 'PositionEvent::OnChainOrderFilled') {
    const filled = nestedRecord(payload, 'OnChainOrderFilled')
    return filled === null ? null : textField(filled, 'block_timestamp')
  }

  if (row.event_type === 'PositionEvent::OffChainOrderFilled') {
    const filled = nestedRecord(payload, 'OffChainOrderFilled')
    return filled === null ? null : textField(filled, 'broker_timestamp')
  }

  if (row.event_type === 'PositionEvent::OffChainOrderPlaced') {
    const placed = nestedRecord(payload, 'OffChainOrderPlaced')
    return placed === null ? null : textField(placed, 'placed_at')
  }

  return null
}

const orderedPositionEvents = (
  rows: SqlPositionEventRow[],
  warnings: string[]
): SqlPositionEventRow[] =>
  [...rows]
    .map((row) => {
      const timestamp = positionEventReplayTimestamp(row)
      const parsed = timestamp === null ? Number.NaN : Date.parse(timestamp)
      const hasTimestamp = Number.isFinite(parsed)
      if (timestamp !== null && !hasTimestamp) {
        warnings.push(
          `PnL audit note: invalid replay timestamp for ${row.symbol} row ${String(row.rowid)}; using event row order`
        )
      }

      return {
        row,
        timestampMs: hasTimestamp ? parsed : Number.POSITIVE_INFINITY
      }
    })
    .sort((left, right) => {
      if (left.timestampMs !== right.timestampMs) return left.timestampMs - right.timestampMs
      return left.row.rowid - right.row.rowid
    })
    .map(({ row }) => row)

const dateKey = (iso: string): string => iso.slice(0, 10)

const newYorkTimeParts = new Intl.DateTimeFormat('en-US', {
  timeZone: 'America/New_York',
  weekday: 'short',
  hour: '2-digit',
  minute: '2-digit',
  hourCycle: 'h23'
})

const parseNewYorkSessionParts = (
  iso: string
): { weekday: string; minuteOfDay: number } | null => {
  const parsed = Date.parse(iso)
  if (!Number.isFinite(parsed)) return null

  const parts = newYorkTimeParts.formatToParts(new Date(parsed))
  const weekday = parts.find((part) => part.type === 'weekday')?.value
  const hour = parts.find((part) => part.type === 'hour')?.value
  const minute = parts.find((part) => part.type === 'minute')?.value
  if (weekday === undefined || hour === undefined || minute === undefined) return null

  return {
    weekday,
    minuteOfDay: Number(hour) * 60 + Number(minute)
  }
}

const isNewYorkWeekendIso = (iso: string): boolean => {
  const parts = parseNewYorkSessionParts(iso)
  return parts === null || parts.weekday === 'Sat' || parts.weekday === 'Sun'
}

const marketSessionForIso = (iso: string): Exclude<PnlMarketSessionFilter, 'all'> => {
  const parts = parseNewYorkSessionParts(iso)
  if (parts === null) return 'overnight'

  const isWeekend = parts.weekday === 'Sat' || parts.weekday === 'Sun'
  if (isWeekend) return 'weekend'

  const preStartMinutes = 4 * 60
  const rthStartMinutes = 9 * 60 + 30
  const postStartMinutes = 16 * 60
  const postEndMinutes = 20 * 60

  if (parts.minuteOfDay >= preStartMinutes && parts.minuteOfDay < rthStartMinutes) return 'pre'
  if (parts.minuteOfDay >= rthStartMinutes && parts.minuteOfDay < postStartMinutes) return 'rth'
  if (parts.minuteOfDay >= postStartMinutes && parts.minuteOfDay < postEndMinutes) return 'post'
  return 'overnight'
}

const counterTradingSessionForIso = (
  iso: string
): Exclude<PnlCounterTradingFilter, 'all'> => {
  // Match the current Alpaca hotpath: hedges are market orders with
  // extended_hours=false, and the executor gates on Alpaca calendar
  // regular-hours open/close. In the dashboard model that maps to RTH.
  const isActive = marketSessionForIso(iso) === 'rth'

  return isActive ? 'counter_trading_active' : 'counter_trading_inactive'
}

const matchesMarketSessionFilter = (
  iso: string,
  filter: PnlMarketSessionFilter | undefined
): boolean => {
  if (filter === undefined || filter === 'all') return true
  return marketSessionForIso(iso) === filter
}

const matchesCounterTradingFilter = (
  iso: string,
  filter: PnlCounterTradingFilter | undefined
): boolean => {
  if (filter === undefined || filter === 'all') return true
  return counterTradingSessionForIso(iso) === filter
}

const matchesTradeFilters = (iso: string, query: SqlPnlQuery): boolean => {
  if (!matchesMarketSessionFilter(iso, query.marketSessionFilter)) return false
  if (!matchesCounterTradingFilter(iso, query.counterTradingFilter)) return false
  return true
}

const matchesDateFilter = (entry: PnlEntry, query: SqlPnlQuery): boolean => {
  const day = dateKey(entry.closedAt)
  if (query.fromDate !== undefined && day < query.fromDate) return false
  if (query.toDate !== undefined && day > query.toDate) return false
  if (!matchesTradeFilters(entry.closedAt, query)) return false
  return true
}

const secondsBetween = (startIso: string, endIso: string): number =>
  Math.max(0, Math.floor((new Date(endIso).getTime() - new Date(startIso).getTime()) / 1000))

const fmtDecimal = (value: Decimal): string => {
  const fixed = value.toFixed(9)
  const trimmed = fixed.replace(/\.?0+$/u, '')
  return trimmed === '-0' || trimmed === '' ? '0' : trimmed
}

const matchesCostDateFilter = (entry: PnlCostEntry, query: SqlPnlQuery): boolean => {
  const day = dateKey(entry.occurredAt)
  if (query.fromDate !== undefined && day < query.fromDate) return false
  if (query.toDate !== undefined && day > query.toDate) return false
  if (
    entry.aggregateType === 'AlpacaAccountActivity' &&
    entry.accountingBucket === 'counter_trade'
  ) {
    if (query.marketSessionFilter !== undefined && query.marketSessionFilter !== 'all') {
      if (query.marketSessionFilter !== 'rth') return false
    }
    if (
      query.counterTradingFilter !== undefined &&
      query.counterTradingFilter !== 'all' &&
      query.counterTradingFilter !== 'counter_trading_active'
    ) {
      return false
    }
    return true
  }

  if (!matchesTradeFilters(entry.occurredAt, query)) return false
  return true
}

const matchesCostSymbolFilter = (entry: PnlCostEntry, symbols: Set<string>): boolean =>
  symbols.size === 0 || entry.symbol === null || symbols.has(entry.symbol)

const costEntry = (
  row: SqlCostEventRow,
  category: PnlCostCategory,
  accountingBucket: PnlAccountingBucket,
  effect: PnlAccountingEffect,
  amountUsd: Decimal,
  occurredAt: string,
  detail: string,
  symbol: string | null = null
): PnlCostEntry => ({
  category,
  accountingBucket,
  effect,
  amountUsd: fmtDecimal(amountUsd),
  occurredAt,
  aggregateType: row.aggregate_type,
  aggregateId: row.aggregate_id,
  eventRowid: row.rowid,
  symbol,
  detail
})

const addCost = (
  summary: CostSummaryAcc,
  category: PnlCostCategory,
  accountingBucket: PnlAccountingBucket,
  effect: PnlAccountingEffect,
  amount: Decimal
): void => {
  if (effect === 'cost') {
    if (accountingBucket === 'counter_trade') {
      summary.counterTradeCostsUsd = summary.counterTradeCostsUsd.plus(amount)
    } else if (accountingBucket === 'onchain_netting') {
      summary.onchainNettingCostsUsd = summary.onchainNettingCostsUsd.plus(amount)
    } else if (accountingBucket === 'directional_exposure') {
      summary.directionalExposureCostsUsd = summary.directionalExposureCostsUsd.plus(amount)
    } else {
      summary.genericCostsUsd = summary.genericCostsUsd.plus(amount)
    }
  } else if (effect === 'revenue' && accountingBucket === 'dividend_revenue') {
    summary.dividendRevenueUsd = summary.dividendRevenueUsd.plus(amount)
  } else if (effect === 'revenue') {
    summary.genericRevenueUsd = summary.genericRevenueUsd.plus(amount)
  }

  if (category === 'tokenization_fee') {
    summary.tokenizationFeesUsd = summary.tokenizationFeesUsd.plus(amount)
  } else if (category === 'cctp_fee') {
    summary.cctpFeesUsd = summary.cctpFeesUsd.plus(amount)
  } else if (category === 'conversion_slippage') {
    summary.conversionSlippageUsd = summary.conversionSlippageUsd.plus(amount)
  } else if (category === 'oracle_write') {
    summary.oracleWriteCostUsd = summary.oracleWriteCostUsd.plus(amount)
  } else if (category === 'broker_fee') {
    summary.brokerFeesUsd = summary.brokerFeesUsd.plus(amount)
  } else if (category === 'regulatory_fee') {
    summary.regulatoryFeesUsd = summary.regulatoryFeesUsd.plus(amount)
  } else if (category === 'margin_interest') {
    summary.marginInterestUsd = summary.marginInterestUsd.plus(amount)
  } else if (category === 'bot_gas') {
    summary.botGasUsd = summary.botGasUsd.plus(amount)
  } else if (category === 'wallet_transfer_fee') {
    summary.walletTransferFeesUsd = summary.walletTransferFeesUsd.plus(amount)
  } else if (category === 'offchain_execution_fee') {
    summary.offchainExecutionFeesUsd = summary.offchainExecutionFeesUsd.plus(amount)
  } else if (category === 'dividend_income') {
    return
  } else {
    summary.unclassifiedCostsUsd = summary.unclassifiedCostsUsd.plus(amount)
  }
}

const totalTrackedCosts = (summary: CostSummaryAcc): Decimal =>
  summary.counterTradeCostsUsd
    .plus(summary.onchainNettingCostsUsd)
    .plus(summary.directionalExposureCostsUsd)
    .plus(summary.genericCostsUsd)

const totalTrackedRevenue = (summary: CostSummaryAcc): Decimal =>
  summary.dividendRevenueUsd.plus(summary.genericRevenueUsd)

const includedWhenNonZero = (value: Decimal): 'included' | 'not_ingested' =>
  value.isZero() ? 'not_ingested' : 'included'

const costSummaryToDto = (summary: CostSummaryAcc, costEntryCount: number): PnlCostSummary => ({
  totalTrackedCostsUsd: fmtDecimal(totalTrackedCosts(summary)),
  totalTrackedRevenueUsd: fmtDecimal(totalTrackedRevenue(summary)),
  counterTradeCostsUsd: fmtDecimal(summary.counterTradeCostsUsd),
  onchainNettingCostsUsd: fmtDecimal(summary.onchainNettingCostsUsd),
  directionalExposureCostsUsd: fmtDecimal(summary.directionalExposureCostsUsd),
  genericCostsUsd: fmtDecimal(summary.genericCostsUsd),
  dividendRevenueUsd: fmtDecimal(summary.dividendRevenueUsd),
  offchainExecutionFeesUsd: fmtDecimal(
    summary.offchainExecutionFeesUsd.plus(summary.regulatoryFeesUsd)
  ),
  tokenizationFeesUsd: fmtDecimal(summary.tokenizationFeesUsd),
  cctpFeesUsd: fmtDecimal(summary.cctpFeesUsd),
  conversionSlippageUsd: fmtDecimal(summary.conversionSlippageUsd),
  oracleWriteCostUsd: fmtDecimal(summary.oracleWriteCostUsd),
  brokerFeesUsd: fmtDecimal(summary.brokerFeesUsd),
  regulatoryFeesUsd: fmtDecimal(summary.regulatoryFeesUsd),
  marginInterestUsd: fmtDecimal(summary.marginInterestUsd),
  botGasUsd: fmtDecimal(summary.botGasUsd),
  walletTransferFeesUsd: fmtDecimal(summary.walletTransferFeesUsd),
  unclassifiedCostsUsd: fmtDecimal(summary.unclassifiedCostsUsd),
  costEntryCount,
  missingCostObservationCount: summary.missingCostObservationCount,
  coverage: [
    {
      source: 'Alpaca fees',
      accountingBucket: 'generic',
      effect: 'cost',
      status: includedWhenNonZero(summary.brokerFeesUsd),
      amountUsd: fmtDecimal(summary.brokerFeesUsd),
      note: 'Read from Alpaca account activity fee rows. These rows are not subtype-classified for now and are not allocated to symbols unless Alpaca supplies a symbol.'
    },
    {
      source: 'On-chain netting execution costs',
      accountingBucket: 'onchain_netting',
      effect: 'none',
      status: 'zero',
      amountUsd: fmtDecimal(summary.onchainNettingCostsUsd),
      note: 'Passive on-chain fills do not create bot-paid trade execution costs for the on-chain netting bucket.'
    },
    {
      source: 'Directional drift direct costs',
      accountingBucket: 'directional_exposure',
      effect: 'none',
      status: 'zero',
      amountUsd: fmtDecimal(summary.directionalExposureCostsUsd),
      note: 'Raw inventory drift is price movement on held exposure; it has no direct execution cost by itself.'
    },
    {
      source: 'Tokenization fees',
      accountingBucket: 'generic',
      effect: 'cost',
      status: 'included',
      amountUsd: fmtDecimal(summary.tokenizationFeesUsd),
      note: 'Read from TokenizedEquityMint terminal events when Alpaca reports fees.'
    },
    {
      source: 'CCTP fees',
      accountingBucket: 'generic',
      effect: 'cost',
      status: 'included',
      amountUsd: fmtDecimal(summary.cctpFeesUsd),
      note: 'Read from UsdcRebalance bridge completion events as fee_collected.'
    },
    {
      source: 'USD/USDC reporting basis',
      accountingBucket: 'generic',
      effect: 'none',
      status: 'zero',
      amountUsd: fmtDecimal(summary.conversionSlippageUsd),
      note: 'USD and USDC are treated as equivalent for reporting; conversion basis is not modeled as PnL. Only explicit persisted fees are deducted.'
    },
    {
      source: 'Oracle writes',
      accountingBucket: 'generic',
      effect: 'none',
      status: 'zero',
      amountUsd: fmtDecimal(summary.oracleWriteCostUsd),
      note: 'Current setup does not pay oracle write cost through the bot.'
    },
    {
      source: 'Dividend income',
      accountingBucket: 'dividend_revenue',
      effect: 'revenue',
      status: includedWhenNonZero(summary.dividendRevenueUsd),
      amountUsd: fmtDecimal(summary.dividendRevenueUsd),
      note: 'Dividend-bearing stock revenue increases net PnL when Alpaca dividend activity rows are available.'
    },
    {
      source: 'Margin interest',
      accountingBucket: 'generic',
      effect: 'cost',
      status: includedWhenNonZero(summary.marginInterestUsd),
      amountUsd: fmtDecimal(summary.marginInterestUsd),
      note: 'Included when Alpaca account activity interest rows are available; negative rows are costs and positive rows are credits.'
    },
    {
      source: 'Bot gas',
      accountingBucket: 'generic',
      effect: 'cost',
      status: 'not_ingested',
      amountUsd: fmtDecimal(summary.botGasUsd),
      note: 'Requires tx receipt ingestion, gas-payer classification, and ETH/USD valuation.'
    },
    {
      source: 'Wallet transfer fees',
      accountingBucket: 'generic',
      effect: 'cost',
      status: 'not_ingested',
      amountUsd: fmtDecimal(summary.walletTransferFeesUsd),
      note: 'Alpaca wallet fee fields are not currently persisted into the event stream.'
    }
  ]
})

const summarizeCostEntries = (
  entries: PnlCostEntry[],
  missingCostObservationCount: number
): PnlCostSummary => {
  const summary = emptyCostSummaryAcc()
  summary.missingCostObservationCount = missingCostObservationCount

  for (const entry of entries) {
    addCost(
      summary,
      entry.category,
      entry.accountingBucket,
      entry.effect,
      new Decimal(entry.amountUsd)
    )
  }

  return costSummaryToDto(summary, entries.length)
}

const withCosts = (summary: PnlSummary, costs: PnlCostSummary): PnlSummary => {
  const gross = new Decimal(summary.totalPnlUsd)
  const trackedCosts = new Decimal(costs.totalTrackedCostsUsd)
  const trackedRevenue = new Decimal(costs.totalTrackedRevenueUsd)
  const net = gross.minus(trackedCosts).plus(trackedRevenue)

  return {
    ...summary,
    grossRealizedPnlUsd: fmtDecimal(gross),
    trackedCostsUsd: fmtDecimal(trackedCosts),
    trackedRevenueUsd: fmtDecimal(trackedRevenue),
    netRealizedPnlUsd: fmtDecimal(net)
  }
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

  return `Allocation note: ${String(allocations.length)} offchain fills opened offchain-origin inventory outside the intended onchain-to-offchain hedge flow (${symbolDetails}). Those shares are carried in the FIFO ledger so later fills can close them.`
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

const appendInvalidQuerySymbols = (warnings: string[], symbols: Set<string>): void => {
  const invalidSymbols = [...symbols].filter((symbol) => !isSafeSqlSymbol(symbol))
  if (invalidSymbols.length === 0) return

  warnings.push(
    `Skipped ${String(invalidSymbols.length)} invalid symbol filters in SQL PnL query: ${invalidSymbols.join(', ')}`
  )
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

const addVenueNotional = (summary: SummaryAcc, venue: Venue, notional: Decimal): void => {
  if (venue === 'onchain') {
    summary.onchainNotionalUsd = summary.onchainNotionalUsd.plus(notional)
  } else {
    summary.offchainNotionalUsd = summary.offchainNotionalUsd.plus(notional)
  }
}

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
  summary.realizedPnlUsd = summary.realizedPnlUsd.plus(value)
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
    executedAt,
    venue: 'onchain'
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
    executedAt,
    venue: 'offchain'
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

const openResidualLot = (book: SymbolBook, fill: Fill, remaining: Decimal): void => {
  const side: LotSide = fill.direction === 'Buy' ? 'long' : 'short'
  const lot: Lot = {
    tradeId: fill.id,
    side,
    remainingShares: remaining,
    price: fill.price,
    openedAt: fill.executedAt,
    openedRowid: fill.rowid,
    openedVenue: fill.venue
  }

  if (side === 'long') {
    book.longLots.push(lot)
  } else {
    book.shortLots.push(lot)
  }
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

  openResidualLot(book, fill, remaining)
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
  if (book.seenOffchainFillIds.has(fill.id)) {
    warnings.push(
      `PnL audit error: duplicate offchain fill ${fill.id} for ${fill.symbol} was skipped`
    )
    return
  }

  book.seenOffchainFillIds.add(fill.id)
  book.summary.offchainFillCount += 1
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
    openResidualLot(book, fill, remaining)
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
      frontLot.openedVenue === 'offchain'
        ? 'directional_exposure'
        : bucket === 'counter_trade' && elapsedSeconds > COUNTER_TRADE_THRESHOLD_SECONDS
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
    addVenueNotional(summary, frontLot.openedVenue, openingNotional)
    addVenueNotional(summary, fill.venue, closingNotional)
    summary.matchedLotCount += 1

    if (frontLot.openedVenue === 'onchain') {
      const matched = matchedOnchainShares.get(frontLot.tradeId) ?? ZERO
      matchedOnchainShares.set(frontLot.tradeId, matched.plus(matchedShares))
    }

    const openingDirection = lotSideToOnchainDirection(frontLot.side)
    const closingDirection = directionLabel(fill.direction)
    const openingPriceText = fmtDecimal(frontLot.price)
    const closingPriceText = fmtDecimal(fill.price)
    const onchainDirection =
      frontLot.openedVenue === 'onchain'
        ? openingDirection
        : fill.venue === 'onchain'
          ? closingDirection
          : ''
    const offchainDirection =
      frontLot.openedVenue === 'offchain'
        ? openingDirection
        : fill.venue === 'offchain'
          ? closingDirection
          : ''
    const onchainTradeId =
      frontLot.openedVenue === 'onchain'
        ? frontLot.tradeId
        : fill.venue === 'onchain'
          ? fill.id
          : ''
    const offchainOrderId =
      frontLot.openedVenue === 'offchain'
        ? frontLot.tradeId
        : fill.venue === 'offchain'
          ? fill.id
          : ''
    const onchainPriceText =
      frontLot.openedVenue === 'onchain'
        ? openingPriceText
        : fill.venue === 'onchain'
          ? closingPriceText
          : ''
    const offchainPriceText =
      frontLot.openedVenue === 'offchain'
        ? openingPriceText
        : fill.venue === 'offchain'
          ? closingPriceText
          : ''

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
      openingVenue: frontLot.openedVenue,
      closingVenue: fill.venue,
      openingDirection,
      closingDirection,
      openingPriceUsd: openingPriceText,
      closingPriceUsd: closingPriceText,
      onchainTradeId,
      offchainOrderId,
      onchainDirection,
      offchainDirection,
      shares: fmtDecimal(matchedShares),
      onchainPriceUsdc: onchainPriceText,
      offchainPriceUsd: offchainPriceText,
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

const finalizeLots = (summary: SummaryAcc, lots: Lot[]): void => {
  for (const lot of lots) {
    const notional = lot.remainingShares.mul(lot.price)

    if (lot.side === 'long') {
      summary.openLongShares = summary.openLongShares.plus(lot.remainingShares)
      summary.openLongNotionalUsd = summary.openLongNotionalUsd.plus(notional)
    } else {
      summary.openShortShares = summary.openShortShares.plus(lot.remainingShares)
      summary.openShortNotionalUsd = summary.openShortNotionalUsd.plus(notional)
    }

    summary.openLotCount += 1
  }
}

const finalizeBook = (
  symbol: string,
  book: SymbolBook,
  positionNets: Map<string, Decimal>,
  warnings: string[],
  positionReplayDeltas: PositionReplayDelta[]
): void => {
  finalizeLots(book.summary, book.longLots)
  finalizeLots(book.summary, book.shortLots)

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
    grossRealizedPnlUsd: fmtDecimal(totalPnl),
    trackedCostsUsd: '0',
    trackedRevenueUsd: '0',
    netRealizedPnlUsd: fmtDecimal(totalPnl),
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
    grossRealizedPnlUsd: dto.grossRealizedPnlUsd,
    trackedCostsUsd: dto.trackedCostsUsd,
    trackedRevenueUsd: dto.trackedRevenueUsd,
    netRealizedPnlUsd: dto.netRealizedPnlUsd,
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

const withReplayExposure = (filtered: PnlSummary, replay: PnlSummary): PnlSummary => ({
  ...filtered,
  inventoryDriftShares: replay.inventoryDriftShares,
  inventoryDriftUsd: replay.inventoryDriftUsd,
  openLongShares: replay.openLongShares,
  openShortShares: replay.openShortShares,
  unmatchedOffchainShares: replay.unmatchedOffchainShares,
  unmatchedOffchainNotionalUsd: replay.unmatchedOffchainNotionalUsd,
  openLotCount: replay.openLotCount,
  unmatchedOffchainFillCount: replay.unmatchedOffchainFillCount
})

const withSymbolReplayExposure = (
  filtered: PnlSymbolSummary,
  replay: PnlSymbolSummary
): PnlSymbolSummary => ({
  ...filtered,
  inventoryDriftShares: replay.inventoryDriftShares,
  inventoryDriftUsd: replay.inventoryDriftUsd,
  openLongShares: replay.openLongShares,
  openShortShares: replay.openShortShares,
  unmatchedOffchainShares: replay.unmatchedOffchainShares,
  unmatchedOffchainFillCount: replay.unmatchedOffchainFillCount
})

const emptySymbolSummary = (symbol: string): PnlSymbolSummary =>
  symbolSummaryToDto(symbol, emptySummary())

const isNonZeroText = (value: string): boolean => {
  try {
    return !new Decimal(value).isZero()
  } catch {
    return false
  }
}

const hasReplayExposure = (summary: PnlSymbolSummary): boolean =>
  isNonZeroText(summary.inventoryDriftShares) ||
  isNonZeroText(summary.inventoryDriftUsd) ||
  isNonZeroText(summary.openLongShares) ||
  isNonZeroText(summary.openShortShares) ||
  isNonZeroText(summary.unmatchedOffchainShares) ||
  summary.unmatchedOffchainFillCount > 0

const mergeSymbolReplayExposure = (
  filteredSymbols: PnlSymbolSummary[],
  replaySymbols: PnlSymbolSummary[]
): PnlSymbolSummary[] => {
  const bySymbol = new Map(filteredSymbols.map((row) => [row.symbol, row]))

  for (const replay of replaySymbols) {
    const existing = bySymbol.get(replay.symbol) ?? emptySymbolSummary(replay.symbol)
    if (bySymbol.has(replay.symbol) || hasReplayExposure(replay)) {
      bySymbol.set(replay.symbol, withSymbolReplayExposure(existing, replay))
    }
  }

  return [...bySymbol.values()].sort((left, right) => left.symbol.localeCompare(right.symbol))
}

const withDirectSymbolCosts = (
  symbols: PnlSymbolSummary[],
  costEntries: PnlCostEntry[]
): PnlSymbolSummary[] => {
  const costsBySymbol = new Map<string, Decimal>()
  const revenueBySymbol = new Map<string, Decimal>()
  for (const entry of costEntries) {
    if (entry.symbol === null) continue
    const amount = new Decimal(entry.amountUsd)
    if (entry.effect === 'revenue') {
      const current = revenueBySymbol.get(entry.symbol) ?? ZERO
      revenueBySymbol.set(entry.symbol, current.plus(amount))
    } else if (entry.effect === 'cost') {
      const current = costsBySymbol.get(entry.symbol) ?? ZERO
      costsBySymbol.set(entry.symbol, current.plus(amount))
    }
  }

  if (costsBySymbol.size === 0 && revenueBySymbol.size === 0) return symbols

  const bySymbol = new Map(symbols.map((row) => [row.symbol, row]))
  const affectedSymbols = new Set([...costsBySymbol.keys(), ...revenueBySymbol.keys()])
  for (const symbol of affectedSymbols) {
    const existing = bySymbol.get(symbol) ?? emptySymbolSummary(symbol)
    const gross = new Decimal(existing.totalPnlUsd)
    const cost = costsBySymbol.get(symbol) ?? ZERO
    const revenue = revenueBySymbol.get(symbol) ?? ZERO
    bySymbol.set(symbol, {
      ...existing,
      grossRealizedPnlUsd: fmtDecimal(gross),
      trackedCostsUsd: fmtDecimal(cost),
      trackedRevenueUsd: fmtDecimal(revenue),
      netRealizedPnlUsd: fmtDecimal(gross.minus(cost).plus(revenue))
    })
  }

  return [...bySymbol.values()].sort((left, right) => left.symbol.localeCompare(right.symbol))
}

const resetSymbolCosts = (symbols: PnlSymbolSummary[]): PnlSymbolSummary[] =>
  symbols.map((row) => ({
    ...row,
    grossRealizedPnlUsd: row.totalPnlUsd,
    trackedCostsUsd: '0',
    trackedRevenueUsd: '0',
    netRealizedPnlUsd: row.totalPnlUsd
  }))

export const mergePnlReportCostEntries = (
  report: PnlResponse,
  additionalCostEntries: PnlCostEntry[],
  query: SqlPnlQuery
): PnlResponse => {
  if (additionalCostEntries.length === 0) return report

  const filteredAdditionalCostEntries = additionalCostEntries
    .filter((entry) => matchesCostSymbolFilter(entry, query.symbols))
    .filter((entry) => matchesCostDateFilter(entry, query))

  if (filteredAdditionalCostEntries.length === 0) return report

  const costEntries = [...report.costEntries, ...filteredAdditionalCostEntries].sort((left, right) =>
    right.occurredAt.localeCompare(left.occurredAt)
  )
  const costSummary = summarizeCostEntries(costEntries, report.costs.missingCostObservationCount)

  return {
    ...report,
    warnings: [
      ...report.warnings,
      `Cost coverage note: ${String(filteredAdditionalCostEntries.length)} Alpaca account activity rows were fetched from the broker API and included as explicit cost/revenue ledger entries.`
    ],
    summary: withCosts(report.summary, costSummary),
    costs: costSummary,
    symbols: withDirectSymbolCosts(resetSymbolCosts(report.symbols), costEntries),
    costEntries
  }
}

const entryBucketToStream = (bucket: string): PnlStreamKey | null => {
  if (bucket === 'counter_trade') return 'counterTradePnlUsd'
  if (bucket === 'onchain_netting') return 'onchainNettingPnlUsd'
  if (bucket === 'directional_exposure') return 'directionalImbalanceExcessPnlUsd'
  return null
}

const entryVenue = (value: string): Venue | null => {
  if (value === 'onchain' || value === 'offchain') return value
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
    const openingVenue = entryVenue(entry.openingVenue)
    const closingVenue = entryVenue(entry.closingVenue)
    if (openingVenue !== null) addVenueNotional(summary, openingVenue, openingNotional)
    if (closingVenue !== null) addVenueNotional(summary, closingVenue, closingNotional)
    summary.matchedLotCount += 1

    if (entry.pnlBucket === 'counter_trade') {
      summary.counterTradePnlUsd = summary.counterTradePnlUsd.plus(pnl)
      summary.realizedPnlUsd = summary.realizedPnlUsd.plus(pnl)
    } else if (entry.pnlBucket === 'onchain_netting') {
      summary.onchainNettingPnlUsd = summary.onchainNettingPnlUsd.plus(pnl)
      summary.realizedPnlUsd = summary.realizedPnlUsd.plus(pnl)
    } else if (entry.pnlBucket === 'directional_exposure') {
      summary.directionalImbalanceExcessPnlUsd = summary.directionalImbalanceExcessPnlUsd.plus(pnl)
      summary.directionalExposurePnlUsd = summary.directionalExposurePnlUsd.plus(pnl)
      summary.realizedPnlUsd = summary.realizedPnlUsd.plus(pnl)
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

const buildWindows = (entries: PnlEntry[], symbols: string[]): PnlWindow[] => {
  const byDate = new Map<string, PnlEntry[]>()

  for (const entry of entries) {
    const key = dateKey(entry.closedAt)
    const dayEntries = byDate.get(key)
    if (dayEntries === undefined) {
      byDate.set(key, [entry])
    } else {
      dayEntries.push(entry)
    }
  }

  return [...byDate.entries()]
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([date, dayEntries]) => {
      const marketSessions = new Set(dayEntries.map((entry) => marketSessionForIso(entry.closedAt)))
      const counterTradingSessions = new Set(
        dayEntries.map((entry) => counterTradingSessionForIso(entry.closedAt))
      )
      const marketSession: PnlWindowMarketSession =
        marketSessions.size === 1 ? [...marketSessions][0] : 'mixed'
      const counterTradingSession: PnlWindowCounterTradingSession =
        counterTradingSessions.size === 1 ? [...counterTradingSessions][0] : 'mixed'
      const rows: PnlWindowSymbol[] = symbols.map((symbol) => {
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
        isWeekend: isNewYorkWeekendIso(`${date}T00:00:00.000Z`),
        marketSession,
        counterTradingSession,
        granularity: 'day',
        symbols: rows
      }
    })
}

const parseTokenizedEquityMintCostEvent = (
  row: SqlCostEventRow,
  payload: Record<string, unknown>,
  symbolsByMintAggregate: Map<string, string>,
  warnings: string[]
): { entry: PnlCostEntry | null; missingCostObservation: boolean } => {
  if (row.event_type === 'TokenizedEquityMintEvent::MintRequested') {
    const requested = nestedRecord(payload, 'MintRequested')
    const symbol = requested === null ? null : textField(requested, 'symbol')
    if (symbol !== null && isSafeSqlSymbol(symbol)) {
      symbolsByMintAggregate.set(row.aggregate_id, symbol)
    } else if (symbol !== null) {
      warnings.push(`Skipped unsafe tokenization cost symbol in SQL PnL response: ${symbol}`)
    }

    return { entry: null, missingCostObservation: false }
  }

  const terminalKey =
    row.event_type === 'TokenizedEquityMintEvent::TokensReceived'
      ? 'TokensReceived'
      : row.event_type === 'TokenizedEquityMintEvent::ProviderCompletionRecovered'
        ? 'ProviderCompletionRecovered'
        : null

  if (terminalKey === null) return { entry: null, missingCostObservation: false }

  const terminal = nestedRecord(payload, terminalKey)
  if (terminal === null) {
    warnings.push(`Skipped malformed tokenization cost event row ${String(row.rowid)}`)
    return { entry: null, missingCostObservation: false }
  }

  const occurredAt =
    terminalKey === 'TokensReceived'
      ? textField(terminal, 'received_at')
      : textField(terminal, 'recovered_at')
  const fees = decimalField(terminal, 'fees')
  if (occurredAt === null) {
    warnings.push(`Skipped tokenization fee row ${String(row.rowid)} without timestamp`)
    return { entry: null, missingCostObservation: false }
  }

  if (fees === null) {
    return { entry: null, missingCostObservation: true }
  }

  if (fees.isZero()) return { entry: null, missingCostObservation: false }

  return {
    entry: costEntry(
      row,
      'tokenization_fee',
      'generic',
      'cost',
      fees,
      occurredAt,
      'Alpaca tokenization fee reported by tokenization provider',
      symbolsByMintAggregate.get(row.aggregate_id) ?? null
    ),
    missingCostObservation: false
  }
}

const parseUsdcRebalanceCostEvent = (
  row: SqlCostEventRow,
  payload: Record<string, unknown>,
  warnings: string[]
): PnlCostEntry | null => {
  const bridgeKey =
    row.event_type === 'UsdcRebalanceEvent::Bridged'
      ? 'Bridged'
      : row.event_type === 'UsdcRebalanceEvent::BridgingCompletionRecovered'
        ? 'BridgingCompletionRecovered'
        : null

  if (bridgeKey === null) return null

  const bridged = nestedRecord(payload, bridgeKey)
  const feeCollected = bridged === null ? null : decimalField(bridged, 'fee_collected')
  const occurredAt =
    bridged === null
      ? null
      : bridgeKey === 'Bridged'
        ? textField(bridged, 'minted_at')
        : textField(bridged, 'recovered_at')
  if (feeCollected === null || occurredAt === null) {
    warnings.push(`Skipped malformed CCTP bridge fee row ${String(row.rowid)}`)
    return null
  }

  if (feeCollected.isZero()) return null

  return costEntry(
    row,
    'cctp_fee',
    'generic',
    'cost',
    feeCollected,
    occurredAt,
    'CCTP fee_collected from bridge mint'
  )
}

const buildCostEntries = (
  rows: SqlCostEventRow[],
  warnings: string[]
): { entries: PnlCostEntry[]; missingCostObservationCount: number } => {
  const entries: PnlCostEntry[] = []
  const symbolsByMintAggregate = new Map<string, string>()
  let missingCostObservationCount = 0

  for (const row of [...rows].sort((left, right) => left.rowid - right.rowid)) {
    const payload = parsePayload(row.payload)
    if (payload === null) {
      warnings.push(`Skipped malformed cost event row ${String(row.rowid)}: invalid payload`)
      continue
    }

    if (row.aggregate_type === 'TokenizedEquityMint') {
      const result = parseTokenizedEquityMintCostEvent(
        row,
        payload,
        symbolsByMintAggregate,
        warnings
      )
      if (result.missingCostObservation) missingCostObservationCount += 1
      if (result.entry !== null) entries.push(result.entry)
      continue
    }

    if (row.aggregate_type === 'UsdcRebalance') {
      const entry = parseUsdcRebalanceCostEvent(row, payload, warnings)
      if (entry !== null) entries.push(entry)
      continue
    }

    warnings.push(
      `Skipped unsupported cost event row ${String(row.rowid)} with aggregate_type ${row.aggregate_type}`
    )
  }

  if (missingCostObservationCount > 0) {
    warnings.push(
      `Cost coverage note: ${String(missingCostObservationCount)} tokenization completion events did not report a fee value; those observations are treated as zero because no fee amount is persisted.`
    )
  }

  return { entries, missingCostObservationCount }
}

const parsePositionView = (
  rows: SqlPositionViewRow[],
  warnings: string[]
): { positionNets: Map<string, Decimal>; symbols: string[] } => {
  const positionNets = new Map<string, Decimal>()
  const symbols = new Set<string>()

  for (const row of rows) {
    if (!isSafeSqlSymbol(row.symbol)) {
      warnings.push(`Skipped unsafe position_view symbol in SQL PnL response: ${row.symbol}`)
      continue
    }

    symbols.add(row.symbol)

    if (row.net_position !== null) {
      try {
        positionNets.set(row.symbol, new Decimal(row.net_position))
      } catch {
        warnings.push(`Skipped malformed position net for ${row.symbol}: ${row.net_position}`)
      }
    }
  }

  return { positionNets, symbols: [...symbols].sort() }
}

type SampleStatsAcc = {
  firstAt: string | null
  lastAt: string | null
  onchainFillCount: number
  offchainFillCount: number
}

const addSampleFill = (sample: SampleStatsAcc, eventType: string, timestamp: string): void => {
  if (eventType === 'PositionEvent::OnChainOrderFilled') {
    sample.onchainFillCount += 1
  } else if (eventType === 'PositionEvent::OffChainOrderFilled') {
    sample.offchainFillCount += 1
  }

  if (sample.firstAt === null || timestamp < sample.firstAt) sample.firstAt = timestamp
  if (sample.lastAt === null || timestamp > sample.lastAt) sample.lastAt = timestamp
}

const buildSampleStats = (
  rows: SqlPositionEventRow[],
  query: SqlPnlQuery,
  warnings: string[]
): PnlSampleStats => {
  const bySymbol = new Map<string, SampleStatsAcc>()
  for (const row of rows) {
    if (
      row.event_type !== 'PositionEvent::OnChainOrderFilled' &&
      row.event_type !== 'PositionEvent::OffChainOrderFilled'
    ) {
      continue
    }

    if (!isSafeSqlSymbol(row.symbol)) {
      warnings.push(`Skipped unsafe sample stats symbol in SQL PnL response: ${row.symbol}`)
      continue
    }

    const timestamp = positionEventReplayTimestamp(row)
    if (timestamp === null) {
      warnings.push(
        `Skipped sample stats row ${String(row.rowid)} for ${row.symbol}: missing fill timestamp`
      )
      continue
    }
    if (!matchesTradeFilters(timestamp, query)) continue

    const sample =
      bySymbol.get(row.symbol) ??
      ({
        firstAt: null,
        lastAt: null,
        onchainFillCount: 0,
        offchainFillCount: 0
      } satisfies SampleStatsAcc)
    addSampleFill(sample, row.event_type, timestamp)
    bySymbol.set(row.symbol, sample)
  }

  const symbols = [...bySymbol.entries()]
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([symbol, row]) => {
      const totalFillCount = row.onchainFillCount + row.offchainFillCount
      return {
        symbol,
        firstAt: row.firstAt,
        lastAt: row.lastAt,
        onchainFillCount: row.onchainFillCount,
        offchainFillCount: row.offchainFillCount,
        totalFillCount
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
  query: SqlPnlQuery,
  costRows: SqlCostEventRow[] = []
): PnlResponse => {
  const warnings = [ATTRIBUTION_WARNING, BASELINE_WARNING, COST_WARNING]
  appendInvalidQuerySymbols(warnings, query.symbols)
  const {
    positionNets,
    symbols: positionSymbols
  } = parsePositionView(positionRows, warnings)
  const sampleStats = buildSampleStats(eventRows, query, warnings)
  const books = new Map<string, SymbolBook>()
  const entries: PnlEntry[] = []
  const unmatchedOffchainAllocations: UnmatchedOffchainAllocation[] = []
  const positionReplayDeltas: PositionReplayDelta[] = []

  for (const row of orderedPositionEvents(eventRows, warnings)) {
    if (!isSafeSqlSymbol(row.symbol)) {
      warnings.push(`Skipped unsafe position event symbol in SQL PnL response: ${row.symbol}`)
      continue
    }

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
  const replaySymbols: PnlSymbolSummary[] = []
  for (const [symbol, book] of books) {
    finalizeBook(symbol, book, positionNets, warnings, positionReplayDeltas)
    addSummary(fullTotal, book.summary)
    replaySymbols.push(symbolSummaryToDto(symbol, book.summary))
  }
  appendReplayDiagnostics(warnings, unmatchedOffchainAllocations, positionReplayDeltas)

  const filteredEntries = entries
    .filter((entry) => matchesDateFilter(entry, query))
    .sort((left, right) => right.matchedAt.localeCompare(left.matchedAt))
  const total = filteredEntries.length
  const start = Math.min(query.offset, total)
  const end = Math.min(start + query.limit, total)
  const pageEntries = filteredEntries.slice(start, end)
  const costReplay = buildCostEntries(costRows, warnings)
  const filteredCostEntries = costReplay.entries
    .filter((entry) => matchesCostSymbolFilter(entry, query.symbols))
    .filter((entry) => matchesCostDateFilter(entry, query))
    .sort((left, right) => right.occurredAt.localeCompare(left.occurredAt))
  const costSummary = summarizeCostEntries(
    filteredCostEntries,
    costReplay.missingCostObservationCount
  )
  const filtered = summaryFromEntries(filteredEntries)
  const replaySummary = summaryToDto(fullTotal)
  const summary = withCosts(withReplayExposure(filtered.summary, replaySummary), costSummary)
  const symbols = withDirectSymbolCosts(
    mergeSymbolReplayExposure(filtered.symbols, replaySymbols),
    filteredCostEntries
  )
  const symbolUniverse = [
    ...new Set([...positionSymbols, ...books.keys(), ...symbols.map((row) => row.symbol)])
  ].sort()

  return {
    attributionMethod: ATTRIBUTION_METHOD,
    warnings,
    sampleStats,
    summary,
    costs: costSummary,
    symbols,
    symbolUniverse,
    entries: pageEntries,
    costEntries: filteredCostEntries,
    total,
    hasMore: end < total,
    windows: buildWindows(filteredEntries, symbolUniverse)
  }
}

export const fetchPnlReportFromSql = async (
  baseUrl: string,
  query: SqlPnlQuery
): Promise<PnlResponse> => {
  const [eventRows, positionRows, costRows] = await Promise.all([
    fetchSqlRows<SqlPositionEventRow>(baseUrl, positionEventsSql(query.symbols)),
    fetchSqlRows<SqlPositionViewRow>(baseUrl, positionViewSql(new Set())),
    fetchSqlRows<SqlCostEventRow>(baseUrl, costEventsSql())
  ])

  return buildPnlResponseFromSqlRows(eventRows, positionRows, query, costRows)
}
