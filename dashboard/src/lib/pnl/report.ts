export type PnlSummary = {
  counterTradePnlUsd: string
  onchainNettingPnlUsd: string
  directionalInventoryBaselinePnlUsd: string
  directionalImbalanceExcessPnlUsd: string
  directionalExposurePnlUsd: string
  totalPnlUsd: string
  grossRealizedPnlUsd: string
  trackedCostsUsd: string
  trackedRevenueUsd: string
  netRealizedPnlUsd: string
  realizedPnlUsd: string
  matchedShares: string
  onchainNotionalUsd: string
  offchainNotionalUsd: string
  inventoryDriftShares: string
  inventoryDriftUsd: string
  openLongShares: string
  openShortShares: string
  unmatchedOffchainShares: string
  unmatchedOffchainNotionalUsd: string
  onchainFillCount: number
  offchainFillCount: number
  matchedLotCount: number
  openLotCount: number
  unmatchedOffchainFillCount: number
}

export type PnlSymbolSummary = {
  symbol: string
  counterTradePnlUsd: string
  onchainNettingPnlUsd: string
  directionalInventoryBaselinePnlUsd: string
  directionalImbalanceExcessPnlUsd: string
  directionalExposurePnlUsd: string
  totalPnlUsd: string
  grossRealizedPnlUsd: string
  trackedCostsUsd: string
  trackedRevenueUsd: string
  netRealizedPnlUsd: string
  realizedPnlUsd: string
  matchedShares: string
  inventoryDriftShares: string
  inventoryDriftUsd: string
  openLongShares: string
  openShortShares: string
  unmatchedOffchainShares: string
  matchedLotCount: number
  onchainFillCount: number
  offchainFillCount: number
  unmatchedOffchainFillCount: number
}

export type PnlEntry = {
  symbol: string
  pnlBucket: string
  matchedAt: string
  openedAt: string
  closedAt: string
  openingFillId: string
  closingFillId: string
  openingRowid: number
  closingRowid: number
  openingVenue: string
  closingVenue: string
  openingDirection: string
  closingDirection: string
  openingPriceUsd: string
  closingPriceUsd: string
  onchainTradeId: string
  offchainOrderId: string
  onchainDirection: string
  offchainDirection: string
  shares: string
  onchainPriceUsdc: string
  offchainPriceUsd: string
  spreadUsd: string
  realizedPnlUsd: string
  elapsedSeconds: number
  counterTradeThresholdSeconds: number
  delayedCounterTrade: boolean
  attributionMethod: string
}

export type PnlSampleSymbolStats = {
  symbol: string
  firstAt: string | null
  lastAt: string | null
  onchainFillCount: number
  offchainFillCount: number
  totalFillCount: number
}

export type PnlSampleStats = {
  firstAt: string | null
  lastAt: string | null
  symbolCount: number
  onchainFillCount: number
  offchainFillCount: number
  totalFillCount: number
  symbols: PnlSampleSymbolStats[]
}

export type PnlCostCategory =
  | 'offchain_execution_fee'
  | 'tokenization_fee'
  | 'cctp_fee'
  | 'conversion_slippage'
  | 'oracle_write'
  | 'broker_fee'
  | 'regulatory_fee'
  | 'margin_interest'
  | 'bot_gas'
  | 'wallet_transfer_fee'
  | 'dividend_income'
  | 'unclassified'

export type PnlAccountingBucket =
  | 'counter_trade'
  | 'onchain_netting'
  | 'directional_exposure'
  | 'generic'
  | 'dividend_revenue'

export type PnlAccountingEffect = 'cost' | 'revenue' | 'none'

export type PnlCostEntry = {
  category: PnlCostCategory
  accountingBucket: PnlAccountingBucket
  effect: PnlAccountingEffect
  amountUsd: string
  occurredAt: string
  aggregateType: string
  aggregateId: string
  eventRowid: number
  symbol: string | null
  detail: string
}

export type PnlCostCoverage = {
  source: string
  accountingBucket: PnlAccountingBucket
  effect: PnlAccountingEffect
  status: 'included' | 'zero' | 'not_ingested'
  amountUsd: string
  note: string
}

export type PnlCostSummary = {
  totalTrackedCostsUsd: string
  totalTrackedRevenueUsd: string
  counterTradeCostsUsd: string
  onchainNettingCostsUsd: string
  directionalExposureCostsUsd: string
  genericCostsUsd: string
  dividendRevenueUsd: string
  offchainExecutionFeesUsd: string
  tokenizationFeesUsd: string
  cctpFeesUsd: string
  conversionSlippageUsd: string
  oracleWriteCostUsd: string
  brokerFeesUsd: string
  regulatoryFeesUsd: string
  marginInterestUsd: string
  botGasUsd: string
  walletTransferFeesUsd: string
  unclassifiedCostsUsd: string
  costEntryCount: number
  missingCostObservationCount: number
  coverage: PnlCostCoverage[]
}

export type PnlStreamKey =
  | 'counterTradePnlUsd'
  | 'onchainNettingPnlUsd'
  | 'directionalInventoryBaselinePnlUsd'
  | 'directionalImbalanceExcessPnlUsd'

export const STREAM_KEYS: PnlStreamKey[] = [
  'counterTradePnlUsd',
  'onchainNettingPnlUsd',
  'directionalInventoryBaselinePnlUsd',
  'directionalImbalanceExcessPnlUsd'
]

export type PnlMarketSessionFilter = 'all' | 'pre' | 'rth' | 'post' | 'overnight' | 'weekend'

export type PnlCounterTradingFilter =
  | 'all'
  | 'counter_trading_active'
  | 'counter_trading_inactive'

export type PnlWindowMarketSession = Exclude<PnlMarketSessionFilter, 'all'> | 'mixed'

export type PnlWindowCounterTradingSession =
  | 'counter_trading_active'
  | 'counter_trading_inactive'
  | 'mixed'

export type PnlWindowSymbol = {
  symbol: string
  counterTradePnlUsd: string
  onchainNettingPnlUsd: string
  directionalInventoryBaselinePnlUsd: string
  directionalImbalanceExcessPnlUsd: string
  directionalExposurePnlUsd: string
  totalPnlUsd: string
}

export type PnlWindow = {
  windowId: string
  startAt: string
  endAt: string
  label: string
  isWeekend: boolean
  marketSession: PnlWindowMarketSession
  counterTradingSession: PnlWindowCounterTradingSession
  granularity: 'day'
  symbols: PnlWindowSymbol[]
}

export type PnlResponse = {
  attributionMethod: string
  warnings: string[]
  sampleStats?: PnlSampleStats
  summary: PnlSummary
  costs: PnlCostSummary
  symbols: PnlSymbolSummary[]
  symbolUniverse?: string[]
  entries: PnlEntry[]
  costEntries: PnlCostEntry[]
  total: number
  hasMore: boolean
  windows?: PnlWindow[]
}
