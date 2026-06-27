export type PnlSummary = {
  counterTradePnlUsd: string
  onchainNettingPnlUsd: string
  directionalInventoryBaselinePnlUsd: string
  directionalImbalanceExcessPnlUsd: string
  directionalExposurePnlUsd: string
  totalPnlUsd: string
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
  granularity: 'day'
  symbols: PnlWindowSymbol[]
}

export type PnlResponse = {
  attributionMethod: string
  warnings: string[]
  sampleStats?: PnlSampleStats
  summary: PnlSummary
  symbols: PnlSymbolSummary[]
  symbolUniverse?: string[]
  entries: PnlEntry[]
  total: number
  hasMore: boolean
  windows?: PnlWindow[]
}
