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
  windows?: SyntheticPnlWindow[]
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

export type SyntheticPnlWindowSymbol = {
  symbol: string
  counterTradePnlUsd: string
  onchainNettingPnlUsd: string
  directionalInventoryBaselinePnlUsd: string
  directionalImbalanceExcessPnlUsd: string
  directionalExposurePnlUsd: string
  totalPnlUsd: string
}

export type SyntheticPnlWindow = {
  windowId: string
  startAt: string
  endAt: string
  label: string
  isWeekend: boolean
  granularity: 'day'
  symbols: SyntheticPnlWindowSymbol[]
}

export type SyntheticPnlDashboard = {
  report: PnlResponse
  windows: SyntheticPnlWindow[]
}

export type SyntheticPnlFill = {
  rowid: number
  fillId: string
  symbol: string
  venue: 'onchain' | 'offchain'
  direction: 'Buy' | 'Sell'
  shares: string
  priceUsd: string
  executedAt: string
}

export type SyntheticPnlMark = {
  symbol: string
  timestamp: string
  priceUsd: string
}

export type SyntheticPnlDataset = {
  generatedAt: string
  startAt: string
  endAt: string
  symbols: string[]
  fills: SyntheticPnlFill[]
  marks: SyntheticPnlMark[]
}
