import {
  type PnlEntry,
  type PnlResponse,
  type PnlSummary,
  type PnlSymbolSummary,
  type PnlWindow,
  type PnlWindowSymbol,
} from './report'

type AssetScenario = {
  symbol: string
  basePrice: string
  dailyDriftBps: number
  volatilityBps: number
}

type SyntheticOptions = {
  startAt?: string
  days?: number
  assets?: AssetScenario[]
  onchainFillsPerAssetDay?: number
}

export type SyntheticPnlDashboard = {
  report: PnlResponse
  windows: PnlWindow[]
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

const DEFAULT_ASSETS: AssetScenario[] = [
  { symbol: 'RKLB', basePrice: '22.40', dailyDriftBps: 18, volatilityBps: 95 },
  { symbol: 'SGOV', basePrice: '100.72', dailyDriftBps: 1, volatilityBps: 6 },
  { symbol: 'CRCL', basePrice: '87.50', dailyDriftBps: -11, volatilityBps: 160 },
  { symbol: 'BMNR', basePrice: '41.80', dailyDriftBps: 24, volatilityBps: 130 },
]

const DEFAULT_START_AT = '2026-05-04T13:30:00.000Z'
const DEFAULT_DASHBOARD_START_AT = '2025-01-01T13:30:00.000Z'
const DEFAULT_DASHBOARD_DAYS = 500
const MS_PER_MINUTE = 60_000
const MS_PER_DAY = 24 * 60 * MS_PER_MINUTE
const priceText = (value: number): string => value.toFixed(4).replace(/\.?0+$/u, '')
const shareText = (value: number): string => value.toFixed(9).replace(/\.?0+$/u, '')
const moneyText = (value: number): string => value.toFixed(2).replace(/\.?0+$/u, '')

const pseudoNoise = (seed: number): number => {
  const raw = Math.sin(seed * 12.9898 + 78.233) * 43758.5453
  return raw - Math.floor(raw)
}

const signedNoise = (seed: number): number => pseudoNoise(seed) * 2 - 1

const marketPrice = (asset: AssetScenario, day: number, minute: number): number => {
  const drift = (asset.dailyDriftBps * day) / 10_000
  const wave = (Math.sin((day + 1) * 0.7 + minute / 83) * asset.volatilityBps) / 10_000
  const micro = (signedNoise((day + 1) * 997 + minute) * asset.volatilityBps) / 30_000

  return Number(asset.basePrice) * (1 + drift + wave + micro)
}

const fillPrice = (
  asset: AssetScenario,
  day: number,
  minute: number,
  venue: 'onchain' | 'offchain',
  direction: 'Buy' | 'Sell',
): number => {
  const mid = marketPrice(asset, day, minute)
  const venueEdgeBps = venue === 'onchain' ? 8 : 5
  const directionSign = direction === 'Buy' ? 1 : -1
  const noiseBps = signedNoise((day + 11) * 1231 + minute * 13 + asset.symbol.length) * 2
  return mid * (1 + (directionSign * venueEdgeBps + noiseBps) / 10_000)
}

const directionFor = (assetIndex: number, day: number, tradeIndex: number): 'Buy' | 'Sell' => {
  const pulse = (tradeIndex + day + assetIndex) % 9
  if (pulse === 0 || pulse === 1) return 'Buy'
  if (pulse === 5 || pulse === 6) return 'Sell'
  return (tradeIndex + assetIndex) % 2 === 0 ? 'Buy' : 'Sell'
}

const opposite = (direction: 'Buy' | 'Sell'): 'Buy' | 'Sell' =>
  direction === 'Buy' ? 'Sell' : 'Buy'

const shareSize = (asset: AssetScenario, day: number, tradeIndex: number): number => {
  const base = asset.symbol === 'SGOV' ? 0.035 : 0.18
  const scale = 1 + ((day + tradeIndex) % 7) / 10
  const jitter = (pseudoNoise(day * 101 + tradeIndex * 17) * base) / 2
  return base * scale + jitter
}

const pushFill = (
  fills: SyntheticPnlFill[],
  fill: Omit<SyntheticPnlFill, 'rowid'>,
): void => {
  fills.push({
    rowid: fills.length + 1,
    ...fill,
  })
}

export const generateSyntheticPnlDataset = (
  options: SyntheticOptions = {},
): SyntheticPnlDataset => {
  const startAt = new Date(options.startAt ?? DEFAULT_START_AT)
  const days = options.days ?? 10
  const assets = options.assets ?? DEFAULT_ASSETS
  const onchainFillsPerAssetDay = options.onchainFillsPerAssetDay ?? 28
  const fills: SyntheticPnlFill[] = []
  const marks: SyntheticPnlMark[] = []

  assets.forEach((asset, assetIndex) => {
    for (let day = 0; day < days; day += 1) {
      for (let markIndex = 0; markIndex <= 8; markIndex += 1) {
        const minute = markIndex * 60
        const timestamp = new Date(startAt.getTime() + day * MS_PER_DAY + minute * MS_PER_MINUTE)
        marks.push({
          symbol: asset.symbol,
          timestamp: timestamp.toISOString(),
          priceUsd: priceText(marketPrice(asset, day, minute)),
        })
      }

      for (let tradeIndex = 0; tradeIndex < onchainFillsPerAssetDay; tradeIndex += 1) {
        const minute = 7 + tradeIndex * 13 + ((day + assetIndex) % 5)
        const direction = directionFor(assetIndex, day, tradeIndex)
        const onchainShares = shareSize(asset, day, tradeIndex)
        const onchainAt = new Date(startAt.getTime() + day * MS_PER_DAY + minute * MS_PER_MINUTE)
        const onchainId = `synthetic-on-${asset.symbol}-${String(day + 1).padStart(2, '0')}-${String(tradeIndex + 1).padStart(3, '0')}`

        pushFill(fills, {
          fillId: onchainId,
          symbol: asset.symbol,
          venue: 'onchain',
          direction,
          shares: shareText(onchainShares),
          priceUsd: priceText(fillPrice(asset, day, minute, 'onchain', direction)),
          executedAt: onchainAt.toISOString(),
        })

        const passiveNetting = tradeIndex % 11 === 4
        if (passiveNetting) {
          const nettingDirection = opposite(direction)
          const nettingShares = onchainShares * (0.45 + ((day + assetIndex) % 3) / 10)
          const nettingMinute = minute + 3
          const nettingAt = new Date(startAt.getTime() + day * MS_PER_DAY + nettingMinute * MS_PER_MINUTE)
          pushFill(fills, {
            fillId: `${onchainId}-passive-net`,
            symbol: asset.symbol,
            venue: 'onchain',
            direction: nettingDirection,
            shares: shareText(nettingShares),
            priceUsd: priceText(fillPrice(asset, day, nettingMinute, 'onchain', nettingDirection)),
            executedAt: nettingAt.toISOString(),
          })
        }

        const counterTrade = tradeIndex % 4 !== 2
        if (counterTrade) {
          const hedgeDirection = opposite(direction)
          const hedgeRatio = tradeIndex % 10 === 7 ? 0.62 : 0.96
          const hedgeShares = onchainShares * hedgeRatio
          const lagMinutes = 2 + ((tradeIndex + day + assetIndex) % 9)
          const hedgeMinute = minute + lagMinutes
          const hedgeAt = new Date(startAt.getTime() + day * MS_PER_DAY + hedgeMinute * MS_PER_MINUTE)

          pushFill(fills, {
            fillId: `synthetic-off-${asset.symbol}-${String(day + 1).padStart(2, '0')}-${String(tradeIndex + 1).padStart(3, '0')}`,
            symbol: asset.symbol,
            venue: 'offchain',
            direction: hedgeDirection,
            shares: shareText(hedgeShares),
            priceUsd: priceText(fillPrice(asset, day, hedgeMinute, 'offchain', hedgeDirection)),
            executedAt: hedgeAt.toISOString(),
          })
        }
      }
    }
  })

  fills.sort((left, right) => left.executedAt.localeCompare(right.executedAt))
  fills.forEach((fill, index) => {
    fill.rowid = index + 1
  })

  const endAt = new Date(startAt.getTime() + days * MS_PER_DAY).toISOString()

  return {
    generatedAt: new Date('2026-05-12T00:00:00.000Z').toISOString(),
    startAt: startAt.toISOString(),
    endAt,
    symbols: assets.map((asset) => asset.symbol),
    fills,
    marks: marks.sort((left, right) => left.timestamp.localeCompare(right.timestamp)),
  }
}

export const syntheticPnlDataset = generateSyntheticPnlDataset()

const dailyPnl = (
  asset: AssetScenario,
  assetIndex: number,
  day: number,
  isWeekend: boolean,
): PnlWindowSymbol => {
  const capitalScale = Number(asset.basePrice) * (asset.symbol === 'SGOV' ? 0.25 : 1)
  const quality = 1 + signedNoise((assetIndex + 1) * 17 + day * 31) * 0.35
  const counterTrade = isWeekend
    ? 0
    : capitalScale * (0.52 + assetIndex * 0.08) * quality
  const onchainNetting = capitalScale * signedNoise(200 + assetIndex * 19 + day * 7) * 0.16
  const frozenNotional = capitalScale * (asset.symbol === 'SGOV' ? 7 : 9 + assetIndex * 2)
  const bucketReturn =
    asset.dailyDriftBps / 10_000
    + Math.sin((day + 1) * 0.41 + assetIndex) * asset.volatilityBps / 18_000
    + signedNoise(700 + assetIndex * 29 + day * 17) * asset.volatilityBps / 35_000
  const baseline = frozenNotional * bucketReturn
  const delayedSpike = day % 5 === 3 ? capitalScale * 0.38 : 0
  const weekendExposure = isWeekend
    ? capitalScale * (0.9 + assetIndex * 0.2) * signedNoise(900 + day * 11 + assetIndex)
    : 0
  const danglingExposure = capitalScale * signedNoise(500 + assetIndex * 23 + day * 13) * 0.12
  const excess = weekendExposure + delayedSpike + danglingExposure
  const directional = baseline + excess
  const total = counterTrade + onchainNetting + directional

  return {
    symbol: asset.symbol,
    counterTradePnlUsd: moneyText(counterTrade),
    onchainNettingPnlUsd: moneyText(onchainNetting),
    directionalInventoryBaselinePnlUsd: moneyText(baseline),
    directionalImbalanceExcessPnlUsd: moneyText(excess),
    directionalExposurePnlUsd: moneyText(directional),
    totalPnlUsd: moneyText(total),
  }
}

const emptySummary = (): PnlSummary => ({
  counterTradePnlUsd: '0',
  onchainNettingPnlUsd: '0',
  directionalInventoryBaselinePnlUsd: '0',
  directionalImbalanceExcessPnlUsd: '0',
  directionalExposurePnlUsd: '0',
  totalPnlUsd: '0',
  realizedPnlUsd: '0',
  matchedShares: '0',
  onchainNotionalUsd: '0',
  offchainNotionalUsd: '0',
  inventoryDriftShares: '0',
  inventoryDriftUsd: '0',
  openLongShares: '0',
  openShortShares: '0',
  unmatchedOffchainShares: '0',
  unmatchedOffchainNotionalUsd: '0',
  onchainFillCount: 0,
  offchainFillCount: 0,
  matchedLotCount: 0,
  openLotCount: 0,
  unmatchedOffchainFillCount: 0,
})

const addSummaryStreams = (
  target: PnlSummary,
  row: PnlWindowSymbol,
): void => {
  const counter = Number(target.counterTradePnlUsd) + Number(row.counterTradePnlUsd)
  const onchain = Number(target.onchainNettingPnlUsd) + Number(row.onchainNettingPnlUsd)
  const baseline = Number(target.directionalInventoryBaselinePnlUsd) + Number(row.directionalInventoryBaselinePnlUsd)
  const excess = Number(target.directionalImbalanceExcessPnlUsd) + Number(row.directionalImbalanceExcessPnlUsd)
  const directional = baseline + excess
  const realized = counter + onchain + excess

  target.counterTradePnlUsd = moneyText(counter)
  target.onchainNettingPnlUsd = moneyText(onchain)
  target.directionalInventoryBaselinePnlUsd = moneyText(baseline)
  target.directionalImbalanceExcessPnlUsd = moneyText(excess)
  target.directionalExposurePnlUsd = moneyText(directional)
  target.realizedPnlUsd = moneyText(realized)
  target.totalPnlUsd = moneyText(realized + baseline)
}

const addSymbolStreams = (
  target: PnlSymbolSummary,
  row: PnlWindowSymbol,
): void => {
  const counter = Number(target.counterTradePnlUsd) + Number(row.counterTradePnlUsd)
  const onchain = Number(target.onchainNettingPnlUsd) + Number(row.onchainNettingPnlUsd)
  const baseline = Number(target.directionalInventoryBaselinePnlUsd) + Number(row.directionalInventoryBaselinePnlUsd)
  const excess = Number(target.directionalImbalanceExcessPnlUsd) + Number(row.directionalImbalanceExcessPnlUsd)
  const directional = baseline + excess
  const realized = counter + onchain + excess

  target.counterTradePnlUsd = moneyText(counter)
  target.onchainNettingPnlUsd = moneyText(onchain)
  target.directionalInventoryBaselinePnlUsd = moneyText(baseline)
  target.directionalImbalanceExcessPnlUsd = moneyText(excess)
  target.directionalExposurePnlUsd = moneyText(directional)
  target.realizedPnlUsd = moneyText(realized)
  target.totalPnlUsd = moneyText(realized + baseline)
}

const symbolSummary = (symbol: string): PnlSymbolSummary => ({
  symbol,
  counterTradePnlUsd: '0',
  onchainNettingPnlUsd: '0',
  directionalInventoryBaselinePnlUsd: '0',
  directionalImbalanceExcessPnlUsd: '0',
  directionalExposurePnlUsd: '0',
  totalPnlUsd: '0',
  realizedPnlUsd: '0',
  matchedShares: '0',
  inventoryDriftShares: '0',
  inventoryDriftUsd: '0',
  openLongShares: '0',
  openShortShares: '0',
  unmatchedOffchainShares: '0',
  matchedLotCount: 0,
  onchainFillCount: 0,
  offchainFillCount: 0,
  unmatchedOffchainFillCount: 0,
})

const syntheticEntry = (
  rowid: number,
  symbol: string,
  bucket: 'counter_trade' | 'onchain_netting' | 'directional_exposure',
  dayStart: Date,
  amount: number,
  delayed: boolean,
): PnlEntry => {
  const openedAt = new Date(dayStart.getTime() + (60 + (rowid % 48) * 9) * MS_PER_MINUTE)
  const elapsedSeconds = delayed ? 540 + (rowid % 5) * 90 : 70 + (rowid % 4) * 45
  const closedAt = new Date(openedAt.getTime() + elapsedSeconds * 1_000)
  const openingDirection = rowid % 2 === 0 ? 'buy' : 'sell'
  const closingDirection = openingDirection === 'buy' ? 'sell' : 'buy'
  const openingPrice = 20 + rowid * 0.07
  const spread = amount >= 0 ? 0.18 + (rowid % 4) * 0.03 : -0.16 - (rowid % 3) * 0.02
  const shares = Math.abs(amount / spread)
  const closingPrice = openingDirection === 'buy'
    ? openingPrice + spread
    : openingPrice - spread

  return {
    symbol,
    pnlBucket: bucket,
    matchedAt: closedAt.toISOString(),
    openedAt: openedAt.toISOString(),
    closedAt: closedAt.toISOString(),
    openingFillId: `mock-on-${symbol}-${String(rowid).padStart(4, '0')}`,
    closingFillId: `mock-close-${symbol}-${String(rowid).padStart(4, '0')}`,
    openingRowid: rowid * 2 - 1,
    closingRowid: rowid * 2,
    openingVenue: 'onchain',
    closingVenue: bucket === 'onchain_netting' ? 'onchain' : 'offchain',
    openingDirection,
    closingDirection,
    openingPriceUsd: priceText(openingPrice),
    closingPriceUsd: priceText(closingPrice),
    onchainTradeId: `mock-on-${symbol}-${String(rowid).padStart(4, '0')}`,
    offchainOrderId: `mock-off-${symbol}-${String(rowid).padStart(4, '0')}`,
    onchainDirection: openingDirection,
    offchainDirection: closingDirection,
    shares: shareText(shares),
    onchainPriceUsdc: priceText(openingPrice),
    offchainPriceUsd: priceText(closingPrice),
    spreadUsd: moneyText(spread),
    realizedPnlUsd: moneyText(amount),
    elapsedSeconds,
    counterTradeThresholdSeconds: 300,
    delayedCounterTrade: delayed,
    attributionMethod: 'synthetic_window_replay',
  }
}

export const generateSyntheticPnlDashboard = (
  options: SyntheticOptions = {},
): SyntheticPnlDashboard => {
  const startAt = new Date(options.startAt ?? DEFAULT_DASHBOARD_START_AT)
  const days = options.days ?? DEFAULT_DASHBOARD_DAYS
  const assets = options.assets ?? DEFAULT_ASSETS
  const windows: PnlWindow[] = []
  const summary = emptySummary()
  const symbols = new Map<string, PnlSymbolSummary>()
  const entries: PnlEntry[] = []
  let entryRowid = 1

  const pushEntry = (
    symbol: string,
    bucket: 'counter_trade' | 'onchain_netting' | 'directional_exposure',
    dayStart: Date,
    amount: number,
    delayed: boolean,
  ) => {
    if (Math.abs(amount) < 0.005) return

    entries.push(syntheticEntry(entryRowid, symbol, bucket, dayStart, amount, delayed))
    entryRowid += 1
  }

  assets.forEach((asset) => {
    symbols.set(asset.symbol, symbolSummary(asset.symbol))
  })

  for (let day = 0; day < days; day += 1) {
    const start = new Date(startAt.getTime() + day * MS_PER_DAY)
    const end = new Date(start.getTime() + MS_PER_DAY)
    const isWeekend = [0, 6].includes(start.getUTCDay())
    const windowSymbols = assets.map((asset, assetIndex) => {
      const row = dailyPnl(asset, assetIndex, day, isWeekend)
      addSummaryStreams(summary, row)
      addSymbolStreams(symbols.get(asset.symbol) ?? symbolSummary(asset.symbol), row)
      return row
    })

    windows.push({
      windowId: `day-${String(day + 1).padStart(2, '0')}`,
      startAt: start.toISOString(),
      endAt: end.toISOString(),
      label: start.toISOString().slice(5, 10),
      isWeekend,
      granularity: 'day',
      symbols: windowSymbols,
    })

    windowSymbols.forEach((row) => {
      if (Number(row.counterTradePnlUsd) !== 0) {
        pushEntry(row.symbol, 'counter_trade', start, Number(row.counterTradePnlUsd), false)
      }
      if (Number(row.onchainNettingPnlUsd) !== 0) {
        pushEntry(row.symbol, 'onchain_netting', start, Number(row.onchainNettingPnlUsd), false)
      }
      if (Number(row.directionalImbalanceExcessPnlUsd) !== 0) {
        pushEntry(row.symbol, 'directional_exposure', start, Number(row.directionalImbalanceExcessPnlUsd), true)
      }
    })
  }

  const symbolRows = [...symbols.values()].sort((left, right) => left.symbol.localeCompare(right.symbol))
  const matchedShares = entries.reduce((total, entry) => total + Number(entry.shares), 0)
  const unmatchedOffchainShares = entries
    .filter((entry) => entry.delayedCounterTrade)
    .reduce((total, entry) => total + Number(entry.shares) * 0.08, 0)

  summary.matchedShares = shareText(matchedShares)
  summary.onchainNotionalUsd = moneyText(matchedShares * 42)
  summary.offchainNotionalUsd = moneyText(matchedShares * 41.9)
  summary.inventoryDriftShares = shareText(assets.length * 0.42)
  summary.inventoryDriftUsd = moneyText(Number(summary.directionalInventoryBaselinePnlUsd))
  summary.openLongShares = shareText(assets.length * 0.76)
  summary.openShortShares = shareText(assets.length * 0.34)
  summary.unmatchedOffchainShares = shareText(unmatchedOffchainShares)
  summary.unmatchedOffchainNotionalUsd = moneyText(unmatchedOffchainShares * 55)
  summary.onchainFillCount = days * assets.length * 28
  summary.offchainFillCount = days * assets.length * 20
  summary.matchedLotCount = entries.length
  summary.openLotCount = days * assets.length
  summary.unmatchedOffchainFillCount = entries.filter((entry) => entry.delayedCounterTrade).length

  symbolRows.forEach((row, index) => {
    row.matchedShares = shareText(matchedShares / symbolRows.length)
    row.inventoryDriftShares = shareText((index + 1) * 0.14)
    row.inventoryDriftUsd = row.directionalInventoryBaselinePnlUsd
    row.openLongShares = shareText(0.45 + index * 0.12)
    row.openShortShares = shareText(0.12 + index * 0.08)
    row.unmatchedOffchainShares = shareText(unmatchedOffchainShares / symbolRows.length)
    row.matchedLotCount = entries.filter((entry) => entry.symbol === row.symbol).length
    row.onchainFillCount = days * 28
    row.offchainFillCount = days * 20
    row.unmatchedOffchainFillCount = entries.filter((entry) => entry.symbol === row.symbol && entry.delayedCounterTrade).length
  })

  const report: PnlResponse = {
    attributionMethod: 'synthetic_window_replay',
    warnings: [
      'Synthetic PnL data: generated locally for dashboard validation, not loaded from production.',
      'Delayed broker fills > 5m are displayed as directional exposure rather than counter-trade PnL.',
    ],
    summary,
    symbols: symbolRows,
    entries: entries.sort((left, right) => right.closedAt.localeCompare(left.closedAt)),
    total: entries.length,
    hasMore: false,
  }

  return { report, windows }
}

export const syntheticPnlDashboard = generateSyntheticPnlDashboard()
