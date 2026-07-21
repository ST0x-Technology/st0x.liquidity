import type { Trade } from '$lib/api/Trade'
import type { TradeOutcome } from '$lib/api/TradeOutcome'
import type { TradingVenue } from '$lib/api/TradingVenue'
import type { LegacyTrade } from '$lib/api/LegacyTrade'
import { matcher } from '$lib/fp'

const matchOutcome = matcher<TradeOutcome>()('status')

export const normalizeTrade = (trade: Trade | LegacyTrade): Trade => {
  if ('occurredAt' in trade) return trade

  return {
    id: trade.id,
    occurredAt: trade.filledAt,
    venue: trade.venue,
    direction: trade.direction,
    symbol: trade.symbol,
    shares: trade.shares,
    outcome: { status: 'filled' }
  }
}

export const tradeOutcomeLabel = (outcome: TradeOutcome): string =>
  matchOutcome(outcome, {
    filled: () => 'Filled',
    failed: () => 'Failed',
    cancelled: () => 'Cancelled'
  })

export const tradeOutcomeClass = (outcome: TradeOutcome): string =>
  matchOutcome(outcome, {
    filled: () => 'text-green-500',
    failed: () => 'text-destructive',
    cancelled: () => 'text-amber-500'
  })

export const tradeFailureReason = (outcome: TradeOutcome): string | null =>
  matchOutcome(outcome, {
    filled: () => null,
    failed: ({ error }) => error,
    cancelled: () => null
  })

export const tradeOutcomeShares = (
  outcome: TradeOutcome
): {
  accepted: string | null
  filled: string | null
  remaining: string | null
  excess: string | null
} | null =>
  matchOutcome(outcome, {
    filled: () => null,
    failed: ({ acceptedShares, filledShares, remainingShares, excessShares }) => ({
      accepted: acceptedShares,
      filled: filledShares,
      remaining: remainingShares,
      excess: excessShares
    }),
    cancelled: ({ acceptedShares, filledShares, remainingShares, excessShares }) => ({
      accepted: acceptedShares,
      filled: filledShares,
      remaining: remainingShares,
      excess: excessShares
    })
  })

export type TradeHistoryFilter = {
  venues: ReadonlySet<TradingVenue>
  symbols: ReadonlySet<string>
  since: string | null
  until: string | null
}

const matchesHistoryFilter = (trade: Trade, filter: TradeHistoryFilter): boolean => {
  if (!filter.venues.has(trade.venue)) return false
  if (filter.symbols.size > 0 && !filter.symbols.has(trade.symbol)) return false

  const occurredAt = Date.parse(trade.occurredAt)
  if (filter.since !== null && occurredAt < Date.parse(filter.since)) return false
  if (filter.until !== null && occurredAt > Date.parse(filter.until)) return false

  return true
}

const parseOnchainTradeId = (id: string): { txHash: string; logIndex: bigint } | null => {
  const separator = id.lastIndexOf(':')
  const logIndex = id.slice(separator + 1)
  if (separator < 0 || !/^\d+$/.test(logIndex)) return null

  return { txHash: id.slice(0, separator), logIndex: BigInt(logIndex) }
}

const compareStrings = (left: string, right: string): number =>
  left < right ? -1 : left > right ? 1 : 0

type UtcTimestampParts = {
  wholeSeconds: number
  fraction: string
}

const parseUtcTimestamp = (timestamp: string): UtcTimestampParts | null => {
  const match = /^(.*:\d{2})(?:\.(\d+))?Z$/.exec(timestamp)
  if (match === null) return null

  const [, wholeTimestamp, fraction = ''] = match
  if (wholeTimestamp === undefined) return null

  const wholeSeconds = Date.parse(`${wholeTimestamp}Z`)
  return Number.isNaN(wholeSeconds) ? null : { wholeSeconds, fraction }
}

const compareRfc3339Timestamps = (left: string, right: string): number => {
  const leftParts = parseUtcTimestamp(left)
  const rightParts = parseUtcTimestamp(right)
  if (leftParts !== null && rightParts !== null) {
    const secondsComparison = leftParts.wholeSeconds - rightParts.wholeSeconds
    if (secondsComparison !== 0) return secondsComparison

    const precision = Math.max(leftParts.fraction.length, rightParts.fraction.length)
    return compareStrings(
      leftParts.fraction.padEnd(precision, '0'),
      rightParts.fraction.padEnd(precision, '0')
    )
  }

  return Date.parse(left) - Date.parse(right)
}

const compareTradeIds = (left: Trade, right: Trade): number => {
  if (left.venue === 'raindex' && right.venue === 'raindex') {
    const leftId = parseOnchainTradeId(left.id)
    const rightId = parseOnchainTradeId(right.id)
    if (leftId !== null && rightId !== null && leftId.txHash === rightId.txHash) {
      return leftId.logIndex < rightId.logIndex ? -1 : leftId.logIndex > rightId.logIndex ? 1 : 0
    }
  }

  return compareStrings(left.id, right.id)
}

export const compareTradesNewestFirst = (left: Trade, right: Trade): number =>
  compareRfc3339Timestamps(right.occurredAt, left.occurredAt) || compareTradeIds(left, right)

export const mergeTradeHistory = (
  current: Trade[],
  live: Trade[],
  filter: TradeHistoryFilter
): Trade[] => {
  const byId = new Map(current.map((trade) => [trade.id, trade]))
  for (const trade of live) byId.set(trade.id, trade)

  return [...byId.values()]
    .filter((trade) => matchesHistoryFilter(trade, filter))
    .sort(compareTradesNewestFirst)
}
