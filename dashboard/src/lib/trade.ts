import type { Trade } from '$lib/api/Trade'
import type { TradeOutcome } from '$lib/api/TradeOutcome'
import type { TradingVenue } from '$lib/api/TradingVenue'
import { matcher } from '$lib/fp'

const matchOutcome = matcher<TradeOutcome>()('status')

export const tradeOutcomeLabel = (outcome: TradeOutcome): string =>
  matchOutcome(outcome, {
    filled: () => 'Filled',
    failed: () => 'Failed'
  })

export const tradeOutcomeClass = (outcome: TradeOutcome): string =>
  matchOutcome(outcome, {
    filled: () => 'text-green-500',
    failed: () => 'text-destructive'
  })

export const tradeFailureReason = (outcome: TradeOutcome): string | null =>
  matchOutcome(outcome, {
    filled: () => null,
    failed: ({ error }) => error
  })

export const tradeFailureShares = (
  outcome: TradeOutcome
): { filled: string; remaining: string; excess: string } | null =>
  matchOutcome(outcome, {
    filled: () => null,
    failed: ({ filledShares, remainingShares, excessShares }) => ({
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

export const mergeTradeHistory = (
  current: Trade[],
  live: Trade[],
  filter: TradeHistoryFilter
): Trade[] => {
  const byId = new Map(current.map((trade) => [trade.id, trade]))
  for (const trade of live) byId.set(trade.id, trade)

  return [...byId.values()]
    .filter((trade) => matchesHistoryFilter(trade, filter))
    .sort(
      (left, right) =>
        Date.parse(right.occurredAt) - Date.parse(left.occurredAt) || compareTradeIds(left, right)
    )
}
