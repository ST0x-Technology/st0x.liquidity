import Decimal from 'decimal.js'
import type { Direction } from '$lib/api/Direction'
import type { LegacyTrade } from '$lib/api/LegacyTrade'
import type { Trade } from '$lib/api/Trade'
import type { TradeOutcome } from '$lib/api/TradeOutcome'
import type { TradingVenue } from '$lib/api/TradingVenue'
import { tryCatch } from '$lib/fp'

type JsonRecord = Record<string, unknown>
type CommonTradeFields = Pick<Trade, 'id' | 'venue' | 'direction' | 'symbol' | 'shares'>

const DECIMAL_PATTERN = /^[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?$/

export type TradeResponse = {
  entries: Array<Trade | LegacyTrade>
  total: number
  hasMore: boolean
}

export class TradePayloadError extends Error {
  constructor(path: string, expected: string) {
    super(`Invalid trade payload at ${path}: expected ${expected}`)
    this.name = 'TradePayloadError'
  }
}

const fieldPath = (path: string, field: string): string =>
  path === '' ? field : `${path}.${field}`

const invalid = (path: string, expected: string): never => {
  throw new TradePayloadError(path, expected)
}

const parseRecord = (value: unknown, path: string): JsonRecord => {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) {
    return invalid(path, 'an object')
  }

  return value as JsonRecord
}

const parseString = (value: unknown, path: string): string => {
  if (typeof value !== 'string') return invalid(path, 'a string')
  return value
}

const parseNonEmptyString = (value: unknown, path: string): string => {
  const parsed = parseString(value, path)
  return parsed.length === 0 ? invalid(path, 'a non-empty string') : parsed
}

const parseSymbol = (value: unknown, path: string): string => {
  const parsed = parseString(value, path)
  return parsed.length > 0 && parsed.trim() === parsed
    ? parsed
    : invalid(path, 'a non-empty trimmed symbol')
}

const parseDecimal = (
  value: unknown,
  path: string,
  minimum: 'positive' | 'non-negative'
): string => {
  const parsed = parseString(value, path)
  if (!DECIMAL_PATTERN.test(parsed)) return invalid(path, `a ${minimum} decimal string`)

  const decimal = tryCatch(() => new Decimal(parsed))
  if (decimal.tag === 'err') return invalid(path, `a ${minimum} decimal string`)

  const valid =
    decimal.value.isFinite() &&
    (minimum === 'positive' ? decimal.value.greaterThan(0) : decimal.value.greaterThanOrEqualTo(0))
  if (valid) return parsed

  return invalid(path, `a ${minimum} decimal string`)
}

const parseNullableDecimal = (
  value: unknown,
  path: string,
  minimum: 'positive' | 'non-negative'
): string | null => (value === null ? null : parseDecimal(value, path, minimum))

const daysInMonth = (year: number, month: number): number => {
  if (month === 2) {
    const leapYear = year % 4 === 0 && (year % 100 !== 0 || year % 400 === 0)
    return leapYear ? 29 : 28
  }

  return [4, 6, 9, 11].includes(month) ? 30 : 31
}

const parseTimestamp = (value: unknown, path: string): string => {
  const parsed = parseString(value, path)
  const match = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.\d{1,9})?Z$/.exec(parsed)
  if (match === null) return invalid(path, 'a UTC RFC 3339 timestamp')

  const [, yearText, monthText, dayText, hourText, minuteText, secondText] = match
  const year = Number(yearText)
  const month = Number(monthText)
  const day = Number(dayText)
  const hour = Number(hourText)
  const minute = Number(minuteText)
  const second = Number(secondText)
  const valid =
    month >= 1 &&
    month <= 12 &&
    day >= 1 &&
    day <= daysInMonth(year, month) &&
    hour <= 23 &&
    minute <= 59 &&
    second <= 59

  return valid ? parsed : invalid(path, 'a UTC RFC 3339 timestamp')
}

const parseVenue = (value: unknown, path: string): TradingVenue => {
  if (value === 'raindex' || value === 'alpaca' || value === 'dry_run') return value
  return invalid(path, 'a known trading venue')
}

const parseDirection = (value: unknown, path: string): Direction => {
  if (value === 'buy' || value === 'sell') return value
  return invalid(path, 'a known trade direction')
}

type OutcomeQuantities = {
  acceptedShares: string | null
  filledShares: string | null
  remainingShares: string | null
  excessShares: string | null
}

const parseOutcomeQuantities = (
  outcome: Record<string, unknown>,
  path: string,
  allowLegacyFailure: boolean
): OutcomeQuantities => {
  const hasAcceptedShares = 'acceptedShares' in outcome
  if (!hasAcceptedShares && !allowLegacyFailure) {
    return invalid(fieldPath(path, 'acceptedShares'), 'an explicit nullable quantity')
  }
  const acceptedShares = parseNullableDecimal(
    outcome['acceptedShares'] ?? null,
    fieldPath(path, 'acceptedShares'),
    'positive'
  )
  let filledShares = hasAcceptedShares
    ? parseNullableDecimal(outcome['filledShares'], fieldPath(path, 'filledShares'), 'non-negative')
    : parseDecimal(outcome['filledShares'], fieldPath(path, 'filledShares'), 'non-negative')
  let remainingShares = hasAcceptedShares
    ? parseNullableDecimal(
        outcome['remainingShares'],
        fieldPath(path, 'remainingShares'),
        'non-negative'
      )
    : parseDecimal(outcome['remainingShares'], fieldPath(path, 'remainingShares'), 'non-negative')
  let excessShares = hasAcceptedShares
    ? parseNullableDecimal(outcome['excessShares'], fieldPath(path, 'excessShares'), 'non-negative')
    : parseDecimal(outcome['excessShares'], fieldPath(path, 'excessShares'), 'non-negative')

  // terminal_outcomes_v1 originally omitted acceptedShares and derived these
  // quantities from the request. It also split overfills between filledShares
  // and excessShares, so reconstruct the complete observed fill before
  // discarding request-derived values that are not broker evidence.
  if (!hasAcceptedShares && allowLegacyFailure) {
    if (filledShares === null || excessShares === null) {
      return invalid(path, 'complete terminal_outcomes_v1 failure quantities')
    }
    const completeFill = new Decimal(filledShares).plus(excessShares)
    // v1 synthesized zero when no fill evidence existed, so only a positive
    // legacy total proves an actual broker fill.
    filledShares = completeFill.isZero() ? null : completeFill.toString()
    remainingShares = null
    excessShares = null
  }

  if (acceptedShares === null || filledShares === null) {
    if (remainingShares !== null) {
      return invalid(fieldPath(path, 'remainingShares'), 'null when fill provenance is incomplete')
    }
    if (excessShares !== null) {
      return invalid(fieldPath(path, 'excessShares'), 'null when fill provenance is incomplete')
    }
  } else {
    if (remainingShares === null) {
      return invalid(fieldPath(path, 'remainingShares'), 'a derived non-negative decimal string')
    }
    if (excessShares === null) {
      return invalid(fieldPath(path, 'excessShares'), 'a derived non-negative decimal string')
    }

    const accepted = new Decimal(acceptedShares)
    const filled = new Decimal(filledShares)
    const expectedRemaining = Decimal.max(accepted.minus(filled), 0)
    const expectedExcess = Decimal.max(filled.minus(accepted), 0)
    if (!new Decimal(remainingShares).equals(expectedRemaining)) {
      return invalid(fieldPath(path, 'remainingShares'), 'the accepted quantity minus the fill')
    }
    if (!new Decimal(excessShares).equals(expectedExcess)) {
      return invalid(fieldPath(path, 'excessShares'), 'the fill beyond the accepted quantity')
    }
  }

  return {
    acceptedShares,
    filledShares,
    remainingShares,
    excessShares
  }
}

const parseOutcome = (value: unknown, path: string): TradeOutcome => {
  const outcome = parseRecord(value, path)
  const statusPath = fieldPath(path, 'status')
  if (outcome['status'] === 'filled') return { status: 'filled' }
  if (outcome['status'] === 'failed') {
    return {
      status: 'failed',
      error: parseString(outcome['error'], fieldPath(path, 'error')),
      ...parseOutcomeQuantities(outcome, path, true)
    }
  }
  if (outcome['status'] === 'cancelled') {
    return {
      status: 'cancelled',
      ...parseOutcomeQuantities(outcome, path, false)
    }
  }

  return invalid(statusPath, 'a known terminal outcome')
}

const parseCommonTradeFields = (trade: JsonRecord, path: string): CommonTradeFields => ({
  id: parseNonEmptyString(trade['id'], fieldPath(path, 'id')),
  venue: parseVenue(trade['venue'], fieldPath(path, 'venue')),
  direction: parseDirection(trade['direction'], fieldPath(path, 'direction')),
  symbol: parseSymbol(trade['symbol'], fieldPath(path, 'symbol')),
  shares: parseDecimal(trade['shares'], fieldPath(path, 'shares'), 'positive')
})

export const parseCanonicalTrade = (value: unknown, path = ''): Trade => {
  const trade = parseRecord(value, path === '' ? 'trade' : path)
  return {
    ...parseCommonTradeFields(trade, path),
    occurredAt: parseTimestamp(trade['occurredAt'], fieldPath(path, 'occurredAt')),
    outcome: parseOutcome(trade['outcome'], fieldPath(path, 'outcome'))
  }
}

export const parseLegacyTrade = (value: unknown, path = ''): LegacyTrade => {
  const trade = parseRecord(value, path === '' ? 'trade' : path)
  return {
    ...parseCommonTradeFields(trade, path),
    filledAt: parseTimestamp(trade['filledAt'], fieldPath(path, 'filledAt'))
  }
}

export const parseTrade = (value: unknown, path = ''): Trade | LegacyTrade => {
  const trade = parseRecord(value, path === '' ? 'trade' : path)
  return 'occurredAt' in trade || 'outcome' in trade
    ? parseCanonicalTrade(trade, path)
    : parseLegacyTrade(trade, path)
}

export const parseTradeEntries = (value: unknown, path = 'trades'): Array<Trade | LegacyTrade> => {
  if (!Array.isArray(value)) return invalid(path, 'an array')
  return value.map((trade, index) => parseTrade(trade, `${path}[${String(index)}]`))
}

export const parseTradeResponse = (value: unknown): TradeResponse => {
  const response = parseRecord(value, 'response')
  const total = response['total']
  if (typeof total !== 'number' || !Number.isSafeInteger(total) || total < 0) {
    return invalid('total', 'a non-negative integer')
  }
  const hasMore = response['hasMore']
  if (typeof hasMore !== 'boolean') return invalid('hasMore', 'a boolean')

  return {
    entries: parseTradeEntries(response['entries'], 'entries'),
    total,
    hasMore
  }
}
