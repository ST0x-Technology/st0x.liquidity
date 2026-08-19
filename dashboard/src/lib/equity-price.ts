import Decimal from 'decimal.js'
import type { EquityPrice } from '$lib/api/EquityPrice'
import type { EquityPriceStatus } from '$lib/api/EquityPriceStatus'
import { err, ok, tryCatch, type Result } from '$lib/fp'

export class EquityPricePayloadError extends Error {
  constructor(path: string, expected: string) {
    super(`Invalid equity price payload at ${path}: expected ${expected}`)
    this.name = 'EquityPricePayloadError'
  }
}

type PriceResult<T> = Result<T, EquityPricePayloadError>

const invalid = (path: string, expected: string): PriceResult<never> =>
  err(new EquityPricePayloadError(path, expected))

const record = (value: unknown, path: string): PriceResult<Record<string, unknown>> =>
  typeof value === 'object' && value !== null && !Array.isArray(value)
    ? ok(value as Record<string, unknown>)
    : invalid(path, 'an object')

const nonEmptyString = (value: unknown, path: string): PriceResult<string> =>
  typeof value === 'string' && value.trim() !== ''
    ? ok(value)
    : invalid(path, 'a non-empty string')

const UTC_TIMESTAMP = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z$/

const timestamp = (value: unknown, path: string): PriceResult<string> => {
  const parsed = nonEmptyString(value, path)
  if (parsed.tag === 'err') return parsed
  if (!UTC_TIMESTAMP.test(parsed.value) || Number.isNaN(Date.parse(parsed.value))) {
    return invalid(path, 'an RFC 3339 UTC timestamp')
  }
  return parsed
}

const positiveDecimal = (value: unknown, path: string): PriceResult<string> => {
  const parsed = nonEmptyString(value, path)
  if (parsed.tag === 'err') return parsed

  const decimal = tryCatch(() => new Decimal(parsed.value))
  if (decimal.tag === 'err' || !decimal.value.isFinite() || !decimal.value.gt(0)) {
    return invalid(path, 'a positive finite decimal string')
  }
  return parsed
}

export const parseEquityPrice = (
  value: unknown,
  path = 'equityPrice'
): PriceResult<EquityPrice> => {
  const price = record(value, path)
  if (price.tag === 'err') return price

  const symbol = nonEmptyString(price.value['symbol'], `${path}.symbol`)
  if (symbol.tag === 'err') return symbol

  const status = record(price.value['status'], `${path}.status`)
  if (status.tag === 'err') return status

  if (status.value['status'] === 'unavailable') {
    return ok({ symbol: symbol.value, status: { status: 'unavailable' } })
  }
  if (status.value['status'] !== 'available') {
    return invalid(`${path}.status.status`, 'available or unavailable')
  }

  const observedAt = timestamp(status.value['observedAt'], `${path}.status.observedAt`)
  if (observedAt.tag === 'err') return observedAt

  const expiresAt = timestamp(status.value['expiresAt'], `${path}.status.expiresAt`)
  if (expiresAt.tag === 'err') return expiresAt
  if (Date.parse(observedAt.value) >= Date.parse(expiresAt.value)) {
    return invalid(`${path}.status.expiresAt`, 'later than observedAt')
  }

  const priceUsd = positiveDecimal(status.value['priceUsd'], `${path}.status.priceUsd`)
  if (priceUsd.tag === 'err') return priceUsd

  return ok({
    symbol: symbol.value,
    status: {
      status: 'available',
      priceUsd: priceUsd.value,
      observedAt: observedAt.value,
      expiresAt: expiresAt.value
    }
  })
}

export const parseEquityPrices = (value: unknown): PriceResult<EquityPrice[]> => {
  if (!Array.isArray(value)) return invalid('equityPrices', 'an array')

  const prices: EquityPrice[] = []
  const symbols = new Set<string>()
  for (const [index, valuePrice] of value.entries()) {
    const price = parseEquityPrice(valuePrice, `equityPrices[${String(index)}]`)
    if (price.tag === 'err') return price
    if (symbols.has(price.value.symbol)) {
      return invalid(`equityPrices[${String(index)}].symbol`, 'a unique symbol')
    }

    symbols.add(price.value.symbol)
    prices.push(price.value)
  }

  return ok(prices)
}

export const availablePriceUsd = (
  status: EquityPriceStatus | undefined,
  nowMs = Date.now()
): string | null =>
  status?.status === 'available' && Date.parse(status.expiresAt) > nowMs
    ? status.priceUsd
    : null

export const equityExposureUsd = (
  shares: string,
  status: EquityPriceStatus | undefined
): string | null => {
  const price = availablePriceUsd(status)
  if (price === null) return null

  const exposure = tryCatch(() => new Decimal(shares).mul(price).toString())
  return exposure.tag === 'err' ? null : exposure.value
}

export const formatExposureUsd = (exposure: string | null): string => {
  if (exposure === null) return '—'

  const value = new Decimal(exposure)
  if (value.abs().lt('0.01')) return '$0'

  const sign = value.isPositive() ? '+' : '-'
  return `${sign}$${value.abs().toFixed(2)}`
}
