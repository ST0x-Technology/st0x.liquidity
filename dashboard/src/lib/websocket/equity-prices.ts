import type { QueryClient } from '@tanstack/svelte-query'
import type { CurrentState } from '$lib/api/CurrentState'
import type { EquityPrice } from '$lib/api/EquityPrice'
import {
  parseEquityPrice,
  parseEquityPrices,
  type EquityPricePayloadError
} from '$lib/equity-price'
import { err, ok, type Result } from '$lib/fp'

const expiryTimers = new WeakMap<QueryClient, Map<string, ReturnType<typeof setTimeout>>>()

const scheduleExpiry = (queryClient: QueryClient, price: EquityPrice) => {
  const timers =
    expiryTimers.get(queryClient) ?? new Map<string, ReturnType<typeof setTimeout>>()
  expiryTimers.set(queryClient, timers)

  const currentTimer = timers.get(price.symbol)
  if (currentTimer !== undefined) clearTimeout(currentTimer)
  timers.delete(price.symbol)
  if (price.status.status !== 'available') return

  const expiresAt = price.status.expiresAt
  const delay = Math.max(0, Date.parse(expiresAt) - Date.now())
  const timer = setTimeout(() => {
    timers.delete(price.symbol)
    queryClient.setQueryData<EquityPrice[]>(['equity-prices'], (current = []) =>
      current.map((candidate) =>
        candidate.symbol === price.symbol &&
        candidate.status.status === 'available' &&
        candidate.status.expiresAt === expiresAt
          ? { symbol: candidate.symbol, status: { status: 'unavailable' } }
          : candidate
      )
    )
  }, delay)
  timers.set(price.symbol, timer)
}

export const parseStateEquityPrices = (
  state: CurrentState
): Result<EquityPrice[], EquityPricePayloadError> => {
  const legacyCompatibleState = state as { equityPrices?: unknown }
  return parseEquityPrices(legacyCompatibleState.equityPrices ?? [])
}

export const seedEquityPrices = (queryClient: QueryClient, prices: EquityPrice[]) => {
  queryClient.setQueryData<EquityPrice[]>(['equity-prices'], prices)
  prices.forEach((price) => scheduleExpiry(queryClient, price))
}

export const upsertEquityPrice = (
  queryClient: QueryClient,
  price: unknown
): Result<void, EquityPricePayloadError> => {
  const parsed = parseEquityPrice(price)
  if (parsed.tag === 'err') return err(parsed.error)

  queryClient.setQueryData<EquityPrice[]>(['equity-prices'], (current = []) => {
    const existing = current.findIndex((candidate) => candidate.symbol === parsed.value.symbol)
    if (existing === -1) return [...current, parsed.value]

    const updated = [...current]
    updated[existing] = parsed.value
    return updated
  })
  scheduleExpiry(queryClient, parsed.value)
  return ok(undefined)
}
