import { describe, expect, it } from 'vitest'
import type { Trade } from '$lib/api/Trade'
import {
  mergeTradeHistory,
  tradeFailureReason,
  tradeFailureShares,
  tradeOutcomeClass,
  tradeOutcomeLabel
} from './trade'

describe('trade outcome presentation', () => {
  it('renders filled outcomes as successful', () => {
    const outcome = { status: 'filled' } as const

    expect(tradeOutcomeLabel(outcome)).toBe('Filled')
    expect(tradeOutcomeClass(outcome)).toBe('text-green-500')
    expect(tradeFailureReason(outcome)).toBeNull()
  })

  it('renders failed outcomes with their error', () => {
    const outcome = {
      status: 'failed',
      error: 'asset is not tradable',
      filledShares: '0.25',
      remainingShares: '0.75',
      excessShares: '0'
    } as const

    expect(tradeOutcomeLabel(outcome)).toBe('Failed')
    expect(tradeOutcomeClass(outcome)).toBe('text-destructive')
    expect(tradeFailureReason(outcome)).toBe('asset is not tradable')
    expect(tradeFailureShares(outcome)).toEqual({
      filled: '0.25',
      remaining: '0.75',
      excess: '0'
    })
  })
})

describe('live trade history', () => {
  const trade = (overrides: Partial<Trade>): Trade => ({
    id: 'existing',
    occurredAt: '2026-01-01T00:00:00Z',
    venue: 'alpaca',
    direction: 'buy',
    symbol: 'AAPL',
    shares: '1',
    outcome: { status: 'filled' },
    ...overrides
  })

  it('merges matching failures into the visible history without duplicates', () => {
    const stale = trade({})
    const failure = trade({
      outcome: {
        status: 'failed',
        error: 'broker rejected remainder',
        filledShares: '0.25',
        remainingShares: '0.75',
        excessShares: '0'
      }
    })

    expect(
      mergeTradeHistory([stale], [failure], {
        venues: new Set(['alpaca']),
        symbols: new Set(),
        since: null,
        until: null
      })
    ).toEqual([failure])
  })

  it('keeps live updates out when they do not match active filters', () => {
    const failure = trade({ symbol: 'SPCX' })

    expect(
      mergeTradeHistory([], [failure], {
        venues: new Set(['alpaca']),
        symbols: new Set(['AAPL']),
        since: null,
        until: null
      })
    ).toEqual([])
  })

  it('applies venue and time filters to live updates', () => {
    const before = trade({ id: 'before', occurredAt: '2025-12-31T23:59:59Z' })
    const atStart = trade({ id: 'at-start', occurredAt: '2026-01-01T00:00:00Z' })
    const atEnd = trade({ id: 'at-end', occurredAt: '2026-01-01T00:01:00Z' })
    const after = trade({ id: 'after', occurredAt: '2026-01-01T00:01:01Z' })
    const wrongVenue = trade({ id: 'wrong-venue', venue: 'raindex' })

    expect(
      mergeTradeHistory([], [before, atStart, atEnd, after, wrongVenue], {
        venues: new Set(['alpaca']),
        symbols: new Set(),
        since: '2026-01-01T00:00:00Z',
        until: '2026-01-01T00:01:00Z'
      })
    ).toEqual([atEnd, atStart])
  })

  it('sorts merged history newest-first with a stable ID tie-breaker', () => {
    const older = trade({ id: 'older', occurredAt: '2026-01-01T00:00:00Z' })
    const sameTimeB = trade({ id: 'b', occurredAt: '2026-01-01T00:01:00Z' })
    const sameTimeA = trade({ id: 'a', occurredAt: '2026-01-01T00:01:00Z' })

    expect(
      mergeTradeHistory([older, sameTimeB], [sameTimeA], {
        venues: new Set(['alpaca']),
        symbols: new Set(),
        since: null,
        until: null
      })
    ).toEqual([sameTimeA, sameTimeB, older])
  })

  it('sorts tied onchain trades by numeric log index', () => {
    const txHash = `0x${'0'.repeat(64)}`
    const log2 = trade({ id: `${txHash}:2`, venue: 'raindex' })
    const log10 = trade({ id: `${txHash}:10`, venue: 'raindex' })

    expect(
      mergeTradeHistory([log2], [log10], {
        venues: new Set(['raindex']),
        symbols: new Set(),
        since: null,
        until: null
      })
    ).toEqual([log2, log10])
  })

  it('preserves sub-millisecond RFC 3339 ordering', () => {
    const earlier = trade({ id: 'a-earlier', occurredAt: '2026-01-01T00:00:00.123456788Z' })
    const later = trade({ id: 'z-later', occurredAt: '2026-01-01T00:00:00.123456789Z' })

    expect(
      mergeTradeHistory([earlier], [later], {
        venues: new Set(['alpaca']),
        symbols: new Set(),
        since: null,
        until: null
      })
    ).toEqual([later, earlier])
  })
})
