import { describe, expect, it } from 'vitest'
import {
  parseCanonicalTrade,
  parseTrade,
  parseTradeEntries,
  parseTradeResponse
} from './trade-payload'

const validTrade = (overrides: Record<string, unknown> = {}): unknown => ({
  id: 'counter-trade-1',
  occurredAt: '2026-07-20T12:00:00.123456789Z',
  venue: 'alpaca',
  direction: 'sell',
  symbol: 'AAPL',
  shares: '1.25',
  outcome: { status: 'filled' },
  ...overrides
})

describe('trade payload validation', () => {
  it('accepts canonical and legacy trade entries', () => {
    expect(parseTrade(validTrade())).toEqual(validTrade())
    expect(
      parseTrade({
        id: 'legacy-fill-1',
        filledAt: '2026-07-20T12:00:00Z',
        venue: 'raindex',
        direction: 'buy',
        symbol: 'TSLA',
        shares: '2'
      })
    ).toEqual({
      id: 'legacy-fill-1',
      filledAt: '2026-07-20T12:00:00Z',
      venue: 'raindex',
      direction: 'buy',
      symbol: 'TSLA',
      shares: '2'
    })
  })

  it.each([
    ['timestamp', { occurredAt: '2026-02-30T12:00:00Z' }, 'occurredAt'],
    ['venue', { venue: 'unknown' }, 'venue'],
    ['direction', { direction: 'hold' }, 'direction'],
    ['symbol', { symbol: '   ' }, 'symbol'],
    ['zero total shares', { shares: '0' }, 'shares'],
    ['negative total shares', { shares: '-0.1' }, 'shares'],
    ['non-numeric total shares', { shares: 'many' }, 'shares'],
    ['non-decimal total shares', { shares: '0x10' }, 'shares'],
    ['outcome variant', { outcome: { status: 'pending' } }, 'outcome.status'],
    [
      'negative filled shares',
      {
        outcome: {
          status: 'failed',
          error: 'rejected',
          filledShares: '-1',
          remainingShares: '2',
          excessShares: '0'
        }
      },
      'outcome.filledShares'
    ],
    [
      'missing failure quantity',
      {
        outcome: {
          status: 'failed',
          error: 'rejected',
          filledShares: '0',
          remainingShares: '1'
        }
      },
      'outcome.excessShares'
    ]
  ])('rejects an invalid %s', (_case, overrides, path) => {
    expect(() => parseTrade(validTrade(overrides))).toThrow(`Invalid trade payload at ${path}`)
  })

  it('identifies the invalid entry in a snapshot', () => {
    expect(() => parseTradeEntries([validTrade(), validTrade({ shares: '0' })])).toThrow(
      'Invalid trade payload at trades[1].shares'
    )
  })

  it('requires the canonical outcome shape for a live trade update', () => {
    const missingOutcome = validTrade() as Record<string, unknown>
    delete missingOutcome['outcome']

    expect(() => parseCanonicalTrade(missingOutcome)).toThrow('Invalid trade payload at outcome')
  })

  it('does not reinterpret a malformed canonical snapshot row as legacy', () => {
    const missingTimestamp = validTrade({
      occurredAt: undefined,
      filledAt: '2026-07-20T12:00:00Z'
    })

    expect(() => parseTrade(missingTimestamp)).toThrow('Invalid trade payload at occurredAt')
  })

  it('validates paginated history metadata and entries', () => {
    expect(
      parseTradeResponse({
        entries: [validTrade()],
        total: 1,
        hasMore: false
      })
    ).toEqual({
      entries: [validTrade()],
      total: 1,
      hasMore: false
    })

    expect(() =>
      parseTradeResponse({
        entries: [validTrade({ shares: '0' })],
        total: 1,
        hasMore: false
      })
    ).toThrow('Invalid trade payload at entries[0].shares')
    expect(() =>
      parseTradeResponse({
        entries: [validTrade()],
        total: -1,
        hasMore: false
      })
    ).toThrow('Invalid trade payload at total')
  })
})
