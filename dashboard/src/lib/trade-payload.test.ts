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
          acceptedShares: '1',
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
          acceptedShares: '1',
          filledShares: '0',
          remainingShares: '1'
        }
      },
      'outcome.excessShares'
    ]
  ])('rejects an invalid %s', (_case, overrides, path) => {
    expect(() => parseTrade(validTrade(overrides))).toThrow(`Invalid trade payload at ${path}`)
  })

  it('accepts explicit unknown provenance without inventing zero quantities', () => {
    const failed = validTrade({
      outcome: {
        status: 'failed',
        error: 'placement failed before acceptance',
        acceptedShares: null,
        filledShares: null,
        remainingShares: null,
        excessShares: null
      }
    })

    expect(parseTrade(failed)).toEqual(failed)
  })

  it('accepts zero-fill and partial-fill cancellations', () => {
    const zeroFill = validTrade({
      outcome: {
        status: 'cancelled',
        acceptedShares: '1.25',
        filledShares: '0',
        remainingShares: '1.25',
        excessShares: '0'
      }
    })
    const partialFill = validTrade({
      outcome: {
        status: 'cancelled',
        acceptedShares: '1.25',
        filledShares: '0.5',
        remainingShares: '0.75',
        excessShares: '0'
      }
    })

    expect(parseTrade(zeroFill)).toEqual(zeroFill)
    expect(parseTrade(partialFill)).toEqual(partialFill)
  })

  it('rejects a cancelled outcome without explicit v2 acceptance provenance', () => {
    expect(() =>
      parseTrade(
        validTrade({
          outcome: {
            status: 'cancelled',
            filledShares: '0',
            remainingShares: '1.25',
            excessShares: '0'
          }
        })
      )
    ).toThrow('Invalid trade payload at outcome.acceptedShares')
  })

  it('normalizes the v1 failure shape without accepted quantity to unknown', () => {
    const legacy = validTrade({
      outcome: {
        status: 'failed',
        error: 'partially filled before cancellation',
        filledShares: '0.25',
        remainingShares: '0.75',
        excessShares: '0'
      }
    }) as Record<string, unknown>

    expect(parseTrade(legacy)).toEqual({
      ...legacy,
      outcome: {
        status: 'failed',
        error: 'partially filled before cancellation',
        acceptedShares: null,
        filledShares: '0.25',
        remainingShares: null,
        excessShares: null
      }
    })
  })

  it('reconstructs the complete fill from a v1 overfill split', () => {
    const legacy = validTrade({
      outcome: {
        status: 'failed',
        error: 'broker failed after overfill',
        filledShares: '1',
        remainingShares: '0',
        excessShares: '0.25'
      }
    }) as Record<string, unknown>

    expect(parseTrade(legacy)).toEqual({
      ...legacy,
      outcome: {
        status: 'failed',
        error: 'broker failed after overfill',
        acceptedShares: null,
        filledShares: '1.25',
        remainingShares: null,
        excessShares: null
      }
    })
  })

  it('keeps a synthetic v1 zero fill unknown', () => {
    const legacy = validTrade({
      outcome: {
        status: 'failed',
        error: 'placement rejected',
        filledShares: '0',
        remainingShares: '1.25',
        excessShares: '0'
      }
    }) as Record<string, unknown>

    expect(parseTrade(legacy)).toEqual({
      ...legacy,
      outcome: {
        status: 'failed',
        error: 'placement rejected',
        acceptedShares: null,
        filledShares: null,
        remainingShares: null,
        excessShares: null
      }
    })
  })

  it('accepts a cancelled trade with explicit unknown provenance', () => {
    const trade = validTrade({
      outcome: {
        status: 'cancelled',
        acceptedShares: null,
        filledShares: null,
        remainingShares: null,
        excessShares: null
      }
    })

    expect(parseTrade(trade)).toEqual(trade)
  })

  it.each(['filledShares', 'remainingShares', 'excessShares'] as const)(
    'rejects a nullable %s in a v1 failure',
    (field) => {
      const outcome = {
        status: 'failed',
        error: 'malformed v1 failure',
        filledShares: '0',
        remainingShares: '1.25',
        excessShares: '0',
        [field]: null
      }

      expect(() => parseTrade(validTrade({ outcome }))).toThrow(
        `Invalid trade payload at outcome.${field}`
      )
    }
  )

  it('rejects derived quantities when acceptance or fill provenance is unknown', () => {
    expect(() =>
      parseTrade(
        validTrade({
          outcome: {
            status: 'failed',
            error: 'legacy failure',
            acceptedShares: null,
            filledShares: null,
            remainingShares: '1',
            excessShares: null
          }
        })
      )
    ).toThrow('Invalid trade payload at outcome.remainingShares')
  })

  it('rejects quantities that do not derive from accepted and filled evidence', () => {
    expect(() =>
      parseTrade(
        validTrade({
          outcome: {
            status: 'failed',
            error: 'inconsistent broker evidence',
            acceptedShares: '1',
            filledShares: '1.5',
            remainingShares: '0',
            excessShares: '0'
          }
        })
      )
    ).toThrow('Invalid trade payload at outcome.excessShares')
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
