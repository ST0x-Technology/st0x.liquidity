import { afterEach, describe, expect, it, vi } from 'vitest'
import { buildAlpacaActivityCostEntries, fetchAlpacaActivityCostEntries } from './alpaca-activities'

afterEach(() => {
  vi.restoreAllMocks()
})

describe('buildAlpacaActivityCostEntries', () => {
  it('classifies Alpaca fee rows as generic fees without subtype assumptions', () => {
    const entries = buildAlpacaActivityCostEntries([
      {
        id: '20260604000000000::fee',
        activity_type: 'FEE',
        activity_sub_type: 'ITNF',
        date: '2026-06-04',
        net_amount: '-0.02',
        symbol: 'MSTR',
        description: 'ITN mint transaction fee'
      }
    ])

    expect(entries).toHaveLength(1)
    expect(entries[0]).toMatchObject({
      category: 'broker_fee',
      accountingBucket: 'generic',
      effect: 'cost',
      amountUsd: '0.02',
      occurredAt: '2026-06-04T16:00:00.000Z',
      symbol: 'MSTR',
      aggregateType: 'AlpacaAccountActivity'
    })
    expect(entries[0]?.detail).toContain('subtype ITNF')
    expect(entries[0]?.detail).toContain('ITN mint transaction fee')
  })

  it('classifies dividends as dividend revenue and withholding as a cost', () => {
    const entries = buildAlpacaActivityCostEntries([
      {
        id: '20260605000000000::div',
        activity_type: 'DIV',
        date: '2026-06-05',
        net_amount: '1.25',
        symbol: 'SGOV'
      },
      {
        id: '20260605000000001::divft',
        activity_type: 'DIVFT',
        date: '2026-06-05',
        net_amount: '-0.19',
        symbol: 'SGOV'
      }
    ])

    expect(entries).toMatchObject([
      {
        category: 'dividend_income',
        accountingBucket: 'dividend_revenue',
        effect: 'revenue',
        amountUsd: '1.25'
      },
      {
        category: 'dividend_income',
        accountingBucket: 'generic',
        effect: 'cost',
        amountUsd: '0.19'
      }
    ])
  })

  it('classifies margin interest by sign', () => {
    const entries = buildAlpacaActivityCostEntries([
      {
        id: '20260606000000000::int',
        activity_type: 'INT',
        date: '2026-06-06',
        net_amount: '-3.45'
      },
      {
        id: '20260606000000001::int',
        activity_type: 'INT',
        date: '2026-06-06',
        net_amount: '0.12'
      }
    ])

    expect(entries.map((entry) => entry.effect)).toEqual(['cost', 'revenue'])
    expect(entries.every((entry) => entry.category === 'margin_interest')).toBe(true)
  })

  it('skips activities without usable signed amounts', () => {
    const entries = buildAlpacaActivityCostEntries([
      {
        id: 'missing-amount',
        activity_type: 'FEE',
        date: '2026-06-04'
      },
      {
        id: 'bad-amount',
        activity_type: 'FEE',
        date: '2026-06-04',
        net_amount: 'not-a-number'
      },
      {
        id: 'fill',
        activity_type: 'FILL',
        transaction_time: '2026-06-04T14:00:00Z',
        net_amount: '-1'
      }
    ])

    expect(entries).toEqual([])
  })
})

describe('fetchAlpacaActivityCostEntries', () => {
  it('fetches with date filters and a next-day settlement grace window', async () => {
    const fetchMock = vi.fn().mockResolvedValue({
      ok: true,
      json: () =>
        Promise.resolve({
        entries: [
          {
            id: '20260604000000000::fee',
            activity_type: 'FEE',
            date: '2026-06-04',
            net_amount: '-0.02'
          }
        ]
      })
    })
    vi.stubGlobal('fetch', fetchMock)

    const entries = await fetchAlpacaActivityCostEntries('/pnl/alpaca-activities', {
      limit: 100,
      offset: 0,
      symbols: new Set(),
      fromDate: '2026-06-03',
      toDate: '2026-06-08'
    })

    expect(fetchMock).toHaveBeenCalledWith(
      '/pnl/alpaca-activities?after=2026-06-03T00%3A00%3A00.000Z&until=2026-06-10T00%3A00%3A00.000Z'
    )
    expect(entries[0]?.amountUsd).toBe('0.02')
  })
})
