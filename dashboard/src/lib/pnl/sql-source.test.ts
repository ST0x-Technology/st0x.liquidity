import { describe, expect, it } from 'vitest'
import {
  buildPnlResponseFromSqlRows,
  buildSqlApiUrl,
  type SqlPositionEventRow,
  type SqlPositionViewRow,
  type SqlSampleStatsRow
} from './sql-source'

const baseQuery = {
  limit: 100,
  offset: 0,
  symbols: new Set<string>(),
  fromDate: '2026-05-15',
  toDate: '2026-05-15',
  dayFilter: 'all' as const
}

const positionRows: SqlPositionViewRow[] = [
  {
    symbol: 'RKLB',
    net_position: '0',
    last_updated: '2026-05-15T10:01:00Z',
    last_price_usdc: '8'
  },
  {
    symbol: 'SPYM',
    net_position: '0',
    last_updated: '2026-05-15T10:01:00Z',
    last_price_usdc: '86'
  }
]

const sampleRows: SqlSampleStatsRow[] = [
  {
    symbol: 'RKLB',
    onchain_fill_count: 1,
    offchain_fill_count: 1,
    first_at: '2026-05-15T10:00:00Z',
    last_at: '2026-05-15T10:01:00Z'
  },
  {
    symbol: 'SPYM',
    onchain_fill_count: 4,
    offchain_fill_count: 3,
    first_at: '2026-05-15T11:00:00Z',
    last_at: '2026-05-15T11:05:00Z'
  }
]

const onchainSell = (rowid: number, price = '10'): SqlPositionEventRow => ({
  rowid,
  symbol: 'RKLB',
  event_type: 'PositionEvent::OnChainOrderFilled',
  payload: JSON.stringify({
    OnChainOrderFilled: {
      amount: '1',
      direction: 'Sell',
      price_usdc: price,
      block_timestamp: '2026-05-15T10:00:00Z',
      trade_id: {
        tx_hash: `0x${String(rowid)}`,
        log_index: 0
      }
    }
  })
})

const offchainBuy = (rowid: number, timestamp: string, price = '8'): SqlPositionEventRow => ({
  rowid,
  symbol: 'RKLB',
  event_type: 'PositionEvent::OffChainOrderFilled',
  payload: {
    OffChainOrderFilled: {
      offchain_order_id: `alpaca-${String(rowid)}`,
      shares_filled: '1',
      direction: 'Buy',
      price,
      broker_timestamp: timestamp
    }
  }
})

describe('buildSqlApiUrl', () => {
  it('uses the Datasette SQL JSON shape expected by the prod read endpoint', () => {
    const url = buildSqlApiUrl(
      'http://host:8081/st0x-hedge.json',
      'SELECT symbol, net_position FROM position_view'
    )

    expect(url).toBe(
      'http://host:8081/st0x-hedge.json?sql=SELECT+symbol%2C+net_position+FROM+position_view&_shape=objects&_size=max'
    )
  })

  it('supports the local same-origin SQL proxy path', () => {
    const previousLocation = (globalThis as { location?: Location }).location
    try {
      Object.defineProperty(globalThis, 'location', {
        value: { origin: 'http://localhost:5174' },
        writable: true,
        configurable: true
      })

      expect(buildSqlApiUrl('/__pnl_sql', 'SELECT 1')).toBe(
        'http://localhost:5174/__pnl_sql?sql=SELECT+1&_shape=objects&_size=max'
      )
    } finally {
      Object.defineProperty(globalThis, 'location', {
        value: previousLocation,
        writable: true,
        configurable: true
      })
    }
  })
})

describe('buildPnlResponseFromSqlRows', () => {
  it('maps prompt counter-trades into counter-trade PnL', () => {
    const report = buildPnlResponseFromSqlRows(
      [onchainSell(1, '10'), offchainBuy(2, '2026-05-15T10:01:00Z', '8')],
      positionRows,
      sampleRows,
      baseQuery
    )

    expect(report.summary.counterTradePnlUsd).toBe('2')
    expect(report.summary.directionalImbalanceExcessPnlUsd).toBe('0')
    expect(report.summary.totalPnlUsd).toBe('2')
    expect(report.entries[0]?.pnlBucket).toBe('counter_trade')
    expect(report.entries[0]?.delayedCounterTrade).toBe(false)
  })

  it('classifies late broker fills as directional exposure', () => {
    const report = buildPnlResponseFromSqlRows(
      [onchainSell(1, '10'), offchainBuy(2, '2026-05-15T10:06:01Z', '8')],
      positionRows,
      sampleRows,
      baseQuery
    )

    expect(report.summary.counterTradePnlUsd).toBe('0')
    expect(report.summary.directionalImbalanceExcessPnlUsd).toBe('2')
    expect(report.summary.directionalExposurePnlUsd).toBe('2')
    expect(report.summary.totalPnlUsd).toBe('2')
    expect(report.entries[0]?.pnlBucket).toBe('directional_exposure')
    expect(report.entries[0]?.delayedCounterTrade).toBe(true)
  })

  it('keeps the full symbol universe separate from filtered PnL rows', () => {
    const report = buildPnlResponseFromSqlRows(
      [onchainSell(1, '10'), offchainBuy(2, '2026-05-15T10:01:00Z', '8')],
      positionRows,
      sampleRows,
      {
        ...baseQuery,
        symbols: new Set(['RKLB'])
      }
    )

    expect(report.symbols.map((row) => row.symbol)).toEqual(['RKLB'])
    expect(report.symbolUniverse).toEqual(['RKLB', 'SPYM'])
  })

  it('reports unfiltered database sample statistics for date-range selection', () => {
    const report = buildPnlResponseFromSqlRows(
      [onchainSell(1, '10'), offchainBuy(2, '2026-05-15T10:01:00Z', '8')],
      positionRows,
      sampleRows,
      baseQuery
    )

    expect(report.sampleStats).toEqual({
      firstAt: '2026-05-15T10:00:00Z',
      lastAt: '2026-05-15T11:05:00Z',
      symbolCount: 2,
      onchainFillCount: 5,
      offchainFillCount: 4,
      totalFillCount: 9,
      symbols: [
        {
          symbol: 'RKLB',
          firstAt: '2026-05-15T10:00:00Z',
          lastAt: '2026-05-15T10:01:00Z',
          onchainFillCount: 1,
          offchainFillCount: 1,
          totalFillCount: 2
        },
        {
          symbol: 'SPYM',
          firstAt: '2026-05-15T11:00:00Z',
          lastAt: '2026-05-15T11:05:00Z',
          onchainFillCount: 4,
          offchainFillCount: 3,
          totalFillCount: 7
        }
      ]
    })
  })

  it('drops unsafe symbols from SQL replay rows and warnings', () => {
    const report = buildPnlResponseFromSqlRows(
      [
        {
          ...onchainSell(1, '10'),
          symbol: "RKLB'); DROP TABLE events; --"
        },
        onchainSell(2, '10'),
        offchainBuy(3, '2026-05-15T10:01:00Z', '8')
      ],
      [
        ...positionRows,
        {
          symbol: "BAD';--",
          net_position: '0',
          last_updated: '2026-05-15T10:01:00Z',
          last_price_usdc: '1'
        }
      ],
      [
        ...sampleRows,
        {
          symbol: "BAD';--",
          onchain_fill_count: 1,
          offchain_fill_count: 1,
          first_at: '2026-05-15T10:00:00Z',
          last_at: '2026-05-15T10:01:00Z'
        }
      ],
      {
        ...baseQuery,
        symbols: new Set(["RKLB'); DROP TABLE events; --"])
      }
    )

    expect(report.summary.counterTradePnlUsd).toBe('2')
    expect(report.symbolUniverse).toEqual(['RKLB', 'SPYM'])
    expect(report.warnings).toEqual(
      expect.arrayContaining([
        expect.stringContaining('Skipped 1 invalid symbol filters'),
        expect.stringContaining('Skipped unsafe position_view symbol'),
        expect.stringContaining('Skipped unsafe sample stats symbol'),
        expect.stringContaining('Skipped unsafe position event symbol')
      ])
    )
  })

  it('summarizes replay diagnostics without raw per-fill allocation warnings', () => {
    const report = buildPnlResponseFromSqlRows(
      [offchainBuy(1, '2026-05-15T10:01:00Z', '8')],
      positionRows,
      sampleRows,
      baseQuery
    )

    expect(report.warnings).toEqual(
      expect.arrayContaining([
        expect.stringContaining(
          'Allocation note: 1 offchain fills are outside the matched onchain replay'
        ),
        expect.stringContaining('Reconciliation note: replayed open lots differ from position_view')
      ])
    )
    expect(report.warnings.some((warning) => warning.includes('no open opposite-side'))).toBe(false)
    expect(report.warnings.some((warning) => warning.includes('PnL audit warning'))).toBe(false)
    expect(report.summary.unmatchedOffchainFillCount).toBe(1)
    expect(report.summary.unmatchedOffchainShares).toBe('1')
    expect(report.summary.openLongShares).toBe('1')
    expect(report.symbols).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          symbol: 'RKLB',
          unmatchedOffchainFillCount: 1,
          unmatchedOffchainShares: '1',
          openLongShares: '1'
        })
      ])
    )
  })

  it('keeps current replay exposure even when there are no closed PnL lots', () => {
    const report = buildPnlResponseFromSqlRows(
      [onchainSell(1, '10')],
      positionRows,
      sampleRows,
      baseQuery
    )

    expect(report.entries).toEqual([])
    expect(report.summary.totalPnlUsd).toBe('0')
    expect(report.summary.openShortShares).toBe('1')
    expect(report.summary.inventoryDriftShares).toBe('-1')
    expect(report.symbols).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          symbol: 'RKLB',
          totalPnlUsd: '0',
          openShortShares: '1',
          inventoryDriftShares: '-1'
        })
      ])
    )
  })
})
