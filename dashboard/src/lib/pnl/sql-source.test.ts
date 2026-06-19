import { afterEach, describe, expect, it, vi } from 'vitest'
import {
  buildPnlResponseFromSqlRows,
  buildSqlApiUrl,
  fetchPnlReportFromSql,
  mergePnlReportCostEntries,
  type SqlCostEventRow,
  type SqlPositionEventRow,
  type SqlPositionViewRow
} from './sql-source'
import type { PnlCostEntry } from './report'

const baseQuery = {
  limit: 100,
  offset: 0,
  symbols: new Set<string>(),
  fromDate: '2026-05-15',
  toDate: '2026-05-15',
  marketSessionFilter: 'all' as const,
  counterTradingFilter: 'all' as const
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

const onchainSell = (
  rowid: number,
  price = '10',
  timestamp = '2026-05-15T10:00:00Z'
): SqlPositionEventRow => ({
  rowid,
  symbol: 'RKLB',
  event_type: 'PositionEvent::OnChainOrderFilled',
  payload: JSON.stringify({
    OnChainOrderFilled: {
      amount: '1',
      direction: 'Sell',
      price_usdc: price,
      block_timestamp: timestamp,
      trade_id: {
        tx_hash: `0x${String(rowid)}`,
        log_index: 0
      }
    }
  })
})

const offchainBuy = (
  rowid: number,
  timestamp: string,
  price = '8',
  shares = '1'
): SqlPositionEventRow => ({
  rowid,
  symbol: 'RKLB',
  event_type: 'PositionEvent::OffChainOrderFilled',
  payload: {
    OffChainOrderFilled: {
      offchain_order_id: `alpaca-${String(rowid)}`,
      shares_filled: shares,
      direction: 'Buy',
      price,
      broker_timestamp: timestamp
    }
  }
})

const tokenizedMintRequested = (
  rowid: number,
  aggregateId = 'mint-rklb-1',
  symbol = 'RKLB'
): SqlCostEventRow => ({
  rowid,
  aggregate_type: 'TokenizedEquityMint',
  aggregate_id: aggregateId,
  event_type: 'TokenizedEquityMintEvent::MintRequested',
  payload: {
    MintRequested: {
      symbol,
      quantity: '1',
      wallet: '0x0000000000000000000000000000000000000001',
      requested_at: '2026-05-15T09:55:00Z'
    }
  }
})

const tokenizedTokensReceived = (
  rowid: number,
  aggregateId = 'mint-rklb-1',
  fees = '0.25'
): SqlCostEventRow => ({
  rowid,
  aggregate_type: 'TokenizedEquityMint',
  aggregate_id: aggregateId,
  event_type: 'TokenizedEquityMintEvent::TokensReceived',
  payload: {
    TokensReceived: {
      tx_hash: '0x1111111111111111111111111111111111111111111111111111111111111111',
      shares_minted: '1000000000000000000',
      fees,
      received_at: '2026-05-15T10:02:00Z'
    }
  }
})

const usdcConversionInitiated = (rowid: number, aggregateId = 'rebalance-1'): SqlCostEventRow => ({
  rowid,
  aggregate_type: 'UsdcRebalance',
  aggregate_id: aggregateId,
  event_type: 'UsdcRebalanceEvent::ConversionInitiated',
  payload: {
    ConversionInitiated: {
      direction: 'AlpacaToBase',
      amount: '10',
      order_id: 'conversion-1',
      initiated_at: '2026-05-15T10:03:00Z'
    }
  }
})

const usdcConversionConfirmed = (
  rowid: number,
  aggregateId = 'rebalance-1'
): SqlCostEventRow => ({
  rowid,
  aggregate_type: 'UsdcRebalance',
  aggregate_id: aggregateId,
  event_type: 'UsdcRebalanceEvent::ConversionConfirmed',
  payload: {
    ConversionConfirmed: {
      direction: 'AlpacaToBase',
      filled_amount: '9.9',
      converted_at: '2026-05-15T10:04:00Z'
    }
  }
})

const usdcBridged = (rowid: number, aggregateId = 'rebalance-1'): SqlCostEventRow => ({
  rowid,
  aggregate_type: 'UsdcRebalance',
  aggregate_id: aggregateId,
  event_type: 'UsdcRebalanceEvent::Bridged',
  payload: {
    Bridged: {
      mint_tx_hash: '0x2222222222222222222222222222222222222222222222222222222222222222',
      amount_received: '9.89',
      fee_collected: '0.01',
      minted_at: '2026-05-15T10:05:00Z'
    }
  }
})

const response = (body: unknown): Response =>
  ({
    ok: true,
    json: () => Promise.resolve(body)
  }) as Response

const fetchInputUrl = (input: RequestInfo | URL): URL => {
  if (input instanceof URL) return input
  if (typeof input === 'string') return new URL(input)
  return new URL(input.url)
}

afterEach(() => {
  vi.unstubAllGlobals()
})

describe('buildSqlApiUrl', () => {
  it('uses the Datasette SQL JSON shape expected by the prod read endpoint', () => {
    const url = buildSqlApiUrl(
      'http://host:8081/st0x-hedge.json',
      'SELECT symbol, net_position FROM position_view'
    )

    expect(url).toBe(
      'http://host:8081/st0x-hedge.json?sql=SELECT+*+FROM+%28SELECT+symbol%2C+net_position+FROM+position_view%29+LIMIT+900+OFFSET+0&_shape=objects&_size=max'
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
        'http://localhost:5174/__pnl_sql?sql=SELECT+*+FROM+%28SELECT+1%29+LIMIT+900+OFFSET+0&_shape=objects&_size=max'
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

describe('fetchPnlReportFromSql', () => {
  it('paginates Datasette SQL reads instead of rendering a capped prefix', async () => {
    const firstEventPage = Array.from({ length: 900 }, (_, index) => onchainSell(index + 1))
    const secondEventPage = [
      onchainSell(901),
      offchainBuy(902, '2026-05-15T10:01:00Z', '8')
    ]
    const fetchMock = vi.fn((input: RequestInfo | URL) => {
      const url = fetchInputUrl(input)
      const sql = url.searchParams.get('sql') ?? ''

      if (sql.includes('FROM position_view')) {
        return Promise.resolve(response({ rows: positionRows, truncated: false }))
      }

      if (sql.includes("aggregate_type = 'TokenizedEquityMint'")) {
        return Promise.resolve(response({ rows: [], truncated: false }))
      }

      if (sql.includes('OFFSET 900')) {
        return Promise.resolve(response({ rows: secondEventPage, truncated: false }))
      }

      if (sql.includes('OFFSET 0')) {
        return Promise.resolve(response({ rows: firstEventPage, truncated: false }))
      }

      return Promise.resolve(response({ rows: [], truncated: false }))
    })

    vi.stubGlobal('fetch', fetchMock)

    const report = await fetchPnlReportFromSql('http://host:8081/st0x-hedge.json', baseQuery)
    const requestedSql = fetchMock.mock.calls.map(([input]) =>
      fetchInputUrl(input).searchParams.get('sql')
    )

    expect(requestedSql.some((sql) => (sql ?? '').includes('OFFSET 900'))).toBe(true)
    expect(report.summary.counterTradePnlUsd).toBe('2')
    expect(report.summary.openShortShares).toBe('900')
    expect(report.entries).toHaveLength(1)
    expect(report.entries[0]).toEqual(
      expect.objectContaining({
        openingRowid: 1,
        closingRowid: 902,
        pnlBucket: 'counter_trade',
        realizedPnlUsd: '2'
      })
    )
  })
})

describe('buildPnlResponseFromSqlRows', () => {
  it('maps prompt counter-trades into counter-trade PnL', () => {
    const report = buildPnlResponseFromSqlRows(
      [onchainSell(1, '10'), offchainBuy(2, '2026-05-15T10:01:00Z', '8')],
      positionRows,
      baseQuery
    )

    expect(report.summary.counterTradePnlUsd).toBe('2')
    expect(report.summary.directionalImbalanceExcessPnlUsd).toBe('0')
    expect(report.summary.totalPnlUsd).toBe('2')
    expect(report.entries[0]?.pnlBucket).toBe('counter_trade')
    expect(report.entries[0]?.delayedCounterTrade).toBe(false)
  })

  it('filters realized rows to counter-trading active sessions', () => {
    const report = buildPnlResponseFromSqlRows(
      [
        onchainSell(1, '10', '2026-05-15T14:00:00Z'),
        offchainBuy(2, '2026-05-15T14:01:00Z', '8'),
        onchainSell(3, '11', '2026-05-15T12:00:00Z'),
        offchainBuy(4, '2026-05-15T12:01:00Z', '9')
      ],
      positionRows,
      {
        ...baseQuery,
        counterTradingFilter: 'counter_trading_active'
      }
    )

    expect(report.entries).toHaveLength(1)
    expect(report.entries[0]?.closingRowid).toBe(2)
    expect(report.summary.grossRealizedPnlUsd).toBe('2')
    expect(report.windows?.[0]?.counterTradingSession).toBe('counter_trading_active')
  })

  it('filters realized rows to counter-trading inactive sessions', () => {
    const report = buildPnlResponseFromSqlRows(
      [
        onchainSell(1, '10', '2026-05-15T14:00:00Z'),
        offchainBuy(2, '2026-05-15T14:01:00Z', '8'),
        onchainSell(3, '11', '2026-05-15T12:00:00Z'),
        offchainBuy(4, '2026-05-15T12:01:00Z', '9')
      ],
      positionRows,
      {
        ...baseQuery,
        counterTradingFilter: 'counter_trading_inactive'
      }
    )

    expect(report.entries).toHaveLength(1)
    expect(report.entries[0]?.closingRowid).toBe(4)
    expect(report.summary.grossRealizedPnlUsd).toBe('2')
    expect(report.windows?.[0]?.counterTradingSession).toBe('counter_trading_inactive')
  })

  it('filters realized rows by market session independently from counter-trading state', () => {
    const report = buildPnlResponseFromSqlRows(
      [
        onchainSell(1, '10', '2026-05-15T12:00:00Z'),
        offchainBuy(2, '2026-05-15T12:01:00Z', '8'),
        onchainSell(3, '11', '2026-05-15T14:00:00Z'),
        offchainBuy(4, '2026-05-15T14:01:00Z', '7')
      ],
      positionRows,
      {
        ...baseQuery,
        marketSessionFilter: 'rth'
      }
    )

    expect(report.entries).toHaveLength(1)
    expect(report.entries[0]?.closingRowid).toBe(4)
    expect(report.summary.grossRealizedPnlUsd).toBe('4')
    expect(report.windows?.[0]?.marketSession).toBe('rth')
    expect(report.windows?.[0]?.counterTradingSession).toBe('counter_trading_active')
  })

  it('filters tracked costs by counter-trading session', () => {
    const activeReport = buildPnlResponseFromSqlRows(
      [onchainSell(1, '10', '2026-05-15T14:00:00Z'), offchainBuy(2, '2026-05-15T14:01:00Z', '8')],
      positionRows,
      {
        ...baseQuery,
        counterTradingFilter: 'counter_trading_active'
      },
      [tokenizedMintRequested(10), tokenizedTokensReceived(11)]
    )

    const inactiveReport = buildPnlResponseFromSqlRows(
      [onchainSell(1, '10', '2026-05-15T14:00:00Z'), offchainBuy(2, '2026-05-15T14:01:00Z', '8')],
      positionRows,
      {
        ...baseQuery,
        counterTradingFilter: 'counter_trading_inactive'
      },
      [tokenizedMintRequested(10), tokenizedTokensReceived(11)]
    )

    expect(activeReport.summary.grossRealizedPnlUsd).toBe('2')
    expect(activeReport.summary.trackedCostsUsd).toBe('0')
    expect(activeReport.summary.netRealizedPnlUsd).toBe('2')
    expect(activeReport.costEntries).toHaveLength(0)

    expect(inactiveReport.summary.grossRealizedPnlUsd).toBe('0')
    expect(inactiveReport.summary.trackedCostsUsd).toBe('0.25')
    expect(inactiveReport.summary.netRealizedPnlUsd).toBe('-0.25')
    expect(inactiveReport.costEntries).toHaveLength(1)
  })

  it('subtracts persisted tracked costs from realized gross PnL', () => {
    const report = buildPnlResponseFromSqlRows(
      [onchainSell(1, '10'), offchainBuy(2, '2026-05-15T10:01:00Z', '8')],
      positionRows,
      baseQuery,
      [
        tokenizedMintRequested(10),
        tokenizedTokensReceived(11),
        usdcConversionInitiated(12),
        usdcConversionConfirmed(13),
        usdcBridged(14)
      ]
    )

    expect(report.summary.grossRealizedPnlUsd).toBe('2')
    expect(report.summary.trackedCostsUsd).toBe('0.26')
    expect(report.summary.trackedRevenueUsd).toBe('0')
    expect(report.summary.netRealizedPnlUsd).toBe('1.74')
    expect(report.costs).toEqual(
      expect.objectContaining({
        totalTrackedCostsUsd: '0.26',
        totalTrackedRevenueUsd: '0',
        counterTradeCostsUsd: '0',
        onchainNettingCostsUsd: '0',
        directionalExposureCostsUsd: '0',
        genericCostsUsd: '0.26',
        dividendRevenueUsd: '0',
        tokenizationFeesUsd: '0.25',
        conversionSlippageUsd: '0',
        cctpFeesUsd: '0.01',
        oracleWriteCostUsd: '0'
      })
    )
    expect(report.costs.coverage).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          source: 'Alpaca fees',
          accountingBucket: 'generic',
          effect: 'cost',
          status: 'not_ingested'
        }),
        expect.objectContaining({
          source: 'On-chain netting execution costs',
          accountingBucket: 'onchain_netting',
          effect: 'none',
          status: 'zero'
        }),
        expect.objectContaining({
          source: 'Dividend income',
          accountingBucket: 'dividend_revenue',
          effect: 'revenue',
          status: 'not_ingested'
        })
      ])
    )
    expect(report.costEntries.map((entry) => entry.category)).toEqual([
      'cctp_fee',
      'tokenization_fee'
    ])
    expect(report.costEntries.every((entry) => entry.accountingBucket === 'generic')).toBe(true)
    expect(report.costEntries.every((entry) => entry.effect === 'cost')).toBe(true)
    expect(report.symbols).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          symbol: 'RKLB',
          grossRealizedPnlUsd: '2',
          trackedCostsUsd: '0.25',
          trackedRevenueUsd: '0',
          netRealizedPnlUsd: '1.75'
        })
      ])
    )
  })

  it('merges Alpaca activity costs and revenues into net PnL without changing gross PnL', () => {
    const report = buildPnlResponseFromSqlRows(
      [onchainSell(1, '10', '2026-05-15T14:00:00Z'), offchainBuy(2, '2026-05-15T14:01:00Z', '8')],
      positionRows,
      {
        ...baseQuery,
        marketSessionFilter: 'rth',
        counterTradingFilter: 'counter_trading_active'
      }
    )
    const alpacaEntries: PnlCostEntry[] = [
      {
        category: 'broker_fee',
        accountingBucket: 'generic',
        effect: 'cost',
        amountUsd: '0.02',
        occurredAt: '2026-05-15T16:00:00.000Z',
        aggregateType: 'AlpacaAccountActivity',
        aggregateId: 'fee-1',
        eventRowid: -1,
        symbol: 'RKLB',
        detail: 'Alpaca account activity FEE'
      },
      {
        category: 'dividend_income',
        accountingBucket: 'dividend_revenue',
        effect: 'revenue',
        amountUsd: '0.25',
        occurredAt: '2026-05-15T16:00:00.000Z',
        aggregateType: 'AlpacaAccountActivity',
        aggregateId: 'div-1',
        eventRowid: -2,
        symbol: 'RKLB',
        detail: 'Alpaca account activity DIV'
      }
    ]

    const merged = mergePnlReportCostEntries(report, alpacaEntries, {
      ...baseQuery,
      marketSessionFilter: 'rth',
      counterTradingFilter: 'counter_trading_active'
    })

    expect(merged.summary.grossRealizedPnlUsd).toBe('2')
    expect(merged.summary.trackedCostsUsd).toBe('0.02')
    expect(merged.summary.trackedRevenueUsd).toBe('0.25')
    expect(merged.summary.netRealizedPnlUsd).toBe('2.23')
    expect(merged.costs.brokerFeesUsd).toBe('0.02')
    expect(merged.costs.dividendRevenueUsd).toBe('0.25')
    expect(merged.symbols).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          symbol: 'RKLB',
          trackedCostsUsd: '0.02',
          trackedRevenueUsd: '0.25',
          netRealizedPnlUsd: '2.23'
        })
      ])
    )
  })

  it('keeps symbolless costs at account level instead of allocating them to asset rows', () => {
    const report = buildPnlResponseFromSqlRows(
      [onchainSell(1, '10', '2026-05-15T14:00:00Z'), offchainBuy(2, '2026-05-15T14:01:00Z', '8')],
      positionRows,
      {
        ...baseQuery,
        symbols: new Set(['RKLB']),
        marketSessionFilter: 'rth',
        counterTradingFilter: 'counter_trading_active'
      }
    )

    const merged = mergePnlReportCostEntries(
      report,
      [
        {
          category: 'broker_fee',
          accountingBucket: 'generic',
          effect: 'cost',
          amountUsd: '0.02',
          occurredAt: '2026-05-15T16:00:00.000Z',
          aggregateType: 'AlpacaAccountActivity',
          aggregateId: 'fee-1',
          eventRowid: -1,
          symbol: null,
          detail: 'Alpaca account activity FEE'
        }
      ],
      {
        ...baseQuery,
        symbols: new Set(['RKLB']),
        marketSessionFilter: 'rth',
        counterTradingFilter: 'counter_trading_active'
      }
    )

    expect(merged.summary.grossRealizedPnlUsd).toBe('2')
    expect(merged.summary.trackedCostsUsd).toBe('0.02')
    expect(merged.summary.netRealizedPnlUsd).toBe('1.98')
    expect(merged.symbols).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          symbol: 'RKLB',
          trackedCostsUsd: '0',
          netRealizedPnlUsd: '2'
        })
      ])
    )
  })

  it('replays fills by execution timestamp rather than event rowid', () => {
    const report = buildPnlResponseFromSqlRows(
      [offchainBuy(1, '2026-05-15T10:01:00Z', '8'), onchainSell(2, '10')],
      positionRows,
      baseQuery
    )

    expect(report.summary.counterTradePnlUsd).toBe('2')
    expect(report.summary.directionalImbalanceExcessPnlUsd).toBe('0')
    expect(report.entries[0]).toEqual(
      expect.objectContaining({
        openingRowid: 2,
        closingRowid: 1,
        pnlBucket: 'counter_trade',
        realizedPnlUsd: '2'
      })
    )
  })

  it('classifies late broker fills as directional exposure', () => {
    const report = buildPnlResponseFromSqlRows(
      [onchainSell(1, '10'), offchainBuy(2, '2026-05-15T10:06:01Z', '8')],
      positionRows,
      baseQuery
    )

    expect(report.summary.counterTradePnlUsd).toBe('0')
    expect(report.summary.directionalImbalanceExcessPnlUsd).toBe('2')
    expect(report.summary.directionalExposurePnlUsd).toBe('2')
    expect(report.summary.totalPnlUsd).toBe('2')
    expect(report.summary.realizedPnlUsd).toBe('2')
    expect(report.entries[0]?.pnlBucket).toBe('directional_exposure')
    expect(report.entries[0]?.delayedCounterTrade).toBe(true)
  })

  it('keeps the full symbol universe separate from filtered PnL rows', () => {
    const report = buildPnlResponseFromSqlRows(
      [onchainSell(1, '10'), offchainBuy(2, '2026-05-15T10:01:00Z', '8')],
      positionRows,
      {
        ...baseQuery,
        symbols: new Set(['RKLB'])
      }
    )

    expect(report.symbols.map((row) => row.symbol)).toEqual(['RKLB'])
    expect(report.symbolUniverse).toEqual(['RKLB', 'SPYM'])
  })

  it('reports filtered database sample statistics for date-range selection', () => {
    const report = buildPnlResponseFromSqlRows(
      [
        onchainSell(1, '10', '2026-05-15T12:00:00Z'),
        offchainBuy(2, '2026-05-15T12:01:00Z', '8'),
        onchainSell(3, '10', '2026-05-15T14:00:00Z'),
        offchainBuy(4, '2026-05-15T14:01:00Z', '8')
      ],
      positionRows,
      {
        ...baseQuery,
        marketSessionFilter: 'pre'
      }
    )

    expect(report.sampleStats).toEqual({
      firstAt: '2026-05-15T12:00:00Z',
      lastAt: '2026-05-15T12:01:00Z',
      symbolCount: 1,
      onchainFillCount: 1,
      offchainFillCount: 1,
      totalFillCount: 2,
      symbols: [
        {
          symbol: 'RKLB',
          firstAt: '2026-05-15T12:00:00Z',
          lastAt: '2026-05-15T12:01:00Z',
          onchainFillCount: 1,
          offchainFillCount: 1,
          totalFillCount: 2
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
      baseQuery
    )

    expect(report.warnings).toEqual(
      expect.arrayContaining([
        expect.stringContaining(
          'Allocation note: 1 offchain fills opened offchain-origin inventory'
        ),
        expect.stringContaining('Reconciliation note: replayed open lots differ from position_view')
      ])
    )
    expect(report.warnings.some((warning) => warning.includes('no open opposite-side'))).toBe(false)
    expect(report.warnings.some((warning) => warning.includes('PnL audit warning'))).toBe(false)
    expect(report.summary.unmatchedOffchainFillCount).toBe(0)
    expect(report.summary.unmatchedOffchainShares).toBe('0')
    expect(report.summary.openLongShares).toBe('1')
    expect(report.symbols).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          symbol: 'RKLB',
          unmatchedOffchainFillCount: 0,
          unmatchedOffchainShares: '0',
          openLongShares: '1'
        })
      ])
    )
  })

  it('carries offchain-origin inventory until later fills close it', () => {
    const report = buildPnlResponseFromSqlRows(
      [
        offchainBuy(1, '2026-05-15T10:01:00Z', '8'),
        onchainSell(2, '10', '2026-05-15T10:02:00Z')
      ],
      positionRows,
      baseQuery
    )

    expect(report.summary.totalPnlUsd).toBe('2')
    expect(report.summary.directionalImbalanceExcessPnlUsd).toBe('2')
    expect(report.summary.realizedPnlUsd).toBe('2')
    expect(report.summary.onchainNotionalUsd).toBe('10')
    expect(report.summary.offchainNotionalUsd).toBe('8')
    expect(report.summary.unmatchedOffchainShares).toBe('0')
    expect(report.summary.openLongShares).toBe('0')
    expect(report.entries[0]).toEqual(
      expect.objectContaining({
        openingVenue: 'offchain',
        closingVenue: 'onchain',
        onchainDirection: 'sell',
        offchainDirection: 'buy',
        onchainPriceUsdc: '10',
        offchainPriceUsd: '8',
        pnlBucket: 'directional_exposure',
        realizedPnlUsd: '2'
      })
    )
    expect(report.warnings).toEqual(
      expect.arrayContaining([
        expect.stringContaining('offchain fills opened offchain-origin inventory')
      ])
    )
  })

  it('splits offchain overshoots between counter-trade close and carried inventory', () => {
    const report = buildPnlResponseFromSqlRows(
      [
        onchainSell(1, '10', '2026-05-15T10:00:00Z'),
        offchainBuy(2, '2026-05-15T10:01:00Z', '8', '2'),
        onchainSell(3, '11', '2026-05-15T10:02:00Z')
      ],
      positionRows,
      baseQuery
    )

    expect(report.summary.totalPnlUsd).toBe('5')
    expect(report.summary.counterTradePnlUsd).toBe('2')
    expect(report.summary.directionalImbalanceExcessPnlUsd).toBe('3')
    expect(report.summary.realizedPnlUsd).toBe('5')
    expect(report.summary.openLongShares).toBe('0')
    expect(report.summary.openShortShares).toBe('0')
    expect(report.summary.unmatchedOffchainShares).toBe('0')
    expect(report.entries).toHaveLength(2)
    expect(report.entries).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          openingVenue: 'onchain',
          closingVenue: 'offchain',
          shares: '1',
          pnlBucket: 'counter_trade',
          realizedPnlUsd: '2'
        }),
        expect.objectContaining({
          openingVenue: 'offchain',
          closingVenue: 'onchain',
          shares: '1',
          pnlBucket: 'directional_exposure',
          realizedPnlUsd: '3'
        })
      ])
    )
  })

  it('keeps current replay exposure even when there are no closed PnL lots', () => {
    const report = buildPnlResponseFromSqlRows(
      [onchainSell(1, '10')],
      positionRows,
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
