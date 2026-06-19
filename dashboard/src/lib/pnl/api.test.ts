import { describe, expect, it } from 'vitest'
import { buildPnlParams } from './api'

describe('buildPnlParams', () => {
  it('serializes filters for the backend PnL endpoint', () => {
    const params = buildPnlParams({
      limit: 500,
      offset: 25,
      symbols: new Set(['SPYM', 'MSTR']),
      fromDate: '2026-05-19',
      toDate: '2026-05-24',
      marketSessionFilter: 'rth',
      counterTradingFilter: 'counter_trading_active'
    })

    expect(params.toString()).toBe(
      'limit=500&offset=25&symbol=MSTR%2CSPYM&fromDate=2026-05-19&toDate=2026-05-24&marketSessionFilter=rth&counterTradingFilter=counter_trading_active'
    )
  })

  it('omits optional filters when all symbols and all sessions are selected', () => {
    const params = buildPnlParams({
      limit: 100,
      offset: 0,
      symbols: new Set(),
      marketSessionFilter: 'all',
      counterTradingFilter: 'all'
    })

    expect(params.toString()).toBe('limit=100&offset=0')
  })
})
