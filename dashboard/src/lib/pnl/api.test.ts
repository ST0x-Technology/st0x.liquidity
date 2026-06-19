import { afterEach, describe, expect, it, vi } from 'vitest'
import { buildPnlParams, fetchCompletePnlReport, fetchPnlReport } from './api'

describe('buildPnlParams', () => {
  it('serializes filters for the backend PnL endpoint', () => {
    const params = buildPnlParams({
      limit: 500,
      offset: 25,
      symbols: new Set(['SPYM', 'MSTR']),
      fromDate: '2026-05-19',
      toDate: '2026-05-24',
      asOfRowid: 42,
      marketSessionFilter: 'rth',
      counterTradingFilter: 'counter_trading_active'
    })

    expect(params.toString()).toBe(
      'limit=500&offset=25&symbol=MSTR%2CSPYM&asOfRowid=42&fromDate=2026-05-19&toDate=2026-05-24&marketSessionFilter=rth&counterTradingFilter=counter_trading_active'
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

describe('fetchPnlReport', () => {
  afterEach(() => {
    vi.unstubAllGlobals()
  })

  it('rejects non-JSON backend responses', async () => {
    vi.stubGlobal('window', {
      location: {
        origin: 'http://localhost:5176'
      }
    })
    vi.stubGlobal(
      'fetch',
      vi.fn().mockResolvedValue(
        new Response('<html></html>', {
          status: 200,
          headers: { 'content-type': 'text/html' }
        })
      )
    )

    await expect(
      fetchPnlReport({
        limit: 1,
        offset: 0,
        symbols: new Set()
      })
    ).rejects.toThrow('Backend /pnl returned a non-JSON response')
  })

  it('preserves backend error messages for failed reports', async () => {
    vi.stubGlobal('window', {
      location: {
        origin: 'http://localhost:5176'
      }
    })
    vi.stubGlobal(
      'fetch',
      vi.fn().mockResolvedValue(
        new Response('invalid symbol filter: DROP', {
          status: 400,
          headers: { 'content-type': 'text/plain' }
        })
      )
    )

    await expect(
      fetchPnlReport({
        limit: 1,
        offset: 0,
        symbols: new Set()
      })
    ).rejects.toThrow('invalid symbol filter: DROP')
  })

  it('fetches all entry pages for complete reports', async () => {
    vi.stubGlobal('window', {
      location: {
        origin: 'http://localhost:5176'
      }
    })
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(
        new Response(
          JSON.stringify({
            entries: [{ symbol: 'SPYM', openingFillId: 'open-1', closingFillId: 'close-1' }],
            hasMore: true,
            total: 2,
            asOfRowid: 123
          }),
          {
            status: 200,
            headers: { 'content-type': 'application/json' }
          }
        )
      )
      .mockResolvedValueOnce(
        new Response(
          JSON.stringify({
            entries: [{ symbol: 'SPYM', openingFillId: 'open-2', closingFillId: 'close-2' }],
            hasMore: false,
            total: 2,
            asOfRowid: 123
          }),
          {
            status: 200,
            headers: { 'content-type': 'application/json' }
          }
        )
      )
    vi.stubGlobal('fetch', fetchMock)

    const report = await fetchCompletePnlReport({
      symbols: new Set(['SPYM']),
      fromDate: '2026-06-03',
      toDate: '2026-07-02',
      marketSessionFilter: 'post'
    })

    expect(report.entries).toHaveLength(2)
    expect(report.hasMore).toBe(false)
    expect(fetchMock).toHaveBeenCalledTimes(2)
    expect(fetchMock.mock.calls[0]?.[0]).toBe(
      'http://localhost:5176/pnl?limit=5000&offset=0&symbol=SPYM&fromDate=2026-06-03&toDate=2026-07-02&marketSessionFilter=post'
    )
    expect(fetchMock.mock.calls[1]?.[0]).toBe(
      'http://localhost:5176/pnl?limit=5000&offset=1&symbol=SPYM&asOfRowid=123&fromDate=2026-06-03&toDate=2026-07-02&marketSessionFilter=post'
    )
  })

  it('fails complete reports when pagination stops before total', async () => {
    vi.stubGlobal('window', {
      location: {
        origin: 'http://localhost:5176'
      }
    })
    vi.stubGlobal(
      'fetch',
      vi
        .fn()
        .mockResolvedValueOnce(
          new Response(
            JSON.stringify({
              entries: [{ symbol: 'SPYM', openingFillId: 'open-1', closingFillId: 'close-1' }],
              hasMore: true,
              total: 2
            }),
            {
              status: 200,
              headers: { 'content-type': 'application/json' }
            }
          )
        )
        .mockResolvedValueOnce(
          new Response(
            JSON.stringify({
              entries: [],
              hasMore: true,
              total: 2
            }),
            {
              status: 200,
              headers: { 'content-type': 'application/json' }
            }
          )
        )
    )

    await expect(
      fetchCompletePnlReport({
        symbols: new Set()
      })
    ).rejects.toThrow('Backend /pnl pagination returned no entries before total was reached')
  })

  it('fails complete reports when pagination total changes between pages', async () => {
    vi.stubGlobal('window', {
      location: {
        origin: 'http://localhost:5176'
      }
    })
    vi.stubGlobal(
      'fetch',
      vi
        .fn()
        .mockResolvedValueOnce(
          new Response(
            JSON.stringify({
              entries: [{ symbol: 'SPYM', openingFillId: 'open-1', closingFillId: 'close-1' }],
              hasMore: true,
              total: 2,
              asOfRowid: 123
            }),
            {
              status: 200,
              headers: { 'content-type': 'application/json' }
            }
          )
        )
        .mockResolvedValueOnce(
          new Response(
            JSON.stringify({
              entries: [{ symbol: 'SPYM', openingFillId: 'open-2', closingFillId: 'close-2' }],
              hasMore: false,
              total: 3,
              asOfRowid: 123
            }),
            {
              status: 200,
              headers: { 'content-type': 'application/json' }
            }
          )
        )
    )

    await expect(
      fetchCompletePnlReport({
        symbols: new Set()
      })
    ).rejects.toThrow('Backend /pnl pagination total changed while fetching complete report')
  })

  it('fails complete reports when the first page says pagination is complete before total', async () => {
    vi.stubGlobal('window', {
      location: {
        origin: 'http://localhost:5176'
      }
    })
    vi.stubGlobal(
      'fetch',
      vi.fn().mockResolvedValueOnce(
        new Response(
          JSON.stringify({
            entries: [{ symbol: 'SPYM', openingFillId: 'open-1', closingFillId: 'close-1' }],
            hasMore: false,
            total: 2,
            asOfRowid: 123
          }),
          {
            status: 200,
            headers: { 'content-type': 'application/json' }
          }
        )
      )
    )

    await expect(
      fetchCompletePnlReport({
        symbols: new Set()
      })
    ).rejects.toThrow('Backend /pnl pagination stopped before total was reached')
  })
})
