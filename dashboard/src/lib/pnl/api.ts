import { getApiBaseUrl, getPnlSqlApiUrl, getPnlAlpacaActivitiesApiUrl } from '$lib/env'
import { FETCH_TIMEOUT_MS } from '$lib/time'
import { fetchAlpacaActivityCostEntries } from '$lib/pnl/alpaca-activities'
import { fetchPnlReportFromSql, mergePnlReportCostEntries } from '$lib/pnl/sql-source'
import type { PnlCounterTradingFilter, PnlMarketSessionFilter, PnlResponse } from '$lib/pnl/report'

export type PnlQuery = {
  limit: number
  offset: number
  symbols: Set<string>
  fromDate?: string
  toDate?: string
  marketSessionFilter?: PnlMarketSessionFilter
  counterTradingFilter?: PnlCounterTradingFilter
}

export const buildPnlParams = (query: PnlQuery): URLSearchParams => {
  const params = new URLSearchParams({
    limit: String(query.limit),
    offset: String(query.offset)
  })

  if (query.symbols.size > 0) {
    params.set('symbol', [...query.symbols].sort().join(','))
  }

  if (query.fromDate !== undefined && query.fromDate !== '') {
    params.set('fromDate', query.fromDate)
  }

  if (query.toDate !== undefined && query.toDate !== '') {
    params.set('toDate', query.toDate)
  }

  if (query.marketSessionFilter !== undefined && query.marketSessionFilter !== 'all') {
    params.set('marketSessionFilter', query.marketSessionFilter)
  }

  if (query.counterTradingFilter !== undefined && query.counterTradingFilter !== 'all') {
    params.set('counterTradingFilter', query.counterTradingFilter)
  }

  return params
}

export const fetchPnlReport = async (query: PnlQuery): Promise<PnlResponse> => {
  const sqlApiUrl = getPnlSqlApiUrl()
  if (sqlApiUrl !== null) {
    const report = await fetchPnlReportFromSql(sqlApiUrl, query)
    const alpacaActivitiesApiUrl = getPnlAlpacaActivitiesApiUrl()
    if (alpacaActivitiesApiUrl === null) return report

    try {
      const entries = await fetchAlpacaActivityCostEntries(alpacaActivitiesApiUrl, query)
      return mergePnlReportCostEntries(report, entries, query)
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error)
      return {
        ...report,
        warnings: [
          ...report.warnings,
          `Cost coverage note: Alpaca account activity API is configured but unavailable; Alpaca fees, margin interest, and dividends were not included. ${message}`
        ]
      }
    }
  }

  const response = await fetch(`${getApiBaseUrl()}/pnl?${buildPnlParams(query).toString()}`, {
    signal: AbortSignal.timeout(FETCH_TIMEOUT_MS)
  })

  if (!response.ok) {
    throw new Error(`HTTP ${String(response.status)}`)
  }

  return (await response.json()) as PnlResponse
}
