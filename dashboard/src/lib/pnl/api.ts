import { getApiBaseUrl, getPnlSqlApiUrl } from '$lib/env'
import { FETCH_TIMEOUT_MS } from '$lib/time'
import { fetchPnlReportFromSql } from '$lib/pnl/sql-source'
import type { PnlResponse } from '$lib/pnl/report'

export type PnlQuery = {
  limit: number
  offset: number
  symbols: Set<string>
  fromDate?: string
  toDate?: string
  dayFilter?: 'all' | 'weekday' | 'weekend'
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

  if (query.dayFilter !== undefined && query.dayFilter !== 'all') {
    params.set('dayFilter', query.dayFilter)
  }

  return params
}

export const fetchPnlReport = async (query: PnlQuery): Promise<PnlResponse> => {
  const sqlApiUrl = getPnlSqlApiUrl()
  if (sqlApiUrl !== null) {
    return await fetchPnlReportFromSql(sqlApiUrl, query)
  }

  const response = await fetch(`${getApiBaseUrl()}/pnl?${buildPnlParams(query).toString()}`, {
    signal: AbortSignal.timeout(FETCH_TIMEOUT_MS)
  })

  if (!response.ok) {
    throw new Error(`HTTP ${String(response.status)}`)
  }

  return (await response.json()) as PnlResponse
}
