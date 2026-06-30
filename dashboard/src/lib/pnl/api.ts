import { getApiBaseUrl } from '$lib/env'
import { FETCH_TIMEOUT_MS } from '$lib/time'
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
  const response = await fetch(`${getApiBaseUrl()}/pnl?${buildPnlParams(query).toString()}`, {
    signal: AbortSignal.timeout(FETCH_TIMEOUT_MS)
  })

  if (!response.ok) {
    const message = (await response.text()).trim()
    throw new Error(message === '' ? `HTTP ${String(response.status)}` : message)
  }

  const contentType = response.headers.get('content-type') ?? ''
  if (!contentType.includes('application/json')) {
    throw new Error('Backend /pnl returned a non-JSON response')
  }

  return (await response.json()) as PnlResponse
}
