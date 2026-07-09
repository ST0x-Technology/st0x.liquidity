import { getApiBaseUrl } from '$lib/env'
import type { PnlCounterTradingFilter, PnlMarketSessionFilter, PnlResponse } from '$lib/pnl/report'

const PNL_FETCH_TIMEOUT_MS = 60000
const PNL_PAGE_LIMIT = 5000

export type PnlQuery = {
  limit: number
  offset: number
  asOfRowid?: number
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

  if (query.asOfRowid !== undefined) {
    params.set('asOfRowid', String(query.asOfRowid))
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
    signal: AbortSignal.timeout(PNL_FETCH_TIMEOUT_MS)
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

export const fetchCompletePnlReport = async (
  query: Omit<PnlQuery, 'limit' | 'offset'>
): Promise<PnlResponse> => {
  const firstPage = await fetchPnlReport({
    ...query,
    limit: PNL_PAGE_LIMIT,
    offset: 0
  })
  const entries = [...firstPage.entries]
  let lastPage = firstPage
  const asOfRowid = firstPage.asOfRowid

  while (lastPage.hasMore) {
    const pageQuery: PnlQuery = {
      ...query,
      limit: PNL_PAGE_LIMIT,
      offset: entries.length
    }
    if (asOfRowid !== undefined) {
      pageQuery.asOfRowid = asOfRowid
    }

    lastPage = await fetchPnlReport(pageQuery)

    if (lastPage.total !== firstPage.total) {
      throw new Error('Backend /pnl pagination total changed while fetching complete report')
    }

    if (lastPage.entries.length === 0) {
      throw new Error('Backend /pnl pagination returned no entries before total was reached')
    }

    entries.push(...lastPage.entries)
  }

  if (entries.length !== firstPage.total) {
    throw new Error('Backend /pnl pagination stopped before total was reached')
  }

  return {
    ...firstPage,
    entries,
    hasMore: lastPage.hasMore
  }
}
