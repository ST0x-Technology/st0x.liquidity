import type { HedgeLatencies } from '$lib/api/HedgeLatencies'
import type { RebalanceTimings } from '$lib/api/RebalanceTimings'
import type { ReliabilityReport } from '$lib/api/ReliabilityReport'
import { getApiBaseUrl } from '$lib/env'

const PERFORMANCE_TIMEOUT_MS = 15_000

export type PerformanceRange = {
  from?: Date
  to?: Date
}

/** Exported for tests: the cards' time window depends on these params. */
export const rangeParams = (range: PerformanceRange): string => {
  const params = new URLSearchParams()

  if (range.from) {
    params.set('from', range.from.toISOString())
  }

  if (range.to) {
    params.set('to', range.to.toISOString())
  }

  const encoded = params.toString()
  return encoded === '' ? '' : `?${encoded}`
}

const fetchPerformanceJson = async <Response>(
  path: string,
  range: PerformanceRange,
): Promise<Response> => {
  const response = await fetch(`${getApiBaseUrl()}${path}${rangeParams(range)}`, {
    signal: AbortSignal.timeout(PERFORMANCE_TIMEOUT_MS),
  })

  if (!response.ok) {
    throw new Error(`HTTP ${String(response.status)}`, {
      cause: {
        status: response.status,
        statusText: response.statusText,
        url: response.url,
      },
    })
  }

  return response.json() as Promise<Response>
}

export const fetchHedgeLatencies = async (
  range: PerformanceRange = {},
): Promise<HedgeLatencies> => fetchPerformanceJson('/performance/latencies', range)

export const fetchRebalanceTimings = async (
  range: PerformanceRange = {},
): Promise<RebalanceTimings> => fetchPerformanceJson('/performance/rebalances', range)

export const fetchReliabilityReport = async (
  range: PerformanceRange = {},
): Promise<ReliabilityReport> => fetchPerformanceJson('/performance/reliability', range)
