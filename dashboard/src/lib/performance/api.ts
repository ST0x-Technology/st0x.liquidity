import type { ChainPollHealth } from '$lib/api/ChainPollHealth'
import type { EquityTimings } from '$lib/api/EquityTimings'
import type { HedgeLatencies } from '$lib/api/HedgeLatencies'
import type { InfraReport } from '$lib/api/InfraReport'
import type { MonitorTelemetry } from '$lib/api/MonitorTelemetry'
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

export const fetchEquityTimings = async (
  range: PerformanceRange = {},
): Promise<EquityTimings> => fetchPerformanceJson('/performance/equity-rebalances', range)

export const fetchReliabilityReport = async (
  range: PerformanceRange = {},
): Promise<ReliabilityReport> => fetchPerformanceJson('/performance/reliability', range)

/** The pre-rollout shape of `monitor.poll`: one report, with no chain on it. */
type PreRolloutPollHealth = Omit<ChainPollHealth, 'chain'>

type InfraResponse = Omit<InfraReport, 'monitor'> & {
  monitor: Omit<MonitorTelemetry, 'poll'> & {
    poll: ChainPollHealth[] | PreRolloutPollHealth
  }
}

/**
 * Rollout shim: the dashboard profile activates before st0x-hedge, so a fresh
 * page can read a backend that still sends one poll report for the whole bot.
 * Attribute that report to the primary chain -- the first block-lag series --
 * so the per-chain readers only ever see the list. Delete once every
 * environment runs a backend that sends the list.
 */
const perChainPoll = (response: InfraResponse): InfraReport => {
  const { blockLag, poll } = response.monitor

  if (Array.isArray(poll)) {
    return {
      ...response,
      monitor: {
        blockLag,
        poll,
      },
    }
  }

  const primary = blockLag[0]

  return {
    ...response,
    monitor: {
      blockLag,
      poll:
        primary === undefined
          ? []
          : [
              {
                chain: primary.chain,
                ...poll,
              },
            ],
    },
  }
}

export const fetchInfraReport = async (range: PerformanceRange = {}): Promise<InfraReport> =>
  perChainPoll(await fetchPerformanceJson<InfraResponse>('/performance/infra', range))
