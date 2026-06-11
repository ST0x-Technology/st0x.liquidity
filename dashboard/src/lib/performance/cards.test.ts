import { describe, expect, it } from 'vitest'

import type { HedgeLatencies } from '$lib/api/HedgeLatencies'
import type { LatencyStats } from '$lib/api/LatencyStats'
import type { ReliabilityReport } from '$lib/api/ReliabilityReport'

import { detectionCard, errorsCard, openExposureCard } from './cards'

const stats: LatencyStats = {
  p50Ms: 5_000,
  p90Ms: 20_000,
  p95Ms: 25_000,
  p99Ms: 60_000,
  maxMs: 61_000,
  sampleCount: 12,
}

const latencies = (overrides: Partial<HedgeLatencies>): HedgeLatencies => ({
  summary: {
    fillCount: 12,
    stages: {
      detection: stats,
      decision: null,
      submission: null,
      execution: null,
      exposureWindow: null,
    },
  },
  buckets: [],
  cycles: [],
  totalCycles: 0,
  openExposures: [],
  ...overrides,
})

const reliability = (overrides: Partial<ReliabilityReport>): ReliabilityReport => ({
  logBuckets: [],
  logTargets: [],
  failureEvents: [],
  jobQueues: [],
  ...overrides,
})

describe('detectionCard', () => {
  it('reports unknown while the report has not loaded', () => {
    const card = detectionCard(null)

    expect(card.status).toBe('unknown')
    expect(card.primary).toBe('—')
  })

  it('classifies p95 against the detection thresholds', () => {
    const card = detectionCard(latencies({}))

    expect(card.status).toBe('good')
    expect(card.primary).toBe('p95 25.0s')
  })
})

describe('errorsCard', () => {
  it('forces critical when any lifecycle failure exists', () => {
    const card = errorsCard(
      reliability({
        failureEvents: [
          {
            eventType: 'OffchainOrderEvent::Failed',
            count: 1,
            lastAt: '2026-06-01T00:00:00Z',
          },
        ],
      }),
      24,
    )

    expect(card.status).toBe('critical')
  })

  it('degrades on high warning volume even with zero errors', () => {
    const card = errorsCard(
      reliability({
        logBuckets: [
          {
            start: '2026-06-01T00:00:00Z',
            errors: 0,
            warnings: 1_000,
          },
        ],
      }),
      24,
    )

    expect(card.status).toBe('critical')
    expect(card.primary).toBe('0')
  })

  it('interpolates the window hours into the title', () => {
    expect(errorsCard(reliability({}), 48).title).toBe('Errors (48h)')
  })
})

describe('openExposureCard', () => {
  it('selects the oldest exposure across symbols', () => {
    const now = new Date('2026-06-01T01:00:00Z')
    const card = openExposureCard(
      latencies({
        openExposures: [
          {
            symbol: 'AAPL',
            fillCount: 2,
            oldestFillBlockTimestamp: '2026-06-01T00:50:00Z',
          },
          {
            symbol: 'TSLA',
            fillCount: 3,
            oldestFillBlockTimestamp: '2026-06-01T00:30:00Z',
          },
        ],
      }),
      now,
    )

    expect(card.primary).toBe('30m 00s')
    expect(card.secondary).toBe('TSLA oldest · 5 uncovered fills across 2 symbols')
    // 30m sits exactly on the warning bound (inclusive).
    expect(card.status).toBe('warning')
  })

  it('reports unknown before the first refresh', () => {
    expect(openExposureCard(null, null).status).toBe('unknown')
  })

  it('reports good when every fill is hedged', () => {
    const card = openExposureCard(latencies({}), new Date())

    expect(card.status).toBe('good')
    expect(card.primary).toBe('none')
  })
})
