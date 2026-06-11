import { describe, expect, it } from 'vitest'

import type { HedgeLatencies } from '$lib/api/HedgeLatencies'
import type { JobQueueHealth } from '$lib/api/JobQueueHealth'
import type { LatencyStats } from '$lib/api/LatencyStats'
import type { ReliabilityReport } from '$lib/api/ReliabilityReport'

import { detectionCard, errorsCard, exposureCard, openExposureCard } from './cards'

const makeStats = (overrides: Partial<LatencyStats> = {}): LatencyStats => ({
  p50Ms: 5_000,
  p90Ms: 20_000,
  p95Ms: 25_000,
  p99Ms: 60_000,
  maxMs: 61_000,
  sampleCount: 12,
  ...overrides,
})

const latencies = (overrides: Partial<HedgeLatencies>): HedgeLatencies => ({
  summary: {
    fillCount: 12,
    stages: {
      detection: makeStats(),
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
  logEntriesTruncated: false,
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

  it('reports good with no-fills secondary when detection stats are null', () => {
    const card = detectionCard(
      latencies({
        summary: {
          fillCount: 0,
          stages: {
            detection: null,
            decision: null,
            submission: null,
            execution: null,
            exposureWindow: null,
          },
        },
      }),
    )

    expect(card.status).toBe('good')
    expect(card.secondary).toBe('no fills in window')
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

  it('reports critical when any queue has killed jobs', () => {
    const queue: JobQueueHealth = {
      jobType: 'DexTradeAccounting',
      pending: 0,
      running: 0,
      done: 100,
      failed: 0,
      awaitingRetry: 0,
      killed: 2,
      retried: 0,
      oldestPendingRunAt: null,
    }

    const card = errorsCard(reliability({ jobQueues: [queue] }), 24)

    expect(card.status).toBe('critical')
    expect(card.secondary).toContain('1 queue(s) killed')
  })

  it('reports warning when a queue has failed jobs but none killed', () => {
    const queue: JobQueueHealth = {
      jobType: 'DexTradeAccounting',
      pending: 0,
      running: 0,
      done: 100,
      failed: 3,
      awaitingRetry: 0,
      killed: 0,
      retried: 0,
      oldestPendingRunAt: null,
    }

    const card = errorsCard(reliability({ jobQueues: [queue] }), 24)

    expect(card.status).toBe('warning')
    expect(card.secondary).toContain('1 queue(s) failed')
  })

  it('reports good when all queues are healthy', () => {
    const queue: JobQueueHealth = {
      jobType: 'DexTradeAccounting',
      pending: 0,
      running: 1,
      done: 100,
      failed: 0,
      awaitingRetry: 0,
      killed: 0,
      retried: 0,
      oldestPendingRunAt: null,
    }

    const card = errorsCard(reliability({ jobQueues: [queue] }), 24)

    expect(card.status).toBe('good')
    expect(card.secondary).not.toContain('queue')
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

  it('clamps a negative age to zero when block timestamp is ahead of client clock', () => {
    // Block timestamp 1 minute in the future relative to `now`.
    const now = new Date('2026-06-01T01:00:00Z')
    const card = openExposureCard(
      latencies({
        openExposures: [
          {
            symbol: 'AAPL',
            fillCount: 1,
            oldestFillBlockTimestamp: '2026-06-01T01:01:00Z',
          },
        ],
      }),
      now,
    )

    expect(card.primary).toBe('0ms')
    expect(card.status).toBe('good')
  })
})

describe('exposureCard', () => {
  it('reports unknown when report is null', () => {
    const card = exposureCard(null)

    expect(card.status).toBe('unknown')
    expect(card.primary).toBe('—')
  })

  it('reports good with no-completed-hedges secondary when exposureWindow stats are null', () => {
    const card = exposureCard(latencies({}))

    expect(card.status).toBe('good')
    expect(card.secondary).toBe('no completed hedges in window')
  })

  it('classifies p95 within good threshold as good and shows p95/p50 display', () => {
    const card = exposureCard(
      latencies({
        summary: {
          fillCount: 5,
          stages: {
            detection: null,
            decision: null,
            submission: null,
            execution: null,
            exposureWindow: {
              p50Ms: 10_000,
              p90Ms: 40_000,
              p95Ms: 50_000,
              p99Ms: 55_000,
              maxMs: 60_000,
              sampleCount: 5,
            },
          },
        },
      }),
    )

    expect(card.status).toBe('good')
    expect(card.primary).toBe('p95 50.0s')
    expect(card.secondary).toContain('p50 10.0s')
    expect(card.secondary).toContain('5 hedges')
  })

  it('classifies p95 above warning threshold as critical', () => {
    // EXPOSURE_WINDOW_THRESHOLDS.warning = 300_000ms; above is critical.
    const card = exposureCard(
      latencies({
        summary: {
          fillCount: 3,
          stages: {
            detection: null,
            decision: null,
            submission: null,
            execution: null,
            exposureWindow: {
              p50Ms: 200_000,
              p90Ms: 310_000,
              p95Ms: 400_000,
              p99Ms: 450_000,
              maxMs: 500_000,
              sampleCount: 3,
            },
          },
        },
      }),
    )

    expect(card.status).toBe('critical')
  })
})
