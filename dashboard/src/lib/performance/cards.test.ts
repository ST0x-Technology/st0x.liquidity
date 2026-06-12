import { describe, expect, it } from 'vitest'

import type { HedgeLatencies } from '$lib/api/HedgeLatencies'
import type { InfraReport } from '$lib/api/InfraReport'
import type { LatencyStats } from '$lib/api/LatencyStats'
import type { ReliabilityReport } from '$lib/api/ReliabilityReport'

import { blockLagCard, detectionCard, errorsCard, openExposureCard } from './cards'

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

const infra = (overrides: Partial<InfraReport['monitor']>): InfraReport => ({
  monitor: {
    currentLagBlocks: 5,
    currentLagSampledAt: '2026-06-01T00:00:00Z',
    blockLag: [],
    poll: {
      cycles: 100,
      errors: 0,
      skippedTicks: 0,
      duration: null,
    },
    ...overrides,
  },
})

describe('blockLagCard', () => {
  // One minute after the helper's currentLagSampledAt: fresh.
  const freshNow = new Date('2026-06-01T00:01:00Z')

  it('reports unknown while the report has not loaded', () => {
    const card = blockLagCard(null, freshNow)

    expect(card.status).toBe('unknown')
    expect(card.primary).toBe('—')
  })

  it('reports unknown before the first refresh sets a clock', () => {
    // The guard is `!report || !now`; this exercises the null-`now` branch
    // independently of the null-report case above.
    const card = blockLagCard(infra({}), null)

    expect(card.status).toBe('unknown')
    expect(card.primary).toBe('—')
  })

  it('reports unknown when no checkpointed sample exists yet', () => {
    const card = blockLagCard(
      infra({
        currentLagBlocks: null,
        currentLagSampledAt: null,
      }),
      freshNow,
    )

    expect(card.status).toBe('unknown')
    expect(card.secondary).toBe('no checkpointed samples yet')
  })

  it('classifies the current lag against the block-lag thresholds', () => {
    const card = blockLagCard(infra({}), freshNow)

    expect(card.status).toBe('good')
    expect(card.primary).toBe('5 blocks')
    expect(card.secondary).toContain('sampled 1m 00s ago')
  })

  it('degrades to critical on a large lag and surfaces skipped ticks', () => {
    const card = blockLagCard(
      infra({
        currentLagBlocks: 2_000,
        poll: {
          cycles: 100,
          errors: 1,
          skippedTicks: 7,
          duration: null,
        },
      }),
      freshNow,
    )

    expect(card.status).toBe('critical')
    expect(card.primary).toBe('2000 blocks')
    expect(card.secondary).toContain('7 skipped ticks')
  })

  it('classifies a mid-range lag as warning', () => {
    // Between the good (30) and warning (300) thresholds.
    const card = blockLagCard(infra({ currentLagBlocks: 100 }), freshNow)

    expect(card.status).toBe('warning')
    expect(card.primary).toBe('100 blocks')
  })

  it('degrades a healthy-looking lag to warning when the sample is stale', () => {
    // A wedged monitor stops sampling: the frozen lag value must not keep
    // rendering green.
    const card = blockLagCard(infra({}), new Date('2026-06-01T03:00:00Z'))

    expect(card.status).toBe('warning')
    expect(card.secondary).toContain('STALE')
    expect(card.secondary).toContain('3h 00m ago')
  })

  it('keeps a stale sample critical when the frozen lag was already critical', () => {
    // Staleness floors the card at warning but must never downgrade a
    // reading that was already worse.
    const card = blockLagCard(
      infra({ currentLagBlocks: 2_000 }),
      new Date('2026-06-01T03:00:00Z'),
    )

    expect(card.status).toBe('critical')
    expect(card.secondary).toContain('STALE')
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
