import { describe, expect, it } from 'vitest'

import type { ChainBlockLag } from '$lib/api/ChainBlockLag'
import type { HedgeLatencies } from '$lib/api/HedgeLatencies'
import type { InfraReport } from '$lib/api/InfraReport'
import type { JobQueueHealth } from '$lib/api/JobQueueHealth'
import type { LatencyStats } from '$lib/api/LatencyStats'
import type { PollHealth } from '$lib/api/PollHealth'
import type { ReliabilityReport } from '$lib/api/ReliabilityReport'

import {
  type SloCard,
  blockLagCards,
  detectionCard,
  errorsCard,
  exposureCard,
  openExposureCard,
} from './cards'

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

const lagSeries = (overrides: Partial<ChainBlockLag>): ChainBlockLag => ({
  chain: 'base',
  currentLagBlocks: 5,
  currentLagSampledAt: '2026-06-01T00:00:00Z',
  points: [],
  ...overrides,
})

/** A Base-only infra report; `poll` overrides the primary chain's poll health. */
const infra = (overrides: Partial<ChainBlockLag>, poll?: Partial<PollHealth>): InfraReport => ({
  monitor: {
    blockLag: [lagSeries(overrides)],
    poll: {
      cycles: 100,
      errors: 0,
      skippedTicks: 0,
      duration: null,
      ...poll,
    },
  },
  dependencies: [],
})

/** The one card a single-chain report renders. */
const onlyCard = (cards: SloCard[]): SloCard => {
  const [card, ...rest] = cards
  if (!card || rest.length > 0) {
    throw new Error(`expected exactly one card, got ${String(cards.length)}`)
  }
  return card
}

describe('blockLagCards', () => {
  // One minute after the helper's currentLagSampledAt: fresh.
  const freshNow = new Date('2026-06-01T00:01:00Z')

  it('reports unknown while the report has not loaded', () => {
    const card = onlyCard(blockLagCards(null, freshNow))

    expect(card.status).toBe('unknown')
    expect(card.primary).toBe('—')
  })

  it('reports unknown before the first refresh sets a clock', () => {
    // The guard is `!report || !now`; this exercises the null-`now` branch
    // independently of the null-report case above.
    const card = onlyCard(blockLagCards(infra({}), null))

    expect(card.status).toBe('unknown')
    expect(card.primary).toBe('—')
  })

  it('reports unknown when no checkpointed sample exists yet', () => {
    const card = onlyCard(blockLagCards(
      infra({
        currentLagBlocks: null,
        currentLagSampledAt: null,
      }),
      freshNow,
    ))

    expect(card.status).toBe('unknown')
    expect(card.secondary).toBe('no checkpointed samples yet')
  })

  it('reports degraded when the latest cutoff is unavailable', () => {
    const card = onlyCard(blockLagCards(
      infra({
        currentLagBlocks: null,
        currentLagSampledAt: '2026-06-01T00:00:30Z'
      }),
      freshNow
    ))

    expect(card.status).toBe('warning')
    expect(card.primary).toBe('—')
    expect(card.secondary).toContain('cutoff unavailable')
  })

  it('classifies the current lag against the block-lag thresholds', () => {
    const card = onlyCard(blockLagCards(infra({}), freshNow))

    expect(card.status).toBe('good')
    expect(card.primary).toBe('5 blocks')
    expect(card.secondary).toContain('sampled 1m 00s ago')
  })

  it('degrades to critical on a large lag and surfaces skipped ticks', () => {
    const card = onlyCard(blockLagCards(
      infra({ currentLagBlocks: 2_000 }, { errors: 1, skippedTicks: 7 }),
      freshNow,
    ))

    expect(card.status).toBe('critical')
    expect(card.primary).toBe('2000 blocks')
    expect(card.secondary).toContain('7 skipped ticks')
  })

  it('classifies a mid-range lag as warning', () => {
    // Between the good (30) and warning (300) thresholds.
    const card = onlyCard(blockLagCards(infra({ currentLagBlocks: 100 }), freshNow))

    expect(card.status).toBe('warning')
    expect(card.primary).toBe('100 blocks')
  })

  it('degrades a healthy-looking lag to warning when the sample is stale', () => {
    // A wedged monitor stops sampling: the frozen lag value must not keep
    // rendering green.
    const card = onlyCard(blockLagCards(infra({}), new Date('2026-06-01T03:00:00Z')))

    expect(card.status).toBe('warning')
    expect(card.secondary).toContain('STALE')
    expect(card.secondary).toContain('3h 00m ago')
  })

  it('keeps a stale sample critical when the frozen lag was already critical', () => {
    // Staleness floors the card at warning but must never downgrade a
    // reading that was already worse.
    const card = onlyCard(blockLagCards(
      infra({ currentLagBlocks: 2_000 }),
      new Date('2026-06-01T03:00:00Z'),
    ))

    expect(card.status).toBe('critical')
    expect(card.secondary).toContain('STALE')
  })
})

describe('blockLagCards per chain', () => {
  const freshNow = new Date('2026-06-01T00:01:00Z')

  it('renders one card per watched chain, each judged on its own lag', () => {
    const report = infra({}, { skippedTicks: 3 })
    report.monitor.blockLag.push(lagSeries({ chain: 'ethereum', currentLagBlocks: 2_000 }))

    const cards = blockLagCards(report, freshNow)

    expect(cards.map((card) => card.title)).toEqual(['Block lag · Base', 'Block lag · Ethereum'])
    expect(cards.map((card) => card.status)).toEqual(['good', 'critical'])
    expect(cards[1]?.primary).toBe('2000 blocks')
  })

  it('attributes skipped ticks to the primary chain only', () => {
    const report = infra({}, { skippedTicks: 3 })
    report.monitor.blockLag.push(lagSeries({ chain: 'ethereum' }))

    const [base, ethereum] = blockLagCards(report, freshNow)

    expect(base?.secondary).toContain('3 skipped ticks')
    expect(ethereum?.secondary).not.toContain('skipped ticks')
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
