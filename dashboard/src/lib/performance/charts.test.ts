import { describe, expect, it } from 'vitest'

import type { HedgeCycleReport } from '$lib/api/HedgeCycleReport'
import type { LatencyBucket } from '$lib/api/LatencyBucket'
import type { LatencyStats } from '$lib/api/LatencyStats'
import type { RebalanceOperationTiming } from '$lib/api/RebalanceOperationTiming'

import {
  layoutAttestationTrend,
  layoutPercentileSeries,
  layoutRebalanceBars,
  layoutWaterfall,
} from './charts'

const cycle = (overrides: Partial<HedgeCycleReport>): HedgeCycleReport => ({
  symbol: 'AAPL',
  offchainOrderId: 'order-1',
  placedAt: '2026-06-01T00:00:10Z',
  coveredFillCount: 1,
  earliestFillBlockTimestamp: '2026-06-01T00:00:00Z',
  submittedAt: '2026-06-01T00:00:12Z',
  status: 'filled',
  completedAt: '2026-06-01T00:00:20Z',
  decisionMs: 5_000,
  submissionMs: 2_000,
  executionMs: 8_000,
  exposureWindowMs: 20_000,
  ...overrides,
})

describe('layoutWaterfall', () => {
  it('splits a filled cycle into unhedged, submission, and execution segments', () => {
    const rows = layoutWaterfall([cycle({})], {
      plotWidth: 100,
      sort: 'slowest',
      maxRows: 10,
      now: new Date('2026-06-02T00:00:00Z'),
    })

    expect(rows).toHaveLength(1)
    expect(rows[0]?.totalMs).toBe(20_000)
    expect(rows[0]?.segments.map((segment) => segment.name)).toEqual([
      'unhedged',
      'submission',
      'execution',
    ])
    expect(rows[0]?.segments.map((segment) => segment.ms)).toEqual([
      10_000, 2_000, 8_000,
    ])
    // Slowest row spans the full plot width.
    const lastSegment = rows[0]?.segments.at(-1)
    expect((lastSegment?.x ?? 0) + (lastSegment?.width ?? 0)).toBeCloseTo(100)
  })

  it('sorts slowest first and caps rows', () => {
    const fast = cycle({
      offchainOrderId: 'fast',
      completedAt: '2026-06-01T00:00:13Z',
    })
    const slow = cycle({
      offchainOrderId: 'slow',
      completedAt: '2026-06-01T00:01:00Z',
    })
    const mid = cycle({
      offchainOrderId: 'mid',
      completedAt: '2026-06-01T00:00:30Z',
    })

    const rows = layoutWaterfall([fast, slow, mid], {
      plotWidth: 100,
      sort: 'slowest',
      maxRows: 2,
      now: new Date('2026-06-02T00:00:00Z'),
    })

    expect(rows.map((row) => row.id)).toEqual(['slow', 'mid'])
  })

  it('sorts newest first by placement time', () => {
    const earlier = cycle({
      offchainOrderId: 'earlier',
      placedAt: '2026-06-01T00:00:10Z',
    })
    const later = cycle({
      offchainOrderId: 'later',
      placedAt: '2026-06-02T00:00:10Z',
    })

    const rows = layoutWaterfall([earlier, later], {
      plotWidth: 100,
      sort: 'newest',
      maxRows: 10,
      now: new Date('2026-06-02T00:00:00Z'),
    })

    expect(rows.map((row) => row.id)).toEqual(['later', 'earlier'])
  })

  it('renders a pending cycle as live exposure up to now', () => {
    const pending = cycle({
      status: 'pending',
      submittedAt: null,
      completedAt: null,
    })

    const rows = layoutWaterfall([pending], {
      plotWidth: 100,
      sort: 'slowest',
      maxRows: 10,
      now: new Date('2026-06-01T00:01:00Z'),
    })

    expect(rows[0]?.segments.map((segment) => segment.name)).toEqual(['unhedged'])
    // Earliest fill was at 00:00:00; the exposure is still open at now.
    expect(rows[0]?.totalMs).toBe(60_000)
  })

  it('returns an empty array for no cycles', () => {
    expect(
      layoutWaterfall([], {
        plotWidth: 100,
        sort: 'slowest',
        maxRows: 10,
        now: new Date('2026-06-02T00:00:00Z'),
      }),
    ).toEqual([])
  })
})

const operation = (
  overrides: Partial<RebalanceOperationTiming>,
): RebalanceOperationTiming => ({
  operationId: 'op-1',
  direction: 'alpaca_to_base',
  amount: '1000',
  startedAt: '2026-06-01T00:00:00Z',
  completedAt: '2026-06-01T00:12:10Z',
  status: 'completed',
  stages: [
    {
      stage: 'withdrawal',
      startedAt: '2026-06-01T00:00:00Z',
      endedAt: '2026-06-01T00:00:30Z',
      durationMs: 30_000,
      failed: false,
    },
    {
      stage: 'attestation',
      startedAt: '2026-06-01T00:01:00Z',
      endedAt: '2026-06-01T00:11:00Z',
      durationMs: 600_000,
      failed: false,
    },
    {
      stage: 'deposit',
      startedAt: '2026-06-01T00:11:30Z',
      endedAt: null,
      durationMs: null,
      failed: false,
    },
  ],
  totalMs: 730_000,
  ...overrides,
})

describe('layoutRebalanceBars', () => {
  it('lays out completed stages proportionally and skips open ones', () => {
    const rows = layoutRebalanceBars([operation({})], {
      plotWidth: 100,
      maxRows: 10,
    })

    expect(rows).toHaveLength(1)
    // The DTO's elapsed time wins over the sum of completed stages so an
    // open stage does not understate the operation.
    expect(rows[0]?.totalMs).toBe(730_000)
    expect(rows[0]?.segments.map((segment) => segment.stage)).toEqual([
      'withdrawal',
      'attestation',
    ])
    const lastSegment = rows[0]?.segments.at(-1)
    // Completed stages cover 630s of the 730s elapsed total; the unfilled
    // remainder represents the still-open deposit stage.
    expect((lastSegment?.x ?? 0) + (lastSegment?.width ?? 0)).toBeCloseTo(
      (630_000 / 730_000) * 100,
    )
  })

  it('caps rows preserving newest-first input order', () => {
    const rows = layoutRebalanceBars(
      [
        operation({ operationId: 'newest' }),
        operation({ operationId: 'older' }),
        operation({ operationId: 'oldest' }),
      ],
      {
        plotWidth: 100,
        maxRows: 2,
      },
    )

    expect(rows.map((row) => row.id)).toEqual(['newest', 'older'])
  })
})

describe('layoutAttestationTrend', () => {
  it('centers a single sample', () => {
    const layout = layoutAttestationTrend(
      [
        {
          burnedAt: '2026-06-01T00:00:00Z',
          durationMs: 300_000,
        },
      ],
      {
        plotWidth: 100,
        plotHeight: 50,
      },
    )

    expect(layout.points[0]?.x).toBeCloseTo(50)
  })

  it('scales durations to the slowest sample', () => {
    const layout = layoutAttestationTrend(
      [
        {
          burnedAt: '2026-06-01T00:00:00Z',
          durationMs: 300_000,
        },
        {
          burnedAt: '2026-06-02T00:00:00Z',
          durationMs: 600_000,
        },
      ],
      {
        plotWidth: 100,
        plotHeight: 50,
      },
    )

    expect(layout.maxMs).toBe(600_000)
    expect(layout.points[0]?.y).toBeCloseTo(25)
    expect(layout.points[1]?.y).toBeCloseTo(0)
    expect(layout.points[1]?.x).toBeCloseTo(100)
  })
})

const stats = (p50: number, p90: number, p99: number): LatencyStats => ({
  p50Ms: p50,
  p90Ms: p90,
  p95Ms: p90,
  p99Ms: p99,
  maxMs: p99,
  sampleCount: 10,
})

const bucket = (start: string, detection: LatencyStats | null): LatencyBucket => ({
  start,
  stages: {
    detection,
    decision: null,
    submission: null,
    execution: null,
    exposureWindow: null,
  },
})

describe('layoutPercentileSeries', () => {
  it('lays out p50/p90/p99 lines scaled to the largest p99', () => {
    const layout = layoutPercentileSeries(
      [
        bucket('2026-06-01T00:00:00Z', stats(100, 200, 400)),
        bucket('2026-06-02T00:00:00Z', stats(150, 300, 800)),
      ],
      'detection',
      {
        plotWidth: 100,
        plotHeight: 50,
        maxXLabels: 6,
      },
    )

    expect(layout.maxMs).toBe(800)
    expect(layout.lines).toHaveLength(3)

    const p99 = layout.lines.find((line) => line.percentile === 'p99Ms')
    expect(p99?.points[1]?.y).toBeCloseTo(0)
    expect(p99?.points[0]?.y).toBeCloseTo(25)
    expect(p99?.points[0]?.x).toBeCloseTo(0)
    expect(p99?.points[1]?.x).toBeCloseTo(100)
  })

  it('skips buckets where the stage has no samples', () => {
    const layout = layoutPercentileSeries(
      [
        bucket('2026-06-01T00:00:00Z', stats(100, 200, 400)),
        bucket('2026-06-02T00:00:00Z', null),
        bucket('2026-06-03T00:00:00Z', stats(100, 200, 400)),
      ],
      'detection',
      {
        plotWidth: 100,
        plotHeight: 50,
        maxXLabels: 6,
      },
    )

    expect(layout.lines[0]?.points).toHaveLength(2)
  })

  it('centers a single bucket', () => {
    const layout = layoutPercentileSeries(
      [bucket('2026-06-01T00:00:00Z', stats(100, 200, 400))],
      'detection',
      {
        plotWidth: 100,
        plotHeight: 50,
        maxXLabels: 6,
      },
    )

    expect(layout.lines[0]?.points[0]?.x).toBeCloseTo(50)
  })
})
