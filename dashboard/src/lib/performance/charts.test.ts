import { describe, expect, it } from 'vitest'

import type { HedgeCycleReport } from '$lib/api/HedgeCycleReport'
import type { LatencyBucket } from '$lib/api/LatencyBucket'
import type { LatencyStats } from '$lib/api/LatencyStats'
import type { RebalanceOperationTiming } from '$lib/api/RebalanceOperationTiming'

import {
  layoutAttestationTrend,
  layoutBlockLagTrend,
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

  it('renders an incomplete cycle as a single unhedged live-exposure segment', () => {
    const submitted = cycle({
      status: 'pending',
      // earliestFillBlockTimestamp default '2026-06-01T00:00:00Z'. A cycle with
      // no terminal state (completedAt null) is live exposure: the whole elapsed
      // window (now - earliest fill) counts as unhedged so it ranks correctly in
      // the slowest sort, regardless of whether it was already submitted.
      submittedAt: '2026-06-01T00:00:15Z',
      completedAt: null,
    })

    const rows = layoutWaterfall([submitted], {
      plotWidth: 100,
      sort: 'slowest',
      maxRows: 10,
      now: new Date('2026-06-02T00:00:00Z'),
    })

    expect(rows[0]?.segments.map((segment) => segment.name)).toEqual(['unhedged'])
    expect(rows[0]?.segments[0]?.ms).toBe(86_400_000)
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
      outcome: 'succeeded',
    },
    {
      stage: 'attestation',
      startedAt: '2026-06-01T00:01:00Z',
      endedAt: '2026-06-01T00:11:00Z',
      durationMs: 600_000,
      outcome: 'succeeded',
    },
    {
      stage: 'deposit',
      startedAt: '2026-06-01T00:11:30Z',
      endedAt: null,
      durationMs: null,
      outcome: 'unmeasured',
    },
  ],
  totalMs: 730_000,
  ...overrides,
})

const NOW_REBALANCE = new Date('2026-06-01T00:12:10Z')

describe('layoutRebalanceBars', () => {
  it('lays out completed stages proportionally and skips open ones', () => {
    const rows = layoutRebalanceBars([operation({})], {
      plotWidth: 100,
      maxRows: 10,
      now: NOW_REBALANCE,
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

  it('uses now - startedAt as totalMs for in-progress operations', () => {
    // Operation started at T+0; now is T+3600s = 3600000ms.
    const startedAt = '2026-06-01T00:00:00Z'
    const now = new Date('2026-06-01T01:00:00Z')

    const rows = layoutRebalanceBars(
      [
        operation({
          startedAt,
          completedAt: null,
          status: 'in_progress',
          totalMs: null,
          stages: [
            {
              stage: 'withdrawal',
              startedAt,
              endedAt: null,
              durationMs: null,
              outcome: 'unmeasured',
            },
          ],
        }),
      ],
      {
        plotWidth: 100,
        maxRows: 10,
        now,
      },
    )

    // Elapsed = now - startedAt = 3600000ms. No completed stages.
    expect(rows[0]?.totalMs).toBe(3_600_000)
    // The open stage has durationMs null so it is filtered out of segments.
    expect(rows[0]?.segments).toHaveLength(0)
  })

  it('falls back to now - startedAt when totalMs is null and startedAt equals now', () => {
    // totalMs: null with one closed stage — fallback is now - startedAt.
    // When now equals startedAt the elapsed basis is zero, so totalMs is 0.
    const startedAt = '2026-06-01T00:00:00Z'
    const now = new Date('2026-06-01T00:00:00Z')

    const rows = layoutRebalanceBars(
      [
        operation({
          startedAt,
          totalMs: null,
          stages: [
            {
              stage: 'withdrawal',
              startedAt,
              endedAt: '2026-06-01T00:00:30Z',
              durationMs: 30_000,
              outcome: 'succeeded',
            },
          ],
        }),
      ],
      {
        plotWidth: 100,
        maxRows: 10,
        now,
      },
    )

    // now - startedAt = 0, so totalMs = 0. The clamped result is 0.
    expect(rows[0]?.totalMs).toBe(0)
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
        now: NOW_REBALANCE,
      },
    )

    expect(rows.map((row) => row.id)).toEqual(['newest', 'older'])
  })

  it('marks a failed stage as a failed segment', () => {
    const rows = layoutRebalanceBars(
      [
        operation({
          status: 'failed',
          completedAt: null,
          totalMs: null,
          stages: [
            {
              stage: 'burn',
              startedAt: '2026-06-01T00:00:00Z',
              endedAt: '2026-06-01T00:00:30Z',
              durationMs: 30_000,
              outcome: 'failed',
            },
          ],
        }),
      ],
      {
        plotWidth: 100,
        maxRows: 10,
        now: NOW_REBALANCE,
      },
    )

    expect(rows[0]?.segments[0]?.failed).toBe(true)
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
    // The only sample is also the max, so y = plotHeight - (max/max)*plotHeight = 0.
    expect(layout.points[0]?.y).toBeCloseTo(0)
    expect(layout.maxMs).toBe(300_000)
    expect(layout.path.length).toBeGreaterThan(0)
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

describe('layoutBlockLagTrend', () => {
  it('centers a single bucket', () => {
    const layout = layoutBlockLagTrend(
      [
        {
          start: '2026-06-01T00:00:00Z',
          maxLagBlocks: 12,
        },
      ],
      {
        plotWidth: 100,
        plotHeight: 50,
      },
    )

    expect(layout.points[0]?.x).toBeCloseTo(50)
    expect(layout.maxLagBlocks).toBe(12)
  })

  it('centers a single zero-lag bucket on the baseline', () => {
    // The common just-started state: one bucket, perfectly caught up. Hits
    // the single-bucket centering and the divide-by-zero floor together.
    const layout = layoutBlockLagTrend(
      [
        {
          start: '2026-06-01T00:00:00Z',
          maxLagBlocks: 0,
        },
      ],
      {
        plotWidth: 100,
        plotHeight: 50,
      },
    )

    expect(layout.points[0]?.x).toBeCloseTo(50)
    expect(layout.points[0]?.y).toBe(50)
    expect(layout.maxLagBlocks).toBe(0)
  })

  it('scales lag to the worst bucket', () => {
    const layout = layoutBlockLagTrend(
      [
        {
          start: '2026-06-01T00:00:00Z',
          maxLagBlocks: 10,
        },
        {
          start: '2026-06-01T01:00:00Z',
          maxLagBlocks: 40,
        },
      ],
      {
        plotWidth: 100,
        plotHeight: 50,
      },
    )

    expect(layout.maxLagBlocks).toBe(40)
    expect(layout.points[0]?.x).toBeCloseTo(0)
    expect(layout.points[0]?.y).toBeCloseTo(37.5)
    expect(layout.points[1]?.y).toBeCloseTo(0)
    expect(layout.points[1]?.x).toBeCloseTo(100)
  })

  it('keeps a flat zero-lag series on the baseline without dividing by zero', () => {
    const layout = layoutBlockLagTrend(
      [
        {
          start: '2026-06-01T00:00:00Z',
          maxLagBlocks: 0,
        },
        {
          start: '2026-06-01T01:00:00Z',
          maxLagBlocks: 0,
        },
      ],
      {
        plotWidth: 100,
        plotHeight: 50,
      },
    )

    // The reported maximum is the true series maximum, not the y-scale's
    // divide-by-zero floor: a healthy panel must read "max 0 blocks".
    expect(layout.maxLagBlocks).toBe(0)
    expect(layout.points.every((point) => point.y === 50)).toBe(true)
  })

  it('returns an empty layout for no buckets', () => {
    const layout = layoutBlockLagTrend([], {
      plotWidth: 100,
      plotHeight: 50,
    })

    expect(layout.points).toEqual([])
    expect(layout.path).toBe('')
    expect(layout.maxLagBlocks).toBe(0)
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
    const plotHeight = 50
    // maxMs = 800 (largest p99 across both buckets)
    const layout = layoutPercentileSeries(
      [
        bucket('2026-06-01T00:00:00Z', stats(100, 200, 400)),
        bucket('2026-06-02T00:00:00Z', stats(150, 300, 800)),
      ],
      'detection',
      {
        plotWidth: 100,
        plotHeight,
        maxXLabels: 6,
      },
    )

    expect(layout.maxMs).toBe(800)
    expect(layout.lines).toHaveLength(3)

    // y = plotHeight - (ms / maxMs) * plotHeight, maxMs = 800
    const p99 = layout.lines.find((line) => line.percentile === 'p99Ms')
    expect(p99?.points[1]?.y).toBeCloseTo(0)
    expect(p99?.points[0]?.y).toBeCloseTo(25)
    expect(p99?.points[0]?.x).toBeCloseTo(0)
    expect(p99?.points[1]?.x).toBeCloseTo(100)

    // bucket 0: p50=100, p90=200; bucket 1: p50=150, p90=300 — all scaled to maxMs=800
    const p50 = layout.lines.find((line) => line.percentile === 'p50Ms')
    // bucket 0: y = 50 - (100/800)*50 = 50 - 6.25 = 43.75
    expect(p50?.points[0]?.y).toBeCloseTo(43.75)
    // bucket 1: y = 50 - (150/800)*50 = 50 - 9.375 = 40.625
    expect(p50?.points[1]?.y).toBeCloseTo(40.625)

    const p90 = layout.lines.find((line) => line.percentile === 'p90Ms')
    // bucket 0: y = 50 - (200/800)*50 = 50 - 12.5 = 37.5
    expect(p90?.points[0]?.y).toBeCloseTo(37.5)
    // bucket 1: y = 50 - (300/800)*50 = 50 - 18.75 = 31.25
    expect(p90?.points[1]?.y).toBeCloseTo(31.25)
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

  it('always includes the last bucket label even when it does not fall on a step boundary', () => {
    // 5 buckets, maxXLabels=2 -> labelStep = ceil(5/2) = 3
    // Step-aligned indices: 0, 3. Index 4 (last) is NOT on a step -> must still appear.
    const buckets = [
      bucket('2026-06-01T00:00:00Z', stats(100, 200, 400)),
      bucket('2026-06-02T00:00:00Z', stats(100, 200, 400)),
      bucket('2026-06-03T00:00:00Z', stats(100, 200, 400)),
      bucket('2026-06-04T00:00:00Z', stats(100, 200, 400)),
      bucket('2026-06-05T00:00:00Z', stats(100, 200, 400)),
    ]

    const layout = layoutPercentileSeries(buckets, 'detection', {
      plotWidth: 100,
      plotHeight: 50,
      maxXLabels: 2,
    })

    // Expect 3 labels: indices 0, 3, and 4 (last)
    expect(layout.xLabels).toHaveLength(3)
    // Last label x should equal the rightmost point (index 4 out of 4 -> x=100)
    expect(layout.xLabels.at(-1)?.x).toBeCloseTo(100)
  })
})
