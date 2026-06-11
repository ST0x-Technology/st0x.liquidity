import { describe, expect, it } from 'vitest'

import type { HedgeCycleReport } from '$lib/api/HedgeCycleReport'
import type { LatencyBucket } from '$lib/api/LatencyBucket'
import type { LatencyStats } from '$lib/api/LatencyStats'

import { layoutPercentileSeries, layoutWaterfall } from './charts'

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
    })

    expect(rows.map((row) => row.id)).toEqual(['later', 'earlier'])
  })

  it('renders a pending cycle without submission or execution segments', () => {
    const pending = cycle({
      status: 'pending',
      submittedAt: null,
      completedAt: null,
    })

    const rows = layoutWaterfall([pending], {
      plotWidth: 100,
      sort: 'slowest',
      maxRows: 10,
    })

    expect(rows[0]?.segments.map((segment) => segment.name)).toEqual(['unhedged'])
  })

  it('renders a submitted-but-not-completed cycle with unhedged and submission segments only', () => {
    const submitted = cycle({
      status: 'pending',
      // placedAt: '2026-06-01T00:00:10Z' (from defaults)
      // earliestFillBlockTimestamp: '2026-06-01T00:00:00Z' (from defaults) -> unhedged = 10s
      submittedAt: '2026-06-01T00:00:15Z', // submission = 5s
      completedAt: null,
    })

    const rows = layoutWaterfall([submitted], {
      plotWidth: 100,
      sort: 'slowest',
      maxRows: 10,
    })

    expect(rows[0]?.segments.map((segment) => segment.name)).toEqual(['unhedged', 'submission'])
    expect(rows[0]?.segments[1]?.ms).toBe(5_000)
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
