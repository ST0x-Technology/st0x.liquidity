import { describe, expect, it } from 'vitest'

import type { EquityOperationTiming } from '$lib/api/EquityOperationTiming'
import type { HedgeCycleReport } from '$lib/api/HedgeCycleReport'
import type { LatencyBucket } from '$lib/api/LatencyBucket'
import type { RebalanceOperationTiming } from '$lib/api/RebalanceOperationTiming'

import {
  buildEquityRows,
  buildPercentileSeries,
  buildRebalanceRows,
  buildWaterfallRows,
  layoutDependencySparkline,
  thinTicks
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
  ...overrides
})

describe('buildWaterfallRows', () => {
  it('splits a filled cycle into unhedged, submission, and execution fields', () => {
    const rows = buildWaterfallRows([cycle({})], {
      sort: 'slowest',
      maxRows: 10,
      now: new Date('2026-06-02T00:00:00Z')
    })

    expect(rows).toHaveLength(1)
    expect(rows[0]?.totalMs).toBe(20_000)
    expect(rows[0]?.unhedged).toBe(10_000)
    expect(rows[0]?.submission).toBe(2_000)
    expect(rows[0]?.executionOk).toBe(8_000)
    expect(rows[0]?.executionFailed).toBe(0)
  })

  it('routes a failed cycle execution segment to executionFailed, not executionOk', () => {
    const rows = buildWaterfallRows([cycle({ status: 'failed' })], {
      sort: 'slowest',
      maxRows: 10,
      now: new Date('2026-06-02T00:00:00Z')
    })

    expect(rows[0]?.executionOk).toBe(0)
    expect(rows[0]?.executionFailed).toBe(8_000)
  })

  it('sorts slowest first and caps rows', () => {
    const fast = cycle({
      offchainOrderId: 'fast',
      completedAt: '2026-06-01T00:00:13Z'
    })
    const slow = cycle({
      offchainOrderId: 'slow',
      completedAt: '2026-06-01T00:01:00Z'
    })
    const mid = cycle({
      offchainOrderId: 'mid',
      completedAt: '2026-06-01T00:00:30Z'
    })

    const rows = buildWaterfallRows([fast, slow, mid], {
      sort: 'slowest',
      maxRows: 2,
      now: new Date('2026-06-02T00:00:00Z')
    })

    expect(rows.map((row) => row.id)).toEqual(['slow', 'mid'])
  })

  it('sorts newest first by placement time', () => {
    const earlier = cycle({
      offchainOrderId: 'earlier',
      placedAt: '2026-06-01T00:00:10Z'
    })
    const later = cycle({
      offchainOrderId: 'later',
      placedAt: '2026-06-02T00:00:10Z'
    })

    const rows = buildWaterfallRows([earlier, later], {
      sort: 'newest',
      maxRows: 10,
      now: new Date('2026-06-02T00:00:00Z')
    })

    expect(rows.map((row) => row.id)).toEqual(['later', 'earlier'])
  })

  it('renders a pending cycle as live exposure up to now', () => {
    const pending = cycle({
      status: 'pending',
      submittedAt: null,
      completedAt: null
    })

    const rows = buildWaterfallRows([pending], {
      sort: 'slowest',
      maxRows: 10,
      now: new Date('2026-06-01T00:01:00Z')
    })

    expect(rows[0]?.unhedged).toBe(60_000)
    expect(rows[0]?.submission).toBe(0)
    expect(rows[0]?.executionOk).toBe(0)
    expect(rows[0]?.executionFailed).toBe(0)
    // Earliest fill was at 00:00:00; the exposure is still open at now.
    expect(rows[0]?.totalMs).toBe(60_000)
  })

  it('returns an empty array for no cycles', () => {
    expect(
      buildWaterfallRows([], {
        sort: 'slowest',
        maxRows: 10,
        now: new Date('2026-06-02T00:00:00Z')
      })
    ).toEqual([])
  })

  it('renders an incomplete cycle as a single unhedged live-exposure field', () => {
    const submitted = cycle({
      status: 'pending',
      // earliestFillBlockTimestamp default '2026-06-01T00:00:00Z'. A cycle with
      // no terminal state (completedAt null) is live exposure: the whole elapsed
      // window (now - earliest fill) counts as unhedged so it ranks correctly in
      // the slowest sort, regardless of whether it was already submitted.
      submittedAt: '2026-06-01T00:00:15Z',
      completedAt: null
    })

    const rows = buildWaterfallRows([submitted], {
      sort: 'slowest',
      maxRows: 10,
      now: new Date('2026-06-02T00:00:00Z')
    })

    expect(rows[0]?.unhedged).toBe(86_400_000)
    expect(rows[0]?.submission).toBe(0)
  })
})

const operation = (overrides: Partial<RebalanceOperationTiming>): RebalanceOperationTiming => ({
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
      outcome: 'succeeded'
    },
    {
      stage: 'attestation',
      startedAt: '2026-06-01T00:01:00Z',
      endedAt: '2026-06-01T00:11:00Z',
      durationMs: 600_000,
      outcome: 'succeeded'
    },
    {
      stage: 'deposit',
      startedAt: '2026-06-01T00:11:30Z',
      endedAt: null,
      durationMs: null,
      outcome: 'unmeasured'
    }
  ],
  totalMs: 730_000,
  ...overrides
})

const NOW_REBALANCE = new Date('2026-06-01T00:12:10Z')

describe('buildRebalanceRows', () => {
  it('shapes completed stages into named fields and skips open ones', () => {
    const rows = buildRebalanceRows([operation({})], {
      maxRows: 10,
      now: NOW_REBALANCE
    })

    expect(rows).toHaveLength(1)
    // The DTO's elapsed time wins over the sum of completed stages so an
    // open stage does not understate the operation.
    expect(rows[0]?.totalMs).toBe(730_000)
    expect(rows[0]?.withdrawalOk).toBe(30_000)
    expect(rows[0]?.attestationOk).toBe(600_000)
    // The still-open deposit stage (durationMs null) contributes nothing.
    expect(rows[0]?.depositOk).toBe(0)
    expect(rows[0]?.depositFailed).toBe(0)
  })

  it('uses now - startedAt as totalMs for in-progress operations', () => {
    // Operation started at T+0; now is T+3600s = 3600000ms.
    const startedAt = '2026-06-01T00:00:00Z'
    const now = new Date('2026-06-01T01:00:00Z')

    const rows = buildRebalanceRows(
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
              outcome: 'unmeasured'
            }
          ]
        })
      ],
      {
        maxRows: 10,
        now
      }
    )

    // Elapsed = now - startedAt = 3600000ms. No completed stages.
    expect(rows[0]?.totalMs).toBe(3_600_000)
    // The open stage has durationMs null so it contributes nothing.
    expect(rows[0]?.withdrawalOk).toBe(0)
    expect(rows[0]?.withdrawalFailed).toBe(0)
    // No completed stage accounts for any of the elapsed time, so the whole
    // elapsed window must surface as a visible unmeasured remainder rather
    // than rendering as an invisible zero-length bar.
    expect(rows[0]?.unmeasuredMs).toBe(3_600_000)
  })

  it('surfaces only the unaccounted-for remainder as unmeasuredMs when some stages completed', () => {
    // Operation started at T+0, one 30s stage completed, now is T+3600s.
    const startedAt = '2026-06-01T00:00:00Z'
    const now = new Date('2026-06-01T01:00:00Z')

    const rows = buildRebalanceRows(
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
              endedAt: '2026-06-01T00:00:30Z',
              durationMs: 30_000,
              outcome: 'succeeded'
            },
            {
              stage: 'attestation',
              startedAt: '2026-06-01T00:00:30Z',
              endedAt: null,
              durationMs: null,
              outcome: 'unmeasured'
            }
          ]
        })
      ],
      {
        maxRows: 10,
        now
      }
    )

    expect(rows[0]?.totalMs).toBe(3_600_000)
    expect(rows[0]?.withdrawalOk).toBe(30_000)
    // 3,600,000 elapsed minus the 30,000ms accounted for by the completed
    // withdrawal stage -- the still-open attestation stage's real elapsed
    // time, made visible instead of silently dropped.
    expect(rows[0]?.unmeasuredMs).toBe(3_570_000)
  })

  it('surfaces an unmeasuredMs remainder for a completed operation whose stages do not fully account for totalMs', () => {
    // A completed, provider-recovered operation can have a real totalMs while
    // one of its stages never got measured -- that elapsed time must still
    // show up as a remainder, not vanish because status isn't 'in_progress'.
    const rows = buildRebalanceRows([operation({})], {
      maxRows: 10,
      now: NOW_REBALANCE
    })

    expect(rows[0]?.status).toBe('completed')
    // 730,000 totalMs minus the 630,000ms accounted for by the withdrawal and
    // attestation stages -- the still-unmeasured deposit stage's real elapsed
    // time.
    expect(rows[0]?.unmeasuredMs).toBe(100_000)
  })

  it('does not fabricate an unmeasuredMs remainder when totalMs is genuinely unmeasured', () => {
    const rows = buildRebalanceRows(
      [
        operation({
          status: 'completed',
          totalMs: null,
          stages: [
            {
              stage: 'withdrawal',
              startedAt: '2026-06-01T00:00:00Z',
              endedAt: '2026-06-01T00:00:30Z',
              durationMs: 30_000,
              outcome: 'succeeded'
            }
          ]
        })
      ],
      {
        maxRows: 10,
        now: NOW_REBALANCE
      }
    )

    expect(rows[0]?.totalMs).toBeNull()
    expect(rows[0]?.unmeasuredMs).toBe(0)
  })

  it('preserves null totalMs for a terminal operator-reconciled operation with a known startedAt', () => {
    // A completed operation with totalMs: null means the backend deliberately
    // suppressed the round-trip metric (e.g. OperatorReconciled) -- it must
    // never be backfilled with now - startedAt, since that would render a
    // fabricated, ever-growing elapsed time as the "total".
    const startedAt = '2026-06-01T00:00:00Z'
    const now = new Date('2026-06-01T00:00:00Z')

    const rows = buildRebalanceRows(
      [
        operation({
          startedAt,
          status: 'completed',
          totalMs: null,
          stages: [
            {
              stage: 'withdrawal',
              startedAt,
              endedAt: '2026-06-01T00:00:30Z',
              durationMs: 30_000,
              outcome: 'succeeded'
            }
          ]
        })
      ],
      {
        maxRows: 10,
        now
      }
    )

    expect(rows[0]?.totalMs).toBeNull()
  })

  it('preserves null totalMs when startedAt is also null, even mid-stream', () => {
    // startedAt: null means the read model first observed the operation
    // mid-stream (no genuine start event). There is no elapsed basis to
    // compute from, so totalMs must stay null rather than fabricate 0ms --
    // even when some stages did report measured durations.
    const rows = buildRebalanceRows(
      [
        operation({
          startedAt: null,
          status: 'completed',
          totalMs: null,
          stages: [
            {
              stage: 'withdrawal',
              startedAt: '2026-06-01T00:00:00Z',
              endedAt: '2026-06-01T00:00:30Z',
              durationMs: 30_000,
              outcome: 'succeeded'
            }
          ]
        })
      ],
      {
        maxRows: 10,
        now: new Date('2026-06-01T00:01:00Z')
      }
    )

    expect(rows[0]?.totalMs).toBeNull()
  })

  it('caps rows preserving newest-first input order', () => {
    const rows = buildRebalanceRows(
      [
        operation({ operationId: 'newest' }),
        operation({ operationId: 'older' }),
        operation({ operationId: 'oldest' })
      ],
      {
        maxRows: 2,
        now: NOW_REBALANCE
      }
    )

    expect(rows.map((row) => row.id)).toEqual(['newest', 'older'])
  })

  it('routes a failed stage to its Failed field, not its Ok field', () => {
    const rows = buildRebalanceRows(
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
              outcome: 'failed'
            }
          ]
        })
      ],
      {
        maxRows: 10,
        now: NOW_REBALANCE
      }
    )

    expect(rows[0]?.burnOk).toBe(0)
    expect(rows[0]?.burnFailed).toBe(30_000)
  })
})

const equityOperation = (overrides: Partial<EquityOperationTiming>): EquityOperationTiming => ({
  operationId: 'eq-1',
  kind: 'mint',
  symbol: 'AAPL',
  quantity: '5',
  startedAt: '2026-06-01T00:00:00Z',
  completedAt: '2026-06-01T00:01:10Z',
  status: 'completed',
  stages: [
    {
      stage: 'mint_acceptance',
      startedAt: '2026-06-01T00:00:00Z',
      endedAt: '2026-06-01T00:00:10Z',
      durationMs: 10_000,
      outcome: 'succeeded'
    },
    {
      stage: 'mint_deposit',
      startedAt: '2026-06-01T00:01:00Z',
      endedAt: null,
      durationMs: null,
      outcome: 'unmeasured'
    }
  ],
  totalMs: 70_000,
  ...overrides
})

const NOW_EQUITY = new Date('2026-06-01T00:01:10Z')

describe('buildEquityRows', () => {
  it('shapes completed mint stages into named fields and skips open ones', () => {
    const rows = buildEquityRows([equityOperation({})], {
      maxRows: 10,
      now: NOW_EQUITY
    })

    expect(rows).toHaveLength(1)
    expect(rows[0]?.totalMs).toBe(70_000)
    expect(rows[0]?.kind).toBe('mint')
    expect(rows[0]?.mintAcceptanceOk).toBe(10_000)
    // The still-open deposit stage (durationMs null) contributes nothing.
    expect(rows[0]?.mintDepositOk).toBe(0)
    expect(rows[0]?.mintDepositFailed).toBe(0)
    // Redemption fields never populate for a mint row.
    expect(rows[0]?.redemptionWithdrawOk).toBe(0)
  })

  it('uses now - startedAt as totalMs for in-progress operations', () => {
    const startedAt = '2026-06-01T00:00:00Z'
    const now = new Date('2026-06-01T01:00:00Z')

    const rows = buildEquityRows(
      [
        equityOperation({
          startedAt,
          completedAt: null,
          status: 'in_progress',
          totalMs: null,
          stages: [
            {
              stage: 'redemption_withdraw',
              startedAt,
              endedAt: null,
              durationMs: null,
              outcome: 'unmeasured'
            }
          ]
        })
      ],
      {
        maxRows: 10,
        now
      }
    )

    expect(rows[0]?.totalMs).toBe(3_600_000)
    expect(rows[0]?.redemptionWithdrawOk).toBe(0)
    // No completed stage accounts for any of the elapsed time, so the whole
    // elapsed window must surface as a visible unmeasured remainder rather
    // than rendering as an invisible zero-length bar.
    expect(rows[0]?.unmeasuredMs).toBe(3_600_000)
  })

  it('surfaces only the unaccounted-for remainder as unmeasuredMs when some stages completed', () => {
    const startedAt = '2026-06-01T00:00:00Z'
    const now = new Date('2026-06-01T01:00:00Z')

    const rows = buildEquityRows(
      [
        equityOperation({
          startedAt,
          completedAt: null,
          status: 'in_progress',
          totalMs: null,
          stages: [
            {
              stage: 'mint_acceptance',
              startedAt,
              endedAt: '2026-06-01T00:00:10Z',
              durationMs: 10_000,
              outcome: 'succeeded'
            },
            {
              stage: 'mint_receipt',
              startedAt: '2026-06-01T00:00:10Z',
              endedAt: null,
              durationMs: null,
              outcome: 'unmeasured'
            }
          ]
        })
      ],
      {
        maxRows: 10,
        now
      }
    )

    expect(rows[0]?.totalMs).toBe(3_600_000)
    expect(rows[0]?.mintAcceptanceOk).toBe(10_000)
    // 3,600,000 elapsed minus the 10,000ms accounted for by the completed
    // mint_acceptance stage -- the still-open mint_receipt stage's real
    // elapsed time, made visible instead of silently dropped.
    expect(rows[0]?.unmeasuredMs).toBe(3_590_000)
  })

  it('surfaces an unmeasuredMs remainder for a completed operation whose stages do not fully account for totalMs', () => {
    // Same rationale as buildRebalanceRows: a completed, provider-recovered
    // equity operation can have a real totalMs while one of its stages never
    // got measured -- that elapsed time must still show up as a remainder.
    const rows = buildEquityRows([equityOperation({})], {
      maxRows: 10,
      now: NOW_EQUITY
    })

    expect(rows[0]?.status).toBe('completed')
    // 70,000 totalMs minus the 10,000ms accounted for by mint_acceptance --
    // the still-unmeasured mint_deposit stage's real elapsed time.
    expect(rows[0]?.unmeasuredMs).toBe(60_000)
  })

  it('does not fabricate an unmeasuredMs remainder when totalMs is genuinely unmeasured', () => {
    const rows = buildEquityRows(
      [
        equityOperation({
          status: 'completed',
          totalMs: null,
          stages: [
            {
              stage: 'mint_acceptance',
              startedAt: '2026-06-01T00:00:00Z',
              endedAt: '2026-06-01T00:00:10Z',
              durationMs: 10_000,
              outcome: 'succeeded'
            }
          ]
        })
      ],
      {
        maxRows: 10,
        now: NOW_EQUITY
      }
    )

    expect(rows[0]?.totalMs).toBeNull()
    expect(rows[0]?.unmeasuredMs).toBe(0)
  })

  it('preserves null totalMs for a terminal operator-reconciled operation with a known startedAt', () => {
    // Same rationale as buildRebalanceRows: a completed operation with
    // totalMs: null is a deliberate backend suppression, never a fallback
    // candidate.
    const startedAt = '2026-06-01T00:00:00Z'
    const now = new Date('2026-06-01T00:00:00Z')

    const rows = buildEquityRows(
      [
        equityOperation({
          startedAt,
          status: 'completed',
          totalMs: null,
          stages: [
            {
              stage: 'mint_acceptance',
              startedAt,
              endedAt: '2026-06-01T00:00:10Z',
              durationMs: 10_000,
              outcome: 'succeeded'
            }
          ]
        })
      ],
      {
        maxRows: 10,
        now
      }
    )

    expect(rows[0]?.totalMs).toBeNull()
  })

  it('preserves null totalMs when startedAt is also null, even mid-stream', () => {
    const rows = buildEquityRows(
      [
        equityOperation({
          startedAt: null,
          status: 'completed',
          totalMs: null,
          stages: [
            {
              stage: 'mint_acceptance',
              startedAt: '2026-06-01T00:00:00Z',
              endedAt: '2026-06-01T00:00:10Z',
              durationMs: 10_000,
              outcome: 'succeeded'
            }
          ]
        })
      ],
      {
        maxRows: 10,
        now: new Date('2026-06-01T00:01:00Z')
      }
    )

    expect(rows[0]?.totalMs).toBeNull()
  })

  it('caps rows preserving newest-first input order', () => {
    const rows = buildEquityRows(
      [
        equityOperation({ operationId: 'newest' }),
        equityOperation({ operationId: 'older' }),
        equityOperation({ operationId: 'oldest' })
      ],
      {
        maxRows: 2,
        now: NOW_EQUITY
      }
    )

    expect(rows.map((row) => row.id)).toEqual(['newest', 'older'])
  })

  it('routes a failed stage to its Failed field, not its Ok field', () => {
    const rows = buildEquityRows(
      [
        equityOperation({
          kind: 'redeem',
          status: 'failed',
          completedAt: null,
          totalMs: null,
          stages: [
            {
              stage: 'redemption_send',
              startedAt: '2026-06-01T00:00:00Z',
              endedAt: '2026-06-01T00:00:30Z',
              durationMs: 30_000,
              outcome: 'failed'
            }
          ]
        })
      ],
      {
        maxRows: 10,
        now: NOW_EQUITY
      }
    )

    expect(rows[0]?.redemptionSendOk).toBe(0)
    expect(rows[0]?.redemptionSendFailed).toBe(30_000)
  })

  it('returns an empty array for no operations', () => {
    expect(buildEquityRows([], { maxRows: 10, now: NOW_EQUITY })).toEqual([])
  })
})

const latencyBucket = (overrides: Partial<LatencyBucket>): LatencyBucket => ({
  start: '2026-06-01T00:00:00Z',
  stages: {
    detection: null,
    decision: null,
    submission: null,
    execution: null,
    exposureWindow: null
  },
  ...overrides
})

describe('buildPercentileSeries', () => {
  it('null-coalesces a bucket with no stats for the selected stage', () => {
    const series = buildPercentileSeries([latencyBucket({})], 'execution')

    expect(series).toEqual([
      {
        start: new Date('2026-06-01T00:00:00Z'),
        p50Ms: null,
        p90Ms: null,
        p99Ms: null
      }
    ])
  })

  it('selects the p50/p90/p99 fields from the requested stage only', () => {
    const buckets = [
      latencyBucket({
        stages: {
          detection: null,
          decision: null,
          submission: null,
          execution: { p50Ms: 100, p90Ms: 200, p95Ms: 250, p99Ms: 300, maxMs: 400, sampleCount: 5 },
          exposureWindow: {
            p50Ms: 900,
            p90Ms: 910,
            p95Ms: 920,
            p99Ms: 930,
            maxMs: 940,
            sampleCount: 9
          }
        }
      })
    ]

    expect(buildPercentileSeries(buckets, 'execution')).toEqual([
      {
        start: new Date('2026-06-01T00:00:00Z'),
        p50Ms: 100,
        p90Ms: 200,
        p99Ms: 300
      }
    ])
    expect(buildPercentileSeries(buckets, 'exposureWindow')).toEqual([
      {
        start: new Date('2026-06-01T00:00:00Z'),
        p50Ms: 900,
        p90Ms: 910,
        p99Ms: 930
      }
    ])
  })

  it('returns an empty array for no buckets', () => {
    expect(buildPercentileSeries([], 'execution')).toEqual([])
  })
})

describe('thinTicks', () => {
  const point = (isoDate: string): { start: Date } => ({ start: new Date(isoDate) })

  it('always includes the last point even when it does not fall on a step boundary', () => {
    // 5 points, maxTicks=2 -> step = ceil(5/2) = 3.
    // Step-aligned indices: 0, 3. Index 4 (last) is NOT on a step -> must still appear.
    const points = [
      point('2026-06-01T00:00:00Z'),
      point('2026-06-02T00:00:00Z'),
      point('2026-06-03T00:00:00Z'),
      point('2026-06-04T00:00:00Z'),
      point('2026-06-05T00:00:00Z')
    ]

    const ticks = thinTicks(points, 2)

    // Expect 3 ticks: indices 0, 3, and 4 (last).
    expect(ticks).toHaveLength(3)
    expect(ticks.at(-1)).toEqual(new Date('2026-06-05T00:00:00Z'))
  })

  it('keeps every point when there are fewer points than maxTicks', () => {
    const points = [point('2026-06-01T00:00:00Z'), point('2026-06-02T00:00:00Z')]

    expect(thinTicks(points, 8)).toEqual(points.map((entry) => entry.start))
  })

  it('returns an empty array for no points', () => {
    expect(thinTicks([], 8)).toEqual([])
  })
})

describe('layoutDependencySparkline', () => {
  it('marks error buckets and scales bars to the slowest median', () => {
    const bars = layoutDependencySparkline(
      [
        {
          start: '2026-06-01T00:00:00Z',
          calls: 10,
          errors: 0,
          p50Ms: 50
        },
        {
          start: '2026-06-01T01:00:00Z',
          calls: 8,
          errors: 2,
          p50Ms: 200
        },
        {
          start: '2026-06-01T02:00:00Z',
          calls: 0,
          errors: 0,
          p50Ms: null
        }
      ],
      { plotHeight: 12 }
    )

    expect(bars).toEqual([
      { height: 3, hasErrors: false },
      { height: 12, hasErrors: true },
      { height: 0, hasErrors: false }
    ])
  })
})
