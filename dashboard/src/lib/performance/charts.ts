/** Pure SVG layout math for the Performance tab's latency charts. */

import type { AttestationSample } from '$lib/api/AttestationSample'
import type { BlockLagPoint } from '$lib/api/BlockLagPoint'
import type { HedgeCycleReport } from '$lib/api/HedgeCycleReport'
import type { LatencyBucket } from '$lib/api/LatencyBucket'
import type { RebalanceOperationTiming } from '$lib/api/RebalanceOperationTiming'
import type { RebalanceStageName } from '$lib/api/RebalanceStageName'
import type { StageLatencies } from '$lib/api/StageLatencies'

export type WaterfallSegmentName = 'unhedged' | 'submission' | 'execution'

export type WaterfallSegment = {
  name: WaterfallSegmentName
  ms: number
  x: number
  width: number
}

export type WaterfallRow = {
  id: string
  symbol: string
  status: HedgeCycleReport['status']
  totalMs: number
  segments: WaterfallSegment[]
}

export type WaterfallSort = 'slowest' | 'newest'

const segmentDurations = (
  cycle: HedgeCycleReport,
  now: Date,
): { name: WaterfallSegmentName; ms: number }[] => {
  const placedAt = new Date(cycle.placedAt).getTime()
  const earliestFill =
    cycle.earliestFillBlockTimestamp !== null
      ? new Date(cycle.earliestFillBlockTimestamp).getTime()
      : placedAt
  const submittedAt =
    cycle.submittedAt !== null ? new Date(cycle.submittedAt).getTime() : null
  const completedAt =
    cycle.completedAt !== null ? new Date(cycle.completedAt).getTime() : null

  // A cycle that has not reached a terminal state is live exposure: its
  // whole elapsed time still counts as unhedged, so it ranks where it
  // belongs in the slowest sort instead of rendering as an invisible bar.
  if (completedAt === null) {
    return [
      {
        name: 'unhedged',
        ms: Math.max(0, now.getTime() - earliestFill),
      },
    ]
  }

  const segments: { name: WaterfallSegmentName; ms: number }[] = [
    {
      name: 'unhedged',
      ms: Math.max(0, placedAt - earliestFill),
    },
  ]

  if (submittedAt !== null) {
    segments.push({
      name: 'submission',
      ms: Math.max(0, submittedAt - placedAt),
    })
    segments.push({
      name: 'execution',
      ms: Math.max(0, completedAt - submittedAt),
    })
  }

  return segments
}

/**
 * Lays out hedge cycles as horizontal stacked bars whose segment widths are
 * proportional to wall-clock time, normalized to the slowest visible cycle.
 */
export const layoutWaterfall = (
  cycles: HedgeCycleReport[],
  options: {
    plotWidth: number
    sort: WaterfallSort
    maxRows: number
    now: Date
  },
): WaterfallRow[] => {
  const rows = cycles.map((cycle) => {
    const durations = segmentDurations(cycle, options.now)
    const totalMs = durations.reduce((sum, segment) => sum + segment.ms, 0)

    return {
      id: cycle.offchainOrderId,
      symbol: cycle.symbol,
      status: cycle.status,
      placedAt: new Date(cycle.placedAt).getTime(),
      totalMs,
      durations,
    }
  })

  rows.sort((left, right) =>
    options.sort === 'slowest'
      ? right.totalMs - left.totalMs
      : right.placedAt - left.placedAt,
  )
  const visible = rows.slice(0, options.maxRows)

  const maxTotal = Math.max(1, ...visible.map((row) => row.totalMs))

  return visible.map((row) => {
    let cursor = 0
    const segments = row.durations.map((segment) => {
      const width = (segment.ms / maxTotal) * options.plotWidth
      const positioned = {
        ...segment,
        x: cursor,
        width,
      }
      cursor += width
      return positioned
    })

    return {
      id: row.id,
      symbol: row.symbol,
      status: row.status,
      totalMs: row.totalMs,
      segments,
    }
  })
}

export type PercentileKey = 'p50Ms' | 'p90Ms' | 'p99Ms'

export type SeriesPoint = {
  x: number
  y: number
}

export type SeriesLine = {
  percentile: PercentileKey
  points: SeriesPoint[]
  path: string
}

export type SeriesLayout = {
  lines: SeriesLine[]
  maxMs: number
  xLabels: { x: number; label: string }[]
}

export type RebalanceBarSegment = {
  stage: RebalanceStageName
  ms: number
  x: number
  width: number
  failed: boolean
}

export type RebalanceBarRow = {
  id: string
  direction: RebalanceOperationTiming['direction']
  amount: string | null
  status: RebalanceOperationTiming['status']
  totalMs: number
  segments: RebalanceBarSegment[]
}

/**
 * Lays out rebalance operations as horizontal stacked bars, one segment per
 * completed stage, normalized to the slowest visible operation. Operations
 * arrive newest-first from the backend and stay in that order.
 *
 * Pass `now` so that in-progress operations (where the backend has not yet
 * set `totalMs`) are rendered with their true elapsed time instead of just
 * the sum of completed stages.
 */
export const layoutRebalanceBars = (
  operations: RebalanceOperationTiming[],
  options: {
    plotWidth: number
    maxRows: number
    now: Date
  },
): RebalanceBarRow[] => {
  const rows = operations.slice(0, options.maxRows).map((operation) => {
    const durations = operation.stages
      .filter((stage) => stage.durationMs !== null)
      .map((stage) => ({
        stage: stage.stage,
        ms: stage.durationMs ?? 0,
        failed: stage.outcome === 'failed',
      }))

    // For in-progress operations the backend sets totalMs only on completion,
    // so use wall-clock elapsed since startedAt instead of summing completed
    // stages (which would understate a stuck operation on an open stage).
    // startedAt is null when the read model first observed the operation
    // mid-stream (no genuine start event), so there is no elapsed basis.
    const totalMs =
      operation.totalMs ??
      (operation.startedAt !== null
        ? Math.max(0, options.now.getTime() - new Date(operation.startedAt).getTime())
        : 0)

    return {
      id: operation.operationId,
      direction: operation.direction,
      amount: operation.amount,
      status: operation.status,
      totalMs,
      durations,
    }
  })

  const maxTotal = Math.max(1, ...rows.map((row) => row.totalMs))

  return rows.map((row) => {
    let cursor = 0
    const segments = row.durations.map((segment) => {
      const width = (segment.ms / maxTotal) * options.plotWidth
      const positioned = {
        ...segment,
        x: cursor,
        width,
      }
      cursor += width
      return positioned
    })

    return {
      id: row.id,
      direction: row.direction,
      amount: row.amount,
      status: row.status,
      totalMs: row.totalMs,
      segments,
    }
  })
}

export type TrendLayout = {
  points: SeriesPoint[]
  path: string
  maxMs: number
}

/** Lays out attestation durations over time as a single polyline. */
export const layoutAttestationTrend = (
  samples: AttestationSample[],
  options: {
    plotWidth: number
    plotHeight: number
  },
): TrendLayout => {
  const maxMs = Math.max(1, ...samples.map((sample) => sample.durationMs))
  const xDenominator = Math.max(1, samples.length - 1)
  const points = samples.map((sample, index) => ({
    x:
      samples.length <= 1
        ? options.plotWidth / 2
        : (index / xDenominator) * options.plotWidth,
    y: options.plotHeight - (sample.durationMs / maxMs) * options.plotHeight,
  }))

  return {
    points,
    path: points
      .map((point) => `${point.x.toFixed(1)},${point.y.toFixed(1)}`)
      .join(' '),
    maxMs,
  }
}

export type BlockLagLayout = {
  points: SeriesPoint[]
  path: string
  maxLagBlocks: number
}

/** Lays out per-bucket worst block lag over time as a single polyline. */
export const layoutBlockLagTrend = (
  buckets: BlockLagPoint[],
  options: {
    plotWidth: number
    plotHeight: number
  },
): BlockLagLayout => {
  const maxLagBlocks = Math.max(0, ...buckets.map((bucket) => bucket.maxLagBlocks))
  // The y-scale denominator is floored at 1 so an all-zero series divides
  // safely; the reported maximum stays the true value (0 for a healthy
  // series), never the scale floor.
  const scaleMax = Math.max(1, maxLagBlocks)
  const xDenominator = Math.max(1, buckets.length - 1)
  const points = buckets.map((bucket, index) => ({
    x:
      buckets.length <= 1
        ? options.plotWidth / 2
        : (index / xDenominator) * options.plotWidth,
    y: options.plotHeight - (bucket.maxLagBlocks / scaleMax) * options.plotHeight,
  }))

  return {
    points,
    path: points
      .map((point) => `${point.x.toFixed(1)},${point.y.toFixed(1)}`)
      .join(' '),
    maxLagBlocks,
  }
}

const PERCENTILE_KEYS: PercentileKey[] = ['p50Ms', 'p90Ms', 'p99Ms']

/**
 * Lays out one stage's percentiles across time buckets as polyline charts.
 * Buckets where the stage has no samples are skipped, keeping the lines
 * continuous across gaps.
 */
export const layoutPercentileSeries = (
  buckets: LatencyBucket[],
  stage: keyof StageLatencies,
  options: {
    plotWidth: number
    plotHeight: number
    maxXLabels: number
  },
): SeriesLayout => {
  const sampled = buckets
    .map((bucket) => ({
      start: bucket.start,
      stats: bucket.stages[stage],
    }))
    .filter((bucket) => bucket.stats !== null)

  const maxMs = Math.max(1, ...sampled.map((bucket) => bucket.stats?.p99Ms ?? 0))
  const xDenominator = Math.max(1, sampled.length - 1)
  const xFor = (index: number): number =>
    sampled.length <= 1
      ? options.plotWidth / 2
      : (index / xDenominator) * options.plotWidth
  const yFor = (ms: number): number =>
    options.plotHeight - (ms / maxMs) * options.plotHeight

  const lines = PERCENTILE_KEYS.map((percentile) => {
    const points = sampled.map((bucket, index) => ({
      x: xFor(index),
      y: yFor(bucket.stats?.[percentile] ?? 0),
    }))

    return {
      percentile,
      points,
      path: points
        .map((point) => `${point.x.toFixed(1)},${point.y.toFixed(1)}`)
        .join(' '),
    }
  })

  const labelStep = Math.max(1, Math.ceil(sampled.length / options.maxXLabels))
  const lastIndex = sampled.length - 1
  const xLabels = sampled
    .map((bucket, index) => ({
      x: xFor(index),
      label: new Date(bucket.start).toLocaleDateString(undefined, {
        month: 'short',
        day: 'numeric',
      }),
      index,
    }))
    .filter(({ index }) => index % labelStep === 0 || index === lastIndex)
    .map(({ x, label }) => ({
      x,
      label,
    }))

  return {
    lines,
    maxMs,
    xLabels,
  }
}
