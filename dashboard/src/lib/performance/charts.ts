/** Pure SVG layout math for the Performance tab's latency charts. */

import type { HedgeCycleReport } from '$lib/api/HedgeCycleReport'
import type { LatencyBucket } from '$lib/api/LatencyBucket'
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

    if (completedAt !== null) {
      segments.push({
        name: 'execution',
        ms: Math.max(0, completedAt - submittedAt),
      })
    }
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
  },
): WaterfallRow[] => {
  const rows = cycles.map((cycle) => {
    const durations = segmentDurations(cycle)
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
