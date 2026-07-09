/**
 * Data-shaping for the Performance tab's charts: the actual layout math
 * (scales, axes, padding) is handled by LayerChart -- these functions only
 * reshape backend DTOs into the flat, per-series-field rows the chart
 * components expect.
 */

import type { DependencyBucket } from '$lib/api/DependencyBucket'
import type { EquityOperationTiming } from '$lib/api/EquityOperationTiming'
import type { EquityStageName } from '$lib/api/EquityStageName'
import type { HedgeCycleReport } from '$lib/api/HedgeCycleReport'
import type { LatencyBucket } from '$lib/api/LatencyBucket'
import type { RebalanceOperationTiming } from '$lib/api/RebalanceOperationTiming'
import type { RebalanceStageName } from '$lib/api/RebalanceStageName'
import type { StageLatencies } from '$lib/api/StageLatencies'

export type WaterfallSegmentName = 'unhedged' | 'submission' | 'execution'

export type WaterfallSort = 'slowest' | 'newest'

export type WaterfallBarRow = {
  id: string
  symbol: string
  status: HedgeCycleReport['status']
  totalMs: number
  unhedged: number
  submission: number
  // Split into two series (rather than one 'execution' series colored per-row)
  // so a failed cycle's final segment renders red through the chart's normal
  // per-series coloring -- each row populates only one of the pair, the other
  // stays 0, so the stack shows exactly one execution segment per row.
  executionOk: number
  executionFailed: number
}

const segmentDurations = (
  cycle: HedgeCycleReport,
  now: Date
): { name: WaterfallSegmentName; ms: number }[] => {
  const placedAt = new Date(cycle.placedAt).getTime()
  const earliestFill =
    cycle.earliestFillBlockTimestamp !== null
      ? new Date(cycle.earliestFillBlockTimestamp).getTime()
      : placedAt
  const submittedAt = cycle.submittedAt !== null ? new Date(cycle.submittedAt).getTime() : null
  const completedAt = cycle.completedAt !== null ? new Date(cycle.completedAt).getTime() : null

  // A cycle that has not reached a terminal state is live exposure: its
  // whole elapsed time still counts as unhedged, so it ranks where it
  // belongs in the slowest sort instead of rendering as an invisible bar.
  if (completedAt === null) {
    return [
      {
        name: 'unhedged',
        ms: Math.max(0, now.getTime() - earliestFill)
      }
    ]
  }

  const segments: { name: WaterfallSegmentName; ms: number }[] = [
    {
      name: 'unhedged',
      ms: Math.max(0, placedAt - earliestFill)
    }
  ]

  if (submittedAt !== null) {
    segments.push({
      name: 'submission',
      ms: Math.max(0, submittedAt - placedAt)
    })
    segments.push({
      name: 'execution',
      ms: Math.max(0, completedAt - submittedAt)
    })
  }

  return segments
}

/**
 * Shapes hedge cycles into rows for a horizontal stacked bar chart, one
 * numeric field per segment, sorted and capped to the visible window.
 */
export const buildWaterfallRows = (
  cycles: HedgeCycleReport[],
  options: {
    sort: WaterfallSort
    maxRows: number
    now: Date
  }
): WaterfallBarRow[] => {
  const rows = cycles.map((cycle) => {
    const durations = segmentDurations(cycle, options.now)
    const totalMs = durations.reduce((sum, segment) => sum + segment.ms, 0)
    const byName: Partial<Record<WaterfallSegmentName, number>> = Object.fromEntries(
      durations.map((segment) => [segment.name, segment.ms])
    )
    const executionMs = byName.execution ?? 0
    const failed = cycle.status === 'failed'

    return {
      id: cycle.offchainOrderId,
      symbol: cycle.symbol,
      status: cycle.status,
      placedAtMs: new Date(cycle.placedAt).getTime(),
      totalMs,
      unhedged: byName.unhedged ?? 0,
      submission: byName.submission ?? 0,
      executionOk: failed ? 0 : executionMs,
      executionFailed: failed ? executionMs : 0
    }
  })

  rows.sort((left, right) =>
    options.sort === 'slowest' ? right.totalMs - left.totalMs : right.placedAtMs - left.placedAtMs
  )

  return rows.slice(0, options.maxRows).map(({ placedAtMs: _placedAtMs, ...row }) => row)
}

export type RebalanceBarRow = {
  id: string
  direction: RebalanceOperationTiming['direction']
  amount: string | null
  status: RebalanceOperationTiming['status']
  totalMs: number | null
  // Each stage splits into an Ok/Failed pair (rather than one series colored
  // per-row) so a failed stage renders red through the chart's normal
  // per-series coloring -- each row populates only one of a pair, the other
  // stays 0.
  conversionOk: number
  conversionFailed: number
  withdrawalOk: number
  withdrawalFailed: number
  burnOk: number
  burnFailed: number
  attestationOk: number
  attestationFailed: number
  mintOk: number
  mintFailed: number
  depositOk: number
  depositFailed: number
  // totalMs minus every measured stage above, for any row with a known
  // totalMs. The stacked bar otherwise only plots completed-stage time, so a
  // stuck operation (or a completed-but-recovered operation whose stages
  // don't fully account for its elapsed time) would render as a
  // shorter-than-real bar despite real elapsed time -- this makes that
  // elapsed remainder its own visible segment instead of silently vanishing.
  unmeasuredMs: number
}

const stageDurations = (
  stages: RebalanceOperationTiming['stages'],
  name: RebalanceStageName
): { ok: number; failed: number } => {
  const stage = stages.find((entry) => entry.stage === name && entry.durationMs !== null)
  if (!stage) return { ok: 0, failed: 0 }

  return stage.outcome === 'failed'
    ? { ok: 0, failed: stage.durationMs ?? 0 }
    : { ok: stage.durationMs ?? 0, failed: 0 }
}

/**
 * Shapes rebalance operations into rows for a horizontal stacked bar chart,
 * one numeric field per stage/outcome. Operations arrive newest-first from
 * the backend and stay in that order.
 *
 * Pass `now` so that in-progress operations (where the backend has not yet
 * set `totalMs`) are shaped with their true elapsed time instead of just the
 * sum of completed stages.
 */
export const buildRebalanceRows = (
  operations: RebalanceOperationTiming[],
  options: {
    maxRows: number
    now: Date
  }
): RebalanceBarRow[] =>
  operations.slice(0, options.maxRows).map((operation) => {
    // For in-progress operations the backend sets totalMs only on completion,
    // so use wall-clock elapsed since startedAt instead of summing completed
    // stages (which would understate a stuck operation on an open stage).
    // For any other status, totalMs stays whatever the backend reported --
    // including null, which means the round-trip is genuinely unmeasured
    // (unknown startedAt, or a terminal OperatorReconciled operation whose
    // manual-response window must not pollute latency metrics).
    const totalMs =
      operation.status === 'in_progress' && operation.startedAt !== null
        ? Math.max(0, options.now.getTime() - new Date(operation.startedAt).getTime())
        : operation.totalMs

    const conversion = stageDurations(operation.stages, 'conversion')
    const withdrawal = stageDurations(operation.stages, 'withdrawal')
    const burn = stageDurations(operation.stages, 'burn')
    const attestation = stageDurations(operation.stages, 'attestation')
    const mint = stageDurations(operation.stages, 'mint')
    const deposit = stageDurations(operation.stages, 'deposit')

    // Any operation with a known totalMs can have a remainder: in-progress
    // operations from the elapsed-time fallback above, but also completed
    // operations recovered via a provider-completion path, whose stages don't
    // fully account for the measured round-trip. totalMs === null means the
    // round-trip is genuinely unmeasured (unknown startedAt, or a terminal
    // OperatorReconciled operation), so there is no remainder to attribute.
    const measuredMs =
      conversion.ok +
      conversion.failed +
      withdrawal.ok +
      withdrawal.failed +
      burn.ok +
      burn.failed +
      attestation.ok +
      attestation.failed +
      mint.ok +
      mint.failed +
      deposit.ok +
      deposit.failed
    const unmeasuredMs = totalMs !== null ? Math.max(0, totalMs - measuredMs) : 0

    return {
      id: operation.operationId,
      direction: operation.direction,
      amount: operation.amount,
      status: operation.status,
      totalMs,
      conversionOk: conversion.ok,
      conversionFailed: conversion.failed,
      withdrawalOk: withdrawal.ok,
      withdrawalFailed: withdrawal.failed,
      burnOk: burn.ok,
      burnFailed: burn.failed,
      attestationOk: attestation.ok,
      attestationFailed: attestation.failed,
      mintOk: mint.ok,
      mintFailed: mint.failed,
      depositOk: deposit.ok,
      depositFailed: deposit.failed,
      unmeasuredMs
    }
  })

export type EquityBarRow = {
  id: string
  kind: EquityOperationTiming['kind']
  symbol: string | null
  status: EquityOperationTiming['status']
  totalMs: number | null
  // Same Ok/Failed-pair-per-stage rationale as RebalanceBarRow. Mint and
  // redemption stages are distinct series (never both populated for the same
  // row, since an operation is one kind or the other) so one combined chart
  // can show both kinds without the legend implying they share a pipeline.
  mintAcceptanceOk: number
  mintAcceptanceFailed: number
  mintReceiptOk: number
  mintReceiptFailed: number
  mintWrapOk: number
  mintWrapFailed: number
  mintDepositOk: number
  mintDepositFailed: number
  redemptionWithdrawOk: number
  redemptionWithdrawFailed: number
  redemptionUnwrapOk: number
  redemptionUnwrapFailed: number
  redemptionSendOk: number
  redemptionSendFailed: number
  redemptionDetectionOk: number
  redemptionDetectionFailed: number
  redemptionCompletionOk: number
  redemptionCompletionFailed: number
  // Same elapsed-remainder rationale as {@link RebalanceBarRow.unmeasuredMs}.
  unmeasuredMs: number
}

const equityStageDurations = (
  stages: EquityOperationTiming['stages'],
  name: EquityStageName
): { ok: number; failed: number } => {
  const stage = stages.find((entry) => entry.stage === name && entry.durationMs !== null)
  if (!stage) return { ok: 0, failed: 0 }

  return stage.outcome === 'failed'
    ? { ok: 0, failed: stage.durationMs ?? 0 }
    : { ok: stage.durationMs ?? 0, failed: 0 }
}

/**
 * Shapes equity mint/redemption operations into rows for a horizontal
 * stacked bar chart, one numeric field per stage/outcome. Same shape and
 * `now`-fallback rationale as {@link buildRebalanceRows}.
 */
export const buildEquityRows = (
  operations: EquityOperationTiming[],
  options: {
    maxRows: number
    now: Date
  }
): EquityBarRow[] =>
  operations.slice(0, options.maxRows).map((operation) => {
    // Same in-progress-only elapsed-time fallback as buildRebalanceRows --
    // any other status preserves the backend's totalMs verbatim, including
    // null for a genuinely unmeasured round-trip.
    const totalMs =
      operation.status === 'in_progress' && operation.startedAt !== null
        ? Math.max(0, options.now.getTime() - new Date(operation.startedAt).getTime())
        : operation.totalMs

    const mintAcceptance = equityStageDurations(operation.stages, 'mint_acceptance')
    const mintReceipt = equityStageDurations(operation.stages, 'mint_receipt')
    const mintWrap = equityStageDurations(operation.stages, 'mint_wrap')
    const mintDeposit = equityStageDurations(operation.stages, 'mint_deposit')
    const redemptionWithdraw = equityStageDurations(operation.stages, 'redemption_withdraw')
    const redemptionUnwrap = equityStageDurations(operation.stages, 'redemption_unwrap')
    const redemptionSend = equityStageDurations(operation.stages, 'redemption_send')
    const redemptionDetection = equityStageDurations(operation.stages, 'redemption_detection')
    const redemptionCompletion = equityStageDurations(operation.stages, 'redemption_completion')

    // Same remainder rationale as buildRebalanceRows.
    const measuredMs =
      mintAcceptance.ok +
      mintAcceptance.failed +
      mintReceipt.ok +
      mintReceipt.failed +
      mintWrap.ok +
      mintWrap.failed +
      mintDeposit.ok +
      mintDeposit.failed +
      redemptionWithdraw.ok +
      redemptionWithdraw.failed +
      redemptionUnwrap.ok +
      redemptionUnwrap.failed +
      redemptionSend.ok +
      redemptionSend.failed +
      redemptionDetection.ok +
      redemptionDetection.failed +
      redemptionCompletion.ok +
      redemptionCompletion.failed
    const unmeasuredMs = totalMs !== null ? Math.max(0, totalMs - measuredMs) : 0

    return {
      id: operation.operationId,
      kind: operation.kind,
      symbol: operation.symbol,
      status: operation.status,
      totalMs,
      mintAcceptanceOk: mintAcceptance.ok,
      mintAcceptanceFailed: mintAcceptance.failed,
      mintReceiptOk: mintReceipt.ok,
      mintReceiptFailed: mintReceipt.failed,
      mintWrapOk: mintWrap.ok,
      mintWrapFailed: mintWrap.failed,
      mintDepositOk: mintDeposit.ok,
      mintDepositFailed: mintDeposit.failed,
      redemptionWithdrawOk: redemptionWithdraw.ok,
      redemptionWithdrawFailed: redemptionWithdraw.failed,
      redemptionUnwrapOk: redemptionUnwrap.ok,
      redemptionUnwrapFailed: redemptionUnwrap.failed,
      redemptionSendOk: redemptionSend.ok,
      redemptionSendFailed: redemptionSend.failed,
      redemptionDetectionOk: redemptionDetection.ok,
      redemptionDetectionFailed: redemptionDetection.failed,
      redemptionCompletionOk: redemptionCompletion.ok,
      redemptionCompletionFailed: redemptionCompletion.failed,
      unmeasuredMs
    }
  })

export type PercentileSeriesPoint = {
  start: Date
  p50Ms: number | null
  p90Ms: number | null
  p99Ms: number | null
}

/**
 * Shapes latency buckets into one row per bucket for the selected stage, null
 * -coalescing buckets that have no stats for that stage (the stage was never
 * reached by any sample in that window). One row per bucket in the full
 * requested range (not just buckets with samples) is intentional: LineChart's
 * default `defined` accessor skips null y-values, so this renders true gaps
 * at the bucket's real time position instead of the line stretching to
 * connect only the populated buckets.
 */
export const buildPercentileSeries = (
  buckets: LatencyBucket[],
  stage: keyof StageLatencies
): PercentileSeriesPoint[] =>
  buckets.map((bucket) => {
    const stats = bucket.stages[stage]
    return {
      start: new Date(bucket.start),
      p50Ms: stats?.p50Ms ?? null,
      p90Ms: stats?.p90Ms ?? null,
      p99Ms: stats?.p99Ms ?? null
    }
  })

/**
 * Thins a sequence of x-axis points down to at most `maxTicks` explicit tick
 * values, keeping every `step`-th point (step = ceil(n / maxTicks)) plus
 * always the last point. The default d3 time-scale tick generator picks
 * ticks by "nice" time boundaries (hour/day/etc), not by point count, so a
 * range with many daily buckets can otherwise emit more ticks than there are
 * distinct days -- rendering the same calendar-date label twice in a row
 * once two ticks land on the same day.
 */
export const thinTicks = (points: { start: Date }[], maxTicks: number): Date[] => {
  const step = Math.max(1, Math.ceil(points.length / maxTicks))
  return points
    .filter((_, index) => index % step === 0 || index === points.length - 1)
    .map((point) => point.start)
}

export type DependencySparkBar = {
  /** Bar height scaled to the slowest bucket's median, in plot units. */
  height: number
  hasErrors: boolean
}

/**
 * Lays out per-bucket median latency as sparkline bars; buckets containing
 * errors are flagged so the template can tint them.
 */
export const layoutDependencySparkline = (
  buckets: DependencyBucket[],
  options: { plotHeight: number }
): DependencySparkBar[] => {
  const maxP50 = Math.max(1, ...buckets.map((bucket) => bucket.p50Ms ?? 0))

  return buckets.map((bucket) => ({
    height: ((bucket.p50Ms ?? 0) / maxP50) * options.plotHeight,
    hasErrors: bucket.errors > 0
  }))
}
