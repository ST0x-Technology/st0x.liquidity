/** Pure SLO card derivation for the Performance tab. */

import type { HedgeLatencies } from '$lib/api/HedgeLatencies'
import type { ReliabilityReport } from '$lib/api/ReliabilityReport'
import {
  DETECTION_THRESHOLDS,
  ERROR_COUNT_THRESHOLDS,
  EXPOSURE_WINDOW_THRESHOLDS,
  OPEN_EXPOSURE_AGE_THRESHOLDS,
  WARNING_COUNT_THRESHOLDS,
  type SloStatus,
  classifySlo,
  formatDurationMs,
  worstStatus,
} from './slo'

export type SloCard = {
  title: string
  primary: string
  secondary: string
  status: SloStatus
}

/** Shown while a report has not loaded: unknown must not look healthy. */
const loadingCard = (title: string): SloCard => ({
  title,
  primary: '—',
  secondary: 'loading',
  status: 'unknown',
})

export const detectionCard = (report: HedgeLatencies | null): SloCard => {
  if (!report) {
    return loadingCard('Detection latency')
  }

  const stats = report.summary.stages.detection

  if (!stats) {
    return {
      title: 'Detection latency',
      primary: '—',
      secondary: 'no fills in window',
      status: 'good',
    }
  }

  return {
    title: 'Detection latency',
    primary: `p95 ${formatDurationMs(stats.p95Ms)}`,
    secondary: `p50 ${formatDurationMs(stats.p50Ms)} · ${String(stats.sampleCount)} fills`,
    status: classifySlo(stats.p95Ms, DETECTION_THRESHOLDS),
  }
}

export const exposureCard = (report: HedgeLatencies | null): SloCard => {
  if (!report) {
    return loadingCard('Exposure window')
  }

  const stats = report.summary.stages.exposureWindow

  if (!stats) {
    return {
      title: 'Exposure window',
      primary: '—',
      secondary: 'no completed hedges in window',
      status: 'good',
    }
  }

  return {
    title: 'Exposure window',
    primary: `p95 ${formatDurationMs(stats.p95Ms)}`,
    secondary: `p50 ${formatDurationMs(stats.p50Ms)} · ${String(stats.sampleCount)} hedges`,
    status: classifySlo(stats.p95Ms, EXPOSURE_WINDOW_THRESHOLDS),
  }
}

export const errorsCard = (
  report: ReliabilityReport | null,
  windowHours: number,
): SloCard => {
  const title = `Errors (${String(windowHours)}h)`

  if (!report) {
    return loadingCard(title)
  }

  const errors = report.logBuckets.reduce((sum, bucket) => sum + bucket.errors, 0)
  const warnings = report.logBuckets.reduce((sum, bucket) => sum + bucket.warnings, 0)
  const failures = report.failureEvents.reduce((sum, event) => sum + event.count, 0)
  const killedQueues = report.jobQueues.filter((queue) => queue.killed > 0)
  const failedQueues = report.jobQueues.filter((queue) => queue.failed > 0)

  const queueStatus: SloStatus =
    killedQueues.length > 0 ? 'critical' : failedQueues.length > 0 ? 'warning' : 'good'

  const queueSuffix =
    killedQueues.length > 0
      ? ` · ${String(killedQueues.length)} queue(s) killed`
      : failedQueues.length > 0
        ? ` · ${String(failedQueues.length)} queue(s) failed`
        : ''

  return {
    title,
    primary: String(errors),
    secondary: `${String(warnings)} warnings · ${String(failures)} lifecycle failures${queueSuffix}`,
    status:
      failures > 0
        ? 'critical'
        : worstStatus([
            classifySlo(errors, ERROR_COUNT_THRESHOLDS),
            classifySlo(warnings, WARNING_COUNT_THRESHOLDS),
            queueStatus,
          ]),
  }
}

export const openExposureCard = (
  report: HedgeLatencies | null,
  now: Date | null,
): SloCard => {
  if (!report || !now) {
    return loadingCard('Oldest unhedged fill')
  }

  const exposures = report.openExposures

  if (exposures.length === 0) {
    return {
      title: 'Oldest unhedged fill',
      primary: 'none',
      secondary: 'all fills hedged',
      status: 'good',
    }
  }

  const oldest = exposures.reduce((candidate, exposure) =>
    new Date(exposure.oldestFillBlockTimestamp).getTime() <
    new Date(candidate.oldestFillBlockTimestamp).getTime()
      ? exposure
      : candidate,
  )
  const ageMs = Math.max(0, now.getTime() - new Date(oldest.oldestFillBlockTimestamp).getTime())
  const fillCount = exposures.reduce((sum, exposure) => sum + exposure.fillCount, 0)

  return {
    title: 'Oldest unhedged fill',
    primary: formatDurationMs(ageMs),
    secondary:
      `${oldest.symbol} oldest · ${String(fillCount)} uncovered fills ` +
      `across ${String(exposures.length)} symbols`,
    status: classifySlo(ageMs, OPEN_EXPOSURE_AGE_THRESHOLDS),
  }
}
