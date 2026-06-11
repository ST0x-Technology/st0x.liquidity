/** Pure SLO classification and formatting for the Performance tab. */

export type SloStatus = 'unknown' | 'good' | 'warning' | 'critical'

export type SloThresholds = {
  /** Inclusive upper bound for `good`. */
  good: number
  /** Inclusive upper bound for `warning`; above is `critical`. */
  warning: number
}

/** How quickly the bot should observe an onchain fill (p95, ms). */
export const DETECTION_THRESHOLDS: SloThresholds = {
  good: 30_000,
  warning: 120_000,
}

/** How long unhedged delta should live, block to broker fill (p95, ms). */
export const EXPOSURE_WINDOW_THRESHOLDS: SloThresholds = {
  good: 60_000,
  warning: 300_000,
}

/** Error log lines per 24h. */
export const ERROR_COUNT_THRESHOLDS: SloThresholds = {
  good: 0,
  warning: 10,
}

/** Warning log lines per 24h: noisier than errors, so wider bounds. */
export const WARNING_COUNT_THRESHOLDS: SloThresholds = {
  good: 25,
  warning: 250,
}

/** Age of the oldest fill not yet covered by a hedge (ms). */
export const OPEN_EXPOSURE_AGE_THRESHOLDS: SloThresholds = {
  good: 300_000,
  warning: 1_800_000,
}

const STATUS_RANK: Record<SloStatus, number> = {
  unknown: 0,
  good: 1,
  warning: 2,
  critical: 3,
}

/** The most severe of the given statuses. */
export const worstStatus = (statuses: SloStatus[]): SloStatus =>
  statuses.reduce(
    (worst, status) => (STATUS_RANK[status] > STATUS_RANK[worst] ? status : worst),
    'unknown',
  )

export const classifySlo = (value: number, thresholds: SloThresholds): SloStatus => {
  if (value <= thresholds.good) {
    return 'good'
  }

  if (value <= thresholds.warning) {
    return 'warning'
  }

  return 'critical'
}

/** Renders a millisecond duration at human scale: 850ms, 12.4s, 3m 20s, 2h 05m. */
export const formatDurationMs = (ms: number): string => {
  if (ms < 0) {
    return `-${formatDurationMs(-ms)}`
  }

  if (ms < 1_000) {
    return `${String(Math.round(ms))}ms`
  }

  // Round before choosing the unit so a value like 59.95s carries into
  // "1m 00s" instead of rendering as "60.0s" or "1m 60s".
  const tenths = Math.round(ms / 100)
  if (tenths < 600) {
    return `${(tenths / 10).toFixed(1)}s`
  }

  const totalSeconds = Math.round(ms / 1_000)
  const totalMinutes = Math.floor(totalSeconds / 60)
  if (totalMinutes < 60) {
    return `${String(totalMinutes)}m ${String(totalSeconds % 60).padStart(2, '0')}s`
  }

  const hours = Math.floor(totalMinutes / 60)
  return `${String(hours)}h ${String(totalMinutes % 60).padStart(2, '0')}m`
}
