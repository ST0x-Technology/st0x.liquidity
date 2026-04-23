const MONTH_NAMES = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']

const pad = (num: number, width = 2): string => String(num).padStart(width, '0')

export const formatUtc = (iso: string): string => {
  if (!iso) return '-'
  const date = new Date(iso)
  if (isNaN(date.getTime())) return iso
  const month = MONTH_NAMES[date.getUTCMonth()] ?? '???'
  const day = date.getUTCDate()
  const hh = pad(date.getUTCHours())
  const mm = pad(date.getUTCMinutes())
  const ss = pad(date.getUTCSeconds())
  return `${month} ${String(day)}, ${hh}:${mm}:${ss} UTC`
}

export const formatUtcMs = (iso: string): string => {
  if (!iso) return '-'
  const date = new Date(iso)
  if (isNaN(date.getTime())) return iso
  const month = MONTH_NAMES[date.getUTCMonth()] ?? '???'
  const day = date.getUTCDate()
  const hh = pad(date.getUTCHours())
  const mm = pad(date.getUTCMinutes())
  const ss = pad(date.getUTCSeconds())
  const ms = pad(date.getUTCMilliseconds(), 3)
  return `${month} ${String(day)}, ${hh}:${mm}:${ss}.${ms} UTC`
}

export const formatUtcClock = (date: Date): string =>
  date.toISOString().replace('T', ' ').slice(0, 19) + ' UTC'

export const toDatetimeLocal = (date: Date): string =>
  date.toISOString().slice(0, 16)

export type TimePreset = { label: string; minutes: number }

export const TIME_PRESETS: TimePreset[] = [
  { label: '15m', minutes: 15 },
  { label: '1h', minutes: 60 },
  { label: '24h', minutes: 60 * 24 },
  { label: '7d', minutes: 60 * 24 * 7 },
]

export const FETCH_TIMEOUT_MS = 5000

/// Convert a datetime-local input value to RFC3339 UTC.
/// Handles both "YYYY-MM-DDTHH:MM" and "YYYY-MM-DDTHH:MM:SS".
export const toRfc3339 = (datetime: string): string => {
  // datetime-local with step=1 can produce seconds
  const parts = datetime.split(':')
  if (parts.length >= 3) return `${datetime}Z`
  return `${datetime}:00Z`
}
