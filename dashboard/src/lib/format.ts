/// Converts a raw integer token amount string to a human-readable decimal.
/// For example, formatBalance("1500000", 6) returns "1.5".
export const formatBalance = (raw: string, decimals: number): string => {
  if (!raw || raw === '0') return '0'

  if (decimals === 0) return raw

  const padded = raw.padStart(decimals + 1, '0')
  const intPart = padded.slice(0, padded.length - decimals) || '0'
  const fracPart = padded.slice(padded.length - decimals)

  const trimmed = fracPart.replace(/0+$/, '')
  return trimmed ? `${intPart}.${trimmed}` : intPart
}

/// Formats a decimal string for display by rounding to a sensible number
/// of significant digits. Handles values the upstream API returns as
/// pre-formatted decimals (e.g. "10.996", "1993.824…").
///
/// Rules:
///  - Values >= 1: show up to 6 decimal places, trim trailing zeros.
///  - Values < 1: preserve up to 6 significant digits after the leading zeros.
///  - Non-numeric / empty: returned as-is.
export const formatDecimal = (value: string): string => {
  if (!value || value === '0') return '0'

  const num = Number(value)
  if (Number.isNaN(num)) return value

  if (num === 0) return '0'

  // For values >= 1, fixed 6 decimal places is plenty.
  if (Math.abs(num) >= 1) {
    return parseFloat(num.toFixed(6)).toString()
  }

  // For tiny values (< 1), toFixed(6) would round to "0.000000".
  // Use toPrecision to keep 6 significant digits instead.
  return parseFloat(num.toPrecision(6)).toString()
}

/// Formats a Unix epoch timestamp (seconds) to a UTC datetime string.
export const formatTimestamp = (epoch: number): string => {
  if (epoch === 0) return '-'
  return new Date(epoch * 1000).toISOString().slice(0, 19).replace('T', ' ')
}
