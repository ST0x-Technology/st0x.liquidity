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

/// Formats a Unix epoch timestamp (seconds) to a UTC datetime string.
export const formatTimestamp = (epoch: number): string => {
  if (epoch === 0) return '-'
  return new Date(epoch * 1000).toISOString().slice(0, 19).replace('T', ' ')
}
