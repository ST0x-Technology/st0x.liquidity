export const decimalPlaces = (value: string): number => {
  const dotIdx = value.indexOf('.')
  if (dotIdx === -1) return 0
  return Math.max(2, value.length - dotIdx - 1)
}

export const fmt = (value: string): string => {
  const num = parseFloat(value)
  const dp = decimalPlaces(value)
  return num.toLocaleString('en-US', { minimumFractionDigits: dp, maximumFractionDigits: dp })
}

export const fmtNum = (value: string): string => {
  const num = parseFloat(value)
  const dp = decimalPlaces(value)
  return num.toLocaleString('en-US', { minimumFractionDigits: dp, maximumFractionDigits: dp })
}

export const fmtPct = (value: number): string => `${(value * 100).toFixed(1)}%`

export const computeTotal = (left: string, right: string): string => {
  const sum = parseFloat(left) + parseFloat(right)
  const maxDp = Math.max(decimalPlaces(left), decimalPlaces(right))
  return sum.toFixed(maxDp)
}

export const computeRatio = (onchain: string, offchain: string): number | null => {
  const on = parseFloat(onchain)
  const off = parseFloat(offchain)
  const total = on + off
  if (total === 0) return null
  return on / total
}

export const isWithinThreshold = (
  ratio: number | null,
  target: number,
  threshold: number
): boolean => {
  if (ratio === null) return true
  return Math.abs(ratio - target) <= threshold
}

export const fmtDeviation = (ratio: number | null, target: number): string => {
  if (ratio === null) return '-'
  const deviationPp = (ratio - target) * 100
  const sign = deviationPp >= 0 ? '+' : ''
  return `${sign}${deviationPp.toFixed(1)}`
}

export const stripPrefix = (symbol: string): string =>
  symbol.startsWith('t') ? symbol.slice(1) : symbol

export type UsdcBalance = {
  onchainAvailable: string
  offchainAvailable: string
}
