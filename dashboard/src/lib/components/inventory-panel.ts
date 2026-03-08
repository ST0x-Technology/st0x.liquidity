import { decimalAdd, decimalIsZero, decimalToNumber } from '$lib/decimal'

export const decimalPlaces = (value: string): number => {
  const dotIdx = value.indexOf('.')
  if (dotIdx === -1) return 0
  return Math.max(2, value.length - dotIdx - 1)
}

export const fmt = (value: string): string => {
  if (decimalIsZero(value)) return '-'
  const dp = decimalPlaces(value)
  return decimalToNumber(value).toLocaleString('en-US', { minimumFractionDigits: dp, maximumFractionDigits: dp })
}

export const fmtNum = (value: string): string => {
  const dp = decimalPlaces(value)
  return decimalToNumber(value).toLocaleString('en-US', { minimumFractionDigits: dp, maximumFractionDigits: dp })
}

export const fmtPct = (value: number): string => `${(value * 100).toFixed(1)}%`

export const computeTotal = (left: string, right: string): string =>
  decimalAdd(left, right)

export const computeRatio = (onchain: string, offchain: string): number | null => {
  const on = decimalToNumber(onchain)
  const off = decimalToNumber(offchain)
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
