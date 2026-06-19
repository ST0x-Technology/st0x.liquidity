import Decimal from 'decimal.js'

// These values come from external APIs (e.g. the Raindex orders proxy) and are
// only typed as `string` after an unchecked cast -- at runtime a field can be
// null, undefined, or whitespace. Treat every genuinely-absent form as "0"
// (matching the original empty-string guard) so decimal.js never throws on
// missing data. A value that is present but unparseable (e.g. "1,000", "0x")
// still throws here; only `formatDecimal` softens that, never the arithmetic
// helpers, which must not fabricate a number from garbage.
const toDecimal = (value: string | null | undefined): Decimal => {
  const trimmed = value?.trim() ?? ''
  return new Decimal(trimmed === '' ? '0' : trimmed)
}

export const decimalAdd = (left: string, right: string): string =>
  toDecimal(left).plus(toDecimal(right)).toString()

export const decimalSub = (left: string, right: string): string =>
  toDecimal(left).minus(toDecimal(right)).toString()

export const decimalMul = (left: string, right: string): string =>
  toDecimal(left).mul(toDecimal(right)).toString()

export const decimalCompare = (left: string, right: string): number =>
  toDecimal(left).cmp(toDecimal(right))

export const decimalIsZero = (value: string): boolean => toDecimal(value).isZero()

export const formatDecimal = (value: string, decimals: number): string => {
  let fixed: string
  try {
    fixed = toDecimal(value).toFixed(decimals)
  } catch {
    // A present-but-unparseable value would throw, and an unhandled throw
    // inside a Svelte {#each} render freezes the whole dashboard. Surface the
    // raw value rather than crashing or fabricating a misleading number.
    return value
  }

  const [intPart = '0', fracPart] = fixed.split('.')
  const withCommas = intPart.replace(/\B(?=(\d{3})+(?!\d))/g, ',')
  return fracPart !== undefined ? `${withCommas}.${fracPart}` : withCommas
}
