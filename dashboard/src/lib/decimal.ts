import Decimal from 'decimal.js'

export const decimalAdd = (left: string, right: string): string =>
  new Decimal(left).plus(new Decimal(right)).toString()

export const decimalCompare = (left: string, right: string): number =>
  new Decimal(left).cmp(new Decimal(right))

export const decimalIsZero = (value: string): boolean => new Decimal(value).isZero()

export const formatDecimal = (value: string, decimals: number): string => {
  const fixed = new Decimal(value).toFixed(decimals)
  const [intPart = '0', fracPart] = fixed.split('.')
  const withCommas = intPart.replace(/\B(?=(\d{3})+(?!\d))/g, ',')
  return fracPart !== undefined ? `${withCommas}.${fracPart}` : withCommas
}
