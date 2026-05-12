import Decimal from 'decimal.js'

const toDecimal = (value: string): Decimal =>
  new Decimal(value !== '' ? value : '0')

export const decimalAdd = (left: string, right: string): string =>
  toDecimal(left).plus(toDecimal(right)).toString()

export const decimalSub = (left: string, right: string): string =>
  toDecimal(left).minus(toDecimal(right)).toString()

export const decimalCompare = (left: string, right: string): number =>
  toDecimal(left).cmp(toDecimal(right))

export const decimalIsZero = (value: string): boolean => toDecimal(value).isZero()

export const formatDecimal = (value: string, decimals: number): string => {
  const fixed = toDecimal(value).toFixed(decimals)
  const [intPart = '0', fracPart] = fixed.split('.')
  const withCommas = intPart.replace(/\B(?=(\d{3})+(?!\d))/g, ',')
  return fracPart !== undefined ? `${withCommas}.${fracPart}` : withCommas
}
