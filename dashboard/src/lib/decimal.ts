import Decimal from 'decimal.js'

export const decimalAdd = (left: string, right: string): string =>
  new Decimal(left).plus(new Decimal(right)).toString()

export const decimalCompare = (left: string, right: string): number =>
  new Decimal(left).cmp(new Decimal(right))

export const decimalIsZero = (value: string): boolean => new Decimal(value).isZero()

export const decimalToNumber = (value: string): number => new Decimal(value).toNumber()
