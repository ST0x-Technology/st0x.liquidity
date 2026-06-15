import { describe, expect, it } from 'vitest'
import {
  decimalAdd,
  decimalCompare,
  decimalIsZero,
  decimalSub,
  formatDecimal,
} from './decimal'

describe('formatDecimal', () => {
  it('formats a normal value with grouping and fixed decimals', () => {
    expect(formatDecimal('1234567.891', 3)).toBe('1,234,567.891')
  })

  it('pads to the requested number of decimals', () => {
    expect(formatDecimal('1.5', 3)).toBe('1.500')
  })

  it('renders absent values as zero', () => {
    expect(formatDecimal('', 3)).toBe('0.000')
    expect(formatDecimal('   ', 3)).toBe('0.000')
    // The typed `string` contract is enforced only by an unchecked cast of the
    // upstream JSON, so null/undefined reach this function at runtime.
    expect(formatDecimal(null as unknown as string, 3)).toBe('0.000')
    expect(formatDecimal(undefined as unknown as string, 3)).toBe('0.000')
  })

  it('surfaces a present-but-unparseable value instead of throwing', () => {
    // Regression: a single bad order value used to throw inside the orders
    // panel's {#each} render and freeze the entire dashboard until refresh.
    expect(formatDecimal('1,000', 3)).toBe('1,000')
    expect(formatDecimal('0x', 3)).toBe('0x')
    expect(formatDecimal('not-a-number', 3)).toBe('not-a-number')
  })
})

describe('decimal arithmetic', () => {
  it('adds and subtracts decimal strings', () => {
    expect(decimalAdd('1.5', '2.25')).toBe('3.75')
    expect(decimalSub('5', '1.5')).toBe('3.5')
  })

  it('treats absent operands as zero', () => {
    expect(decimalAdd('', '2')).toBe('2')
    expect(decimalSub('5', '  ')).toBe('5')
  })

  it('compares decimal strings', () => {
    expect(decimalCompare('1', '2')).toBe(-1)
    expect(decimalCompare('2', '2')).toBe(0)
    expect(decimalCompare('3', '2')).toBe(1)
  })

  it('detects zero across absent forms', () => {
    expect(decimalIsZero('0')).toBe(true)
    expect(decimalIsZero('')).toBe(true)
    expect(decimalIsZero('0.5')).toBe(false)
  })

  it('throws on a present-but-unparseable operand rather than fabricating a number', () => {
    expect(() => decimalAdd('1,000', '1')).toThrow()
  })
})
