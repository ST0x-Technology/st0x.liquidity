import { describe, expect, it } from 'vitest'
import { formatBalance, formatDecimal, formatTimestamp } from './format'

describe('formatBalance', () => {
  it('returns "0" for empty string', () => {
    expect(formatBalance('', 6)).toBe('0')
  })

  it('returns "0" for zero', () => {
    expect(formatBalance('0', 18)).toBe('0')
  })

  it('formats a normal value with 6 decimals', () => {
    expect(formatBalance('1500000', 6)).toBe('1.5')
  })

  it('formats a normal value with 18 decimals', () => {
    expect(formatBalance('500000000000000000000', 18)).toBe('500')
  })

  it('handles values shorter than decimals', () => {
    expect(formatBalance('500', 6)).toBe('0.0005')
  })

  it('handles decimals = 0', () => {
    expect(formatBalance('12345', 0)).toBe('12345')
  })

  it('trims trailing zeros', () => {
    expect(formatBalance('1000000', 6)).toBe('1')
  })

  it('preserves significant fractional digits', () => {
    expect(formatBalance('1234567', 6)).toBe('1.234567')
  })

  it('handles large values', () => {
    expect(formatBalance('100000000000000000000000', 18)).toBe('100000')
  })
})

describe('formatDecimal', () => {
  it('returns "0" for empty or zero', () => {
    expect(formatDecimal('')).toBe('0')
    expect(formatDecimal('0')).toBe('0')
  })

  it('returns non-numeric strings as-is', () => {
    expect(formatDecimal('abc')).toBe('abc')
  })

  it('trims excessive decimals on values >= 1', () => {
    expect(formatDecimal('1993.82413955615987854')).toBe('1993.82414')
  })

  it('trims trailing zeros', () => {
    expect(formatDecimal('100.500000')).toBe('100.5')
  })

  it('handles integers', () => {
    expect(formatDecimal('42')).toBe('42')
  })

  it('preserves significant digits for tiny values', () => {
    expect(formatDecimal('0.00000000000010996')).toBe('1.0996e-13')
  })

  it('formats normal small values', () => {
    expect(formatDecimal('0.123456789')).toBe('0.123457')
  })

  it('handles IO ratio style values', () => {
    expect(formatDecimal('180.79830445')).toBe('180.798304')
    expect(formatDecimal('0.5')).toBe('0.5')
  })
})

describe('formatTimestamp', () => {
  it('returns "-" for epoch 0', () => {
    expect(formatTimestamp(0)).toBe('-')
  })

  it('formats a known epoch', () => {
    expect(formatTimestamp(1718452800)).toBe('2024-06-15 12:00:00')
  })
})
