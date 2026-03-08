import { describe, it, expect } from 'vitest'
import {
  decimalPlaces,
  fmt,
  fmtNum,
  fmtPct,
  computeTotal,
  computeRatio,
  isWithinThreshold,
  fmtDeviation,
  stripPrefix
} from './inventory-panel'

describe('decimalPlaces', () => {
  it('returns 0 for integers', () => {
    expect(decimalPlaces('100')).toBe(0)
    expect(decimalPlaces('0')).toBe(0)
  })

  it('returns minimum of 2 for values with fewer decimal places', () => {
    expect(decimalPlaces('1.5')).toBe(2)
  })

  it('preserves higher precision', () => {
    expect(decimalPlaces('150.253847')).toBe(6)
    expect(decimalPlaces('33.891274')).toBe(6)
    expect(decimalPlaces('92.334')).toBe(3)
  })

  it('returns 2 for exactly 2 decimal places', () => {
    expect(decimalPlaces('80.00')).toBe(2)
  })
})

describe('fmt', () => {
  it('formats zero as 0', () => {
    expect(fmt('0')).toBe('0')
    expect(fmt('0.00')).toBe('0.00')
  })

  it('formats non-zero with locale and preserves precision', () => {
    expect(fmt('1200.00')).toBe('1,200.00')
    expect(fmt('150.253847')).toBe('150.253847')
  })
})

describe('fmtNum', () => {
  it('formats zero as 0 (not dash)', () => {
    expect(fmtNum('0')).toBe('0')
  })

  it('formats with locale and preserves precision', () => {
    expect(fmtNum('1200.00')).toBe('1,200.00')
    expect(fmtNum('150.253847')).toBe('150.253847')
  })
})

describe('fmtPct', () => {
  it('formats ratio as percentage with one decimal', () => {
    expect(fmtPct(0.5)).toBe('50.0%')
    expect(fmtPct(0.429)).toBe('42.9%')
    expect(fmtPct(1.0)).toBe('100.0%')
    expect(fmtPct(0.0)).toBe('0.0%')
  })

  it('rounds to one decimal place', () => {
    expect(fmtPct(0.3333)).toBe('33.3%')
    expect(fmtPct(0.6667)).toBe('66.7%')
  })
})

describe('computeTotal', () => {
  it('adds two values and rounds to max decimal places', () => {
    expect(computeTotal('33.891274', '67.50')).toBe('101.391274')
  })

  it('avoids floating point noise', () => {
    const total = computeTotal('33.891274', '67.50')
    expect(total).not.toContain('00000001')
  })

  it('handles integers', () => {
    expect(computeTotal('100', '200')).toBe('300')
  })

  it('uses higher precision of the two inputs', () => {
    expect(computeTotal('125000.500000', '80000.00')).toBe('205000.500000')
  })
})

describe('computeRatio', () => {
  it('returns null when both are zero', () => {
    expect(computeRatio('0', '0')).toBeNull()
  })

  it('computes onchain / (onchain + offchain)', () => {
    expect(computeRatio('150', '200')).toBeCloseTo(150 / 350)
    expect(computeRatio('75', '50')).toBeCloseTo(75 / 125)
  })

  it('returns 1.0 when all is onchain', () => {
    expect(computeRatio('1200', '0')).toBe(1.0)
  })

  it('returns 0.0 when all is offchain', () => {
    expect(computeRatio('0', '85.25')).toBe(0.0)
  })

  it('handles string decimals correctly', () => {
    expect(computeRatio('150.253847', '200.00')).toBeCloseTo(150.253847 / 350.253847)
  })
})

describe('isWithinThreshold', () => {
  it('returns true for null ratio', () => {
    expect(isWithinThreshold(null, 0.5, 0.1)).toBe(true)
  })

  it('returns true when ratio equals target', () => {
    expect(isWithinThreshold(0.5, 0.5, 0.1)).toBe(true)
  })

  it('returns true when within threshold', () => {
    expect(isWithinThreshold(0.55, 0.5, 0.1)).toBe(true)
    expect(isWithinThreshold(0.45, 0.5, 0.1)).toBe(true)
  })

  it('returns true at exact threshold boundary', () => {
    expect(isWithinThreshold(0.6, 0.5, 0.1)).toBe(true)
    expect(isWithinThreshold(0.4, 0.5, 0.1)).toBe(true)
  })

  it('returns false when outside threshold', () => {
    expect(isWithinThreshold(0.61, 0.5, 0.1)).toBe(false)
    expect(isWithinThreshold(0.39, 0.5, 0.1)).toBe(false)
  })

  it('returns false for extreme ratios', () => {
    expect(isWithinThreshold(0.0, 0.5, 0.1)).toBe(false)
    expect(isWithinThreshold(1.0, 0.5, 0.1)).toBe(false)
  })
})

describe('fmtDeviation', () => {
  it('returns dash for null ratio', () => {
    expect(fmtDeviation(null, 0.5)).toBe('-')
  })

  it('shows positive deviation with + prefix', () => {
    expect(fmtDeviation(0.6, 0.5)).toBe('+10.0')
  })

  it('shows negative deviation with - prefix', () => {
    expect(fmtDeviation(0.429, 0.5)).toBe('-7.1')
  })

  it('shows zero deviation as +0.0', () => {
    expect(fmtDeviation(0.5, 0.5)).toBe('+0.0')
  })

  it('handles extreme deviations', () => {
    expect(fmtDeviation(1.0, 0.5)).toBe('+50.0')
    expect(fmtDeviation(0.0, 0.5)).toBe('-50.0')
  })
})

describe('stripPrefix', () => {
  it('removes t prefix from tokenized symbols', () => {
    expect(stripPrefix('tAAPL')).toBe('AAPL')
    expect(stripPrefix('tTSLA')).toBe('TSLA')
    expect(stripPrefix('tSPYM')).toBe('SPYM')
  })

  it('preserves symbols without t prefix', () => {
    expect(stripPrefix('AAPL')).toBe('AAPL')
    expect(stripPrefix('USD')).toBe('USD')
  })

  it('only strips leading t', () => {
    expect(stripPrefix('test')).toBe('est')
  })
})
