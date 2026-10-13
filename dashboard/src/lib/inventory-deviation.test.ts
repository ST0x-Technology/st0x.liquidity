import { describe, expect, it } from 'vitest'

import { type DeviationBands, ratioDeviation } from './inventory-deviation'

const bands = (overrides: Partial<DeviationBands>): DeviationBands => ({
  equityTarget: 0.5,
  equityDeviation: 0.1,
  usdcTarget: 0.4,
  usdcDeviation: 0.05,
  ...overrides
})

describe('ratioDeviation', () => {
  it('returns no verdict without settings', () => {
    expect(ratioDeviation(undefined, 0.9, true)).toBeNull()
    expect(ratioDeviation(undefined, 0.9, false)).toBeNull()
  })

  it('styles cash against the USDC band', () => {
    expect(ratioDeviation(bands({}), 0.5, true)).toEqual({ style: 'high' })
    expect(ratioDeviation(bands({}), 0.3, true)).toEqual({ style: 'low' })
    expect(ratioDeviation(bands({}), 0.42, true)).toEqual({ style: 'normal' })
  })

  it('styles equity against the equity band', () => {
    expect(ratioDeviation(bands({}), 0.65, false)).toEqual({ style: 'high' })
    expect(ratioDeviation(bands({}), 0.35, false)).toEqual({ style: 'low' })
    expect(ratioDeviation(bands({}), 0.5, false)).toEqual({ style: 'normal' })
  })

  it('returns no cash verdict for a missing USDC band while equity settings are present', () => {
    const noUsdcBand = bands({ usdcTarget: null, usdcDeviation: null })

    expect(ratioDeviation(noUsdcBand, 0.9, true)).toBeNull()
    expect(ratioDeviation(noUsdcBand, 0.5, true)).toBeNull()
    expect(ratioDeviation(noUsdcBand, 0.1, true)).toBeNull()
    expect(ratioDeviation(noUsdcBand, 0.65, false)).toEqual({ style: 'high' })
  })

  it('returns no cash verdict when only one half of the USDC band is set', () => {
    expect(ratioDeviation(bands({ usdcTarget: null }), 0.9, true)).toBeNull()
    expect(ratioDeviation(bands({ usdcDeviation: null }), 0.9, true)).toBeNull()
  })

  it('returns no equity verdict when the equity target is missing', () => {
    expect(ratioDeviation(bands({ equityTarget: null }), 0.9, false)).toBeNull()
  })
})
