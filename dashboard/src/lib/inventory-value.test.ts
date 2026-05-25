import { describe, expect, it } from 'vitest'
import { cashUsdTooltip, equityUsdTooltip, positionSharesTooltip } from './inventory-value'

describe('cashUsdTooltip', () => {
  it('formats cash inventory as USD', () => {
    expect(cashUsdTooltip('1234.5')).toBe('Current USD value: $1,234.50')
  })

  it('does not include the full amount when display was truncated', () => {
    expect(cashUsdTooltip('1.234567')).toBe('Current USD value: $1.23')
  })
})

describe('equityUsdTooltip', () => {
  it('multiplies shares by the latest USDC price', () => {
    expect(equityUsdTooltip('3.5', '187.25')).toBe('Current USD value: $655.38')
  })

  it('reports missing price data instead of guessing', () => {
    expect(equityUsdTooltip('3.5', null)).toBe(
      'Current USD value unavailable: missing latest price'
    )
  })
})

describe('positionSharesTooltip', () => {
  it('shows the underlying signed stock amount', () => {
    expect(positionSharesTooltip('-1.2500')).toBe('Stock amount: -1.25 shares')
  })
})
