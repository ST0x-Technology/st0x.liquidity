import { describe, expect, it } from 'vitest'
import type { UsdcInventory } from '$lib/api/UsdcInventory'

import { cashInventoryAmounts } from './inventory-cash'

const usdc = (overrides: Partial<UsdcInventory>): UsdcInventory => ({
  onchainAvailable: '100',
  onchainInflight: '0',
  offchainAvailable: '50',
  offchainInflight: '0',
  offchainGross: null,
  withdrawableCash: null,
  alpacaUsdc: null,
  inflightCash: {
    ethereumWallet: null,
    baseWallet: null
  },
  ...overrides
})

describe('cashInventoryAmounts', () => {
  it('derives Alpaca total, reserve, rebalanceable, and counter-tradeable cash from gross cash', () => {
    const amounts = cashInventoryAmounts(
      usdc({
        onchainAvailable: '100',
        offchainAvailable: '60',
        offchainGross: '80',
        withdrawableCash: '70'
      }),
      '20'
    )

    expect(amounts.alpacaUsd).toBe('80')
    expect(amounts.alpacaUsdc).toBeNull()
    expect(amounts.reserve).toBe('20')
    expect(amounts.rebalanceableUsd).toBe('50')
    expect(amounts.counterTradeUsd).toBe('80')
    expect(amounts.trackedTotal).toBe('180')
    expect(amounts.venueRatio).toBeCloseTo(100 / 180)
  })

  it('derives reserve from gross - available when configured reserve is absent', () => {
    const amounts = cashInventoryAmounts(
      usdc({
        onchainAvailable: '100',
        offchainAvailable: '60',
        offchainGross: '85',
        withdrawableCash: '70',
        alpacaUsdc: '12.5'
      })
    )

    expect(amounts.reserve).toBe('25')
    expect(amounts.rebalanceableUsd).toBe('45')
    expect(amounts.alpacaUsdc).toBe('12.5')
  })

  it('falls back to available Alpaca cash when gross cash is absent', () => {
    const amounts = cashInventoryAmounts(
      usdc({
        onchainAvailable: '100',
        offchainAvailable: '50',
        withdrawableCash: '40'
      })
    )

    expect(amounts.alpacaUsd).toBe('50')
    expect(amounts.reserve).toBeNull()
    expect(amounts.rebalanceableUsd).toBe('40')
    expect(amounts.counterTradeUsd).toBe('50')
    expect(amounts.trackedTotal).toBe('150')
    expect(amounts.venueRatio).toBeCloseTo(100 / 150)
  })

  it('does not let reserve-adjusted rebalanceable cash go negative', () => {
    const amounts = cashInventoryAmounts(
      usdc({
        offchainAvailable: '60',
        offchainGross: '100',
        withdrawableCash: '25'
      }),
      '40'
    )

    expect(amounts.reserve).toBe('40')
    expect(amounts.rebalanceableUsd).toBe('0')
    expect(amounts.counterTradeUsd).toBe('100')
  })

  it('uses configured reserve when gross cash is below the reserve target', () => {
    const amounts = cashInventoryAmounts(
      usdc({
        offchainAvailable: '0',
        offchainGross: '20',
        withdrawableCash: '20'
      }),
      '50'
    )

    expect(amounts.alpacaUsd).toBe('20')
    expect(amounts.alpacaUsdc).toBeNull()
    expect(amounts.reserve).toBe('50')
    expect(amounts.rebalanceableUsd).toBe('0')
    expect(amounts.counterTradeUsd).toBe('20')
  })

  it('keeps tracked inflight cash in total but out of the venue ratio', () => {
    const amounts = cashInventoryAmounts(
      usdc({
        onchainAvailable: '100',
        onchainInflight: '5',
        offchainAvailable: '60',
        offchainGross: '80',
        offchainInflight: '15'
      })
    )

    expect(amounts.trackedTotal).toBe('200')
    expect(amounts.venueRatio).toBeCloseTo(100 / 180)
  })

  it('leaves rebalanceable unavailable when withdrawable cash is absent', () => {
    const amounts = cashInventoryAmounts(
      usdc({
        offchainAvailable: '60',
        offchainGross: '80',
        withdrawableCash: null
      })
    )

    expect(amounts.rebalanceableUsd).toBeNull()
  })
})
