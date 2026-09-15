import { describe, expect, it } from 'vitest'
import type { QueryClient } from '@tanstack/svelte-query'
import type { CurrentState } from '$lib/api/CurrentState'
import type { Inventory } from '$lib/api/Inventory'
import type { InventorySnapshot } from '$lib/api/InventorySnapshot'
import { seedInventory, updateSnapshot } from './inventory'

const createQueryClient = () => {
  const cache = new Map<string, unknown>()
  const queryClient = {
    setQueryData: (key: unknown[], data: unknown) => {
      cache.set(JSON.stringify(key), data)
    }
  } as unknown as QueryClient

  return { queryClient, cache }
}

/** An inventory from a backend that predates the settlement-stable symbol. */
const preRolloutInventory = () => ({
  perSymbol: [],
  usdc: {
    onchainAvailable: '10',
    onchainInflight: '0',
    offchainAvailable: '5',
    offchainInflight: '0',
    offchainGross: null,
    withdrawableCash: null,
    alpacaUsdc: null,
    inflightCash: { ethereumWallet: null, baseWallet: null }
  }
})

const cachedInventory = (cache: Map<string, unknown>): Inventory =>
  cache.get('["inventory"]') as Inventory

describe('seedInventory', () => {
  it("names a pre-rollout inventory's cash USDC", () => {
    const { queryClient, cache } = createQueryClient()
    const state = {
      inventory: preRolloutInventory(),
      positions: [],
      settings: {}
    } as unknown as CurrentState

    seedInventory(queryClient, state)

    expect(cachedInventory(cache).usdc.symbol).toBe('USDC')
  })

  it('keeps the stable a current backend names', () => {
    const { queryClient, cache } = createQueryClient()
    const inventory = preRolloutInventory()
    const state = {
      inventory: { ...inventory, usdc: { ...inventory.usdc, symbol: 'USDG' } },
      positions: [],
      settings: {}
    } as unknown as CurrentState

    seedInventory(queryClient, state)

    expect(cachedInventory(cache).usdc.symbol).toBe('USDG')
  })
})

describe('updateSnapshot', () => {
  it("names a pre-rollout snapshot's cash USDC", () => {
    const { queryClient, cache } = createQueryClient()
    const snapshot = {
      inventory: preRolloutInventory(),
      fetchedAt: '2026-09-15T00:00:00Z'
    } as unknown as InventorySnapshot

    updateSnapshot(queryClient, snapshot)

    expect(cachedInventory(cache).usdc.symbol).toBe('USDC')
  })
})
