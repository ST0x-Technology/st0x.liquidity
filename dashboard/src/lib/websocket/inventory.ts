import type { QueryClient } from '@tanstack/svelte-query'
import type { CurrentState } from '$lib/api/CurrentState'
import type { Inventory } from '$lib/api/Inventory'
import type { InventorySnapshot } from '$lib/api/InventorySnapshot'
import type { Position } from '$lib/api/Position'
import type { UsdcInventory } from '$lib/api/UsdcInventory'

/** The pre-rollout shape of `Inventory`: cash counted with no stable named. */
type PreRolloutInventory = Omit<Inventory, 'usdc'> & {
  usdc: Omit<UsdcInventory, 'symbol'> & { symbol?: string }
}

/**
 * Rollout shim: the dashboard profile activates before st0x-hedge, so a fresh
 * page can read a backend that still counts cash without naming its stable.
 * Every backend before the symbol settled in USDC, so name it once here and
 * the readers only ever see a named stable. Delete once every environment
 * runs a backend that sends the symbol.
 */
const namedStable = (inventory: PreRolloutInventory): Inventory => ({
  ...inventory,
  usdc: { ...inventory.usdc, symbol: inventory.usdc.symbol ?? 'USDC' }
})

export const seedInventory = (queryClient: QueryClient, state: CurrentState) => {
  queryClient.setQueryData(['inventory'], namedStable(state.inventory))
  queryClient.setQueryData(['positions'], state.positions)
  queryClient.setQueryData(['settings'], state.settings)
}

export const updateSnapshot = (queryClient: QueryClient, snapshot: InventorySnapshot) => {
  queryClient.setQueryData<Inventory>(['inventory'], namedStable(snapshot.inventory))
}

export const upsertPosition = (queryClient: QueryClient, updated: Position) => {
  queryClient.setQueryData<Position[]>(['positions'], (prev) => {
    if (!prev) return [updated]

    const index = prev.findIndex((position) => position.symbol === updated.symbol)

    if (index === -1) return [...prev, updated]

    const next = [...prev]
    next[index] = updated
    return next
  })
}
