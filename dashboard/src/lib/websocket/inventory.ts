import type { QueryClient } from '@tanstack/svelte-query'
import type { CurrentState } from '$lib/api/CurrentState'
import type { Inventory } from '$lib/api/Inventory'
import type { InventorySnapshot } from '$lib/api/InventorySnapshot'
import type { Position } from '$lib/api/Position'

export const seedInventory = (queryClient: QueryClient, state: CurrentState) => {
  queryClient.setQueryData(['inventory'], state.inventory)
  queryClient.setQueryData(['positions'], state.positions)
  queryClient.setQueryData(['settings'], state.settings)
}

export const updateSnapshot = (queryClient: QueryClient, snapshot: InventorySnapshot) => {
  queryClient.setQueryData<Inventory>(['inventory'], snapshot.inventory)
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
