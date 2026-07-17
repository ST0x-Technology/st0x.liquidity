import type { QueryClient } from '@tanstack/svelte-query'
import type { CurrentState } from '$lib/api/CurrentState'
import type { Trade } from '$lib/api/Trade'

const MAX_TRADES = 100

export const seedTrades = (queryClient: QueryClient, state: CurrentState) => {
  queryClient.setQueryData<Trade[]>(['trades'], state.trades)
}

export const appendTrade = (queryClient: QueryClient, trade: Trade) => {
  queryClient.setQueryData<Trade[]>(['trades'], (old) =>
    [trade, ...(old ?? []).filter((existing) => existing.id !== trade.id)].slice(0, MAX_TRADES)
  )
}
