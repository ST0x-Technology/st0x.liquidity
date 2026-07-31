import type { QueryClient } from '@tanstack/svelte-query'
import type { CurrentState } from '$lib/api/CurrentState'
import type { LegacyTrade } from '$lib/api/LegacyTrade'
import type { Trade } from '$lib/api/Trade'
import { compareTradesNewestFirst, normalizeTrade } from '$lib/trade'

const MAX_TRADES = 100

export const seedTrades = (queryClient: QueryClient, state: CurrentState) => {
  queryClient.setQueryData<Trade[]>(
    ['trades'],
    state.trades.map(normalizeTrade).sort(compareTradesNewestFirst)
  )
}

export const appendTrade = (queryClient: QueryClient, wireTrade: Trade | LegacyTrade) => {
  const trade = normalizeTrade(wireTrade)
  queryClient.setQueryData<Trade[]>(['trades'], (old) => {
    const merged = [trade, ...(old ?? []).filter((existing) => existing.id !== trade.id)]
    return merged.sort(compareTradesNewestFirst).slice(0, MAX_TRADES)
  })
}
