import { describe, expect, it } from 'vitest'
import type { QueryClient } from '@tanstack/svelte-query'
import type { TransferOperation } from '$lib/api/TransferOperation'
import { upsertTransfer } from './transfers'

type MockQueryClient = QueryClient & {
  cache: Map<string, unknown>
}

const createMockQueryClient = (): MockQueryClient => {
  const cache = new Map<string, unknown>()

  const setQueryData = (key: unknown[], data: unknown) => {
    if (typeof data === 'function') {
      const current = cache.get(JSON.stringify(key))
      cache.set(JSON.stringify(key), (data as (old: unknown) => unknown)(current))
    } else {
      cache.set(JSON.stringify(key), data)
    }

    return undefined
  }

  return {
    setQueryData,
    cache,
  } as unknown as MockQueryClient
}

const makeTransfer = (overrides: Partial<TransferOperation> = {}): TransferOperation =>
  ({
    kind: 'equity_mint',
    id: 'transfer-1',
    symbol: 'AAPL',
    quantity: '10',
    status: { status: 'minting' },
    startedAt: '2024-01-01T12:00:00Z',
    updatedAt: '2024-01-01T12:00:00Z',
    ...overrides,
  }) as TransferOperation

describe('upsertTransfer', () => {
  it('moves a completed transfer from active to recent', () => {
    const queryClient = createMockQueryClient()
    const active = makeTransfer({ id: 'transfer-1', status: { status: 'minting' } })

    queryClient.setQueryData(['transfers', 'active'], [active])
    queryClient.setQueryData(['transfers', 'recent'], [])

    const completed = makeTransfer({
      id: 'transfer-1',
      status: { status: 'completed', completedAt: '2024-01-01T13:00:00Z' },
    })

    upsertTransfer(queryClient, completed)

    const activeCache = queryClient.cache.get('["transfers","active"]') as TransferOperation[]
    const recentCache = queryClient.cache.get('["transfers","recent"]') as TransferOperation[]

    expect(activeCache).toHaveLength(0)
    expect(recentCache).toHaveLength(1)
    expect(recentCache[0]).toEqual(completed)
  })

  it('moves a reconciled transfer from active to recent', () => {
    const queryClient = createMockQueryClient()
    const active = makeTransfer({ id: 'transfer-2', status: { status: 'minting' } })

    queryClient.setQueryData(['transfers', 'active'], [active])
    queryClient.setQueryData(['transfers', 'recent'], [])

    const reconciled = makeTransfer({
      id: 'transfer-2',
      status: {
        status: 'reconciled',
        reconciledAt: '2024-01-01T14:00:00Z',
        failureReason: 'deposit timed out',
        reconcileReason: 'funds moved manually',
      },
    })

    upsertTransfer(queryClient, reconciled)

    const activeCache = queryClient.cache.get('["transfers","active"]') as TransferOperation[]
    const recentCache = queryClient.cache.get('["transfers","recent"]') as TransferOperation[]

    expect(activeCache).toHaveLength(0)
    expect(recentCache).toHaveLength(1)
    expect(recentCache[0]).toEqual(reconciled)
  })

  it('moves a failed transfer from active to recent', () => {
    const queryClient = createMockQueryClient()
    const active = makeTransfer({ id: 'transfer-3', status: { status: 'minting' } })

    queryClient.setQueryData(['transfers', 'active'], [active])
    queryClient.setQueryData(['transfers', 'recent'], [])

    const failed = makeTransfer({
      id: 'transfer-3',
      status: { status: 'failed', failedAt: '2024-01-01T13:00:00Z' },
    })

    upsertTransfer(queryClient, failed)

    const activeCache = queryClient.cache.get('["transfers","active"]') as TransferOperation[]
    const recentCache = queryClient.cache.get('["transfers","recent"]') as TransferOperation[]

    expect(activeCache).toHaveLength(0)
    expect(recentCache).toHaveLength(1)
    expect(recentCache[0]).toEqual(failed)
  })

  it('inserts a reconciled transfer directly into recent when not in active', () => {
    const queryClient = createMockQueryClient()

    queryClient.setQueryData(['transfers', 'active'], [])
    queryClient.setQueryData(['transfers', 'recent'], [])

    const reconciled = makeTransfer({
      id: 'transfer-4',
      status: {
        status: 'reconciled',
        reconciledAt: '2024-01-01T14:00:00Z',
        failureReason: 'deposit timed out',
        reconcileReason: 'funds moved manually',
      },
    })

    upsertTransfer(queryClient, reconciled)

    const activeCache = queryClient.cache.get('["transfers","active"]') as TransferOperation[]
    const recentCache = queryClient.cache.get('["transfers","recent"]') as TransferOperation[]

    expect(activeCache).toHaveLength(0)
    expect(recentCache).toHaveLength(1)
    expect(recentCache[0]).toEqual(reconciled)
  })

  it('inserts an in-flight transfer into active (not recent)', () => {
    const queryClient = createMockQueryClient()

    queryClient.setQueryData(['transfers', 'active'], [])
    queryClient.setQueryData(['transfers', 'recent'], [])

    const inflight = makeTransfer({ id: 'transfer-3', status: { status: 'minting' } })

    upsertTransfer(queryClient, inflight)

    const activeCache = queryClient.cache.get('["transfers","active"]') as TransferOperation[]
    const recentCache = queryClient.cache.get('["transfers","recent"]') as TransferOperation[]

    expect(activeCache).toHaveLength(1)
    expect(activeCache[0]).toEqual(inflight)
    expect(recentCache).toHaveLength(0)
  })
})
