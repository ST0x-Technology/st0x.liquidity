import { describe, expect, it, vi, beforeEach, afterEach } from 'vitest'
import { createWebSocket } from './websocket.svelte'
import type { QueryClient } from '@tanstack/svelte-query'
import type { EventStoreEntry } from '$lib/api/EventStoreEntry'
import type { InitialState } from '$lib/api/InitialState'
import type { Inventory } from '$lib/api/Inventory'
import type { ServerMessage } from '$lib/api/ServerMessage'
import type { TransferOperation } from '$lib/api/TransferOperation'

class MockWebSocket {
  static instances: MockWebSocket[] = []

  url: string
  onopen: (() => void) | null = null
  onclose: (() => void) | null = null
  onmessage: ((event: { data: string }) => void) | null = null
  onerror: (() => void) | null = null

  constructor(url: string) {
    this.url = url
    MockWebSocket.instances.push(this)
  }

  close() {
    this.onclose?.()
  }

  simulateOpen() {
    this.onopen?.()
  }

  simulateMessage(msg: ServerMessage) {
    this.onmessage?.({ data: JSON.stringify(msg) })
  }

  simulateRawMessage(data: string) {
    this.onmessage?.({ data })
  }

  simulateError() {
    this.onerror?.()
  }

  simulateClose() {
    this.onclose?.()
  }

  static getInstance(index: number): MockWebSocket {
    const instance = MockWebSocket.instances[index]
    if (!instance) {
      throw new Error(`MockWebSocket instance ${String(index)} not found`)
    }
    return instance
  }
}

type MockQueryClient = QueryClient & {
  cache: Map<string, unknown>
  setQueryDataSpy: ReturnType<typeof vi.fn>
  getQueryDataSpy: ReturnType<typeof vi.fn>
  clearSpy: ReturnType<typeof vi.fn>
}

const createMockQueryClient = (): MockQueryClient => {
  const cache = new Map<string, unknown>()

  const setQueryDataSpy = vi.fn((key: unknown[], data: unknown) => {
    cache.set(JSON.stringify(key), data)
    return undefined
  })

  const getQueryDataSpy = vi.fn((key: unknown[]) => cache.get(JSON.stringify(key)))

  const clearSpy = vi.fn(() => {
    cache.clear()
  })

  return {
    setQueryData: setQueryDataSpy,
    getQueryData: getQueryDataSpy,
    clear: clearSpy,
    cache,
    setQueryDataSpy,
    getQueryDataSpy,
    clearSpy
  } as unknown as MockQueryClient
}

const createTimeframeMetrics = () => ({
  aum: '0',
  pnl: { absolute: '0', percent: '0' },
  volume: '0',
  tradeCount: 0,
  sharpeRatio: null,
  sortinoRatio: null,
  maxDrawdown: '0',
  hedgeLagMs: null,
  uptimePercent: '100'
})

const createUsdcInventory = () => ({
  onchainAvailable: '0',
  onchainInflight: '0',
  offchainAvailable: '0',
  offchainInflight: '0'
})

const createInitialState = (): InitialState => ({
  recentTrades: [],
  recentEvents: [],
  inventory: { perSymbol: [], usdc: createUsdcInventory(), snapshotAt: null },
  metrics: {
    '1h': createTimeframeMetrics(),
    '1d': createTimeframeMetrics(),
    '1w': createTimeframeMetrics(),
    '1m': createTimeframeMetrics(),
    all: createTimeframeMetrics()
  },
  spreads: [],
  activeTransfers: [],
  recentTransfers: [],
  authStatus: { status: 'not_configured' },
  circuitBreaker: { status: 'active' },
  rebalancing: {
    equityOnchainRatio: '0.5',
    equityTriggerThreshold: '0.2',
    cashOnchainRatio: '0.5',
    cashTriggerThreshold: '0.2'
  }
})

describe('createWebSocket', () => {
  beforeEach(() => {
    MockWebSocket.instances = []
    vi.stubGlobal('WebSocket', MockWebSocket)
    vi.useFakeTimers()
  })

  afterEach(() => {
    vi.unstubAllGlobals()
    vi.useRealTimers()
  })

  it('starts in disconnected state', () => {
    const queryClient = createMockQueryClient()
    const ws = createWebSocket('ws://localhost:8080', queryClient)

    expect(ws.state).toBe('disconnected')
  })

  it('transitions to connecting then connected on successful connect', () => {
    const queryClient = createMockQueryClient()
    const ws = createWebSocket('ws://localhost:8080', queryClient)

    ws.connect()
    expect(ws.state).toBe('connecting')

    MockWebSocket.getInstance(0).simulateOpen()
    expect(ws.state).toBe('connected')
  })

  it('populates query cache on initial message', () => {
    const queryClient = createMockQueryClient()
    const ws = createWebSocket('ws://localhost:8080', queryClient)

    ws.connect()
    MockWebSocket.getInstance(0).simulateOpen()

    const initialState = createInitialState()
    const initialMessage: ServerMessage = {
      type: 'initial',
      data: initialState
    }

    MockWebSocket.getInstance(0).simulateMessage(initialMessage)

    expect(queryClient.setQueryDataSpy).toHaveBeenCalledWith(['events'], [])
    expect(queryClient.setQueryDataSpy).toHaveBeenCalledWith(['trades'], [])
    expect(queryClient.setQueryDataSpy).toHaveBeenCalledWith(['inventory'], initialState.inventory)
    expect(queryClient.setQueryDataSpy).toHaveBeenCalledWith(['auth'], { status: 'not_configured' })
    expect(queryClient.setQueryDataSpy).toHaveBeenCalledWith(['circuitBreaker'], { status: 'active' })
    expect(queryClient.setQueryDataSpy).toHaveBeenCalledWith(['rebalancing'], initialState.rebalancing)
  })

  it('prepends events and caps at 100', () => {
    const queryClient = createMockQueryClient()
    const ws = createWebSocket('ws://localhost:8080', queryClient)

    ws.connect()
    MockWebSocket.getInstance(0).simulateOpen()

    const existingEvents: EventStoreEntry[] = Array.from({ length: 99 }, (_, i) => ({
      aggregate_type: 'Position',
      aggregate_id: `existing-${String(i)}`,
      sequence: i,
      event_type: 'Created',
      timestamp: '2024-01-01T00:00:00Z'
    }))

    queryClient.setQueryDataSpy.mockImplementation(
      (key: unknown[], updater: unknown): EventStoreEntry[] | undefined => {
        if (JSON.stringify(key) === '["events"]' && typeof updater === 'function') {
          const result = (updater as (old: EventStoreEntry[] | undefined) => EventStoreEntry[])(
            existingEvents
          )
          queryClient.cache.set(JSON.stringify(key), result)
          return result
        }
        return undefined
      }
    )

    const newEvent: EventStoreEntry = {
      aggregate_type: 'Position',
      aggregate_id: 'new-event',
      sequence: 1,
      event_type: 'Updated',
      timestamp: '2024-01-02T00:00:00Z'
    }

    MockWebSocket.getInstance(0).simulateMessage({ type: 'event', data: newEvent })

    const setQueryDataCalls = queryClient.setQueryDataSpy.mock.calls
    const eventsCall = setQueryDataCalls.find(
      (call: unknown[]) => JSON.stringify(call[0]) === '["events"]'
    )

    expect(eventsCall).toBeDefined()
    if (eventsCall === undefined) {
      throw new Error('eventsCall should be defined')
    }
    const updater = eventsCall[1] as (old: EventStoreEntry[] | undefined) => EventStoreEntry[]
    const result = updater(existingEvents)

    expect(result.length).toBe(100)
    expect(result[0]).toEqual(newEvent)
  })

  it('schedules reconnect on close with exponential backoff', () => {
    const queryClient = createMockQueryClient()
    const ws = createWebSocket('ws://localhost:8080', queryClient)

    ws.connect()
    MockWebSocket.getInstance(0).simulateOpen()
    MockWebSocket.getInstance(0).simulateClose()

    expect(ws.state).toBe('error')

    vi.advanceTimersByTime(1000)
    expect(MockWebSocket.instances.length).toBe(2)

    MockWebSocket.getInstance(1).simulateClose()
    vi.advanceTimersByTime(2000)
    expect(MockWebSocket.instances.length).toBe(3)

    MockWebSocket.getInstance(2).simulateClose()
    vi.advanceTimersByTime(4000)
    expect(MockWebSocket.instances.length).toBe(4)
  })

  it('resets reconnect attempts on successful connection', () => {
    const queryClient = createMockQueryClient()
    const ws = createWebSocket('ws://localhost:8080', queryClient)

    ws.connect()
    MockWebSocket.getInstance(0).simulateOpen()
    MockWebSocket.getInstance(0).simulateClose()

    vi.advanceTimersByTime(1000)
    MockWebSocket.getInstance(1).simulateClose()

    vi.advanceTimersByTime(2000)
    MockWebSocket.getInstance(2).simulateOpen()
    MockWebSocket.getInstance(2).simulateClose()

    vi.advanceTimersByTime(1000)
    expect(MockWebSocket.instances.length).toBe(4)
  })

  it('disconnect cancels reconnect and closes socket', () => {
    const queryClient = createMockQueryClient()
    const ws = createWebSocket('ws://localhost:8080', queryClient)

    ws.connect()
    MockWebSocket.getInstance(0).simulateOpen()
    MockWebSocket.getInstance(0).simulateClose()

    ws.disconnect()

    vi.advanceTimersByTime(10000)
    expect(MockWebSocket.instances.length).toBe(1)
    expect(ws.state).toBe('disconnected')
  })

  it('handles multiple concurrent clients', () => {
    const queryClient1 = createMockQueryClient()
    const queryClient2 = createMockQueryClient()

    const ws1 = createWebSocket('ws://localhost:8080', queryClient1)
    const ws2 = createWebSocket('ws://localhost:8081', queryClient2)

    ws1.connect()
    ws2.connect()

    expect(MockWebSocket.instances.length).toBe(2)
    expect(MockWebSocket.getInstance(0).url).toBe('ws://localhost:8080')
    expect(MockWebSocket.getInstance(1).url).toBe('ws://localhost:8081')

    MockWebSocket.getInstance(0).simulateOpen()
    MockWebSocket.getInstance(1).simulateOpen()

    expect(ws1.state).toBe('connected')
    expect(ws2.state).toBe('connected')
  })

  it('handles event messages by prepending to events list', () => {
    const queryClient = createMockQueryClient()
    const ws = createWebSocket('ws://localhost:8080', queryClient)

    ws.connect()
    MockWebSocket.getInstance(0).simulateOpen()

    const event: EventStoreEntry = {
      aggregate_type: 'TokenizedEquityMint',
      aggregate_id: 'mint-123',
      sequence: 1,
      event_type: 'MintRequested',
      timestamp: '2024-01-01T00:00:00Z'
    }

    MockWebSocket.getInstance(0).simulateMessage({ type: 'event', data: event })

    const setQueryDataCalls = queryClient.setQueryDataSpy.mock.calls
    const eventsCall = setQueryDataCalls.find(
      (call: unknown[]) => JSON.stringify(call[0]) === '["events"]'
    )

    expect(eventsCall).toBeDefined()
    if (eventsCall === undefined) {
      throw new Error('eventsCall should be defined')
    }

    const updater = eventsCall[1] as (old: EventStoreEntry[] | undefined) => EventStoreEntry[]
    const result = updater([])

    expect(result.length).toBe(1)
    expect(result[0]).toEqual(event)
  })

  it('handles error by closing socket', () => {
    const queryClient = createMockQueryClient()
    const ws = createWebSocket('ws://localhost:8080', queryClient)

    ws.connect()
    MockWebSocket.getInstance(0).simulateOpen()

    const mockSocket = MockWebSocket.getInstance(0)
    const closeSpy = vi.spyOn(mockSocket, 'close')

    mockSocket.simulateError()

    expect(closeSpy).toHaveBeenCalled()
  })

  it('does not connect if already connected', () => {
    const queryClient = createMockQueryClient()
    const ws = createWebSocket('ws://localhost:8080', queryClient)
    const consoleSpy = vi.spyOn(console, 'warn').mockImplementation(() => {})

    ws.connect()
    MockWebSocket.getInstance(0).simulateOpen()

    ws.connect()

    expect(MockWebSocket.instances.length).toBe(1)
    expect(consoleSpy).toHaveBeenCalledWith(
      'No action defined for event',
      'connect',
      'in state',
      'connected'
    )
    consoleSpy.mockRestore()
  })

  it('caps reconnect delay at maximum', () => {
    const queryClient = createMockQueryClient()
    const ws = createWebSocket('ws://localhost:8080', queryClient)

    ws.connect()

    for (let i = 0; i < 10; i++) {
      MockWebSocket.getInstance(i).simulateClose()
      vi.advanceTimersByTime(30000)
    }

    MockWebSocket.getInstance(10).simulateClose()
    vi.advanceTimersByTime(30000)
    expect(MockWebSocket.instances.length).toBe(12)
  })

  it('exposes null error when not in error state', () => {
    const queryClient = createMockQueryClient()
    const ws = createWebSocket('ws://localhost:8080', queryClient)

    expect(ws.error).toBeNull()

    ws.connect()
    expect(ws.error).toBeNull()

    MockWebSocket.getInstance(0).simulateOpen()
    expect(ws.error).toBeNull()
  })

  it('exposes error context with attempt count and retry delay', () => {
    const queryClient = createMockQueryClient()
    const ws = createWebSocket('ws://localhost:8080', queryClient)

    ws.connect()
    MockWebSocket.getInstance(0).simulateOpen()
    MockWebSocket.getInstance(0).simulateClose()

    expect(ws.error).toEqual({ attempts: 1, nextRetryMs: 1000 })

    vi.advanceTimersByTime(1000)
    MockWebSocket.getInstance(1).simulateClose()

    expect(ws.error).toEqual({ attempts: 2, nextRetryMs: 2000 })
  })

  it('clears error context on successful reconnection', () => {
    const queryClient = createMockQueryClient()
    const ws = createWebSocket('ws://localhost:8080', queryClient)

    ws.connect()
    MockWebSocket.getInstance(0).simulateOpen()
    MockWebSocket.getInstance(0).simulateClose()

    expect(ws.error).not.toBeNull()

    vi.advanceTimersByTime(1000)
    MockWebSocket.getInstance(1).simulateOpen()

    expect(ws.error).toBeNull()
  })

  it('rejects messages with invalid JSON', () => {
    const queryClient = createMockQueryClient()
    const ws = createWebSocket('ws://localhost:8080', queryClient)
    const consoleSpy = vi.spyOn(console, 'error').mockImplementation(() => {})

    ws.connect()
    MockWebSocket.getInstance(0).simulateOpen()

    MockWebSocket.getInstance(0).simulateRawMessage('not valid json')

    expect(queryClient.setQueryDataSpy).not.toHaveBeenCalled()
    expect(consoleSpy).toHaveBeenCalled()
    consoleSpy.mockRestore()
  })

  it('rejects messages without type field', () => {
    const queryClient = createMockQueryClient()
    const ws = createWebSocket('ws://localhost:8080', queryClient)
    const consoleSpy = vi.spyOn(console, 'error').mockImplementation(() => {})

    ws.connect()
    MockWebSocket.getInstance(0).simulateOpen()

    MockWebSocket.getInstance(0).simulateRawMessage(JSON.stringify({ data: {} }))

    expect(queryClient.setQueryDataSpy).not.toHaveBeenCalled()
    expect(consoleSpy).toHaveBeenCalledWith('Invalid ServerMessage structure:', { data: {} })
    consoleSpy.mockRestore()
  })

  it('rejects messages with unknown type', () => {
    const queryClient = createMockQueryClient()
    const ws = createWebSocket('ws://localhost:8080', queryClient)
    const consoleSpy = vi.spyOn(console, 'error').mockImplementation(() => {})

    ws.connect()
    MockWebSocket.getInstance(0).simulateOpen()

    MockWebSocket.getInstance(0).simulateRawMessage(
      JSON.stringify({ type: 'unknown', data: {} })
    )

    expect(queryClient.setQueryDataSpy).not.toHaveBeenCalled()
    expect(consoleSpy).toHaveBeenCalledWith('Invalid ServerMessage structure:', {
      type: 'unknown',
      data: {}
    })
    consoleSpy.mockRestore()
  })

  it('rejects initial message with missing data fields', () => {
    const queryClient = createMockQueryClient()
    const ws = createWebSocket('ws://localhost:8080', queryClient)
    const consoleSpy = vi.spyOn(console, 'error').mockImplementation(() => {})

    ws.connect()
    MockWebSocket.getInstance(0).simulateOpen()

    MockWebSocket.getInstance(0).simulateRawMessage(
      JSON.stringify({
        type: 'initial',
        data: { recentTrades: [] }
      })
    )

    expect(queryClient.setQueryDataSpy).not.toHaveBeenCalled()
    expect(consoleSpy).toHaveBeenCalledWith('Invalid ServerMessage structure:', {
      type: 'initial',
      data: { recentTrades: [] }
    })
    consoleSpy.mockRestore()
  })

  it('rejects event message with missing fields', () => {
    const queryClient = createMockQueryClient()
    const ws = createWebSocket('ws://localhost:8080', queryClient)
    const consoleSpy = vi.spyOn(console, 'error').mockImplementation(() => {})

    ws.connect()
    MockWebSocket.getInstance(0).simulateOpen()

    MockWebSocket.getInstance(0).simulateRawMessage(
      JSON.stringify({
        type: 'event',
        data: { aggregate_type: 'Position' }
      })
    )

    expect(queryClient.setQueryDataSpy).not.toHaveBeenCalled()
    expect(consoleSpy).toHaveBeenCalledWith('Invalid ServerMessage structure:', {
      type: 'event',
      data: { aggregate_type: 'Position' }
    })
    consoleSpy.mockRestore()
  })

  it('rejects event message with wrong field types', () => {
    const queryClient = createMockQueryClient()
    const ws = createWebSocket('ws://localhost:8080', queryClient)
    const consoleSpy = vi.spyOn(console, 'error').mockImplementation(() => {})

    ws.connect()
    MockWebSocket.getInstance(0).simulateOpen()

    MockWebSocket.getInstance(0).simulateRawMessage(
      JSON.stringify({
        type: 'event',
        data: {
          aggregate_type: 'Position',
          aggregate_id: 123,
          sequence: 'not a number',
          event_type: 'Created',
          timestamp: '2024-01-01T00:00:00Z'
        }
      })
    )

    expect(queryClient.setQueryDataSpy).not.toHaveBeenCalled()
    expect(consoleSpy).toHaveBeenCalledWith('Invalid ServerMessage structure:', {
      type: 'event',
      data: {
        aggregate_type: 'Position',
        aggregate_id: 123,
        sequence: 'not a number',
        event_type: 'Created',
        timestamp: '2024-01-01T00:00:00Z'
      }
    })
    consoleSpy.mockRestore()
  })

  it('handles inventory_update by replacing inventory cache', () => {
    const queryClient = createMockQueryClient()
    const ws = createWebSocket('ws://localhost:8080', queryClient)

    ws.connect()
    MockWebSocket.getInstance(0).simulateOpen()

    const inventory: Inventory = {
      perSymbol: [
        {
          symbol: 'tAAPL',
          onchainAvailable: '100.5',
          onchainInflight: '0',
          offchainAvailable: '50',
          offchainInflight: '10'
        }
      ],
      usdc: createUsdcInventory(),
      snapshotAt: '2024-01-01T00:00:00Z'
    }

    const message: ServerMessage = { type: 'inventory_update', data: inventory }
    MockWebSocket.getInstance(0).simulateMessage(message)

    expect(queryClient.setQueryDataSpy).toHaveBeenCalledWith(['inventory'], inventory)
  })

  it('handles transfer_update by adding new active transfer', () => {
    const queryClient = createMockQueryClient()
    const ws = createWebSocket('ws://localhost:8080', queryClient)

    ws.connect()
    MockWebSocket.getInstance(0).simulateOpen()

    const transfer: TransferOperation = {
      kind: 'equity_mint',
      id: 'mint-1',
      symbol: 'tAAPL',
      quantity: '100',
      status: { status: 'minting' },
      completedStages: [],
      startedAt: '2024-01-01T00:00:00Z',
      updatedAt: '2024-01-01T00:00:00Z'
    }

    const message: ServerMessage = { type: 'transfer_update', data: transfer }
    MockWebSocket.getInstance(0).simulateMessage(message)

    const setQueryDataCalls = queryClient.setQueryDataSpy.mock.calls
    const transfersCall = setQueryDataCalls.find(
      (call: unknown[]) => JSON.stringify(call[0]) === '["transfers","active"]'
    )

    expect(transfersCall).toBeDefined()
    if (transfersCall === undefined) throw new Error('transfersCall should be defined')

    const updater = transfersCall[1] as (
      old: TransferOperation[] | undefined
    ) => TransferOperation[]
    const result = updater([])

    expect(result).toEqual([transfer])
  })

  it('handles transfer_update by moving completed transfer to recent', () => {
    const queryClient = createMockQueryClient()
    const ws = createWebSocket('ws://localhost:8080', queryClient)

    ws.connect()
    MockWebSocket.getInstance(0).simulateOpen()

    const activeTransfer: TransferOperation = {
      kind: 'equity_mint',
      id: 'mint-1',
      symbol: 'tAAPL',
      quantity: '100',
      status: { status: 'minting' },
      completedStages: [],
      startedAt: '2024-01-01T00:00:00Z',
      updatedAt: '2024-01-01T00:00:00Z'
    }

    const completedTransfer: TransferOperation = {
      kind: 'equity_mint',
      id: 'mint-1',
      symbol: 'tAAPL',
      quantity: '100',
      status: {
        status: 'completed',
        completed_at: '2024-01-01T00:01:00Z',
        token: '0xaaaa000000000000000000000000000000000000000000000000000000000001',
        wrap: '0xaaaa000000000000000000000000000000000000000000000000000000000002',
        vault_deposit: '0xaaaa000000000000000000000000000000000000000000000000000000000003'
      },
      completedStages: [],
      startedAt: '2024-01-01T00:00:00Z',
      updatedAt: '2024-01-01T00:01:00Z'
    }

    const message: ServerMessage = { type: 'transfer_update', data: completedTransfer }
    MockWebSocket.getInstance(0).simulateMessage(message)

    const setQueryDataCalls = queryClient.setQueryDataSpy.mock.calls
    const activeCall = setQueryDataCalls.find(
      (call: unknown[]) => JSON.stringify(call[0]) === '["transfers","active"]'
    )

    expect(activeCall).toBeDefined()
    if (activeCall === undefined) throw new Error('activeCall should be defined')

    const activeUpdater = activeCall[1] as (
      old: TransferOperation[] | undefined
    ) => TransferOperation[]
    const activeResult = activeUpdater([activeTransfer])

    expect(activeResult).toEqual([])

    const recentCall = setQueryDataCalls.find(
      (call: unknown[]) => JSON.stringify(call[0]) === '["transfers","recent"]'
    )

    expect(recentCall).toBeDefined()
    if (recentCall === undefined) throw new Error('recentCall should be defined')

    const recentUpdater = recentCall[1] as (
      old: TransferOperation[] | undefined
    ) => TransferOperation[]
    const recentResult = recentUpdater([])

    expect(recentResult).toEqual([completedTransfer])
  })

  it('validates inventory_update message structure', () => {
    const queryClient = createMockQueryClient()
    const ws = createWebSocket('ws://localhost:8080', queryClient)
    const consoleSpy = vi.spyOn(console, 'error').mockImplementation(() => {})

    ws.connect()
    MockWebSocket.getInstance(0).simulateOpen()

    MockWebSocket.getInstance(0).simulateRawMessage(
      JSON.stringify({ type: 'inventory_update', data: { invalid: true } })
    )

    expect(queryClient.setQueryDataSpy).not.toHaveBeenCalled()
    expect(consoleSpy).toHaveBeenCalledWith('Invalid ServerMessage structure:', {
      type: 'inventory_update',
      data: { invalid: true }
    })
    consoleSpy.mockRestore()
  })

  it('validates transfer_update message structure', () => {
    const queryClient = createMockQueryClient()
    const ws = createWebSocket('ws://localhost:8080', queryClient)
    const consoleSpy = vi.spyOn(console, 'error').mockImplementation(() => {})

    ws.connect()
    MockWebSocket.getInstance(0).simulateOpen()

    MockWebSocket.getInstance(0).simulateRawMessage(
      JSON.stringify({ type: 'transfer_update', data: { noKind: true } })
    )

    expect(queryClient.setQueryDataSpy).not.toHaveBeenCalled()
    expect(consoleSpy).toHaveBeenCalledWith('Invalid ServerMessage structure:', {
      type: 'transfer_update',
      data: { noKind: true }
    })
    consoleSpy.mockRestore()
  })

  it('populates transfer cache keys from initial state', () => {
    const queryClient = createMockQueryClient()
    const ws = createWebSocket('ws://localhost:8080', queryClient)

    ws.connect()
    MockWebSocket.getInstance(0).simulateOpen()

    const initialState = createInitialState()
    MockWebSocket.getInstance(0).simulateMessage({ type: 'initial', data: initialState })

    expect(queryClient.setQueryDataSpy).toHaveBeenCalledWith(['transfers', 'active'], [])
    expect(queryClient.setQueryDataSpy).toHaveBeenCalledWith(['transfers', 'recent'], [])
  })
})
