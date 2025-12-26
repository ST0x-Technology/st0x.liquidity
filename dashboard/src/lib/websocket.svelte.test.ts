import { describe, expect, it, vi, beforeEach, afterEach } from 'vitest'
import { createWebSocket } from './websocket.svelte'
import type { QueryClient } from '@tanstack/svelte-query'
import type { EventStoreEntry } from '$lib/api/EventStoreEntry'
import type { InitialState } from '$lib/api/InitialState'
import type { ServerMessage } from '$lib/api/ServerMessage'

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

const createInitialState = (): InitialState => ({
  recentTrades: [],
  inventory: { perSymbol: [], usdc: { onchain: '0', offchain: '0' } },
  metrics: {
    '1h': createTimeframeMetrics(),
    '1d': createTimeframeMetrics(),
    '1w': createTimeframeMetrics(),
    '1m': createTimeframeMetrics(),
    all: createTimeframeMetrics()
  },
  spreads: [],
  activeRebalances: [],
  recentRebalances: [],
  authStatus: { status: 'not_configured' },
  circuitBreaker: { status: 'active' }
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

    expect(ws.status).toBe('disconnected')
  })

  it('transitions to connecting then connected on successful connect', () => {
    const queryClient = createMockQueryClient()
    const ws = createWebSocket('ws://localhost:8080', queryClient)

    ws.connect()
    expect(ws.status).toBe('connecting')

    MockWebSocket.getInstance(0).simulateOpen()
    expect(ws.status).toBe('connected')
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

    expect(ws.status).toBe('disconnected')

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
    expect(ws.status).toBe('disconnected')
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

    expect(ws1.status).toBe('connected')
    expect(ws2.status).toBe('connected')
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

    ws.connect()
    MockWebSocket.getInstance(0).simulateOpen()

    ws.connect()

    expect(MockWebSocket.instances.length).toBe(1)
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
})
