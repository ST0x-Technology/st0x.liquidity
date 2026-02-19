import { describe, expect, it, vi, beforeEach, afterEach } from 'vitest'
import { createWebSocket } from './websocket.svelte'
import type { QueryClient } from '@tanstack/svelte-query'
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

    expect(queryClient.setQueryDataSpy).toHaveBeenCalledWith(['trades'], [])
    expect(queryClient.setQueryDataSpy).toHaveBeenCalledWith(['inventory'], initialState.inventory)
    expect(queryClient.setQueryDataSpy).toHaveBeenCalledWith(['auth'], { status: 'not_configured' })
    expect(queryClient.setQueryDataSpy).toHaveBeenCalledWith(['circuitBreaker'], {
      status: 'active'
    })
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

    MockWebSocket.getInstance(0).simulateRawMessage(JSON.stringify({ type: 'unknown', data: {} }))

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
})
