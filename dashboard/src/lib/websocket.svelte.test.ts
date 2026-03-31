import { describe, expect, it, vi, beforeEach, afterEach } from 'vitest'
import { createWebSocket } from './websocket.svelte'
import type { QueryClient } from '@tanstack/svelte-query'
import type { ServerMessage } from '$lib/api/ServerMessage'
import type { Trade } from '$lib/api/Trade'

class MockWebSocket {
  static instances: MockWebSocket[] = []

  url: string
  onopen: (() => void) | null = null
  onclose: ((event: CloseEvent) => void) | null = null
  onmessage: ((event: { data: string }) => void) | null = null
  onerror: (() => void) | null = null

  constructor(url: string) {
    this.url = url
    MockWebSocket.instances.push(this)
  }

  close() {
    this.onclose?.({ code: 1000, reason: '' } as CloseEvent)
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

  simulateClose(code = 1000, reason = '') {
    this.onclose?.({ code, reason } as CloseEvent)
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
}

const createMockQueryClient = (): MockQueryClient => {
  const cache = new Map<string, unknown>()

  const setQueryDataSpy = vi.fn((key: unknown[], data: unknown) => {
    if (typeof data === 'function') {
      const current = cache.get(JSON.stringify(key))
      cache.set(JSON.stringify(key), (data as (old: unknown) => unknown)(current))
    } else {
      cache.set(JSON.stringify(key), data)
    }
    return undefined
  })

  return {
    setQueryData: setQueryDataSpy,
    cache,
    setQueryDataSpy
  } as unknown as MockQueryClient
}

const WS_URL = 'ws://localhost:8001/api/ws'

const makeTrade = (overrides: Partial<Trade> = {}): Trade => ({
  filledAt: '2024-01-01T12:00:00Z',
  venue: 'raindex',
  direction: 'buy',
  symbol: 'AAPL',
  shares: '10',
  ...overrides
})

describe('createWebSocket', () => {
  beforeEach(() => {
    MockWebSocket.instances = []
    vi.stubGlobal('WebSocket', MockWebSocket)
    vi.useFakeTimers()
  })

  afterEach(() => {
    vi.useRealTimers()
    vi.restoreAllMocks()
  })

  describe('connection lifecycle', () => {
    it('starts disconnected', () => {
      const queryClient = createMockQueryClient()
      const ws = createWebSocket(WS_URL, queryClient)

      expect(ws.state).toBe('disconnected')
    })

    it('transitions to connecting then connected on open', () => {
      const queryClient = createMockQueryClient()
      const ws = createWebSocket(WS_URL, queryClient)

      ws.connect()
      expect(ws.state).toBe('connecting')

      MockWebSocket.getInstance(0).simulateOpen()
      expect(ws.state).toBe('connected')
    })

    it('transitions to error on close from connected', () => {
      const queryClient = createMockQueryClient()
      const ws = createWebSocket(WS_URL, queryClient)

      ws.connect()
      MockWebSocket.getInstance(0).simulateOpen()
      MockWebSocket.getInstance(0).simulateClose()
      expect(ws.state).toBe('error')
    })

    it('returns to disconnected on explicit disconnect', () => {
      const queryClient = createMockQueryClient()
      const ws = createWebSocket(WS_URL, queryClient)

      ws.connect()
      MockWebSocket.getInstance(0).simulateOpen()
      ws.disconnect()
      expect(ws.state).toBe('disconnected')
    })
  })

  describe('message handling', () => {
    it('seeds trades from initial message', () => {
      const queryClient = createMockQueryClient()
      const ws = createWebSocket(WS_URL, queryClient)

      ws.connect()
      MockWebSocket.getInstance(0).simulateOpen()

      const trade = makeTrade()
      const message: ServerMessage = {
        type: 'initial',
        data: {
          trades: [trade],
          inventory: { perSymbol: [], usdc: { onchainAvailable: '0', onchainInflight: '0', offchainAvailable: '0', offchainInflight: '0' } },
          positions: [],
          config: { equityTarget: 0.5, equityDeviation: 0.2, usdcTarget: null, usdcDeviation: null, executionThreshold: '$2', assets: [] },
          activeTransfers: [],
          recentTransfers: []
        }
      }

      MockWebSocket.getInstance(0).simulateMessage(message)

      const trades = queryClient.cache.get('["trades"]') as Trade[] | undefined
      expect(trades).toBeDefined()
      expect(trades).toEqual([trade])
    })

    it('prepends fill to trades cache', () => {
      const queryClient = createMockQueryClient()
      const ws = createWebSocket(WS_URL, queryClient)

      ws.connect()
      MockWebSocket.getInstance(0).simulateOpen()

      // Seed with existing trade
      queryClient.cache.set('["trades"]', [makeTrade({ symbol: 'TSLA' })])

      const newTrade = makeTrade({ symbol: 'AAPL' })
      const message: ServerMessage = { type: 'fill', data: newTrade }

      MockWebSocket.getInstance(0).simulateMessage(message)

      const trades = queryClient.cache.get('["trades"]') as Trade[]
      expect(trades).toHaveLength(2)
      expect(trades[0]?.symbol).toBe('AAPL')
      expect(trades[1]?.symbol).toBe('TSLA')
    })

    it('limits trades to 100', () => {
      const queryClient = createMockQueryClient()
      const ws = createWebSocket(WS_URL, queryClient)

      ws.connect()
      MockWebSocket.getInstance(0).simulateOpen()

      const existing = Array.from({ length: 100 }, (_, idx) =>
        makeTrade({ symbol: `SYM${String(idx)}` })
      )
      queryClient.cache.set('["trades"]', existing)

      const newTrade = makeTrade({ symbol: 'NEW' })
      MockWebSocket.getInstance(0).simulateMessage({ type: 'fill', data: newTrade })

      const trades = queryClient.cache.get('["trades"]') as Trade[]
      expect(trades).toHaveLength(100)
      expect(trades[0]?.symbol).toBe('NEW')
    })

    it('updates inventory from snapshot', () => {
      const queryClient = createMockQueryClient()
      const ws = createWebSocket(WS_URL, queryClient)

      ws.connect()
      MockWebSocket.getInstance(0).simulateOpen()

      const inventory = {
        perSymbol: [{ symbol: 'AAPL', onchainAvailable: '10', onchainInflight: '0', offchainAvailable: '5', offchainInflight: '0' }],
        usdc: { onchainAvailable: '1000', onchainInflight: '0', offchainAvailable: '500', offchainInflight: '0' }
      }

      const message: ServerMessage = {
        type: 'snapshot',
        data: { inventory, fetchedAt: '2024-01-01T12:00:00Z' }
      }

      MockWebSocket.getInstance(0).simulateMessage(message)

      expect(queryClient.cache.get('["inventory"]')).toEqual(inventory)
    })

    it('ignores invalid messages', () => {
      const queryClient = createMockQueryClient()
      const ws = createWebSocket(WS_URL, queryClient)

      ws.connect()
      MockWebSocket.getInstance(0).simulateOpen()

      MockWebSocket.getInstance(0).simulateRawMessage('{"not":"valid"}')

      expect(queryClient.setQueryDataSpy).not.toHaveBeenCalled()
    })
  })

  describe('reconnection', () => {
    it('schedules reconnect on error', () => {
      const queryClient = createMockQueryClient()
      const ws = createWebSocket(WS_URL, queryClient)

      ws.connect()
      MockWebSocket.getInstance(0).simulateError()

      expect(ws.state).toBe('error')
      expect(ws.error).not.toBeNull()
      expect(ws.error?.attempts).toBe(1)
    })

    it('reconnects after delay', () => {
      const queryClient = createMockQueryClient()
      const ws = createWebSocket(WS_URL, queryClient)

      ws.connect()
      MockWebSocket.getInstance(0).simulateError()

      expect(MockWebSocket.instances).toHaveLength(1)

      vi.advanceTimersByTime(1000)

      expect(MockWebSocket.instances).toHaveLength(2)
      expect(ws.state).toBe('connecting')
    })

    it('resets error state on successful reconnection', () => {
      const queryClient = createMockQueryClient()
      const ws = createWebSocket(WS_URL, queryClient)

      ws.connect()
      MockWebSocket.getInstance(0).simulateError()

      vi.advanceTimersByTime(1000)
      MockWebSocket.getInstance(1).simulateOpen()

      expect(ws.state).toBe('connected')
      expect(ws.error).toBeNull()
    })
  })
})
