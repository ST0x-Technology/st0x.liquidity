import { describe, expect, it, vi, beforeEach, afterEach } from 'vitest'
import { createWebSocket } from '.'
import { QueryClient } from '@tanstack/svelte-query'
import type { CurrentState } from '$lib/api/CurrentState'
import type { Statement } from '$lib/api/Statement'
import type { Trade } from '$lib/api/Trade'
import type { TransferOperation } from '$lib/api/TransferOperation'

class MockWebSocket {
  url: string
  onopen: (() => void) | null = null
  onclose: ((event: CloseEvent) => void) | null = null
  onmessage: ((event: { data: string }) => void) | null = null
  onerror: (() => void) | null = null

  private instances: MockWebSocket[]

  constructor(url: string, instances: MockWebSocket[]) {
    this.url = url
    this.instances = instances
    this.instances.push(this)
  }

  close() {
    this.onclose?.({ code: 1000, reason: '' } as CloseEvent)
  }

  simulateOpen() {
    this.onopen?.()
  }

  simulateMessage(msg: Statement) {
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
  id: 'trade-1',
  occurredAt: '2024-01-01T12:00:00Z',
  venue: 'raindex',
  direction: 'buy',
  symbol: 'AAPL',
  shares: '10',
  outcome: { status: 'filled' },
  ...overrides
})

const makeTransfer = (overrides: Partial<TransferOperation> = {}): TransferOperation =>
  ({
    kind: 'equity_mint',
    id: 'transfer-1',
    symbol: 'AAPL',
    quantity: '10',
    status: { status: 'minting' },
    startedAt: '2024-01-01T12:00:00Z',
    updatedAt: '2024-01-01T12:00:00Z',
    ...overrides
  }) as TransferOperation

const makeCurrentState = (overrides: Partial<CurrentState> = {}): CurrentState => ({
  trades: [makeTrade()],
  inventory: {
    perSymbol: [],
    usdc: {
      onchainAvailable: '0',
      onchainInflight: '0',
      offchainAvailable: '0',
      offchainInflight: '0',
      offchainGross: null,
      withdrawableCash: null,
      alpacaUsdc: null,
      inflightCash: {
        ethereumWallet: null,
        baseWallet: null
      }
    }
  },
  positions: [],
  settings: {
    equityTarget: 0.5,
    equityDeviation: 0.2,
    usdcTarget: null,
    usdcDeviation: null,
    cashReserved: null,
    executionThreshold: '$2',
    assets: [],
    wallet: null,
    logLevel: 'Debug',
    serverPort: 8001,
    orderbook: '0x0',
    deploymentBlock: 0,
    tradingMode: 'standalone',
    broker: 'dry_run',
    orderPollingInterval: 5,
    inventoryPollInterval: 15
  },
  activeTransfers: [],
  recentTransfers: [],
  warnings: [],
  ...overrides
})

const setupWebSocketTest = () => {
  const instances: MockWebSocket[] = []

  const BoundMockWebSocket = class extends MockWebSocket {
    constructor(url: string) {
      super(url, instances)
    }
  }

  vi.stubGlobal('WebSocket', BoundMockWebSocket)

  const getInstance = (index: number): MockWebSocket => {
    const instance = instances[index]
    if (!instance) {
      throw new Error(`MockWebSocket instance ${String(index)} not found`)
    }
    return instance
  }

  return { instances, getInstance }
}

describe('createWebSocket', () => {
  beforeEach(() => {
    vi.useFakeTimers()
  })

  afterEach(() => {
    vi.useRealTimers()
    vi.restoreAllMocks()
    vi.unstubAllGlobals()
  })

  describe('connection lifecycle', () => {
    it('starts disconnected', () => {
      setupWebSocketTest()
      const queryClient = createMockQueryClient()
      const ws = createWebSocket(WS_URL, queryClient)

      expect(ws.state).toBe('disconnected')
    })

    it('transitions to connecting then connected on open', () => {
      const { getInstance } = setupWebSocketTest()
      const queryClient = createMockQueryClient()
      const ws = createWebSocket(WS_URL, queryClient)

      ws.connect()
      expect(ws.state).toBe('connecting')

      getInstance(0).simulateOpen()
      expect(ws.state).toBe('connected')
    })

    it('transitions to error on close from connected', () => {
      const { getInstance } = setupWebSocketTest()
      const queryClient = createMockQueryClient()
      const ws = createWebSocket(WS_URL, queryClient)

      ws.connect()
      getInstance(0).simulateOpen()
      getInstance(0).simulateClose()
      expect(ws.state).toBe('error')
    })

    it('returns to disconnected on explicit disconnect', () => {
      const { getInstance } = setupWebSocketTest()
      const queryClient = createMockQueryClient()
      const ws = createWebSocket(WS_URL, queryClient)

      ws.connect()
      getInstance(0).simulateOpen()
      ws.disconnect()
      expect(ws.state).toBe('disconnected')
    })
  })

  describe('message handling', () => {
    it('seeds trades and transfers from initial message', () => {
      const { getInstance } = setupWebSocketTest()
      const queryClient = createMockQueryClient()
      const ws = createWebSocket(WS_URL, queryClient)

      ws.connect()
      getInstance(0).simulateOpen()

      const trade = makeTrade()
      const activeTransfer = makeTransfer({ id: 'active-1' })
      const recentTransfer = makeTransfer({
        id: 'recent-1',
        status: { status: 'completed', completedAt: '2024-01-01T13:00:00Z' }
      })

      const message: Statement = {
        type: 'current_state',
        data: makeCurrentState({
          trades: [trade],
          activeTransfers: [activeTransfer],
          recentTransfers: [recentTransfer]
        })
      }

      getInstance(0).simulateMessage(message)

      const trades = queryClient.cache.get('["trades"]') as Trade[] | undefined
      expect(trades).toBeDefined()
      expect(trades).toEqual([trade])

      const active = queryClient.cache.get('["transfers","active"]') as
        | TransferOperation[]
        | undefined
      expect(active).toBeDefined()
      expect(active).toEqual([activeTransfer])

      const recent = queryClient.cache.get('["transfers","recent"]') as
        | TransferOperation[]
        | undefined
      expect(recent).toBeDefined()
      expect(recent).toEqual([recentTransfer])
    })

    it('prepends trade updates to the trades cache', () => {
      const { getInstance } = setupWebSocketTest()
      const queryClient = createMockQueryClient()
      const ws = createWebSocket(WS_URL, queryClient)

      ws.connect()
      getInstance(0).simulateOpen()

      // Seed with existing trade
      queryClient.cache.set('["trades"]', [makeTrade({ id: 'trade-old', symbol: 'TSLA' })])

      const newTrade = makeTrade({ symbol: 'AAPL' })
      const message: Statement = { type: 'trade_update', data: newTrade }

      getInstance(0).simulateMessage(message)

      const trades = queryClient.cache.get('["trades"]') as Trade[]
      expect(trades).toHaveLength(2)
      expect(trades[0]?.symbol).toBe('AAPL')
      expect(trades[1]?.symbol).toBe('TSLA')
    })

    it('normalizes legacy fills from an old server snapshot', () => {
      const { getInstance } = setupWebSocketTest()
      const queryClient = createMockQueryClient()
      const ws = createWebSocket(WS_URL, queryClient)

      ws.connect()
      getInstance(0).simulateOpen()
      getInstance(0).simulateRawMessage(
        JSON.stringify({
          type: 'current_state',
          data: {
            trades: [
              {
                id: 'legacy-snapshot',
                filledAt: '2024-01-01T12:00:00.123456789Z',
                venue: 'alpaca',
                direction: 'sell',
                symbol: 'TSLA',
                shares: '2'
              }
            ],
            inventory: {
              perSymbol: [],
              usdc: {
                onchainAvailable: '0',
                onchainInflight: '0',
                offchainAvailable: '0',
                offchainInflight: '0',
                offchainGross: null,
                withdrawableCash: null,
                alpacaUsdc: null,
                inflightCash: { ethereumWallet: null, baseWallet: null }
              }
            },
            positions: [],
            settings: {},
            activeTransfers: [],
            recentTransfers: [],
            warnings: []
          }
        })
      )

      expect(queryClient.cache.get('["trades"]')).toEqual([
        {
          id: 'legacy-snapshot',
          occurredAt: '2024-01-01T12:00:00.123456789Z',
          venue: 'alpaca',
          direction: 'sell',
          symbol: 'TSLA',
          shares: '2',
          outcome: { status: 'filled' }
        }
      ])
    })

    it('accepts legacy live fills from an old server', () => {
      const { getInstance } = setupWebSocketTest()
      const queryClient = createMockQueryClient()
      const ws = createWebSocket(WS_URL, queryClient)

      ws.connect()
      getInstance(0).simulateOpen()
      getInstance(0).simulateRawMessage(
        JSON.stringify({
          type: 'trade_fill',
          data: {
            id: 'legacy-live',
            filledAt: '2024-01-01T12:00:00.123456789Z',
            venue: 'raindex',
            direction: 'buy',
            symbol: 'AAPL',
            shares: '1'
          }
        })
      )

      expect(queryClient.cache.get('["trades"]')).toEqual([
        {
          id: 'legacy-live',
          occurredAt: '2024-01-01T12:00:00.123456789Z',
          venue: 'raindex',
          direction: 'buy',
          symbol: 'AAPL',
          shares: '1',
          outcome: { status: 'filled' }
        }
      ])
    })

    it.each([
      [
        'initial snapshot',
        {
          type: 'current_state',
          data: { trades: [makeTrade({ shares: '0' })] }
        }
      ],
      [
        'live update',
        {
          type: 'trade_update',
          data: makeTrade({ shares: '0' })
        }
      ]
    ])('rejects an invalid trade from %s without replacing the cache', (_case, message) => {
      const { getInstance } = setupWebSocketTest()
      const queryClient = createMockQueryClient()
      const knownGood = makeTrade({ id: 'known-good' })
      queryClient.cache.set('["trades"]', [knownGood])
      const consoleError = vi.spyOn(console, 'error').mockImplementation(() => undefined)
      const ws = createWebSocket(WS_URL, queryClient)

      ws.connect()
      getInstance(0).simulateOpen()
      getInstance(0).simulateRawMessage(JSON.stringify(message))

      expect(queryClient.cache.get('["trades"]')).toEqual([knownGood])
      expect(ws.state).toBe('error')
      expect(ws.error?.message).toContain('Invalid trade payload')
      expect(consoleError).toHaveBeenCalledWith(
        'Failed to parse WebSocket message:',
        expect.objectContaining({ message: expect.stringContaining('shares') }),
        'Raw data:',
        JSON.stringify(message)
      )
    })

    it('seeds non-trade snapshot domains when a snapshot trade is invalid', () => {
      const { getInstance } = setupWebSocketTest()
      const queryClient = createMockQueryClient()
      const knownGood = makeTrade({ id: 'known-good' })
      const activeTransfer = makeTransfer({ id: 'fresh-transfer' })
      const state = makeCurrentState({
        trades: [makeTrade({ shares: '0' })],
        activeTransfers: [activeTransfer]
      })
      queryClient.cache.set('["trades"]', [knownGood])
      vi.spyOn(console, 'error').mockImplementation(() => undefined)
      const ws = createWebSocket(WS_URL, queryClient)

      ws.connect()
      getInstance(0).simulateOpen()
      getInstance(0).simulateRawMessage(
        JSON.stringify({
          type: 'current_state',
          data: state
        })
      )

      expect(queryClient.cache.get('["trades"]')).toEqual([knownGood])
      expect(queryClient.cache.get('["inventory"]')).toEqual(state.inventory)
      expect(queryClient.cache.get('["transfers","active"]')).toEqual([activeTransfer])
      expect(ws.state).toBe('error')
    })

    it('replaces a matching initial trade with its live update', () => {
      const { getInstance } = setupWebSocketTest()
      const queryClient = new QueryClient({
        defaultOptions: { queries: { retry: false } }
      })
      const ws = createWebSocket(WS_URL, queryClient)

      ws.connect()
      getInstance(0).simulateOpen()

      queryClient.setQueryData(['trades'], [makeTrade({ symbol: 'STALE' })])
      const failure = makeTrade({
        symbol: 'SPCX',
        outcome: {
          status: 'failed',
          error: 'asset is not tradable',
          filledShares: '0',
          remainingShares: '10',
          excessShares: '0'
        }
      })
      getInstance(0).simulateMessage({ type: 'trade_update', data: failure })

      const trades = queryClient.getQueryData<Trade[]>(['trades'])
      expect(trades).toEqual([failure])
    })

    it('limits trades to 100', () => {
      const { getInstance } = setupWebSocketTest()
      const queryClient = createMockQueryClient()
      const ws = createWebSocket(WS_URL, queryClient)

      ws.connect()
      getInstance(0).simulateOpen()

      const existing = Array.from({ length: 100 }, (_, idx) =>
        makeTrade({ id: `trade-${String(idx)}`, symbol: `SYM${String(idx)}` })
      )
      queryClient.cache.set('["trades"]', existing)

      const newTrade = makeTrade({
        symbol: 'NEW',
        occurredAt: '2024-01-01T12:00:01Z'
      })
      getInstance(0).simulateMessage({ type: 'trade_update', data: newTrade })

      const trades = queryClient.cache.get('["trades"]') as Trade[]
      expect(trades).toHaveLength(100)
      expect(trades[0]?.symbol).toBe('NEW')
    })

    it('updates inventory from snapshot', () => {
      const { getInstance } = setupWebSocketTest()
      const queryClient = createMockQueryClient()
      const ws = createWebSocket(WS_URL, queryClient)

      ws.connect()
      getInstance(0).simulateOpen()

      const inventory = {
        perSymbol: [
          {
            symbol: 'AAPL',
            onchainAvailable: '10',
            onchainInflight: '0',
            offchainAvailable: '5',
            offchainInflight: '0',
            inflightEquity: {
              baseWalletUnwrapped: '0',
              baseWalletWrapped: '0'
            }
          }
        ],
        usdc: {
          onchainAvailable: '1000',
          onchainInflight: '0',
          offchainAvailable: '500',
          offchainInflight: '0',
          offchainGross: null,
          withdrawableCash: null,
          alpacaUsdc: null,
          inflightCash: { ethereumWallet: null, baseWallet: null }
        }
      }

      const message: Statement = {
        type: 'inventory_snapshot',
        data: { inventory, fetchedAt: '2024-01-01T12:00:00Z' }
      }

      getInstance(0).simulateMessage(message)

      expect(queryClient.cache.get('["inventory"]')).toEqual(inventory)
    })

    it('moves completed transfer from active to recent', () => {
      const { getInstance } = setupWebSocketTest()
      const queryClient = createMockQueryClient()
      const ws = createWebSocket(WS_URL, queryClient)

      ws.connect()
      getInstance(0).simulateOpen()

      const activeTransfer = makeTransfer({ id: 'mint-1', status: { status: 'minting' } })
      queryClient.cache.set('["transfers","active"]', [activeTransfer])
      queryClient.cache.set('["transfers","recent"]', [])

      const completedTransfer = makeTransfer({
        id: 'mint-1',
        status: { status: 'completed', completedAt: '2024-01-01T13:00:00Z' }
      })
      const message: Statement = { type: 'transfer_update', data: completedTransfer }

      getInstance(0).simulateMessage(message)

      const active = queryClient.cache.get('["transfers","active"]') as TransferOperation[]
      expect(active).toEqual([])

      const recent = queryClient.cache.get('["transfers","recent"]') as TransferOperation[]
      expect(recent).toHaveLength(1)
      expect(recent[0]?.id).toBe('mint-1')
      expect(recent[0]?.status).toEqual({
        status: 'completed',
        completedAt: '2024-01-01T13:00:00Z'
      })
    })

    it('ignores invalid messages', () => {
      const { getInstance } = setupWebSocketTest()
      const queryClient = createMockQueryClient()
      const ws = createWebSocket(WS_URL, queryClient)

      ws.connect()
      getInstance(0).simulateOpen()

      getInstance(0).simulateRawMessage('{"not":"valid"}')

      expect(queryClient.setQueryDataSpy).not.toHaveBeenCalled()
    })
  })

  describe('reconnection', () => {
    it('schedules reconnect on error', () => {
      const { getInstance } = setupWebSocketTest()
      const queryClient = createMockQueryClient()
      const ws = createWebSocket(WS_URL, queryClient)

      ws.connect()
      getInstance(0).simulateError()

      expect(ws.state).toBe('error')
      expect(ws.error).not.toBeNull()
      expect(ws.error?.attempts).toBe(1)
      expect(ws.error?.message).toBe('WebSocket connection error')
    })

    it('reconnects after delay', () => {
      const { instances, getInstance } = setupWebSocketTest()
      const queryClient = createMockQueryClient()
      const ws = createWebSocket(WS_URL, queryClient)

      ws.connect()
      getInstance(0).simulateError()

      expect(instances).toHaveLength(1)

      vi.advanceTimersByTime(1000)

      expect(instances).toHaveLength(2)
      expect(ws.state).toBe('connecting')
    })

    it('resets error state on successful reconnection', () => {
      const { getInstance } = setupWebSocketTest()
      const queryClient = createMockQueryClient()
      const ws = createWebSocket(WS_URL, queryClient)

      ws.connect()
      getInstance(0).simulateError()

      vi.advanceTimersByTime(1000)
      getInstance(1).simulateOpen()

      expect(ws.state).toBe('connected')
      expect(ws.error).not.toBeNull()

      getInstance(1).simulateMessage({ type: 'trade_update', data: makeTrade() })

      expect(ws.error).toBeNull()
    })

    it('backs off repeated invalid messages until a valid message arrives', () => {
      const { instances, getInstance } = setupWebSocketTest()
      const queryClient = createMockQueryClient()
      const consoleError = vi.spyOn(console, 'error').mockImplementation(() => undefined)
      const ws = createWebSocket(WS_URL, queryClient)

      ws.connect()
      getInstance(0).simulateOpen()
      getInstance(0).simulateRawMessage('{"not":"valid"}')

      expect(ws.error?.attempts).toBe(1)
      expect(ws.error?.nextRetryMs).toBe(1000)

      vi.advanceTimersByTime(1000)
      getInstance(1).simulateOpen()
      getInstance(1).simulateRawMessage('{"not":"valid"}')

      expect(ws.error?.attempts).toBe(2)
      expect(ws.error?.nextRetryMs).toBe(2000)

      vi.advanceTimersByTime(1000)
      expect(instances).toHaveLength(2)
      vi.advanceTimersByTime(1000)
      expect(instances).toHaveLength(3)
      expect(consoleError).toHaveBeenCalledTimes(2)
    })
  })
})
