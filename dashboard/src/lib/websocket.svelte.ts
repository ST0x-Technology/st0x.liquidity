import { FiniteStateMachine } from 'runed'
import type { QueryClient } from '@tanstack/svelte-query'
import type { EventStoreEntry } from '$lib/api/EventStoreEntry'
import type { Inventory } from '$lib/api/Inventory'
import type { ServerMessage } from '$lib/api/ServerMessage'
import type { TransferOperation } from '$lib/api/TransferOperation'
import { matcher } from '$lib/fp'
import { reactive } from '$lib/frp.svelte'

export type ConnectionState = 'disconnected' | 'connecting' | 'connected' | 'error'
type ConnectionEvent = 'connect' | 'open' | 'close' | 'error' | 'disconnect'

const RECONNECT_DELAY_MS = 5000
const MAX_EVENTS = 100

const isObject = (value: unknown): value is Record<string, unknown> =>
  typeof value === 'object' && value !== null

const isEventStoreEntry = (value: unknown): boolean => {
  if (!isObject(value)) return false
  return (
    typeof value['aggregate_type'] === 'string' &&
    typeof value['aggregate_id'] === 'string' &&
    typeof value['sequence'] === 'number' &&
    typeof value['event_type'] === 'string' &&
    typeof value['timestamp'] === 'string'
  )
}

const isInitialState = (value: unknown): boolean => {
  if (!isObject(value)) return false
  return (
    Array.isArray(value['recentTrades']) &&
    isInventory(value['inventory']) &&
    isObject(value['metrics']) &&
    Array.isArray(value['spreads']) &&
    Array.isArray(value['activeTransfers']) &&
    Array.isArray(value['recentTransfers']) &&
    isObject(value['authStatus']) &&
    isObject(value['circuitBreaker']) &&
    Array.isArray(value['warnings'])
  )
}

const isInventory = (value: unknown): boolean => {
  if (!isObject(value)) return false
  if (!Array.isArray(value['perSymbol']) || !isObject(value['usdc'])) return false
  const usdc = value['usdc']
  return (
    typeof usdc['onchainAvailable'] === 'string' &&
    typeof usdc['onchainInflight'] === 'string' &&
    typeof usdc['offchainAvailable'] === 'string' &&
    typeof usdc['offchainInflight'] === 'string'
  )
}

const isInventorySnapshot = (value: unknown): boolean => {
  if (!isObject(value)) return false
  return isInventory(value['inventory']) && typeof value['fetchedAt'] === 'string'
}

const isTransferOperation = (value: unknown): boolean => {
  if (!isObject(value)) return false
  if (typeof value['kind'] !== 'string' || typeof value['id'] !== 'string') return false
  if (!isObject(value['status'])) return false
  const status = value['status']
  return typeof status['status'] === 'string'
}

const isServerMessage = (value: unknown): value is ServerMessage => {
  if (!isObject(value)) return false
  if (!('type' in value) || !('data' in value)) return false

  const { type, data } = value

  if (type === 'initial') return isInitialState(data)
  if (type === 'event') return isEventStoreEntry(data)
  if (type === 'snapshot') return isInventorySnapshot(data)
  if (type === 'transfer') return isTransferOperation(data)

  return false
}

const matchMessage = matcher<ServerMessage>()('type')

const getReconnectDelay = (): number => RECONNECT_DELAY_MS

export type ErrorContext = {
  attempts: number
  nextRetryMs: number
}

export const createWebSocket = (url: string, queryClient: QueryClient) => {
  let socket: WebSocket | null = null
  let reconnectTimeoutId: ReturnType<typeof setTimeout> | null = null
  const reconnectAttempts = reactive(0)
  const error = reactive<ErrorContext | null>(null)

  const handleMessage = (msg: ServerMessage) => {
    matchMessage(msg, {
      initial: ({ data }) => {
        queryClient.setQueryData<EventStoreEntry[]>(['events'], [])
        queryClient.setQueryData(['trades'], data.recentTrades)
        queryClient.setQueryData(['inventory'], data.inventory)
        queryClient.setQueryData(['metrics'], data.metrics)
        queryClient.setQueryData(['spreads'], data.spreads)
        queryClient.setQueryData(['transfers', 'active'], data.activeTransfers)
        queryClient.setQueryData(['transfers', 'recent'], data.recentTransfers)
        queryClient.setQueryData(['auth'], data.authStatus)
        queryClient.setQueryData(['circuitBreaker'], data.circuitBreaker)
        queryClient.setQueryData(['warnings'], data.warnings)
      },

      event: ({ data }) => {
        queryClient.setQueryData<EventStoreEntry[]>(['events'], (old) =>
          [data, ...(old ?? [])].slice(0, MAX_EVENTS)
        )
      },

      snapshot: ({ data }) => {
        queryClient.setQueryData<Inventory>(['inventory'], data.inventory)
      },

      transfer: ({ data }) => {
        queryClient.setQueryData<TransferOperation[]>(['transfers', 'active'], (old) => {
          const existing = old ?? []
          const index = existing.findIndex((transfer) => transfer.id === data.id)

          if (data.status.status === 'completed' || data.status.status === 'failed') {
            const filtered = index >= 0 ? existing.filter((_, idx) => idx !== index) : existing
            queryClient.setQueryData<TransferOperation[]>(
              ['transfers', 'recent'],
              (recent) => [data, ...(recent ?? []).filter((transfer) => transfer.id !== data.id)].slice(0, MAX_EVENTS)
            )
            return filtered
          }

          if (index >= 0) {
            return existing.map((transfer, idx) => (idx === index ? data : transfer))
          }
          return [data, ...existing]
        })
      }
    })
  }

  const createSocket = () => {
    socket = new WebSocket(url)

    socket.onopen = () => {
      console.log(`[ws] connected to ${url}`)
      fsm.send('open')
    }

    socket.onmessage = (event) => {
      try {
        const parsed: unknown = JSON.parse(event.data as string)

        if (!isServerMessage(parsed)) {
          console.error('Invalid ServerMessage structure:', parsed)
          return
        }

        const { type, data } = parsed
        console.log(`[ws] received "${type}"`, data)

        handleMessage(parsed)
      } catch (e) {
        console.error('Failed to parse WebSocket message:', e, 'Raw data:', event.data)
      }
    }

    socket.onclose = (event) => {
      console.log(`[ws] closed (code=${String(event.code)}, reason="${event.reason}")`)
      socket = null
      fsm.send('close')
    }

    socket.onerror = () => {
      fsm.send('error')
    }
  }

  const cleanupSocket = () => {
    if (socket !== null) {
      socket.onclose = null
      socket.onerror = null
      socket.onmessage = null
      socket.onopen = null
      socket.close()
      socket = null
    }
  }

  const cancelReconnect = () => {
    if (reconnectTimeoutId !== null) {
      clearTimeout(reconnectTimeoutId)
      reconnectTimeoutId = null
    }
  }

  const scheduleReconnect = () => {
    cancelReconnect()
    const delay = getReconnectDelay()
    error.update(() => ({ attempts: reconnectAttempts.current + 1, nextRetryMs: delay }))
    reconnectAttempts.update(attempts => attempts + 1)
    reconnectTimeoutId = setTimeout(() => fsm.send('connect'), delay)
  }

  const fsm = new FiniteStateMachine<ConnectionState, ConnectionEvent>('disconnected', {
    disconnected: {
      connect: 'connecting',
      _enter: () => {
        cancelReconnect()
        cleanupSocket()
      }
    },

    connecting: {
      open: 'connected',
      error: 'error',
      close: 'error',
      disconnect: 'disconnected',
      _enter: () => {
        createSocket()
      }
    },

    connected: {
      close: 'error',
      error: 'error',
      disconnect: 'disconnected',
      _enter: () => {
        reconnectAttempts.update(() => 0)
        error.update(() => null)
      }
    },

    error: {
      disconnect: 'disconnected',
      connect: 'connecting',
      _enter: () => {
        cleanupSocket()
        scheduleReconnect()
      }
    },

    '*': {
      disconnect: 'disconnected'
    }
  })

  return {
    get state(): ConnectionState {
      return fsm.current
    },
    get error(): ErrorContext | null {
      return error.current
    },
    connect: () => fsm.send('connect'),
    disconnect: () => fsm.send('disconnect')
  }
}

export type WebSocketConnection = ReturnType<typeof createWebSocket>
