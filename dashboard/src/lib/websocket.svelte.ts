import { FiniteStateMachine } from 'runed'
import type { QueryClient } from '@tanstack/svelte-query'
import type { EventStoreEntry } from '$lib/api/EventStoreEntry'
import type { ServerMessage } from '$lib/api/ServerMessage'
import { matcher } from '$lib/fp'
import { reactive } from '$lib/frp.svelte'

export type ConnectionState = 'disconnected' | 'connecting' | 'connected' | 'error'
type ConnectionEvent = 'connect' | 'open' | 'close' | 'error' | 'disconnect'

const RECONNECT_DELAY_MS = 1000
const MAX_RECONNECT_DELAY_MS = 30000
const MAX_EVENTS = 100

const isObject = (v: unknown): v is Record<string, unknown> =>
  typeof v === 'object' && v !== null

const isEventStoreEntry = (v: unknown): boolean => {
  if (!isObject(v)) return false
  return (
    typeof v['aggregate_type'] === 'string' &&
    typeof v['aggregate_id'] === 'string' &&
    typeof v['sequence'] === 'number' &&
    typeof v['event_type'] === 'string' &&
    typeof v['timestamp'] === 'string'
  )
}

const isInitialState = (v: unknown): boolean => {
  if (!isObject(v)) return false
  return (
    Array.isArray(v['recentTrades']) &&
    isObject(v['inventory']) &&
    isObject(v['metrics']) &&
    Array.isArray(v['spreads']) &&
    Array.isArray(v['activeRebalances']) &&
    Array.isArray(v['recentRebalances']) &&
    isObject(v['authStatus']) &&
    isObject(v['circuitBreaker'])
  )
}

const isServerMessage = (value: unknown): value is ServerMessage => {
  if (!isObject(value)) return false
  if (!('type' in value) || !('data' in value)) return false

  const { type, data } = value

  if (type === 'initial') return isInitialState(data)
  if (type === 'event') return isEventStoreEntry(data)

  return false
}

const matchMessage = matcher<ServerMessage>()('type')

const getReconnectDelay = (attempts: number): number =>
  Math.min(RECONNECT_DELAY_MS * Math.pow(2, attempts), MAX_RECONNECT_DELAY_MS)

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
        queryClient.setQueryData(['rebalances', 'active'], data.activeRebalances)
        queryClient.setQueryData(['rebalances', 'recent'], data.recentRebalances)
        queryClient.setQueryData(['auth'], data.authStatus)
        queryClient.setQueryData(['circuitBreaker'], data.circuitBreaker)
      },

      event: ({ data }) => {
        queryClient.setQueryData<EventStoreEntry[]>(['events'], (old) =>
          [data, ...(old ?? [])].slice(0, MAX_EVENTS)
        )
      }
    })
  }

  const createSocket = () => {
    socket = new WebSocket(url)

    socket.onopen = () => {
      fsm.send('open')
    }

    socket.onmessage = (event) => {
      try {
        const parsed: unknown = JSON.parse(event.data as string)

        if (!isServerMessage(parsed)) {
          console.error('Invalid ServerMessage structure:', parsed)
          return
        }

        handleMessage(parsed)
      } catch (e) {
        console.error('Failed to parse WebSocket message:', e, 'Raw data:', event.data)
      }
    }

    socket.onclose = () => {
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
    const delay = getReconnectDelay(reconnectAttempts.current)
    error.update(() => ({ attempts: reconnectAttempts.current + 1, nextRetryMs: delay }))
    reconnectAttempts.update(n => n + 1)
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
