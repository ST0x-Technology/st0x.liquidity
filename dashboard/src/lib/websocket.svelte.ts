import type { QueryClient } from '@tanstack/svelte-query'
import type { EventStoreEntry } from '$lib/api/EventStoreEntry'
import type { ServerMessage } from '$lib/api/ServerMessage'
import { matcher } from '$lib/fp'

export type ConnectionStatus = 'connecting' | 'connected' | 'disconnected'

const RECONNECT_DELAY_MS = 1000
const MAX_RECONNECT_DELAY_MS = 30000
const MAX_EVENTS = 100

const matchMessage = matcher<ServerMessage>()('type')

export const createWebSocket = (url: string, queryClient: QueryClient) => {
  let status = $state<ConnectionStatus>('disconnected')
  let socket: WebSocket | null = null
  let reconnectAttempts = 0
  let reconnectTimeoutId: ReturnType<typeof setTimeout> | null = null

  const handleMessage = (msg: ServerMessage) => {
    matchMessage(msg, {
      initial: ({ data }) => {
        queryClient.setQueryData<EventStoreEntry[]>(['events'], [])
        queryClient.setQueryData(['trades'], data.recentTrades)
        queryClient.setQueryData(['inventory'], data.inventory)
        queryClient.setQueryData(['metrics'], data.metrics)
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

  const scheduleReconnect = () => {
    if (reconnectTimeoutId !== null) return

    const delay = Math.min(RECONNECT_DELAY_MS * Math.pow(2, reconnectAttempts), MAX_RECONNECT_DELAY_MS)
    reconnectAttempts++

    reconnectTimeoutId = setTimeout(() => {
      reconnectTimeoutId = null
      connect()
    }, delay)
  }

  const connect = () => {
    if (socket !== null) return

    status = 'connecting'
    socket = new WebSocket(url)

    socket.onopen = () => {
      status = 'connected'
      reconnectAttempts = 0
    }

    socket.onmessage = (event) => {
      try {
        const msg = JSON.parse(event.data as string) as ServerMessage
        handleMessage(msg)
      } catch (e) {
        // eslint-disable-next-line no-console
        console.error('Failed to parse WebSocket message:', e, 'Raw data:', event.data)
      }
    }

    socket.onclose = () => {
      socket = null
      status = 'disconnected'
      scheduleReconnect()
    }

    socket.onerror = () => {
      socket?.close()
    }
  }

  const disconnect = () => {
    if (reconnectTimeoutId !== null) {
      clearTimeout(reconnectTimeoutId)
      reconnectTimeoutId = null
    }

    reconnectAttempts = 0

    if (socket !== null) {
      socket.onclose = null
      socket.close()
      socket = null
    }

    status = 'disconnected'
  }

  return {
    get status() {
      return status
    },
    connect,
    disconnect
  }
}

export type WebSocketConnection = ReturnType<typeof createWebSocket>
