import { FiniteStateMachine } from 'runed'
import type { QueryClient } from '@tanstack/svelte-query'
import type { Inventory } from '$lib/api/Inventory'
import type { ServerMessage } from '$lib/api/ServerMessage'
import type { Trade } from '$lib/api/Trade'
import type { TransferOperation } from '$lib/api/TransferOperation'
import { matcher } from '$lib/fp'
import { reactive } from '$lib/frp.svelte'

export const transferKey = (transfer: TransferOperation): string => `${transfer.kind}:${transfer.id}`

export type ConnectionState = 'disconnected' | 'connecting' | 'connected' | 'error'
type ConnectionEvent = 'connect' | 'open' | 'close' | 'error' | 'disconnect'

const RECONNECT_DELAY_MS = 1000
const MAX_RECONNECT_DELAY_MS = 10000
const MAX_TRADES = 100

const isObject = (value: unknown): value is Record<string, unknown> =>
  typeof value === 'object' && value !== null

const isServerMessage = (value: unknown): value is ServerMessage => {
  if (!isObject(value)) return false
  if (!('type' in value) || !('data' in value)) return false

  const { type } = value
  return type === 'initial' || type === 'fill' || type === 'snapshot' || type === 'transfer' || type === 'statement'
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
        queryClient.setQueryData<Trade[]>(['trades'], data.trades)
        queryClient.setQueryData(['inventory'], data.inventory)
        queryClient.setQueryData(['transfers', 'active'], data.activeTransfers)
        queryClient.setQueryData(['transfers', 'recent'], data.recentTransfers)
      },

      fill: ({ data }) => {
        queryClient.setQueryData<Trade[]>(['trades'], (old) =>
          [data, ...(old ?? [])].slice(0, MAX_TRADES)
        )
      },

      snapshot: ({ data }) => {
        queryClient.setQueryData<Inventory>(['inventory'], data.inventory)
      },

      transfer: ({ data }) => {
        const key = transferKey(data)

        queryClient.setQueryData<TransferOperation[]>(['transfers', 'active'], (old) => {
          const existing = old ?? []
          const index = existing.findIndex((transfer) => transferKey(transfer) === key)

          if (data.status.status === 'completed' || data.status.status === 'failed') {
            const filtered = index >= 0 ? existing.filter((_, idx) => idx !== index) : existing

            queryClient.setQueryData<TransferOperation[]>(
              ['transfers', 'recent'],
              (recent) => [data, ...(recent ?? []).filter((transfer) => transferKey(transfer) !== key)].slice(0, MAX_TRADES)
            )

            return filtered
          }

          if (index >= 0) {
            return existing.map((transfer, idx) => (idx === index ? data : transfer))
          }

          return [data, ...existing]
        })
      },

      statement: () => {
        // Future: use concern type to invalidate specific query keys
      }
    })
  }

  const createSocket = () => {
    socket = new WebSocket(url)

    socket.onopen = () => {
      if (import.meta.env.DEV) console.log(`[ws] connected to ${url}`)
      fsm.send('open')
    }

    socket.onmessage = (event) => {
      try {
        const parsed: unknown = JSON.parse(event.data as string)

        if (!isServerMessage(parsed)) {
          console.error('Invalid ServerMessage structure:', parsed)
          return
        }

        const { type } = parsed
        if (import.meta.env.DEV) console.log(`[ws] received "${type}"`, parsed)

        handleMessage(parsed)
      } catch (parseError) {
        console.error('Failed to parse WebSocket message:', parseError, 'Raw data:', event.data)
      }
    }

    socket.onclose = (event) => {
      if (import.meta.env.DEV) console.log(`[ws] closed (code=${String(event.code)}, reason="${event.reason}")`)
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
