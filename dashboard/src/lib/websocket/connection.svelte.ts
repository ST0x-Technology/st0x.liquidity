import { FiniteStateMachine } from 'runed'
import type { QueryClient } from '@tanstack/svelte-query'
import type { Statement } from '$lib/api/Statement'
import { isStatement } from '$lib/api/StatementGuard'
import { matcher } from '$lib/fp'
import { reactive } from '$lib/frp.svelte'
import { seedTrades, appendFill } from './trades'
import { seedInventory, updateSnapshot, upsertPosition } from './inventory'
import { seedTransfers, upsertTransfer } from './transfers'

const matchMessage = matcher<Statement>()('type')

export type ConnectionState = 'disconnected' | 'connecting' | 'connected' | 'error'
type ConnectionEvent = 'connect' | 'open' | 'close' | 'error' | 'disconnect'

const RECONNECT_DELAY_MS = 1000
const MAX_RECONNECT_DELAY_MS = 10000

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

  const handleMessage = (msg: Statement) => {
    matchMessage(msg, {
      current_state: ({ data }) => {
        seedTrades(queryClient, data)
        seedInventory(queryClient, data)
        seedTransfers(queryClient, data)
      },

      trade_fill: ({ data }) => { appendFill(queryClient, data); },

      position_update: ({ data }) => { upsertPosition(queryClient, data); },

      inventory_snapshot: ({ data }) => { updateSnapshot(queryClient, data); },

      transfer_update: ({ data }) => { upsertTransfer(queryClient, data); }
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

        if (!isStatement(parsed)) {
          console.error('Invalid Statement structure:', parsed)
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
        reconnectAttempts.update(() => 0)
        error.update(() => null)
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
