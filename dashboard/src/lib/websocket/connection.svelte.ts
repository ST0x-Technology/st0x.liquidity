import { FiniteStateMachine } from 'runed'
import type { QueryClient } from '@tanstack/svelte-query'
import type { Statement } from '$lib/api/Statement'
import { isStatement } from '$lib/api/StatementGuard'
import { matcher, tryCatch } from '$lib/fp'
import { reactive } from '$lib/frp.svelte'
import { parseCanonicalTrade, parseLegacyTrade, parseTradeEntries } from '$lib/trade-payload'
import { seedTrades, appendTrade } from './trades'
import { seedInventory, updateSnapshot, upsertPosition } from './inventory'
import { seedTransfers, upsertTransfer } from './transfers'

const matchMessage = matcher<Statement>()('type')

export type ConnectionState = 'disconnected' | 'connecting' | 'connected' | 'error'
type ConnectionEvent = 'connect' | 'open' | 'close' | 'error' | 'disconnect'

const RECONNECT_DELAY_MS = 1000
const MAX_RECONNECT_DELAY_MS = 10000

const withTradeProtocol = (url: string): string => {
  const protocolUrl = new URL(url)
  protocolUrl.searchParams.set('trade_protocol', 'terminal_outcomes_v1')
  return protocolUrl.toString()
}

const getReconnectDelay = (attempts: number): number =>
  Math.min(RECONNECT_DELAY_MS * Math.pow(2, attempts), MAX_RECONNECT_DELAY_MS)

export type ErrorContext = {
  attempts: number
  nextRetryMs: number
  message: string
}

export const createWebSocket = (url: string, queryClient: QueryClient) => {
  const protocolUrl = withTradeProtocol(url)
  let socket: WebSocket | null = null
  let reconnectTimeoutId: ReturnType<typeof setTimeout> | null = null
  let failureMessage = 'WebSocket connection error'
  const reconnectAttempts = reactive(0)
  const error = reactive<ErrorContext | null>(null)

  const markHealthy = () => {
    reconnectAttempts.update(() => 0)
    error.update(() => null)
  }

  const handleMessage = (msg: Statement) => {
    matchMessage(msg, {
      current_state: ({ data }) => {
        seedInventory(queryClient, data)
        seedTransfers(queryClient, data)
        seedTrades(queryClient, parseTradeEntries(data.trades))
      },

      trade_update: ({ data }) => {
        appendTrade(queryClient, parseCanonicalTrade(data))
      },

      trade_fill: ({ data }) => {
        appendTrade(queryClient, parseLegacyTrade(data))
      },

      position_update: ({ data }) => {
        upsertPosition(queryClient, data)
      },

      inventory_snapshot: ({ data }) => {
        updateSnapshot(queryClient, data)
      },

      transfer_update: ({ data }) => {
        upsertTransfer(queryClient, data)
      }
    })
  }

  const createSocket = () => {
    socket = new WebSocket(protocolUrl)

    socket.onopen = () => {
      if (import.meta.env.DEV) console.log(`[ws] connected to ${protocolUrl}`)
      fsm.send('open')
    }

    socket.onmessage = (event) => {
      const parsed = tryCatch((): unknown => JSON.parse(event.data as string))
      if (parsed.tag === 'err') {
        failureMessage = 'Rejected malformed WebSocket message'
        console.error('Failed to parse WebSocket message:', parsed.error, 'Raw data:', event.data)
        fsm.send('error')
        return
      }

      if (!isStatement(parsed.value)) {
        failureMessage = 'Rejected invalid WebSocket message'
        console.error('Invalid Statement structure:', parsed.value)
        fsm.send('error')
        return
      }

      const statement = parsed.value
      const { type } = statement
      if (import.meta.env.DEV) console.log(`[ws] received "${type}"`, statement)

      const handled = tryCatch(() => handleMessage(statement))
      if (handled.tag === 'ok') {
        markHealthy()
        return
      }

      failureMessage =
        handled.error instanceof Error
          ? handled.error.message
          : 'Rejected invalid WebSocket message'
      console.error('Failed to parse WebSocket message:', handled.error, 'Raw data:', event.data)
      fsm.send('error')
    }

    socket.onclose = (event) => {
      if (import.meta.env.DEV)
        console.log(`[ws] closed (code=${String(event.code)}, reason="${event.reason}")`)
      failureMessage = 'WebSocket connection closed'
      socket = null
      fsm.send('close')
    }

    socket.onerror = () => {
      failureMessage = 'WebSocket connection error'
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
    error.update(() => ({
      attempts: reconnectAttempts.current + 1,
      nextRetryMs: delay,
      message: failureMessage
    }))
    reconnectAttempts.update((attempts) => attempts + 1)
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
      disconnect: 'disconnected'
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
