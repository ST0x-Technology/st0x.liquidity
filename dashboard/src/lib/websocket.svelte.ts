import { FiniteStateMachine } from 'runed'
import type { QueryClient } from '@tanstack/svelte-query'
import { reactive } from '$lib/frp.svelte'

export const transferKey = (transfer: TransferOperation): string => `${transfer.kind}:${transfer.id}`

export type ConnectionState = 'disconnected' | 'connecting' | 'connected' | 'error'
type ConnectionEvent = 'connect' | 'open' | 'close' | 'error' | 'disconnect'

const RECONNECT_DELAY_MS = 5000

const getReconnectDelay = (): number => RECONNECT_DELAY_MS

export type ErrorContext = {
  attempts: number
  nextRetryMs: number
}

export const createWebSocket = (url: string, _queryClient: QueryClient) => {
  let socket: WebSocket | null = null
  let reconnectTimeoutId: ReturnType<typeof setTimeout> | null = null
  const reconnectAttempts = reactive(0)
  const error = reactive<ErrorContext | null>(null)

  // TODO: re-integrate message handling for Statement/Concern types
  const handleMessage = (_parsed: unknown) => {}

  const createSocket = () => {
    socket = new WebSocket(url)

    socket.onopen = () => {
      console.log(`[ws] connected to ${url}`)
      fsm.send('open')
    }

    socket.onmessage = (event) => {
      try {
        const parsed: unknown = JSON.parse(event.data as string)
        console.log('[ws] received message', parsed)
        handleMessage(parsed)
      } catch (parseError) {
        console.error('Failed to parse WebSocket message:', parseError, 'Raw data:', event.data)
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
