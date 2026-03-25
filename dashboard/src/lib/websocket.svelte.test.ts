import { describe, expect, it, vi, beforeEach, afterEach } from 'vitest'
import { createWebSocket } from './websocket.svelte'
import type { QueryClient } from '@tanstack/svelte-query'

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

  simulateError() {
    this.onerror?.()
  }

  simulateClose(code = 1000, reason = '') {
    this.onclose?.({ code, reason } as unknown as Event)
  }

  static getInstance(index: number): MockWebSocket {
    const instance = MockWebSocket.instances[index]
    if (!instance) {
      throw new Error(`MockWebSocket instance ${String(index)} not found`)
    }
    return instance
  }
}

const createMockQueryClient = (): QueryClient => {
  const setQueryDataSpy = vi.fn()
  const getQueryDataSpy = vi.fn()
  const clearSpy = vi.fn()

  return {
    setQueryData: setQueryDataSpy,
    getQueryData: getQueryDataSpy,
    clear: clearSpy
  } as unknown as QueryClient
}

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

  it('schedules reconnect on close with fixed delay', () => {
    const queryClient = createMockQueryClient()
    const ws = createWebSocket('ws://localhost:8080', queryClient)

    ws.connect()
    MockWebSocket.getInstance(0).simulateOpen()
    MockWebSocket.getInstance(0).simulateClose()

    expect(ws.state).toBe('error')

    vi.advanceTimersByTime(5000)
    expect(MockWebSocket.instances.length).toBe(2)

    MockWebSocket.getInstance(1).simulateClose()
    vi.advanceTimersByTime(5000)
    expect(MockWebSocket.instances.length).toBe(3)

    MockWebSocket.getInstance(2).simulateClose()
    vi.advanceTimersByTime(5000)
    expect(MockWebSocket.instances.length).toBe(4)
  })

  it('resets reconnect attempts on successful connection', () => {
    const queryClient = createMockQueryClient()
    const ws = createWebSocket('ws://localhost:8080', queryClient)

    ws.connect()
    MockWebSocket.getInstance(0).simulateOpen()
    MockWebSocket.getInstance(0).simulateClose()

    vi.advanceTimersByTime(5000)
    MockWebSocket.getInstance(1).simulateClose()

    vi.advanceTimersByTime(5000)
    MockWebSocket.getInstance(2).simulateOpen()
    MockWebSocket.getInstance(2).simulateClose()

    vi.advanceTimersByTime(5000)
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

  it('keeps retrying with fixed delay indefinitely', () => {
    const queryClient = createMockQueryClient()
    const ws = createWebSocket('ws://localhost:8080', queryClient)

    ws.connect()

    for (let i = 0; i < 10; i++) {
      MockWebSocket.getInstance(i).simulateClose()
      vi.advanceTimersByTime(5000)
    }

    MockWebSocket.getInstance(10).simulateClose()
    vi.advanceTimersByTime(5000)
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

    expect(ws.error).toEqual({ attempts: 1, nextRetryMs: 5000 })

    vi.advanceTimersByTime(5000)
    MockWebSocket.getInstance(1).simulateClose()

    expect(ws.error).toEqual({ attempts: 2, nextRetryMs: 5000 })
  })

  it('clears error context on successful reconnection', () => {
    const queryClient = createMockQueryClient()
    const ws = createWebSocket('ws://localhost:8080', queryClient)

    ws.connect()
    MockWebSocket.getInstance(0).simulateOpen()
    MockWebSocket.getInstance(0).simulateClose()

    expect(ws.error).not.toBeNull()

    vi.advanceTimersByTime(5000)
    MockWebSocket.getInstance(1).simulateOpen()

    expect(ws.error).toBeNull()
  })
})
