import { beforeEach, describe, expect, it, vi } from 'vitest'
import { getWebSocketUrl, getExplorerTxUrl } from './env'

const mockEnv = { current: {} as Record<string, string | undefined> }
const mockDev = { current: true }

vi.mock('$app/environment', () => ({
  browser: true,
  get dev() {
    return mockDev.current
  }
}))
vi.mock('$env/dynamic/public', () => ({
  get env() {
    return mockEnv.current
  }
}))

describe('getWebSocketUrl', () => {
  beforeEach(() => {
    mockEnv.current = {}
    mockDev.current = true
    Object.defineProperty(globalThis, 'window', {
      value: {
        location: {
          hostname: 'example.com',
          port: '80',
          host: 'example.com',
          protocol: 'https:'
        }
      },
      writable: true,
      configurable: true
    })
  })

  it('returns env value when set', () => {
    mockEnv.current = { PUBLIC_ALPACA_WS_URL: 'wss://custom.example.com/ws' }

    expect(getWebSocketUrl()).toBe('wss://custom.example.com/ws')
  })

  it('returns default URL when env is undefined', () => {
    expect(getWebSocketUrl()).toBe('wss://example.com/api/ws')
  })

  it('returns default URL when env is empty string', () => {
    mockEnv.current = { PUBLIC_ALPACA_WS_URL: '' }

    expect(getWebSocketUrl()).toBe('wss://example.com/api/ws')
  })

  it('returns default URL when env is whitespace only', () => {
    mockEnv.current = { PUBLIC_ALPACA_WS_URL: '   ' }

    expect(getWebSocketUrl()).toBe('wss://example.com/api/ws')
  })

  it('trims whitespace from valid env values', () => {
    mockEnv.current = { PUBLIC_ALPACA_WS_URL: '  wss://trimmed.example.com/ws  ' }

    expect(getWebSocketUrl()).toBe('wss://trimmed.example.com/ws')
  })

  it('uses default port 8001 in local dev when PUBLIC_BACKEND_PORT is unset', () => {
    Object.defineProperty(globalThis, 'window', {
      value: {
        location: {
          hostname: 'localhost',
          port: '5173',
          host: 'localhost:5173',
          protocol: 'http:'
        }
      },
      writable: true,
      configurable: true
    })

    expect(getWebSocketUrl()).toBe('ws://localhost:8001/api/ws')
  })

  it('honors PUBLIC_BACKEND_PORT in local dev for concurrent simulate instances', () => {
    mockEnv.current = { PUBLIC_BACKEND_PORT: '8002' }
    Object.defineProperty(globalThis, 'window', {
      value: {
        location: {
          hostname: 'localhost',
          port: '5174',
          host: 'localhost:5174',
          protocol: 'http:'
        }
      },
      writable: true,
      configurable: true
    })

    expect(getWebSocketUrl()).toBe('ws://localhost:8002/api/ws')
  })

  it('uses the dev proxy websocket path when a backend API URL is configured', () => {
    mockEnv.current = { PUBLIC_BACKEND_API_URL: 'https://backend.example.com' }
    Object.defineProperty(globalThis, 'window', {
      value: {
        location: {
          hostname: 'localhost',
          port: '5173',
          host: 'localhost:5173',
          protocol: 'http:'
        }
      },
      writable: true,
      configurable: true
    })

    expect(getWebSocketUrl()).toBe('ws://localhost:5173/api/ws')
  })

  // The built bundle behind an IAP tunnel: served by nginx at
  // http://localhost:8080 alongside the bot, so the socket must stay on the
  // page's own origin. Keying off the hostname sent it to :8001 instead --
  // a port nobody forwards -- and the dashboard sat "Disconnected".
  it('uses the page origin for a built bundle served over a localhost tunnel', () => {
    mockDev.current = false
    Object.defineProperty(globalThis, 'window', {
      value: {
        location: {
          hostname: 'localhost',
          port: '8080',
          host: 'localhost:8080',
          protocol: 'http:'
        }
      },
      writable: true,
      configurable: true
    })

    expect(getWebSocketUrl()).toBe('ws://localhost:8080/api/ws')
  })

  it('still honors an explicit PUBLIC_ALPACA_WS_URL over a localhost tunnel', () => {
    mockDev.current = false
    mockEnv.current = { PUBLIC_ALPACA_WS_URL: 'ws://localhost:9999/api/ws' }
    Object.defineProperty(globalThis, 'window', {
      value: {
        location: {
          hostname: 'localhost',
          port: '8080',
          host: 'localhost:8080',
          protocol: 'http:'
        }
      },
      writable: true,
      configurable: true
    })

    expect(getWebSocketUrl()).toBe('ws://localhost:9999/api/ws')
  })
})

describe('getExplorerTxUrl', () => {
  beforeEach(() => {
    mockEnv.current = {}
  })

  it('defaults to flarescan when env is not set', () => {
    expect(getExplorerTxUrl('0xabc123')).toBe('https://basescan.org/tx/0xabc123')
  })

  it('uses custom explorer URL from env', () => {
    mockEnv.current = { PUBLIC_EXPLORER_URL: 'https://arbiscan.io' }

    expect(getExplorerTxUrl('0xabc123')).toBe('https://arbiscan.io/tx/0xabc123')
  })

  it('trims whitespace from env value', () => {
    mockEnv.current = { PUBLIC_EXPLORER_URL: '  https://arbiscan.io  ' }

    expect(getExplorerTxUrl('0xabc123')).toBe('https://arbiscan.io/tx/0xabc123')
  })
})
