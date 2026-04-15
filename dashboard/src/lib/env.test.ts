import { beforeEach, describe, expect, it, vi } from 'vitest'
import { getWebSocketUrl } from './env'

const mockEnv = { current: {} as Record<string, string | undefined> }

vi.mock('$app/environment', () => ({ browser: true }))
vi.mock('$env/dynamic/public', () => ({
  get env() {
    return mockEnv.current
  }
}))

describe('getWebSocketUrl', () => {
  beforeEach(() => {
    mockEnv.current = {}
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
})
