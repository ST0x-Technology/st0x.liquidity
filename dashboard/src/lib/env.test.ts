import { beforeEach, describe, expect, it, vi } from 'vitest'

const mockEnv = vi.hoisted(() => ({ current: {} as Record<string, string | undefined> }))

vi.mock('$app/environment', () => ({ browser: true }))
vi.mock('$env/dynamic/public', () => ({
  get env() {
    return mockEnv.current
  }
}))

describe('getWebSocketUrl', () => {
  beforeEach(() => {
    vi.resetModules()
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

  it('returns env value when set', async () => {
    mockEnv.current = { PUBLIC_ALPACA_WS_URL: 'wss://custom.example.com/ws' }
    const { getWebSocketUrl } = await import('./env')

    expect(getWebSocketUrl()).toBe('wss://custom.example.com/ws')
  })

  it('returns default URL when env is undefined', async () => {
    mockEnv.current = {}
    const { getWebSocketUrl } = await import('./env')

    expect(getWebSocketUrl()).toBe('wss://example.com/api/alpaca/ws')
  })

  it('returns default URL when env is empty string', async () => {
    mockEnv.current = { PUBLIC_ALPACA_WS_URL: '' }
    const { getWebSocketUrl } = await import('./env')

    expect(getWebSocketUrl()).toBe('wss://example.com/api/alpaca/ws')
  })

  it('returns default URL when env is whitespace only', async () => {
    mockEnv.current = { PUBLIC_ALPACA_WS_URL: '   ' }
    const { getWebSocketUrl } = await import('./env')

    expect(getWebSocketUrl()).toBe('wss://example.com/api/alpaca/ws')
  })

  it('trims whitespace from valid env values', async () => {
    mockEnv.current = { PUBLIC_ALPACA_WS_URL: '  wss://trimmed.example.com/ws  ' }
    const { getWebSocketUrl } = await import('./env')

    expect(getWebSocketUrl()).toBe('wss://trimmed.example.com/ws')
  })
})
