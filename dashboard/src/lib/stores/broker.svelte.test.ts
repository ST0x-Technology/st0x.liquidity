import { describe, it, expect, beforeEach, vi } from 'vitest'

// Mock $app/environment before importing the module
vi.mock('$app/environment', () => ({
  browser: true
}))

// Mock localStorage
const localStorageMock = (() => {
  let store: Record<string, string> = {}
  return {
    getItem: vi.fn((key: string) => store[key] ?? null),
    setItem: vi.fn((key: string, value: string) => {
      store[key] = value
    }),
    clear: () => {
      store = {}
    }
  }
})()

Object.defineProperty(globalThis, 'localStorage', { value: localStorageMock })

describe('brokerStore', () => {
  beforeEach(() => {
    localStorageMock.clear()
    vi.clearAllMocks()
    vi.resetModules()
  })

  it('defaults to schwab when localStorage is empty', async () => {
    const { brokerStore } = await import('./broker.svelte')
    expect(brokerStore.value).toBe('schwab')
  })

  it('loads valid broker from localStorage', async () => {
    localStorageMock.getItem.mockReturnValueOnce('"alpaca"')
    const { brokerStore } = await import('./broker.svelte')
    expect(brokerStore.value).toBe('alpaca')
  })

  it('defaults to schwab for invalid broker in localStorage', async () => {
    localStorageMock.getItem.mockReturnValueOnce('"invalid"')
    const { brokerStore } = await import('./broker.svelte')
    expect(brokerStore.value).toBe('schwab')
  })

  it('defaults to schwab for malformed JSON in localStorage', async () => {
    localStorageMock.getItem.mockReturnValueOnce('not-valid-json')
    const { brokerStore } = await import('./broker.svelte')
    expect(brokerStore.value).toBe('schwab')
  })

  it('saves broker to localStorage when set', async () => {
    const { brokerStore } = await import('./broker.svelte')
    brokerStore.set('alpaca')
    expect(localStorageMock.setItem).toHaveBeenCalledWith('selected-broker', '"alpaca"')
  })

  it('updates value when set is called', async () => {
    const { brokerStore } = await import('./broker.svelte')
    expect(brokerStore.value).toBe('schwab')
    brokerStore.set('alpaca')
    expect(brokerStore.value).toBe('alpaca')
  })
})
