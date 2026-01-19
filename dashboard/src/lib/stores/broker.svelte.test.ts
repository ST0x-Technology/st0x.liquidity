import { describe, it, expect, vi, afterEach } from 'vitest'
import type { Broker } from '$lib/env'

type MockPersistedStateConfig = {
  initialValue: Broker
}

const createMockPersistedState = (config: MockPersistedStateConfig) => {
  let value = config.initialValue

  return class MockPersistedState {
    get current() {
      return value
    }
    set current(newValue: Broker) {
      value = newValue
    }
  }
}

describe('brokerStore', () => {
  afterEach(() => {
    vi.resetModules()
    vi.restoreAllMocks()
  })

  const setupTest = async (config: MockPersistedStateConfig) => {
    const MockPersistedState = createMockPersistedState(config)

    vi.doMock('runed', () => ({
      PersistedState: MockPersistedState
    }))

    const { brokerStore } = await import('./broker.svelte')
    return { brokerStore }
  }

  it('exposes the current value via getter', async () => {
    const { brokerStore } = await setupTest({ initialValue: 'schwab' })
    expect(brokerStore.value).toBe('schwab')
  })

  it('updates value when set is called', async () => {
    const { brokerStore } = await setupTest({ initialValue: 'schwab' })
    brokerStore.set('alpaca')
    expect(brokerStore.value).toBe('alpaca')
  })

  it('can be initialized with alpaca', async () => {
    const { brokerStore } = await setupTest({ initialValue: 'alpaca' })
    expect(brokerStore.value).toBe('alpaca')
  })
})
