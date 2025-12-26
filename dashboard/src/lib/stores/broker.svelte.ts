import { PersistedState } from 'runed'
import type { Broker } from '$lib/env'

const isBroker = (value: unknown): value is Broker =>
  value === 'schwab' || value === 'alpaca'

const brokerState = new PersistedState<Broker>('selected-broker', 'schwab', {
  storage: 'local',
  syncTabs: true,
  serializer: {
    serialize: JSON.stringify,
    deserialize: (raw) => {
      const parsed: unknown = JSON.parse(raw)
      return isBroker(parsed) ? parsed : 'schwab'
    }
  }
})

export const brokerStore = {
  get value() {
    return brokerState.current
  },

  set(broker: Broker) {
    brokerState.current = broker
  }
}
