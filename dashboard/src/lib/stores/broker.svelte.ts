import { PersistedState } from 'runed'
import { isBroker, type Broker } from '$lib/env'
import { tryCatch, isOk } from '$lib/fp'

const brokerState = new PersistedState<Broker>('selected-broker', 'schwab', {
  storage: 'local',
  syncTabs: true,
  serializer: {
    serialize: JSON.stringify,
    deserialize: (raw) => {
      const result = tryCatch(() => JSON.parse(raw) as unknown)
      if (!isOk(result)) return 'schwab'
      return isBroker(result.value) ? result.value : 'schwab'
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
