import { PersistedState } from 'runed'
import { isBroker, defaultBroker, type Broker } from '$lib/env'
import { tryCatch, isOk } from '$lib/fp'

const brokerState = new PersistedState<Broker>('selected-broker', defaultBroker(), {
  storage: 'local',
  syncTabs: true,
  serializer: {
    serialize: JSON.stringify,
    deserialize: (raw) => {
      const result = tryCatch(() => JSON.parse(raw) as unknown)
      if (!isOk(result)) return defaultBroker()
      return isBroker(result.value) ? result.value : defaultBroker()
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
