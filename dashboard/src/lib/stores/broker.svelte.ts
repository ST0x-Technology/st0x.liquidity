import { browser } from '$app/environment'
import type { Broker } from '$lib/env'
import { tryCatch, isOk } from '$lib/fp'

const STORAGE_KEY = 'selected-broker'
const DEFAULT_BROKER: Broker = 'schwab'

const loadBroker = (): Broker => {
  if (!browser) return DEFAULT_BROKER

  const result = tryCatch(() => localStorage.getItem(STORAGE_KEY))
  if (!isOk(result)) return DEFAULT_BROKER

  const stored = result.value
  if (stored === 'schwab' || stored === 'alpaca') {
    return stored
  }

  return DEFAULT_BROKER
}

const saveBroker = (broker: Broker) => {
  if (browser) {
    tryCatch(() => localStorage.setItem(STORAGE_KEY, broker))
  }
}

let selectedBroker = $state<Broker>(loadBroker())

export const brokerStore = {
  get value() {
    return selectedBroker
  },

  set(broker: Broker) {
    selectedBroker = broker
    saveBroker(broker)
  }
}
