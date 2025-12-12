import { browser } from '$app/environment'
import type { Broker } from '$lib/env'

const STORAGE_KEY = 'selected-broker'
const DEFAULT_BROKER: Broker = 'schwab'

const loadBroker = (): Broker => {
  if (!browser) return DEFAULT_BROKER

  const stored = localStorage.getItem(STORAGE_KEY)
  if (stored === 'schwab' || stored === 'alpaca') {
    return stored
  }

  return DEFAULT_BROKER
}

const saveBroker = (broker: Broker) => {
  if (browser) {
    localStorage.setItem(STORAGE_KEY, broker)
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
