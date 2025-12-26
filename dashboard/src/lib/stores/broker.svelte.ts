import { browser } from '$app/environment'
import type { Broker } from '$lib/env'
import { tryCatch, isOk, isErr } from '$lib/fp'

const STORAGE_KEY = 'selected-broker'
const DEFAULT_BROKER: Broker = 'schwab'

const VALID_BROKERS: readonly Broker[] = ['schwab', 'alpaca'] as const

const isBroker = (value: unknown): value is Broker =>
  typeof value === 'string' && (VALID_BROKERS as readonly string[]).includes(value)

const loadBroker = (): Broker => {
  if (!browser) return DEFAULT_BROKER

  const result = tryCatch(() => {
    const raw = localStorage.getItem(STORAGE_KEY)
    if (raw === null) return null

    return JSON.parse(raw) as unknown
  })

  if (!isOk(result)) return DEFAULT_BROKER

  return isBroker(result.value) ? result.value : DEFAULT_BROKER
}

const saveBroker = (broker: Broker) => {
  if (!browser) return

  const result = tryCatch(() => {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(broker))
  })

  if (isErr(result)) {
    console.error(`Failed to save broker to localStorage (key: ${STORAGE_KEY}):`, result.error)
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
