import { browser } from '$app/environment'
import { env } from '$env/dynamic/public'

export const VALID_BROKERS = ['schwab', 'alpaca'] as const
export type Broker = (typeof VALID_BROKERS)[number]

export const isBroker = (value: unknown): value is Broker =>
  typeof value === 'string' && VALID_BROKERS.includes(value as Broker)

const LOCAL_DEV_PORTS: Record<Broker, number> = {
  schwab: 8000,
  alpaca: 8001
}

const isLocalDev = (): boolean => {
  if (!browser) return true
  const { hostname, port } = window.location
  return hostname === 'localhost' && port !== '80' && port !== ''
}

const getDefaultWsUrl = (broker: Broker): string => {
  if (isLocalDev()) {
    return `ws://localhost:${String(LOCAL_DEV_PORTS[broker])}/api/ws`
  }

  const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:'
  return `${protocol}//${window.location.host}/api/${broker}/ws`
}

export const getWebSocketUrl = (broker: Broker): string => {
  const envKey = broker === 'schwab' ? 'PUBLIC_SCHWAB_WS_URL' : 'PUBLIC_ALPACA_WS_URL'
  return env[envKey] ?? getDefaultWsUrl(broker)
}
