import { browser } from '$app/environment'
import { env } from '$env/dynamic/public'

export type Broker = 'schwab' | 'alpaca'

const getDefaultWsUrl = (): string => {
  if (!browser) return 'ws://localhost:8080/api/ws'

  const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:'
  return `${protocol}//${window.location.host}/api/ws`
}

export const getWebSocketUrl = (broker: Broker): string => {
  const envKey = broker === 'schwab' ? 'PUBLIC_SCHWAB_WS_URL' : 'PUBLIC_ALPACA_WS_URL'
  return env[envKey] ?? getDefaultWsUrl()
}
