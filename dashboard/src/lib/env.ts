import { env } from '$env/dynamic/public'

export type Broker = 'schwab' | 'alpaca'

const DEFAULT_WS_URLS: Record<Broker, string> = {
  schwab: 'ws://localhost:8080/api/ws',
  alpaca: 'ws://localhost:8080/api/ws'
}

export const getWebSocketUrl = (broker: Broker): string => {
  const envKey = broker === 'schwab' ? 'PUBLIC_SCHWAB_WS_URL' : 'PUBLIC_ALPACA_WS_URL'
  return env[envKey] ?? DEFAULT_WS_URLS[broker]
}
