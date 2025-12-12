import { env } from '$env/dynamic/public'

export type Broker = 'schwab' | 'alpaca'

const WS_URLS: Record<Broker, string | undefined> = {
  schwab: env.PUBLIC_SCHWAB_WS_URL,
  alpaca: env.PUBLIC_ALPACA_WS_URL
}

export const getWebSocketUrl = (broker: Broker): string => {
  const url = WS_URLS[broker]
  if (url === undefined || url === '') {
    throw new Error(`WebSocket URL not configured for broker: ${broker}`)
  }
  return url
}
