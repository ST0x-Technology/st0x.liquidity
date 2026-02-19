import { browser } from '$app/environment'
import { env } from '$env/dynamic/public'

const LOCAL_DEV_PORT = 8001

const isLocalDev = (): boolean => {
  if (!browser) return true
  const { hostname, port } = window.location
  return hostname === 'localhost' && port !== '80' && port !== ''
}

const getDefaultWsUrl = (): string => {
  if (isLocalDev()) {
    return `ws://localhost:${String(LOCAL_DEV_PORT)}/api/ws`
  }

  const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:'
  return `${protocol}//${window.location.host}/api/alpaca/ws`
}

export const getWebSocketUrl = (): string => {
  const val = env['PUBLIC_ALPACA_WS_URL']?.trim()
  return val !== undefined && val !== '' ? val : getDefaultWsUrl()
}
