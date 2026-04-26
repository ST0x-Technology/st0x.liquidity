import { browser } from '$app/environment'
import { env } from '$env/dynamic/public'

const DEFAULT_LOCAL_DEV_PORT = '8001'

const localDevPort = (): string => {
  const configured = env['PUBLIC_BACKEND_PORT']?.trim()
  return configured !== undefined && configured !== '' ? configured : DEFAULT_LOCAL_DEV_PORT
}

const isLocalDev = (): boolean => {
  if (!browser) return true
  const { hostname, port } = window.location
  return hostname === 'localhost' && port !== '80' && port !== ''
}

const getDefaultWsUrl = (): string => {
  if (isLocalDev()) {
    return `ws://localhost:${localDevPort()}/api/ws`
  }

  const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:'
  return `${protocol}//${window.location.host}/api/ws`
}

export const getWebSocketUrl = (): string => {
  const envKey = 'PUBLIC_ALPACA_WS_URL'
  const val = env[envKey]?.trim()
  return val !== undefined && val !== '' ? val : getDefaultWsUrl()
}

export const getApiBaseUrl = (): string => window.location.origin

export const getExplorerTxUrl = (txHash: string): string => {
  const envKey = 'PUBLIC_EXPLORER_URL'
  const base = env[envKey]?.trim()
  const explorerUrl = base !== undefined && base !== '' ? base : 'https://basescan.org'
  return `${explorerUrl}/tx/${txHash}`
}
