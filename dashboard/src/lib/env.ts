import { browser, dev } from '$app/environment'
import { env } from '$env/dynamic/public'

const DEFAULT_LOCAL_DEV_PORT = '8001'
const BACKEND_API_URL_ENV = 'PUBLIC_BACKEND_API_URL'

const localDevPort = (): string => {
  const configured = env['PUBLIC_BACKEND_PORT']?.trim()
  return configured !== undefined && configured !== '' ? configured : DEFAULT_LOCAL_DEV_PORT
}

// `dev` is the discriminator, not the hostname. A built bundle is served by
// nginx next to the bot, so REST and the socket share an origin -- but it is
// now routinely reached at http://localhost:8080 through a `gcloud compute
// start-iap-tunnel` (the GCP VM has no public IP and no tailnet, so that
// tunnel is the ONLY way in). On hostname alone that looked like `vite dev`,
// and the socket was sent to ws://localhost:8001 -- a port nobody has
// forwarded -- while every REST call, which uses window.location.origin,
// worked. Result: a permanently "Disconnected" dashboard whose tables render.
// `dev` is true only under the Vite dev server, which is exactly the case
// that needs a different port.
const isLocalDev = (): boolean => {
  if (!browser) return true
  if (!dev) return false
  const { hostname, port } = window.location
  return ['localhost', '127.0.0.1', '::1'].includes(hostname) && port !== '80' && port !== ''
}

const isAbsoluteHttpUrl = (value: string): boolean =>
  value.startsWith('http://') || value.startsWith('https://')

const getConfiguredBackendApiUrl = (): string | null => {
  const val = env[BACKEND_API_URL_ENV]?.trim()
  return val !== undefined && val !== '' ? val : null
}

const getSameOriginWsUrl = (): string => {
  const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:'
  return `${protocol}//${window.location.host}/api/ws`
}

const getDefaultWsUrl = (): string => {
  if (isLocalDev()) {
    const backendApiUrl = getConfiguredBackendApiUrl()
    if (browser && backendApiUrl !== null && isAbsoluteHttpUrl(backendApiUrl)) {
      return getSameOriginWsUrl()
    }

    return `ws://localhost:${localDevPort()}/api/ws`
  }

  return getSameOriginWsUrl()
}

export const getWebSocketUrl = (): string => {
  const envKey = 'PUBLIC_ALPACA_WS_URL'
  const val = env[envKey]?.trim()
  return val !== undefined && val !== '' ? val : getDefaultWsUrl()
}

export const getApiBaseUrl = (): string => window.location.origin

export const getSimulateRev = (): string | null => {
  const val = env['PUBLIC_SIMULATE_REV']?.trim()
  return val !== undefined && val !== '' ? val : null
}

export const getSimulateBackendPort = (): string | null => {
  const val = env['PUBLIC_BACKEND_PORT']?.trim()
  return val !== undefined && val !== '' ? val : null
}

export const getSimulateSourceId = (): string | null => {
  const val = env['PUBLIC_SIMULATE_SOURCE_ID']?.trim()
  return val !== undefined && val !== '' ? val : null
}

export const isDashboardMockMode = (): boolean => {
  const val = env['PUBLIC_DASHBOARD_MOCK_MODE']?.trim().toLowerCase()
  return val === '1' || val === 'true'
}

export const getExplorerTxUrl = (txHash: string): string => {
  const envKey = 'PUBLIC_EXPLORER_URL'
  const base = env[envKey]?.trim()
  const explorerUrl = base !== undefined && base !== '' ? base : 'https://basescan.org'
  return `${explorerUrl}/tx/${txHash}`
}
