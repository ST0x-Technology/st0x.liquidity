import { browser, dev } from '$app/environment'
import { env } from '$env/dynamic/public'

const DEFAULT_LOCAL_DEV_PORT = '8001'
const PNL_SQL_DEV_PROXY_PATH = '/__pnl_sql'
const PNL_ALPACA_ACTIVITIES_DEV_PROXY_PATH = '/__pnl_alpaca_activities'
const BACKEND_API_URL_ENV = 'PUBLIC_BACKEND_API_URL'

const localDevPort = (): string => {
  const configured = env['PUBLIC_BACKEND_PORT']?.trim()
  return configured !== undefined && configured !== '' ? configured : DEFAULT_LOCAL_DEV_PORT
}

const isLocalDev = (): boolean => {
  if (!browser) return true
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

export const getPnlSqlApiUrl = (): string | null => {
  const envKey = 'PUBLIC_PNL_SQL_API_URL'
  const rawEnv = env as Record<string, string | undefined>
  const val = rawEnv[envKey]?.trim()
  if (val === undefined || val === '') return null
  if (dev && isAbsoluteHttpUrl(val)) return PNL_SQL_DEV_PROXY_PATH
  return val
}

export const getPnlAlpacaActivitiesApiUrl = (): string | null => {
  const envKey = 'PUBLIC_PNL_ALPACA_ACTIVITIES_API_URL'
  const rawEnv = env as Record<string, string | undefined>
  const explicit = rawEnv[envKey]?.trim()
  if (explicit !== undefined && explicit !== '') {
    if (dev && isAbsoluteHttpUrl(explicit)) return PNL_ALPACA_ACTIVITIES_DEV_PROXY_PATH
    return explicit
  }

  const backendApiUrl = getConfiguredBackendApiUrl()
  if (backendApiUrl === null) return null
  return '/pnl/alpaca-activities'
}

export const isDashboardMockMode = (): boolean => {
  const val = env['PUBLIC_DASHBOARD_MOCK_MODE']?.trim().toLowerCase()
  return val === '1' || val === 'true'
}

export const isPnlSqlOnlyMode = (): boolean => {
  const val = env['PUBLIC_PNL_SQL_ONLY_MODE']?.trim().toLowerCase()
  return val === '1' || val === 'true'
}

export const getExplorerTxUrl = (txHash: string): string => {
  const envKey = 'PUBLIC_EXPLORER_URL'
  const base = env[envKey]?.trim()
  const explorerUrl = base !== undefined && base !== '' ? base : 'https://basescan.org'
  return `${explorerUrl}/tx/${txHash}`
}
