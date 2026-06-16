import tailwindcss from '@tailwindcss/vite'
import { sveltekit } from '@sveltejs/kit/vite'
import { defineConfig } from 'vite'

declare const process: {
  env: Record<string, string | undefined>
}
const nonEmptyEnv = (key: string): string | undefined => {
  const value = process.env[key]?.trim()
  return value !== undefined && value !== '' ? value : undefined
}

const backendPort = nonEmptyEnv('PUBLIC_BACKEND_PORT') ?? nonEmptyEnv('BACKEND_PORT') ?? '8001'
const configuredBackendUrl = process.env['PUBLIC_BACKEND_API_URL']?.trim()
const backendUrl =
  configuredBackendUrl !== undefined && configuredBackendUrl !== ''
    ? configuredBackendUrl
    : `http://localhost:${backendPort}`

type MockRequest = {
  url?: string | null
}

type MockResponse = {
  statusCode: number
  setHeader: (name: string, value: string) => void
  end: (body?: string) => void
}

type MockNext = () => void

const isMockMode = (): boolean => {
  const value = process.env['PUBLIC_DASHBOARD_MOCK_MODE']?.trim().toLowerCase()
  return value === '1' || value === 'true'
}

const pnlSqlApiUrl = (): string | null => {
  const value = process.env['PUBLIC_PNL_SQL_API_URL']?.trim()
  return value !== undefined && value !== '' ? value : null
}

const pnlSqlProxy = () => {
  const value = pnlSqlApiUrl()
  if (value === null || !(value.startsWith('http://') || value.startsWith('https://'))) {
    return {}
  }

  const url = new URL(value)
  return {
    '/__pnl_sql': {
      target: url.origin,
      changeOrigin: true,
      rewrite: (path: string) => {
        const queryStart = path.indexOf('?')
        const query = queryStart === -1 ? '' : path.slice(queryStart)
        return `${url.pathname}${query}`
      }
    }
  }
}

const backendProxy = (websocket = false) => ({
  target: backendUrl,
  changeOrigin: true,
  ...(websocket ? { ws: true } : {})
})

const mockJson = (res: MockResponse, body: unknown): void => {
  res.statusCode = 200
  res.setHeader('content-type', 'application/json')
  res.end(JSON.stringify(body))
}

const emptyPage = {
  entries: [],
  total: 0,
  hasMore: false
}

const mockApi = () => ({
  name: 'st0x-dashboard-mock-api',
  configureServer(server: import('vite').ViteDevServer) {
    if (!isMockMode()) return

    server.middlewares.use((req: unknown, res: unknown, next: unknown) => {
      const request = req as MockRequest
      const response = res as MockResponse
      const goNext = next as MockNext
      const path = new URL(request.url ?? '/', 'http://localhost').pathname

      if (path === '/health') {
        mockJson(response, { gitCommit: 'mock', uptimeSeconds: 0 })
        return
      }

      if (path === '/orders/pending') {
        mockJson(response, [])
        return
      }

      if (path === '/orders/raindex') {
        mockJson(response, {
          orders: [],
          pagination: {
            page: 1,
            pageSize: 100,
            totalOrders: 0,
            totalPages: 0,
            hasMore: false
          }
        })
        return
      }

      if (path === '/trades') {
        mockJson(response, emptyPage)
        return
      }

      if (path.startsWith('/trades/') && path.endsWith('/events')) {
        mockJson(response, { events: [] })
        return
      }

      if (path === '/transfers') {
        mockJson(response, emptyPage)
        return
      }

      if (path.startsWith('/transfers/') && path.endsWith('/events')) {
        mockJson(response, { events: [] })
        return
      }

      if (path === '/logs') {
        mockJson(response, emptyPage)
        return
      }

      if (path === '/performance/latencies') {
        mockJson(response, {
          summary: {
            fillCount: 0,
            stages: {
              detection: null,
              decision: null,
              submission: null,
              execution: null,
              exposureWindow: null,
            },
          },
          buckets: [],
          cycles: [],
          totalCycles: 0,
          openExposures: [],
        })
        return
      }

      if (path === '/performance/rebalances') {
        mockJson(response, {
          operations: [],
          totalOperations: 0,
          skippedOperations: 0,
          stageSummary: [],
          attestationTrend: [],
        })
        return
      }

      if (path === '/performance/reliability') {
        mockJson(response, {
          logBuckets: [],
          logTargets: [],
          failureEvents: [],
          jobQueues: [],
        })
        return
      }

      goNext()
    })
  }
})

export default defineConfig({
  plugins: [mockApi(), tailwindcss(), sveltekit()],
  server: {
    proxy: isMockMode()
      ? {}
      : {
          '/api/ws': backendProxy(true),
          '/logs': backendProxy(),
          '/health': backendProxy(),
          '/orders': backendProxy(),
          '/performance': backendProxy(),
          '/trades': backendProxy(),
          '/transfers': backendProxy(),
          '/performance': backendProxy(),
          ...pnlSqlProxy()
        }
  }
})
