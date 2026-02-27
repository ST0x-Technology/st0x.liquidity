import { defineConfig } from '@playwright/test'

const BACKEND_PORT = 8001
const FRONTEND_PORT = 18081
const backendBin = process.env['BACKEND_BIN'] ?? '../target/release/server'
const dashboardDir = process.env['DASHBOARD_DIR'] ?? '../dashboard'

export default defineConfig({
  testDir: './tests',
  timeout: 30_000,
  retries: 0,
  snapshotPathTemplate: '{testDir}/snapshots/{arg}{ext}',
  use: {
    baseURL: `http://localhost:${String(FRONTEND_PORT)}`,
    headless: true,
    viewport: { width: 1440, height: 900 }
  },
  webServer: [
    {
      command: `${backendBin} --config config.toml --secrets secrets.toml`,
      port: BACKEND_PORT,
      reuseExistingServer: false,
      timeout: 10_000
    },
    {
      command: `bun run --cwd ${dashboardDir} preview --port ${String(FRONTEND_PORT)}`,
      port: FRONTEND_PORT,
      reuseExistingServer: false,
      timeout: 10_000
    }
  ]
})
