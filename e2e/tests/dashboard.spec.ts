import { test, expect } from '@playwright/test'

test.describe('dashboard', () => {
  test('loads and renders header', async ({ page }) => {
    await page.goto('/')
    await expect(page.locator('h1')).toContainText('st0x.liquidity')
  })

  test('websocket connects and shows Live badge', async ({ page }) => {
    await page.goto('/')
    const badge = page.getByText('Live')
    await expect(badge).toBeVisible({ timeout: 5_000 })
  })

  test('all panels render', async ({ page }) => {
    await page.goto('/')

    await expect(page.getByText('Spreads')).toBeVisible()
    await expect(page.getByText('Trade Log')).toBeVisible()
    await expect(page.getByText('Inventory')).toBeVisible()
    await expect(page.getByText('Rebalancing', { exact: true })).toBeVisible()

    await expect(page).toHaveScreenshot('dashboard-full.png', { fullPage: true })
  })

  test('KPI strip shows placeholder values', async ({ page }) => {
    await page.goto('/')

    const placeholders = page.getByText('--')
    await expect(placeholders.first()).toBeVisible()

    for (const label of ['P&L', 'Volume', 'Trades', 'Hedge Lag', 'Uptime', 'Sharpe']) {
      await expect(page.getByText(label, { exact: true })).toBeVisible()
    }
  })

  test('inventory table renders with column headers', async ({ page }) => {
    await page.goto('/')

    const inventorySection = page.locator('[data-slot="card"]', { hasText: 'Inventory' })
    await expect(inventorySection.getByText('Symbol')).toBeVisible()
    await expect(inventorySection.getByText('Onchain')).toBeVisible()
    await expect(inventorySection.getByText('Offchain')).toBeVisible()
  })

  test('spreads table renders with empty state', async ({ page }) => {
    await page.goto('/')

    const spreadsSection = page.locator('[data-slot="card"]', { hasText: 'Spreads' })
    await expect(spreadsSection).toBeVisible()

    const headers = spreadsSection.locator('[data-slot="table-head"]')
    const emptyState = spreadsSection.getByText('No spread data available')

    const hasTable = (await headers.count()) > 0
    const hasEmpty = await emptyState.isVisible().catch(() => false)
    expect(hasTable || hasEmpty).toBe(true)
  })
})
