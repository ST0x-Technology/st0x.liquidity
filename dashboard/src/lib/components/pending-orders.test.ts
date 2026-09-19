// @vitest-environment happy-dom

import { mount, tick, unmount } from 'svelte'
import { afterEach, describe, expect, it, vi } from 'vitest'
import PendingOrders from './pending-orders.svelte'

vi.mock('$env/dynamic/public', () => ({ env: {} }))

type PendingOrderPayload = {
  viewId: string
  status: string
  symbol: string
  direction: string
  shares: string
  executor: string
  placedAt: string
  submittedAt: string | null
  sharesFilled: string | null
  avgPrice: string | null
  marketSession: string
}

const pendingOrder = (marketSession: string): PendingOrderPayload => ({
  viewId: 'offchain-order-1',
  status: 'Submitted',
  symbol: 'SPCX',
  direction: 'Sell',
  shares: '1',
  executor: 'AlpacaBrokerApi',
  placedAt: '2026-01-01T00:00:00Z',
  submittedAt: '2026-01-01T00:00:01Z',
  sharesFilled: null,
  avgPrice: null,
  marketSession
})

/// Renders the panel against a single stubbed `/orders/pending` response and
/// returns the resulting text, so a test asserts on what an operator sees.
const renderWith = async (marketSession: string): Promise<string> => {
  vi.stubGlobal(
    'fetch',
    vi.fn(() =>
      Promise.resolve({
        ok: true,
        status: 200,
        json: () => Promise.resolve([pendingOrder(marketSession)])
      })
    )
  )

  const target = document.createElement('div')
  document.body.appendChild(target)
  const component = mount(PendingOrders, { target })

  // The mount-time fetch resolves over several microtasks before the
  // reactive update reaches the DOM; wait for the row itself rather than
  // guessing a tick count.
  await vi.waitFor(() => {
    expect(target.textContent ?? '').toContain('SPCX')
  })
  await tick()
  const rendered = target.textContent ?? ''

  unmount(component)
  target.remove()
  return rendered
}

afterEach(() => {
  vi.unstubAllGlobals()
})

describe('pending orders market session', () => {
  it('marks a non-regular session so an operator can spot it', async () => {
    const rendered = await renderWith('Overnight')

    expect(rendered).toContain('Overnight')
  })

  it('leaves a regular-session order unmarked', async () => {
    // Regular is the overwhelming majority, so marking it would add noise to
    // every row and hide the sessions that matter.
    const rendered = await renderWith('Regular')

    expect(rendered).not.toContain('Regular')
  })
})
