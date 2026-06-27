import { beforeEach, describe, expect, it, vi } from 'vitest'

import { resolveLogFilter } from './log-filter-request.svelte'

const CATEGORIES = ['hedge', 'rebalance'] as const
const CRATES = ['st0x_hedge', 'apalis'] as const

describe('resolveLogFilter', () => {
  it('maps known categories to the category filter', () => {
    expect(resolveLogFilter('hedge', CATEGORIES, CRATES)).toEqual({
      kind: 'category',
      target: 'hedge',
    })
  })

  it('maps known crates to the crate filter', () => {
    expect(resolveLogFilter('apalis', CATEGORIES, CRATES)).toEqual({
      kind: 'crate',
      target: 'apalis',
    })
  })

  it('falls back to free-text search for unknown targets', () => {
    expect(
      resolveLogFilter('st0x_hedge::conductor::monitor', CATEGORIES, CRATES),
    ).toEqual({
      kind: 'search',
      target: 'st0x_hedge::conductor::monitor',
    })
  })
})

describe('takeRequestedLogTarget', () => {
  // Reset module state between tests so the shared `requestedLogTarget`
  // reactive does not carry state from one test into the next.
  beforeEach(() => {
    vi.resetModules()
  })

  it('returns the stored value and clears it on the second call', async () => {
    const { requestedLogTarget, takeRequestedLogTarget } = await import(
      './log-filter-request.svelte'
    )

    requestedLogTarget.update(() => 'apalis::pool')

    const first = takeRequestedLogTarget()
    expect(first).toBe('apalis::pool')

    const second = takeRequestedLogTarget()
    expect(second).toBeNull()
  })

  it('returns null when no target has been set', async () => {
    const { takeRequestedLogTarget } = await import('./log-filter-request.svelte')

    expect(takeRequestedLogTarget()).toBeNull()
  })
})
