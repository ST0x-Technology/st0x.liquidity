import { describe, expect, it } from 'vitest'

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
