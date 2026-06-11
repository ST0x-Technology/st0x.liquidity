import { describe, expect, it } from 'vitest'

import { rangeParams } from './api'

describe('rangeParams', () => {
  it('returns an empty string when no bounds are given', () => {
    expect(rangeParams({})).toBe('')
  })

  it('encodes a lone lower bound', () => {
    expect(rangeParams({ from: new Date('2026-06-01T00:00:00Z') })).toBe(
      '?from=2026-06-01T00%3A00%3A00.000Z',
    )
  })

  it('encodes both bounds', () => {
    expect(
      rangeParams({
        from: new Date('2026-06-01T00:00:00Z'),
        to: new Date('2026-06-02T00:00:00Z'),
      }),
    ).toBe('?from=2026-06-01T00%3A00%3A00.000Z&to=2026-06-02T00%3A00%3A00.000Z')
  })
})
