import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { fetchHedgeLatencies, rangeParams } from './api'

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

describe('fetchHedgeLatencies error handling', () => {
  const fetchMock = vi.fn()
  let originalWindow: PropertyDescriptor | undefined
  let originalFetch: PropertyDescriptor | undefined

  beforeEach(() => {
    originalWindow = Object.getOwnPropertyDescriptor(globalThis, 'window')
    originalFetch = Object.getOwnPropertyDescriptor(globalThis, 'fetch')

    Object.defineProperty(globalThis, 'window', {
      value: { location: { origin: 'http://localhost:8001' } },
      writable: true,
      configurable: true,
    })

    Object.defineProperty(globalThis, 'fetch', {
      value: fetchMock,
      writable: true,
      configurable: true,
    })
  })

  afterEach(() => {
    fetchMock.mockReset()

    if (originalWindow) {
      Object.defineProperty(globalThis, 'window', originalWindow)
    } else {
      delete (globalThis as { window?: unknown }).window
    }

    if (originalFetch) {
      Object.defineProperty(globalThis, 'fetch', originalFetch)
    } else {
      delete (globalThis as { fetch?: unknown }).fetch
    }
  })

  it('throws an HTTP error message for non-ok responses', async () => {
    fetchMock.mockResolvedValue({
      ok: false,
      status: 503,
    } as Response)

    await expect(fetchHedgeLatencies()).rejects.toThrow('HTTP 503')
  })
})
