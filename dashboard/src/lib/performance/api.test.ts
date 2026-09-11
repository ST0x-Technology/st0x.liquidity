import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { fetchHedgeLatencies, fetchInfraReport, rangeParams } from './api'

/** Stubs `window` and `fetch` for the calling describe, restoring both after. */
const stubbedFetch = () => {
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

  return fetchMock
}

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
  const fetchMock = stubbedFetch()

  it('throws an HTTP error message for non-ok responses', async () => {
    fetchMock.mockResolvedValue({
      ok: false,
      status: 503,
    } as Response)

    await expect(fetchHedgeLatencies()).rejects.toThrow('HTTP 503')
  })
})

describe('fetchInfraReport', () => {
  const fetchMock = stubbedFetch()

  const lagSeries = (chain: string) => ({
    chain,
    currentLagBlocks: 5,
    currentLagSampledAt: '2026-06-01T00:00:00Z',
    points: [],
  })

  /** An `/performance/infra` body from a backend that predates per-chain poll health. */
  const preRolloutBody = (blockLag: unknown[]) => ({
    monitor: {
      blockLag,
      poll: {
        cycles: 100,
        errors: 1,
        skippedTicks: 3,
        duration: null,
      },
    },
    dependencies: [],
  })

  const respondWith = (body: unknown) => {
    fetchMock.mockResolvedValue({
      ok: true,
      json: () => Promise.resolve(body),
    } as Response)
  }

  it("reads a pre-rollout poll object as the primary chain's report", async () => {
    respondWith(preRolloutBody([lagSeries('base'), lagSeries('ethereum')]))

    const report = await fetchInfraReport()

    expect(report.monitor.poll).toEqual([
      {
        chain: 'base',
        cycles: 100,
        errors: 1,
        skippedTicks: 3,
        duration: null,
      },
    ])
  })

  it('reads a pre-rollout report with no chain to attribute the poll to', async () => {
    respondWith(preRolloutBody([]))

    const report = await fetchInfraReport()

    expect(report.monitor.poll).toEqual([])
  })

  it('passes a per-chain poll list through untouched', async () => {
    const poll = [
      {
        chain: 'ethereum',
        cycles: 7,
        errors: 0,
        skippedTicks: 2,
        duration: null,
      },
    ]
    respondWith({
      monitor: {
        blockLag: [lagSeries('base'), lagSeries('ethereum')],
        poll,
      },
      dependencies: [],
    })

    const report = await fetchInfraReport()

    expect(report.monitor.poll).toEqual(poll)
  })
})
