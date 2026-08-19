import { describe, expect, it } from 'vitest'

import type { EquityPriceStatus } from '$lib/api/EquityPriceStatus'

import {
  EquityPricePayloadError,
  availablePriceUsd,
  equityExposureUsd,
  formatExposureUsd,
  parseEquityPrice,
  parseEquityPrices
} from './equity-price'

const makeAvailable = (): Extract<EquityPriceStatus, { status: 'available' }> => ({
  status: 'available',
  priceUsd: '187.25',
  observedAt: '2099-08-19T10:00:00Z',
  expiresAt: '2099-08-19T10:00:30Z'
})

describe('dashboard equity prices', () => {
  it('uses a live available price without converting through Number', () => {
    const available = makeAvailable()

    expect(availablePriceUsd(available)).toBe('187.25')
    expect(equityExposureUsd('1.234567890123456789', available)).toBe('231.17283742561728374')
  })

  it('keeps unavailable exposure unknown instead of fabricating zero', () => {
    expect(equityExposureUsd('8', { status: 'unavailable' })).toBeNull()
    expect(formatExposureUsd(null)).toBe('—')
  })

  it('keeps exposure unavailable when inventory shares are malformed', () => {
    expect(equityExposureUsd('not-a-number', makeAvailable())).toBeNull()
  })

  it('distinguishes a known zero from an unavailable value', () => {
    expect(formatExposureUsd(equityExposureUsd('0', makeAvailable()))).toBe('$0')
  })

  it('treats an elapsed available price as unavailable', () => {
    const available = makeAvailable()

    expect(availablePriceUsd(available, Date.parse(available.expiresAt))).toBeNull()
  })

  it.each([
    [
      'non-decimal price',
      {
        ...makeAvailable(),
        priceUsd: 'not-a-number'
      },
      'status.priceUsd'
    ],
    [
      'non-positive price',
      {
        ...makeAvailable(),
        priceUsd: '0'
      },
      'status.priceUsd'
    ],
    [
      'invalid timestamp',
      {
        ...makeAvailable(),
        observedAt: 'not-a-time'
      },
      'status.observedAt'
    ],
    [
      'local timestamp',
      {
        ...makeAvailable(),
        observedAt: '2099-08-19T10:00:00'
      },
      'status.observedAt'
    ],
    [
      'non-increasing expiry',
      {
        ...makeAvailable(),
        expiresAt: '2099-08-19T10:00:00Z'
      },
      'status.expiresAt'
    ]
  ])('rejects an available price with %s', (_case, status, path) => {
    const result = parseEquityPrice({
      symbol: 'AAPL',
      status
    })

    expect(result.tag).toBe('err')
    if (result.tag === 'err') {
      expect(result.error).toBeInstanceOf(EquityPricePayloadError)
      expect(result.error.message).toContain(`equityPrice.${path}`)
    }
  })

  it.each([
    ['available', makeAvailable()],
    ['unavailable', { status: 'unavailable' } as const]
  ])('accepts a valid %s price', (_case, status) => {
    const result = parseEquityPrice({
      symbol: 'AAPL',
      status
    })

    expect(result.tag).toBe('ok')
    if (result.tag === 'ok') {
      expect(result.value).toEqual({
        symbol: 'AAPL',
        status
      })
    }
  })

  it.each([
    ['non-object payload', 'not-an-object', 'equityPrice'],
    ['missing symbol', { status: { status: 'unavailable' } }, 'equityPrice.symbol'],
    [
      'blank symbol',
      {
        symbol: ' ',
        status: { status: 'unavailable' }
      },
      'equityPrice.symbol'
    ],
    [
      'unknown status',
      {
        symbol: 'AAPL',
        status: { status: 'pending' }
      },
      'equityPrice.status.status'
    ]
  ])('rejects %s', (_case, payload, path) => {
    const result = parseEquityPrice(payload)

    expect(result.tag).toBe('err')
    if (result.tag === 'err') expect(result.error.message).toContain(path)
  })

  it('rejects a non-array snapshot', () => {
    const result = parseEquityPrices({ symbol: 'AAPL' })

    expect(result.tag).toBe('err')
    if (result.tag === 'err') expect(result.error.message).toContain('equityPrices')
  })

  it('rejects duplicate symbols in a snapshot', () => {
    const result = parseEquityPrices([
      {
        symbol: 'AAPL',
        status: { status: 'unavailable' }
      },
      {
        symbol: 'AAPL',
        status: makeAvailable()
      }
    ])

    expect(result.tag).toBe('err')
    if (result.tag === 'err') expect(result.error.message).toContain('a unique symbol')
  })
})
