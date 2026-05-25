import { describe, expect, it } from 'vitest'
import { statusStyle, isTxHash, extractTimestamp, detailFields, apiErrorStatus } from './transfer'

describe('statusStyle', () => {
  it('matches completed substring case-insensitively', () => {
    expect(statusStyle('completed').text).toBe('text-green-500')
    expect(statusStyle('COMPLETED').text).toBe('text-green-500')
  })

  it('matches PascalCase event names via substring', () => {
    expect(statusStyle('MintCompleted').text).toBe('text-green-500')
    expect(statusStyle('TransferFailed').text).toBe('text-destructive')
  })

  it('green branch takes priority over red when both substrings match', () => {
    // "completed" check runs before "failed" -- a status containing both
    // should hit green first. This tests the if-chain ordering.
    const style = statusStyle('completed_after_failed')
    expect(style.text).toBe('text-green-500')
  })

  it('falls through to muted for unrecognized statuses', () => {
    expect(statusStyle('pending').text).toBe('text-muted-foreground')
    expect(statusStyle('').text).toBe('text-muted-foreground')
  })
})

describe('isTxHash', () => {
  it('accepts valid 66-char hex string', () => {
    expect(isTxHash('0x' + 'a1B2'.repeat(16))).toBe(true)
  })

  it('rejects without 0x prefix', () => {
    expect(isTxHash('a'.repeat(64))).toBe(false)
  })

  it('rejects wrong length', () => {
    expect(isTxHash('0x' + 'a'.repeat(63))).toBe(false)
    expect(isTxHash('0x' + 'a'.repeat(65))).toBe(false)
  })

  it('rejects non-hex characters', () => {
    expect(isTxHash('0x' + 'g'.repeat(64))).toBe(false)
  })

  it('rejects non-string types', () => {
    expect(isTxHash(42)).toBe(false)
    expect(isTxHash(null)).toBe(false)
    expect(isTxHash(undefined)).toBe(false)
  })
})

describe('extractTimestamp', () => {
  it('returns the first _at field it encounters', () => {
    // Object.entries order matters -- first _at field wins
    const result = extractTimestamp({
      submitted_at: '2024-01-01T00:00:00Z',
      confirmed_at: '2024-01-02T00:00:00Z'
    })
    expect(result).toBe('2024-01-01T00:00:00Z')
  })

  it('skips _at fields with non-string values', () => {
    expect(
      extractTimestamp({
        created_at: 1234567890,
        confirmed_at: '2024-06-15T00:00:00Z'
      })
    ).toBe('2024-06-15T00:00:00Z')
  })

  it('returns null when no _at fields exist', () => {
    expect(extractTimestamp({ amount: '100', status: 'ok' })).toBeNull()
  })

  it('returns null on empty payload', () => {
    expect(extractTimestamp({})).toBeNull()
  })
})

describe('detailFields', () => {
  it('filters out timestamp fields and attestation', () => {
    const result = detailFields({
      amount: '100',
      created_at: '2024-01-01T00:00:00Z',
      attestation: 'long-blob',
      tx_hash: '0xabc',
      confirmed_at: '2024-01-02T00:00:00Z'
    })
    expect(result).toEqual([
      ['amount', '100'],
      ['tx_hash', '0xabc']
    ])
  })

  it('preserves field order from the original object', () => {
    const result = detailFields({ z_field: '1', a_field: '2' })
    expect(result).toEqual([
      ['z_field', '1'],
      ['a_field', '2']
    ])
  })

  it('returns empty when all fields are filtered', () => {
    expect(
      detailFields({
        created_at: '2024-01-01',
        attestation: 'x',
        submitted_at: 'y'
      })
    ).toEqual([])
  })
})

describe('apiErrorStatus', () => {
  it('reads status_code from the nested ApiError payload', () => {
    expect(apiErrorStatus({ ApiError: { status_code: 404 } })).toBe('404')
  })

  it('stringifies a numeric status_code', () => {
    expect(apiErrorStatus({ ApiError: { status_code: 500 } })).toBe('500')
  })

  it('passes through a string status_code', () => {
    expect(apiErrorStatus({ ApiError: { status_code: '403' } })).toBe('403')
  })

  it('returns null when status_code is absent (None)', () => {
    expect(apiErrorStatus({ ApiError: { status_code: null } })).toBeNull()
    expect(apiErrorStatus({ ApiError: {} })).toBeNull()
  })

  it('returns null when the ApiError payload is missing', () => {
    expect(apiErrorStatus({ Timeout: null })).toBeNull()
    expect(apiErrorStatus({ status_code: 404 })).toBeNull()
  })

  it('returns null for non-object input', () => {
    expect(apiErrorStatus(null)).toBeNull()
    expect(apiErrorStatus('ApiError')).toBeNull()
  })
})
