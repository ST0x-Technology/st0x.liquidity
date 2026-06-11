import { describe, expect, it } from 'vitest'
import {
  kindLabel,
  transferTypeLabel,
  statusStyle,
  isTxHash,
  formatNumericDetailValue,
  extractTimestamp,
  detailFields,
  apiErrorStatus,
  stuckLocationLabel,
  stuckReasonLabel,
  recoveryCommand,
} from './transfer'

describe('transferTypeLabel', () => {
  it('spells out the destination venue for each bridge direction', () => {
    expect(transferTypeLabel({ kind: 'usdc_bridge', direction: 'alpaca_to_base' })).toBe(
      'Alpaca → Raindex'
    )
    expect(transferTypeLabel({ kind: 'usdc_bridge', direction: 'base_to_alpaca' })).toBe(
      'Raindex → Alpaca'
    )
  })

  it('falls back to the bare kind label for equity transfers', () => {
    expect(transferTypeLabel({ kind: 'equity_mint' })).toBe('Mint')
    expect(transferTypeLabel({ kind: 'equity_redemption' })).toBe('Redeem')
  })

  it('falls back to "USDC Bridge" when a bridge has no direction', () => {
    // The DTO always carries a direction; this guards the optional-field path
    // so a missing direction degrades to the generic label rather than blank.
    expect(transferTypeLabel({ kind: 'usdc_bridge' })).toBe(kindLabel('usdc_bridge'))
  })
})

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

describe('formatNumericDetailValue', () => {
  it('formats raw wrapped token units as shares', () => {
    expect(formatNumericDetailValue('wrapped_amount', '31688483870000000000')).toBe('31.688')
  })

  it('formats raw received token units as shares', () => {
    expect(formatNumericDetailValue('shares_minted', '12500000000000000000')).toBe('12.500')
  })

  it('formats decimal-serialized token addresses as hex addresses', () => {
    expect(
      formatNumericDetailValue('token', '7973173272142053871140891859049224849605192591')
    ).toBe('0x0165878a594ca255338adfa4d48449f69242eb8f')
  })

  it('formats wallet address fields as hex, not comma-separated decimals', () => {
    // wallet/redemption_wallet are Address fields; without address handling they
    // fall through to formatDecimal and render as a giant comma-separated number.
    expect(formatNumericDetailValue('wallet', '0xd8da6bf26964af9d7eed9e03e53415d37aa96045')).toBe(
      '0xd8da6bf26964af9d7eed9e03e53415d37aa96045'
    )
    expect(
      formatNumericDetailValue('redemption_wallet', '0xd8da6bf26964af9d7eed9e03e53415d37aa96045')
    ).toBe('0xd8da6bf26964af9d7eed9e03e53415d37aa96045')
  })

  it('keeps regular numeric fields as decimal values', () => {
    expect(formatNumericDetailValue('poll_count', '1200')).toBe('1,200.000')
  })

  it('formats hex-encoded U256 token units as shares', () => {
    // alloy serializes U256 as hex; 0xde0b6b3a7640000 == 1e18 == 1 share.
    expect(formatNumericDetailValue('shares_minted', '0xde0b6b3a7640000')).toBe('1.000')
    expect(formatNumericDetailValue('wrapped_amount', '0x1bc16d674ec80000')).toBe('2.000')
  })

  it('does not throw on a small hex token amount (regression: frozen modal)', () => {
    // The exact value from the production crash report. decimal.js rejected the
    // "0x" prefix, throwing during render and stranding the modal on its spinner.
    expect(formatNumericDetailValue('shares_minted', '0x8ac7230489e8')).toBe('0.000')
  })

  it('normalizes hex for non-token numeric fields', () => {
    expect(formatNumericDetailValue('poll_count', '0x10')).toBe('16.000')
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

describe('stuckLocationLabel', () => {
  it('maps known location codes to human labels', () => {
    expect(stuckLocationLabel('issuer')).toBe('Issuer')
    expect(stuckLocationLabel('redemption_wallet')).toBe('Redemption wallet')
    expect(stuckLocationLabel('bot_wallet_unwrapped')).toBe('Bot wallet')
    expect(stuckLocationLabel('bot_wallet_wrapped')).toBe('Bot wallet (wrapped)')
  })

  it('passes through unknown codes unchanged', () => {
    expect(stuckLocationLabel('somewhere_else')).toBe('somewhere_else')
  })
})

describe('stuckReasonLabel', () => {
  it('title-cases snake_case reasons', () => {
    expect(stuckReasonLabel('redemption_rejected')).toBe('Redemption Rejected')
    expect(stuckReasonLabel('timeout')).toBe('Timeout')
  })
})

describe('recoveryCommand', () => {
  it('builds the mock recheck command for an equity mint in a simulation build', () => {
    const command = recoveryCommand({
      simulateSourceId: 'sim-1',
      backendPort: '8123',
      kind: 'equity_mint',
      id: 'ISS001',
    })
    expect(command).toEqual({
      mode: 'simulation',
      command:
        'nix develop --command cargo run --features mock --bin cli -- --config /tmp/st0x-simulate-failures-8123.config.toml --secrets /tmp/st0x-simulate-failures-8123.secrets.toml transfer recheck --kind mint --id ISS001',
    })
  })

  it('maps equity_redemption to the redemption transfer type', () => {
    const command = recoveryCommand({
      simulateSourceId: 'sim-1',
      backendPort: '8123',
      kind: 'equity_redemption',
      id: 'RED001',
    })
    expect(command?.command).toContain('transfer recheck --kind redemption --id RED001')
  })

  it('builds the stox command for an equity mint on a live deployment', () => {
    const command = recoveryCommand({
      simulateSourceId: null,
      backendPort: '8123',
      kind: 'equity_mint',
      id: 'ISS001',
    })
    expect(command).toEqual({
      mode: 'production',
      command: 'stox transfer recheck --kind mint --id ISS001',
    })
  })

  it('builds the stox command for an equity redemption on a live deployment', () => {
    const command = recoveryCommand({
      simulateSourceId: null,
      backendPort: null,
      kind: 'equity_redemption',
      id: 'RED001',
    })
    expect(command).toEqual({
      mode: 'production',
      command: 'stox transfer recheck --kind redemption --id RED001',
    })
  })

  it('returns null in a simulation build when the backend port is unknown', () => {
    expect(
      recoveryCommand({
        simulateSourceId: 'sim-1',
        backendPort: null,
        kind: 'equity_mint',
        id: 'ISS001',
      }),
    ).toBeNull()
  })

  it('returns null for non-equity transfer kinds in a simulation build', () => {
    expect(
      recoveryCommand({
        simulateSourceId: 'sim-1',
        backendPort: '8123',
        kind: 'usdc_bridge',
        id: 'BRIDGE001',
      }),
    ).toBeNull()
  })

  it('returns null for non-equity transfer kinds on a live deployment', () => {
    expect(
      recoveryCommand({
        simulateSourceId: null,
        backendPort: null,
        kind: 'usdc_bridge',
        id: 'BRIDGE001',
      }),
    ).toBeNull()
  })
})
