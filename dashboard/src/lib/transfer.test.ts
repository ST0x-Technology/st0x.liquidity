import { describe, expect, it } from 'vitest'
import {
  statusStyle,
  isTxHash,
  formatNumericDetailValue,
  extractTimestamp,
  detailFields,
  apiErrorStatus,
  stuckLocationLabel,
  stuckReasonLabel,
  transferRecoveryCommands,
  tradeRecoveryCommands,
  recoveryModeLabel,
  RECOVERY_GUIDE
} from './transfer'

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

const PROD = { simulateSourceId: null, backendPort: null }
const SIM = { simulateSourceId: 'sim-1', backendPort: '8123' }
const SIM_PREFIX =
  'nix develop --command cargo run --features mock --bin cli -- --config /tmp/st0x-simulate-failures-8123.config.toml --secrets /tmp/st0x-simulate-failures-8123.secrets.toml'

const commandFor = (commands: { label: string; command: string }[], label: string): string => {
  const found = commands.find((entry) => entry.label === label)
  if (!found) throw new Error(`no command labeled ${label}`)
  return found.command
}

describe('transferRecoveryCommands', () => {
  it('shows recheck/resume/fail for an in-flight (stuck, non-terminal) equity mint', () => {
    const commands = transferRecoveryCommands({
      deployment: PROD,
      kind: 'equity_mint',
      id: 'ISS001',
      status: 'wrapping'
    })
    expect(commands.map((entry) => entry.label)).toEqual(['Recheck', 'Resume (all equity)', 'Fail'])
    expect(commandFor(commands, 'Recheck')).toBe('stox transfer recheck --kind mint --id ISS001')
    expect(commandFor(commands, 'Resume (all equity)')).toBe('stox transfer resume --kind equity')
    expect(commandFor(commands, 'Fail')).toBe(
      'stox transfer fail --kind mint --id ISS001 -r "<reason>"'
    )
  })

  it('shows recheck + reconcile (not resume/fail) for a failed equity mint', () => {
    const commands = transferRecoveryCommands({
      deployment: PROD,
      kind: 'equity_mint',
      id: 'ISS001',
      status: 'failed'
    })
    expect(commands.map((entry) => entry.label)).toEqual(['Recheck', 'Reconcile'])
    expect(commandFor(commands, 'Reconcile')).toBe(
      'stox transfer reconcile --kind mint --id ISS001 -r "<reason>"'
    )
  })

  it('maps an in-flight equity redemption to the redemption kind', () => {
    const commands = transferRecoveryCommands({
      deployment: PROD,
      kind: 'equity_redemption',
      id: 'RED001',
      status: 'sending'
    })
    expect(commandFor(commands, 'Recheck')).toBe(
      'stox transfer recheck --kind redemption --id RED001'
    )
    expect(commandFor(commands, 'Fail')).toBe(
      'stox transfer fail --kind redemption --id RED001 -r "<reason>"'
    )
  })

  it('marks recheck as requires-bot and fail/reconcile as direct-db', () => {
    const commands = transferRecoveryCommands({
      deployment: PROD,
      kind: 'equity_mint',
      id: 'ISS001',
      status: 'failed'
    })
    const modeFor = (label: string) => commands.find((entry) => entry.label === label)?.mode
    expect(modeFor('Recheck')).toBe('requires-bot')
    expect(modeFor('Reconcile')).toBe('direct-db')
  })

  it('shows only resume for an in-flight usdc bridge, placeholder direction when unknown', () => {
    const commands = transferRecoveryCommands({
      deployment: PROD,
      kind: 'usdc_bridge',
      id: 'BRIDGE001',
      status: 'bridging'
    })
    expect(commands.map((entry) => entry.label)).toEqual(['Resume'])
    expect(commandFor(commands, 'Resume')).toBe(
      'stox transfer resume --kind usdc --id BRIDGE001 --direction <to-raindex|to-alpaca>'
    )
    expect(commands.find((entry) => entry.label === 'Resume')?.mode).toBe('direct-db-live-rpc')
  })

  it('translates the DTO direction to the CLI flag vocabulary on the usdc resume command', () => {
    // The DTO names the venue flow (alpaca_to_base) while the CLI names the
    // Raindex-relative leg (to-raindex); the command must carry the CLI value.
    const toRaindex = transferRecoveryCommands({
      deployment: PROD,
      kind: 'usdc_bridge',
      id: 'BRIDGE001',
      status: 'bridging',
      direction: 'alpaca_to_base'
    })
    expect(commandFor(toRaindex, 'Resume')).toBe(
      'stox transfer resume --kind usdc --id BRIDGE001 --direction to-raindex'
    )

    const toAlpaca = transferRecoveryCommands({
      deployment: PROD,
      kind: 'usdc_bridge',
      id: 'BRIDGE002',
      status: 'bridging',
      direction: 'base_to_alpaca'
    })
    expect(commandFor(toAlpaca, 'Resume')).toBe(
      'stox transfer resume --kind usdc --id BRIDGE002 --direction to-alpaca'
    )
  })

  it('shows reconcile for a post-burn failed usdc bridge (any casing)', () => {
    // The CLI accepts `transfer reconcile --kind usdc` only for post-burn
    // failures, which the postBurn discriminator marks true.
    for (const status of ['failed', 'Failed', 'FAILED']) {
      const commands = transferRecoveryCommands({
        deployment: PROD,
        kind: 'usdc_bridge',
        id: 'BRIDGE001',
        status,
        postBurn: true
      })
      expect(commands.map((entry) => entry.label)).toEqual(['Reconcile'])
      expect(commandFor(commands, 'Reconcile')).toBe(
        'stox transfer reconcile --kind usdc --id BRIDGE001 -r "<reason>"'
      )
      expect(commands.find((entry) => entry.label === 'Reconcile')?.mode).toBe('direct-db')
    }
  })

  it('shows no per-object command for a pre-burn failed usdc bridge', () => {
    // A pre-burn failure strands nothing on-chain, so the CLI rejects reconcile;
    // postBurn=false must surface nothing rather than a false affordance.
    for (const postBurn of [false, null]) {
      expect(
        transferRecoveryCommands({
          deployment: PROD,
          kind: 'usdc_bridge',
          id: 'BRIDGE001',
          status: 'failed',
          postBurn
        })
      ).toEqual([])
    }
  })

  it('shows no per-object command for a failed usdc bridge with no discriminator', () => {
    // An absent postBurn (older payloads / non-failed-shaped status) is treated
    // as not-post-burn -- the reconcile affordance requires an explicit true.
    expect(
      transferRecoveryCommands({
        deployment: PROD,
        kind: 'usdc_bridge',
        id: 'BRIDGE001',
        status: 'failed'
      })
    ).toEqual([])
  })

  it('treats the failed status case-insensitively for equity (recheck + reconcile)', () => {
    const commands = transferRecoveryCommands({
      deployment: PROD,
      kind: 'equity_mint',
      id: 'ISS001',
      status: 'Failed'
    })
    expect(commands.map((entry) => entry.label)).toEqual(['Recheck', 'Reconcile'])
    expect(commandFor(commands, 'Reconcile')).toBe(
      'stox transfer reconcile --kind mint --id ISS001 -r "<reason>"'
    )
  })

  it('returns no commands for a completed transfer (any casing)', () => {
    for (const status of ['completed', 'Completed', 'COMPLETED']) {
      expect(
        transferRecoveryCommands({
          deployment: PROD,
          kind: 'equity_mint',
          id: 'ISS001',
          status
        })
      ).toEqual([])
    }
  })

  it('uses the mock cli prefix in a simulation build', () => {
    const commands = transferRecoveryCommands({
      deployment: SIM,
      kind: 'equity_mint',
      id: 'ISS001',
      status: 'wrapping'
    })
    expect(commandFor(commands, 'Recheck')).toBe(
      `${SIM_PREFIX} transfer recheck --kind mint --id ISS001`
    )
  })

  it('returns no commands in a simulation build with an unknown backend port', () => {
    expect(
      transferRecoveryCommands({
        deployment: { simulateSourceId: 'sim-1', backendPort: null },
        kind: 'equity_mint',
        id: 'ISS001',
        status: 'wrapping'
      })
    ).toEqual([])
  })
})

describe('tradeRecoveryCommands', () => {
  it('includes process-tx with the tx hash when known', () => {
    const txHash = '0x' + 'a'.repeat(64)
    const commands = tradeRecoveryCommands({
      deployment: PROD,
      symbol: 'MSTR',
      txHash
    })
    expect(commands.map((entry) => entry.label)).toEqual([
      'Process tx',
      'Release hedge',
      'Set position',
      'Rebuild view'
    ])
    expect(commandFor(commands, 'Process tx')).toBe(`stox process-tx --tx-hash ${txHash}`)
    expect(commandFor(commands, 'Release hedge')).toBe(
      'stox position release-hedge -s MSTR -o <order-id> -r "<reason>"'
    )
    expect(commandFor(commands, 'Rebuild view')).toBe('stox view rebuild -a position --id MSTR')
  })

  it('marks process-tx as a CLI-local op (direct-db-live-rpc), not requires-bot', () => {
    // process-tx is a CLI-local ProviderCommand: it records the fill and places
    // the hedge from the CLI's own RPC + broker, so it must NOT be labelled
    // requires-bot (which would invert the stop-the-bot race warning).
    const commands = tradeRecoveryCommands({
      deployment: PROD,
      symbol: 'MSTR',
      txHash: '0x' + 'a'.repeat(64)
    })
    expect(commands.find((entry) => entry.label === 'Process tx')?.mode).toBe('direct-db-live-rpc')
  })

  it('omits process-tx when there is no tx hash', () => {
    const commands = tradeRecoveryCommands({
      deployment: PROD,
      symbol: 'MSTR',
      txHash: null
    })
    expect(commands.map((entry) => entry.label)).toEqual([
      'Release hedge',
      'Set position',
      'Rebuild view'
    ])
  })

  it('uses the mock cli prefix in a simulation build', () => {
    const commands = tradeRecoveryCommands({
      deployment: SIM,
      symbol: 'MSTR',
      txHash: null
    })
    expect(commandFor(commands, 'Set position')).toBe(
      `${SIM_PREFIX} position set -s MSTR (--zero | --long <N> | --short <N>) [--price <USDC_PER_SHARE>] -r "<reason>"`
    )
  })

  it('returns no commands for a trade in a simulation build with an unknown backend port', () => {
    const commands = tradeRecoveryCommands({
      deployment: { simulateSourceId: 'sim-1', backendPort: null },
      symbol: 'MSTR',
      txHash: null
    })
    expect(commands).toEqual([])
  })
})

describe('RECOVERY_GUIDE', () => {
  it('groups commands by object and uses the unified verb names', () => {
    expect(RECOVERY_GUIDE.map((group) => group.object)).toEqual([
      'transfer',
      'position',
      'view',
      'cctp',
      'trade'
    ])

    const allCommands = RECOVERY_GUIDE.flatMap((group) => group.commands)
    expect(allCommands.some((entry) => entry.command.startsWith('stox transfer recheck'))).toBe(
      true
    )
    expect(allCommands.some((entry) => entry.command.startsWith('stox cctp complete-mint'))).toBe(
      true
    )
    expect(allCommands.every((entry) => !entry.command.includes('recheck-transfer'))).toBe(true)
  })

  it('labels each guide command with the SPEC execution mode', () => {
    const modeOf = (prefix: string): string | undefined =>
      RECOVERY_GUIDE.flatMap((group) => group.commands).find((entry) =>
        entry.command.startsWith(prefix)
      )?.mode

    // requires-bot: only recheck and equity resume dispatch through the bot.
    expect(modeOf('stox transfer recheck')).toBe('requires-bot')
    expect(modeOf('stox transfer resume --kind equity')).toBe('requires-bot')
    // live-rpc-only: cctp touches no DB state.
    expect(modeOf('stox cctp complete-mint')).toBe('live-rpc-only')
    // direct-db-live-rpc: usdc resume and process-tx drive on-chain from the CLI.
    expect(modeOf('stox transfer resume --kind usdc')).toBe('direct-db-live-rpc')
    expect(modeOf('stox process-tx')).toBe('direct-db-live-rpc')
    // direct-db: pure local CQRS mutations.
    expect(modeOf('stox transfer fail')).toBe('direct-db')
    expect(modeOf('stox transfer reconcile')).toBe('direct-db')
    expect(modeOf('stox position release-hedge')).toBe('direct-db')
    expect(modeOf('stox position set')).toBe('direct-db')
    expect(modeOf('stox view rebuild')).toBe('direct-db')
  })
})

describe('recoveryModeLabel', () => {
  it('renders a distinct badge for each of the four execution modes', () => {
    expect(recoveryModeLabel('requires-bot')).toBe('REST — requires the running bot')
    expect(recoveryModeLabel('live-rpc-only')).toBe(
      'live RPC — ensure the bot is not driving this same on-chain action'
    )
    expect(recoveryModeLabel('direct-db-live-rpc')).toBe(
      'direct DB + live RPC — stop the bot / ensure it is not driving this id'
    )
    expect(recoveryModeLabel('direct-db')).toBe(
      'direct DB — stop the bot / ensure it is not driving this id'
    )
  })
})
