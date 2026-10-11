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
  transferRecoveryCommands,
  tradeRecoveryCommands,
  recoveryModeLabel,
  recoveryModeColor,
  transferWarningText,
  forClientEnv,
  RECOVERY_GUIDE
} from './transfer'

describe('transferWarningText', () => {
  it('identifies every lifecycle failure by transfer category and id', () => {
    expect(transferWarningText({ kind: 'mint_lifecycle_failed', id: 'mint-1' })).toBe(
      'Mint mint-1 has an invalid lifecycle.'
    )
    expect(transferWarningText({ kind: 'redemption_lifecycle_failed', id: 'redemption-1' })).toBe(
      'Redemption redemption-1 has an invalid lifecycle.'
    )
    expect(transferWarningText({ kind: 'bridge_lifecycle_failed', id: 'bridge-1' })).toBe(
      'USDC bridge bridge-1 has an invalid lifecycle.'
    )
  })
})

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

  it('returns amber for reconciled status (case-insensitive)', () => {
    expect(statusStyle('reconciled').text).toBe('text-amber-500')
    expect(statusStyle('reconciled').dot).toBe('bg-amber-500')
    expect(statusStyle('RECONCILED').text).toBe('text-amber-500')
    expect(statusStyle('Reconciled').text).toBe('text-amber-500')
  })

  it('returns amber for OperatorReconciled event name (timeline side-effect)', () => {
    // OperatorReconciled is a raw PascalCase event name that contains 'reconciled';
    // the substring check intentionally renders it amber in the event timeline.
    expect(statusStyle('OperatorReconciled').text).toBe('text-amber-500')
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

const PROD = { simulateSourceId: null, backendPort: null, clientEnv: 'production' } as const
const STAGING = { simulateSourceId: null, backendPort: null, clientEnv: 'staging' } as const
const UNKNOWN_HOST = { simulateSourceId: null, backendPort: null, clientEnv: null } as const
const SIM = { simulateSourceId: 'sim-1', backendPort: '8123', clientEnv: 'production' } as const
const CLIENT = 'st0x-liquidity-client --env production'
const SIM_PREFIX =
  'nix develop --command cargo run -p st0x-cli --features mock -- --config /tmp/st0x-simulate-failures-8123.config.toml --secrets /tmp/st0x-simulate-failures-8123.secrets.toml'

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
    expect(commandFor(commands, 'Recheck')).toBe(`${CLIENT} debug recheck mint ISS001`)
    expect(commandFor(commands, 'Resume (all equity)')).toBe(`${CLIENT} debug resume`)
    expect(commandFor(commands, 'Fail')).toBe(
      `${CLIENT} debug fail-equity-transfer mint ISS001 --reason "<reason>"`
    )
  })

  it('says a reconciled redemption stays in flight until the next restart', () => {
    const reconcileText = (kind: 'equity_mint' | 'equity_redemption') =>
      transferRecoveryCommands({ deployment: PROD, kind, id: 'X1', status: 'failed' }).find(
        (entry) => entry.label === 'Reconcile'
      )?.description ?? ''
    // Startup seeds a DetectionFailed or RedemptionRejected redemption's
    // stranded amount into inflight; reconcile clears it on the next start.
    expect(
      reconcileText('equity_redemption').includes(
        'If the bot started after this redemption failed at detection or was rejected'
      )
    ).toBe(true)
    expect(reconcileText('equity_mint').includes('stays in flight')).toBe(false)
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
      `${CLIENT} debug reconcile-equity mint ISS001 --reason "<reason>"`
    )
  })

  it('maps an in-flight equity redemption to the redemption kind', () => {
    const commands = transferRecoveryCommands({
      deployment: PROD,
      kind: 'equity_redemption',
      id: 'RED001',
      status: 'sending'
    })
    expect(commandFor(commands, 'Recheck')).toBe(`${CLIENT} debug recheck redemption RED001`)
    expect(commandFor(commands, 'Fail')).toBe(
      `${CLIENT} debug fail-equity-transfer redemption RED001 --reason "<reason>"`
    )
  })

  it('targets the deployment environment with the client', () => {
    const transfer = transferRecoveryCommands({
      deployment: STAGING,
      kind: 'equity_mint',
      id: 'ISS001',
      status: 'wrapping'
    })
    const trade = tradeRecoveryCommands({ deployment: STAGING, symbol: 'MSTR' })
    expect(commandFor(transfer, 'Recheck')).toBe(
      'st0x-liquidity-client --env staging debug recheck mint ISS001'
    )
    expect(commandFor(trade, 'Rebuild view')).toBe(
      'st0x-liquidity-client --env staging debug view rebuild position --id MSTR'
    )
  })

  it('shows an environment placeholder when the host is unknown', () => {
    const trade = tradeRecoveryCommands({ deployment: UNKNOWN_HOST, symbol: 'MSTR' })
    expect(commandFor(trade, 'Rebuild view')).toBe(
      'st0x-liquidity-client --env <production|staging> debug view rebuild position --id MSTR'
    )
  })

  it('marks every production command requires-bot, since the client calls the bot', () => {
    const commands = [
      ...transferRecoveryCommands({
        deployment: PROD,
        kind: 'equity_mint',
        id: 'ISS001',
        status: 'wrapping'
      }),
      ...transferRecoveryCommands({
        deployment: PROD,
        kind: 'equity_mint',
        id: 'ISS001',
        status: 'failed'
      }),
      ...tradeRecoveryCommands({ deployment: PROD, symbol: 'MSTR' })
    ]
    expect(commands.every((entry) => entry.mode === 'requires-bot')).toBe(true)
  })

  it('marks the mock cli fail and usdc resume as requires-bot and reconcile as direct-db', () => {
    // The stox CLI's equity `transfer fail` and `transfer resume --kind usdc`
    // POST to the running bot, so stopping the bot first would break them.
    const modeFor = (commands: { label: string; mode: string }[], label: string) =>
      commands.find((entry) => entry.label === label)?.mode
    const inFlight = transferRecoveryCommands({
      deployment: SIM,
      kind: 'equity_mint',
      id: 'ISS001',
      status: 'wrapping'
    })
    const failed = transferRecoveryCommands({
      deployment: SIM,
      kind: 'equity_mint',
      id: 'ISS001',
      status: 'failed'
    })
    const bridging = transferRecoveryCommands({
      deployment: SIM,
      kind: 'usdc_bridge',
      id: 'BRIDGE001',
      status: 'bridging'
    })
    expect(modeFor(inFlight, 'Fail')).toBe('requires-bot')
    expect(modeFor(failed, 'Reconcile')).toBe('direct-db')
    expect(modeFor(bridging, 'Resume')).toBe('requires-bot')
  })

  it('fills a placeholder direction on resume when the bridge direction is unknown', () => {
    const commands = transferRecoveryCommands({
      deployment: PROD,
      kind: 'usdc_bridge',
      id: 'BRIDGE001',
      status: 'bridging'
    })
    expect(commands.map((entry) => entry.label)).toEqual(['Resume', 'Fail (pre-burn)'])
    expect(commandFor(commands, 'Resume')).toBe(
      `${CLIENT} debug resume-usdc <alpaca-to-base|base-to-alpaca> BRIDGE001`
    )
    expect(commands.find((entry) => entry.label === 'Resume')?.mode).toBe('requires-bot')
  })

  it('offers resume for the depositing (post-burn) usdc bridge status', () => {
    const commands = transferRecoveryCommands({
      deployment: PROD,
      kind: 'usdc_bridge',
      id: 'BRIDGE001',
      status: 'depositing'
    })
    expect(commands.map((entry) => entry.label)).toEqual(['Resume'])
  })

  it('offers only resume for a converting usdc bridge', () => {
    // converting comes after the deposit, where the bot always refuses
    // fail-usdc-transfer, so resume is the only command that can work.
    const commands = transferRecoveryCommands({
      deployment: PROD,
      kind: 'usdc_bridge',
      id: 'BRIDGE001',
      status: 'converting',
      direction: 'base_to_alpaca'
    })
    expect(commands.map((entry) => entry.label)).toEqual(['Resume'])
    expect(commandFor(commands, 'Resume')).toBe(
      `${CLIENT} debug resume-usdc base-to-alpaca BRIDGE001`
    )
    // Base to Alpaca resumes a conversion from its stored order.
    expect(commands[0]?.description.includes('cannot re-drive')).toBe(false)
  })

  it('gives both outcomes of an Alpaca to Base converting resume', () => {
    // The status covers Converting, which resume_alpaca_to_base can only fail
    // (no stored broker order id), and ConversionComplete, which it continues
    // with the withdrawal. Moving USDC by hand first would double the funds leg.
    const resumeText = (direction: 'alpaca_to_base' | null) =>
      transferRecoveryCommands({
        deployment: PROD,
        kind: 'usdc_bridge',
        id: 'BRIDGE001',
        status: 'converting',
        direction
      }).find((entry) => entry.label === 'Resume')?.description ?? ''
    for (const direction of ['alpaca_to_base', null] as const) {
      const text = resumeText(direction)
      expect(text.includes('this resume continues with the Alpaca withdrawal')).toBe(true)
      expect(text.includes('cannot re-drive the USD to USDC conversion')).toBe(true)
      expect(text.includes('fails the transfer (ConversionFailed) and releases the guard')).toBe(true)
      // After ConversionCancelNotSettled or ConversionOrderNotFound the order
      // can still fill, and the resume does not ask Alpaca, so check it first.
      const check = text.indexOf('no USD to USDC order for this transfer is still open')
      expect(check).toBeGreaterThan(-1)
      expect(check).toBeLessThan(text.indexOf('Do not move USDC by hand before you run the resume'))
      // Settling at Alpaca fits only a ConversionFailed end; a later failure
      // (say an Alpaca to Base BridgingFailed) has funds off Alpaca.
      expect(text.includes('If it ends in ConversionFailed, settle any converted USDC by hand at Alpaca')).toBe(true)
      expect(text.includes('reconcile-usdc does not apply')).toBe(true)
      expect(text.includes('A later failure has its own recovery')).toBe(true)
    }
  })

  const bridgeLabels = (status: string, direction: 'alpaca_to_base' | 'base_to_alpaca' | null) =>
    transferRecoveryCommands({ deployment: PROD, kind: 'usdc_bridge', id: 'BRIDGE001', status, direction })

  it('offers fail-usdc-transfer only where the running bot can accept it', () => {
    // Alpaca to Base: WithdrawalComplete (withdrawing) and a BridgingSubmitting
    // with no recorded burn (bridging). Base to Alpaca: an unrecorded
    // WithdrawalSubmitting (withdrawing) only.
    const labels = (status: string, direction: 'alpaca_to_base' | 'base_to_alpaca' | null) =>
      bridgeLabels(status, direction).map((entry) => entry.label)
    expect(labels('withdrawing', 'alpaca_to_base')).toEqual(['Resume', 'Fail (pre-burn)'])
    expect(labels('bridging', 'alpaca_to_base')).toEqual(['Resume', 'Fail (pre-burn)'])
    expect(labels('converting', 'alpaca_to_base')).toEqual(['Resume'])
    expect(labels('withdrawing', 'base_to_alpaca')).toEqual(['Resume', 'Fail (pre-burn)'])
    expect(labels('bridging', 'base_to_alpaca')).toEqual(['Resume'])
    expect(labels('depositing', 'base_to_alpaca')).toEqual(['Resume'])
    expect(labels('bridging', null)).toEqual(['Resume', 'Fail (pre-burn)'])
  })

  it('gives the pre-fail check that applies to the bridge direction', () => {
    const failText = (status: string, direction: 'alpaca_to_base' | 'base_to_alpaca' | null) =>
      bridgeLabels(status, direction).find((entry) => entry.label === 'Fail (pre-burn)')
        ?.description ?? ''
    expect(commandFor(bridgeLabels('withdrawing', 'alpaca_to_base'), 'Fail (pre-burn)')).toBe(
      `${CLIENT} debug fail-usdc-transfer BRIDGE001 --reason "<reason>"`
    )
    const toBase = failText('withdrawing', 'alpaca_to_base')
    expect(toBase.startsWith('Check the transfer first.')).toBe(true)
    // The checks and the nonce close live in one place, the runbook, so the
    // dialog gates the fail on them instead of keeping a second copy. The
    // Alpaca to Base burn runs on Ethereum, and a nonce is per chain.
    expect(
      toBase.includes(
        'do not fail the transfer until you have done every check and the nonce close on Ethereum in docs/cli-ops.md, "Clearing a pre-burn guard latch", with the bot stopped'
      )
    ).toBe(true)
    expect(toBase.includes('If a burn landed or you are not certain, run resume-usdc instead')).toBe(true)
    // A restart sends the burn of a completed withdrawal, so the fail is the
    // offline command, before the start.
    expect(
      toBase.includes('fail it with the offline stox fail-usdc-transfer before you start the bot again')
    ).toBe(true)
    expect(toBase.includes('the guard stays held until you settle them with reconcile-usdc')).toBe(true)
    expect(toBase.includes('OperatorWithdraw')).toBe(false)

    // A Base to Alpaca transfer's checks and nonce close run on Base.
    const toAlpaca = failText('withdrawing', 'base_to_alpaca')
    expect(toAlpaca.includes('only while the vault withdrawal is unrecorded')).toBe(true)
    // The runbook below this stack has no full withdrawal procedure, so the
    // dialog keeps it: both withdrawal events, the cash vault, the pending and
    // queued checks, the nonce close and the recheck.
    expect(toAlpaca.includes('OperatorWithdraw')).toBe(true)
    expect(toAlpaca.includes('no WithdrawV2 on the orderbook either')).toBe(true)
    expect(toAlpaca.includes('cash vault_id in the bot config')).toBe(true)
    expect(toAlpaca.includes('pending nonce equals latest nonce on more than one RPC provider')).toBe(true)
    // A queued send waits behind a nonce gap; only empty nonces are filled,
    // and only after the kept-send check.
    expect(toAlpaca.includes('fill each empty nonce below it')).toBe(true)
    const close = toAlpaca.indexOf('close the nonce before you fail the transfer')
    expect(close).toBeGreaterThan(-1)
    expect(toAlpaca.indexOf('check the withdrawal logs again')).toBeGreaterThan(close)
    expect(
      toAlpaca.includes(
        'If a withdrawal mined instead, a transaction is pending or queued, or you cannot tell, start the bot and run resume-usdc'
      )
    ).toBe(true)
    // Resume adopts only the full amount; anything else ends in
    // WithdrawalFailed with the USDC in the wallet.
    expect(toAlpaca.includes('any other amount ends the transfer in WithdrawalFailed')).toBe(true)
    // The unrecorded withdrawal can be failed through the live route too.
    expect(toAlpaca.includes('Otherwise run the offline stox fail-usdc-transfer, or this command after you restart the bot')).toBe(
      true
    )
    // Once recorded, the burn checks apply, with the runbook as the source.
    expect(toAlpaca.includes('"Clearing a pre-burn guard latch"')).toBe(true)
    // A late burn is finished with the bot stopped, for the amount the mint
    // printed.
    expect(toAlpaca.includes('Keep the bot stopped until that USDC is at Alpaca')).toBe(true)
    expect(toAlpaca.includes('divided by 1,000,000 (it prints USDC base units)')).toBe(true)
    // A burn found by the recheck of a confirmed withdrawal is resumed,
    // never failed.
    expect(
      toAlpaca.includes(
        'If a burn landed, something is pending or queued, or you are not certain, start the bot and run resume-usdc, and do not fail the transfer'
      )
    ).toBe(true)
    expect(toAlpaca.includes('on Ethereum')).toBe(false)

    const unknown = failText('withdrawing', null)
    expect(unknown.includes('nonce close on Ethereum')).toBe(true)
    expect(unknown.includes('nonce close on Base')).toBe(true)
    expect(unknown.includes('no reconcile is needed')).toBe(false)
  })

  it('points a Base to Alpaca bridge stuck at bridging to the offline fail path', () => {
    const resumeText = (status: string, direction: 'alpaca_to_base' | 'base_to_alpaca' | null) =>
      bridgeLabels(status, direction).find((entry) => entry.label === 'Resume')?.description ?? ''
    // The running bot refuses fail-usdc-transfer once the vault withdrawal
    // confirmed, so the dialog offers no Fail there and the resume says where to go.
    expect(bridgeLabels('bridging', 'base_to_alpaca').map((entry) => entry.label)).toEqual(['Resume'])
    expect(resumeText('bridging', 'base_to_alpaca').includes('CLI recovery guide for the offline path')).toBe(
      true
    )
    for (const [status, direction] of [
      ['withdrawing', 'base_to_alpaca'],
      ['depositing', 'base_to_alpaca'],
      ['bridging', 'alpaca_to_base'],
      ['bridging', null]
    ] as const) {
      expect(resumeText(status, direction).includes('offline path')).toBe(false)
    }
  })

  it('fills the bridge direction on the usdc resume command for the client', () => {
    const toBase = transferRecoveryCommands({
      deployment: PROD,
      kind: 'usdc_bridge',
      id: 'BRIDGE001',
      status: 'bridging',
      direction: 'alpaca_to_base'
    })
    expect(commandFor(toBase, 'Resume')).toBe(`${CLIENT} debug resume-usdc alpaca-to-base BRIDGE001`)

    const toAlpaca = transferRecoveryCommands({
      deployment: PROD,
      kind: 'usdc_bridge',
      id: 'BRIDGE002',
      status: 'bridging',
      direction: 'base_to_alpaca'
    })
    expect(commandFor(toAlpaca, 'Resume')).toBe(
      `${CLIENT} debug resume-usdc base-to-alpaca BRIDGE002`
    )
  })

  it('translates the DTO direction to the mock CLI flag vocabulary on the usdc resume command', () => {
    // The DTO names the venue flow (alpaca_to_base) while the mock CLI names the
    // Raindex-relative leg (to-raindex); the command must carry the CLI value.
    const toRaindex = transferRecoveryCommands({
      deployment: SIM,
      kind: 'usdc_bridge',
      id: 'BRIDGE001',
      status: 'bridging',
      direction: 'alpaca_to_base'
    })
    expect(commandFor(toRaindex, 'Resume')).toBe(
      `${SIM_PREFIX} transfer resume --kind usdc --id BRIDGE001 --direction to-raindex`
    )

    const toAlpaca = transferRecoveryCommands({
      deployment: SIM,
      kind: 'usdc_bridge',
      id: 'BRIDGE002',
      status: 'bridging',
      direction: 'base_to_alpaca'
    })
    expect(commandFor(toAlpaca, 'Resume')).toBe(
      `${SIM_PREFIX} transfer resume --kind usdc --id BRIDGE002 --direction to-alpaca`
    )
  })

  it('shows reconcile for a post-burn failed usdc bridge (any casing)', () => {
    // The CLI accepts `transfer reconcile --kind usdc` only for
    // reconcile-eligible failures (funds provably off their source venue),
    // which the postBurn discriminator marks true (historical name, kept
    // for wire compatibility).
    for (const status of ['failed', 'Failed', 'FAILED']) {
      const commands = transferRecoveryCommands({
        deployment: PROD,
        kind: 'usdc_bridge',
        id: 'BRIDGE001',
        status,
        postBurn: true
      })
      expect(commands.map((entry) => entry.label)).toEqual(['Reconcile', 'Reconcile (offline)'])
      expect(commandFor(commands, 'Reconcile')).toBe(
        `${CLIENT} debug reconcile-usdc BRIDGE001 --reason <funds-moved-manually|deposit-credited-offline>`
      )
      expect(commands.find((entry) => entry.label === 'Reconcile')?.mode).toBe('requires-bot')
      // A post-burn Base to Alpaca failure is settled with the bot stopped,
      // where the client cannot reach it, so the offline form is offered too.
      expect(commandFor(commands, 'Reconcile (offline)')).toBe(
        'stox transfer reconcile --kind usdc --id BRIDGE001 --reason <funds-moved-manually|deposit-credited-offline>'
      )
      expect(commands.find((entry) => entry.label === 'Reconcile (offline)')?.mode).toBe('direct-db')
    }
  })

  it('offers no offline reconcile for an Alpaca to Base failure, which nothing re-runs', () => {
    const commands = transferRecoveryCommands({
      deployment: PROD,
      kind: 'usdc_bridge',
      id: 'BRIDGE001',
      status: 'failed',
      direction: 'alpaca_to_base',
      postBurn: true
    })
    expect(commands.map((entry) => entry.label)).toEqual(['Reconcile'])
  })

  it('asks to stop the Base to Alpaca recovery before a post-burn bridge is settled by hand', () => {
    const reconcileText = (direction: 'alpaca_to_base' | 'base_to_alpaca' | null) =>
      transferRecoveryCommands({
        deployment: PROD,
        kind: 'usdc_bridge',
        id: 'BRIDGE001',
        status: 'failed',
        direction,
        postBurn: true
      }).find((entry) => entry.label === 'Reconcile')?.description ?? ''
    // The bot re-runs a post-burn Base to Alpaca failure at every start and
    // can send the USDC itself, so a hand settlement runs with the bot stopped
    // and ends with the offline reconcile.
    for (const direction of ['base_to_alpaca', null] as const) {
      const text = reconcileText(direction)
      expect(text.includes('re-runs its')).toBe(true)
      // The bot finishes it on a resume unless the mint is unresolvable or
      // funds were moved by hand.
      expect(text.includes('keep the bot running and run resume-usdc base-to-alpaca')).toBe(true)
      // One copy of the steps, in the runbook; the dialog points to it.
      expect(text.includes('do not mint, send or reconcile it with the bot running')).toBe(true)
      expect(text.includes('docs/cli-ops.md, "Settling a post-burn Base to Alpaca failure by hand"')).toBe(true)
    }
    expect(reconcileText('alpaca_to_base').includes('re-runs its recovery')).toBe(false)
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
      `${CLIENT} debug reconcile-equity mint ISS001 --reason "<reason>"`
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

  it('returns no commands for a reconciled equity mint (any casing)', () => {
    for (const status of ['reconciled', 'Reconciled', 'RECONCILED']) {
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

  it('returns no commands for a reconciled equity redemption', () => {
    expect(
      transferRecoveryCommands({
        deployment: PROD,
        kind: 'equity_redemption',
        id: 'RED001',
        status: 'reconciled'
      })
    ).toEqual([])
  })

  it('returns no commands for a reconciled usdc bridge', () => {
    expect(
      transferRecoveryCommands({
        deployment: PROD,
        kind: 'usdc_bridge',
        id: 'BRIDGE001',
        status: 'reconciled'
      })
    ).toEqual([])
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
        deployment: { simulateSourceId: 'sim-1', backendPort: null, clientEnv: 'production' },
        kind: 'equity_mint',
        id: 'ISS001',
        status: 'wrapping'
      })
    ).toEqual([])
  })
})

describe('tradeRecoveryCommands', () => {
  it('does not offer process-tx -- only the position/view commands', () => {
    // process-tx re-accounts a MISSED fill, but a trade only reaches the history
    // panel once recorded, and re-running it on an older trade is not guarded by
    // the Position single-slot last_acknowledged_trade_id. It stays in the static
    // guide for the genuinely-missed case, not the per-trade modal.
    const commands = tradeRecoveryCommands({
      deployment: PROD,
      symbol: 'MSTR'
    })
    expect(commands.map((entry) => entry.label)).toEqual([
      'Release hedge',
      'Set position',
      'Rebuild view'
    ])
    expect(commands.every((entry) => !entry.command.includes('process-tx'))).toBe(true)
    expect(commandFor(commands, 'Release hedge')).toBe(
      `${CLIENT} debug position release-hedge MSTR --order-id <order-id> --reason "<reason>"`
    )
    expect(commandFor(commands, 'Set position')).toBe(
      `${CLIENT} debug position set MSTR --target-net <N> [--price-usdc <USDC_PER_SHARE>] --reason "<reason>"`
    )
    expect(commandFor(commands, 'Rebuild view')).toBe(`${CLIENT} debug view rebuild position --id MSTR`)
  })

  it('uses the mock cli prefix in a simulation build', () => {
    const commands = tradeRecoveryCommands({
      deployment: SIM,
      symbol: 'MSTR'
    })
    expect(commandFor(commands, 'Set position')).toBe(
      `${SIM_PREFIX} position set -s MSTR (--zero | --long <N> | --short <N>) [--price <USDC_PER_SHARE>] -r "<reason>"`
    )
  })

  it('returns no commands for a trade in a simulation build with an unknown backend port', () => {
    const commands = tradeRecoveryCommands({
      deployment: { simulateSourceId: 'sim-1', backendPort: null, clientEnv: 'production' },
      symbol: 'MSTR'
    })
    expect(commands).toEqual([])
  })
})

describe('forClientEnv', () => {
  it('retargets a guide command at staging and leaves production as is', () => {
    const command = `${CLIENT} debug resume`
    expect(forClientEnv(command, 'staging')).toBe('st0x-liquidity-client --env staging debug resume')
    expect(forClientEnv(command, 'production')).toBe(command)
    expect(forClientEnv(command, null)).toBe(
      'st0x-liquidity-client --env <production|staging> debug resume'
    )
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
    const has = (prefix: string) =>
      allCommands.some((entry) => entry.command.startsWith(`${CLIENT} ${prefix}`))
    expect(has('debug recheck')).toBe(true)
    expect(has('debug cctp complete-mint')).toBe(true)
    expect(has('debug fail-usdc-transfer')).toBe(true)
    expect(has('debug reconcile-usdc')).toBe(true)
    expect(has('debug reconcile-equity')).toBe(true)
    expect(allCommands.every((entry) => !entry.command.includes('recheck-transfer'))).toBe(true)
  })

  it('uses the operations client for every guide command, so each requires the bot', () => {
    const allCommands = RECOVERY_GUIDE.flatMap((group) => group.commands)
    expect(allCommands.every((entry) => entry.command.startsWith(`${CLIENT} `))).toBe(true)
    expect(allCommands.every((entry) => entry.mode === 'requires-bot')).toBe(true)
  })

  it('offers recheck only for the USDC deposit failures the bot rechecks', () => {
    const recheck = RECOVERY_GUIDE.flatMap((group) => group.commands).find((entry) =>
      entry.command.startsWith(`${CLIENT} debug recheck`)
    )
    // The bot refuses an Alpaca to Base deposit and one with no onchain deposit
    // transaction, so the guide sends those to manual settlement instead.
    expect(recheck?.appliesTo.includes('failed Base to Alpaca USDC deposit with an onchain')).toBe(true)
    expect(recheck?.appliesTo.includes('Alpaca to Base deposit needs manual settlement')).toBe(true)
  })

  it('asks for the Alpaca order check before a guide resume at converting', () => {
    const resume = RECOVERY_GUIDE.flatMap((group) => group.commands).find((entry) =>
      entry.command.startsWith(`${CLIENT} debug resume-usdc`)
    )
    // The same check as the converting note in the transfer dialog.
    expect(
      resume?.whenToUse.includes('first confirm at Alpaca that no USD to USDC order for the transfer is still open')
    ).toBe(true)
  })
})

describe('RECOVERY_GUIDE follow-up steps', () => {
  const guideEntry = (prefix: string) =>
    RECOVERY_GUIDE.flatMap((group) => group.commands).find((entry) =>
      entry.command.startsWith(`${CLIENT} ${prefix}`)
    )

  it('gives complete-mint its next step', () => {
    expect(
      guideEntry('debug cctp complete-mint')?.description.includes(
        'run resume-usdc afterwards while the bridge is not terminal. Run reconcile-usdc only after a terminal failure whose funds leg is done and verified'
      )
    ).toBe(true)
  })

  it('gives the reconcile entries the same notes as their rows', () => {
    expect(
      guideEntry('debug reconcile-usdc')?.description.includes('re-runs its recovery at every start')
    ).toBe(true)
    expect(
      guideEntry('debug reconcile-equity')?.description.includes(
        'For a redemption that failed at detection or was rejected before the bot started'
      )
    ).toBe(true)
  })
})

describe('recoveryModeLabel', () => {
  it('renders a distinct badge for each execution mode', () => {
    expect(recoveryModeLabel('requires-bot')).toBe('REST — requires the running bot')
    expect(recoveryModeLabel('direct-db')).toBe(
      'direct DB — stop the bot / ensure it is not driving this id'
    )
  })
})

describe('recoveryModeColor', () => {
  it('gives requires-bot amber and direct-db the bot-stop red', () => {
    expect(recoveryModeColor('requires-bot').text).toBe('text-amber-500')
    expect(recoveryModeColor('direct-db').text).toBe('text-destructive')
  })
})
