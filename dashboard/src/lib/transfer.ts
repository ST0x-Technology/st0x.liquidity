import { formatDecimal } from './decimal'
import { formatBalance } from './format'

export type StatusStyle = {
  text: string
  dot: string
}

export const kindLabel = (kind: string): string => {
  switch (kind) {
    case 'equity_mint':
      return 'Mint'
    case 'equity_redemption':
      return 'Redeem'
    case 'usdc_bridge':
      return 'USDC Bridge'
    default:
      return kind
  }
}

/// Maps a status string to colour classes.  Works for both DTO statuses
/// (snake_case, used in the table) and raw event names (PascalCase,
/// used in the detail modal timeline).
export const statusStyle = (status: string): StatusStyle => {
  const lower = status.toLowerCase()

  if (lower.includes('completed') || lower.includes('deposited') || lower.includes('confirmed')) {
    return { text: 'text-green-500', dot: 'bg-green-500' }
  }

  if (lower.includes('failed') || lower.includes('rejected')) {
    return { text: 'text-destructive', dot: 'bg-destructive' }
  }

  return { text: 'text-muted-foreground', dot: 'bg-muted-foreground' }
}

export const humanizeStatus = (status: string): string =>
  status
    .split('_')
    .map((word) => word.charAt(0).toUpperCase() + word.slice(1))
    .join(' ')

export const humanizeStep = (step: string): string => step.replace(/([A-Z])/g, ' $1').trim()

export const isTxHash = (value: unknown): value is string =>
  typeof value === 'string' && /^0x[0-9a-fA-F]{64}$/.test(value)

const SKIP_FIELDS = new Set(['attestation'])
const TOKEN_UNIT_FIELDS = new Set([
  'actual_wrapped_amount',
  'shares_minted',
  'unwrapped_amount',
  'wrapped_amount',
  'wrapped_shares',
])
const ADDRESS_FIELDS = new Set(['token', 'underlying_token', 'wallet', 'redemption_wallet'])

export const isTimestampField = (key: string): boolean => key.endsWith('_at')

export const isTransferRef = (value: unknown): value is Record<string, string> =>
  typeof value === 'object' && value !== null && ('AlpacaId' in value || 'OnchainTx' in value)

export const formatFieldName = (key: string): string =>
  key.replace(/_/g, ' ').replace(/\b\w/g, (char) => char.toUpperCase())

const formatDecimalAddress = (value: string): string | null => {
  try {
    return `0x${BigInt(value).toString(16).padStart(40, '0')}`
  } catch {
    return null
  }
}

export const formatNumericDetailValue = (key: string, value: string): string => {
  const address = ADDRESS_FIELDS.has(key) ? formatDecimalAddress(value) : null
  if (address !== null) return address

  // alloy serializes U256 fields as hex ("0x.."), but formatBalance and
  // formatDecimal expect base-10 digit strings. Normalize integer hex to
  // decimal first so the "0x" prefix doesn't make decimal.js throw (an
  // unhandled throw here froze the detail modal on "Loading events...").
  const normalized = /^0x[0-9a-fA-F]+$/.test(value) ? BigInt(value).toString() : value

  const displayValue = TOKEN_UNIT_FIELDS.has(key) ? formatBalance(normalized, 18) : normalized
  return formatDecimal(displayValue, 3)
}

export const extractTimestamp = (payload: Record<string, unknown>): string | null => {
  for (const [key, value] of Object.entries(payload)) {
    if (isTimestampField(key) && typeof value === 'string') return value
  }
  return null
}

export const detailFields = (payload: Record<string, unknown>): Array<[string, unknown]> =>
  Object.entries(payload).filter(([key]) => !isTimestampField(key) && !SKIP_FIELDS.has(key))

// `failure` is the externally-tagged DetectionFailure enum, so an ApiError
// serializes as `{ ApiError: { status_code } }`. The status lives in the nested
// payload, not on the wrapper, so unwrap before reading it.
export const apiErrorStatus = (value: unknown): string | null => {
  if (typeof value !== 'object' || value === null) return null

  const payload = (value as Record<string, unknown>)['ApiError']

  if (typeof payload !== 'object' || payload === null) return null

  const status = (payload as Record<string, unknown>)['status_code']

  if (typeof status === 'string') return status
  if (typeof status === 'number') return String(status)
  return null
}

/// Human-readable label for a stranded-equity location code.
export const stuckLocationLabel = (location: string): string => {
  switch (location) {
    case 'issuer':
      return 'Issuer'
    case 'redemption_wallet':
      return 'Redemption wallet'
    case 'bot_wallet_unwrapped':
      return 'Bot wallet'
    case 'bot_wallet_wrapped':
      return 'Bot wallet (wrapped)'
    default:
      return location
  }
}

/// Title-cases a snake_case stranded-equity reason code.
export const stuckReasonLabel = (reason: string): string =>
  reason
    .split('_')
    .map((word) => word.charAt(0).toUpperCase() + word.slice(1))
    .join(' ')

/// Maps a transfer kind to the `transfer recheck --kind` flag value, or null
/// for kinds that are not equity transfers (e.g. usdc_bridge).
const recheckTransferType = (kind: string): string | null => {
  switch (kind) {
    case 'equity_mint':
      return 'mint'
    case 'equity_redemption':
      return 'redemption'
    default:
      return null
  }
}

/// A `transfer recheck` command for a stranded transfer. The `mode`
/// distinguishes a local simulation command from one an operator runs on the
/// deployed server, so the UI can label them differently.
export type RecoveryCommand =
  | { mode: 'simulation'; command: string }
  | { mode: 'production'; command: string }

/// Builds the `transfer recheck` CLI command shown to operators for a stranded
/// transfer, or null for kinds with no recovery path (e.g. usdc_bridge).
///
/// Simulation builds set `simulateSourceId` (`PUBLIC_SIMULATE_SOURCE_ID`, set
/// solely by the `simulate-failures` flake apps) and run the mock CLI against
/// the harness's `/tmp` config, so they also need `backendPort`. Live
/// deployments invoke the `stox` wrapper, which auto-loads prod config/secrets,
/// so the production command needs no config paths or port.
export const recoveryCommand = (params: {
  simulateSourceId: string | null
  backendPort: string | null
  kind: string
  id: string
}): RecoveryCommand | null => {
  const transferType = recheckTransferType(params.kind)
  if (transferType === null) return null

  if (params.simulateSourceId !== null) {
    if (params.backendPort === null) return null

    const basePath = `/tmp/st0x-simulate-failures-${params.backendPort}`
    return {
      mode: 'simulation',
      command: `nix develop --command cargo run --features mock --bin cli -- --config ${basePath}.config.toml --secrets ${basePath}.secrets.toml transfer recheck --kind ${transferType} --id ${params.id}`,
    }
  }

  return {
    mode: 'production',
    command: `stox transfer recheck --kind ${transferType} --id ${params.id}`,
  }
}
