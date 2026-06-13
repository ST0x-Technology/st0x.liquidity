import type { TransferOperation } from './api/TransferOperation'
import type { UsdcBridgeDirection } from './api/UsdcBridgeDirection'
import { formatDecimal } from './decimal'
import { formatBalance } from './format'

/// The transfer `kind` discriminator, derived from the generated
/// `TransferOperation` binding (an internally-tagged enum keyed on `kind`).
/// There is deliberately no standalone `TransferCategory` Rust DTO -- the
/// `TransferOperation` variant names encode the category -- so the dashboard
/// extracts the union from the real binding rather than importing a phantom
/// `api/TransferCategory` file that `st0x-dto` never generates.
export type TransferCategory = TransferOperation['kind']

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
  'wrapped_shares'
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

/// Maps a transfer kind to the `--kind` flag value used by the equity-only
/// `transfer fail` / `transfer recheck` verbs, or null for kinds that are not
/// equity transfers (e.g. usdc_bridge).
const equityTransferKind = (kind: TransferCategory): 'mint' | 'redemption' | null => {
  switch (kind) {
    case 'equity_mint':
      return 'mint'
    case 'equity_redemption':
      return 'redemption'
    case 'usdc_bridge':
      return null
  }
}

/// Execution mode for a recovery command, mirroring SPEC's four execution-mode
/// contracts in the Operator Recovery Surface section:
///   - `direct-db`: mutates local CQRS state directly; the bot must not be
///     concurrently driving the same id.
///   - `direct-db-live-rpc`: same direct-DB caveat, and also drives an on-chain
///     flow against a live RPC provider (e.g. `transfer resume --kind usdc`,
///     `process-tx`).
///   - `live-rpc-only`: touches no database state, runs against a live RPC
///     provider only; the caveat is the bot concurrently driving the same
///     on-chain action (e.g. `cctp complete-mint`).
///   - `requires-bot`: dispatches through the bot's REST API and only works
///     while the bot is running (`recheck`, `transfer resume --kind equity`).
export type RecoveryMode = 'direct-db' | 'direct-db-live-rpc' | 'live-rpc-only' | 'requires-bot'

/// A single copy-pasteable recovery command applicable to one object in its
/// current state. `label` names the action, `description` says when to use it,
/// and `mode` drives the inline execution-mode warning.
export type RecoveryCommand = {
  command: string
  label: string
  description: string
  mode: RecoveryMode
}

/// Deployment context that determines the CLI invocation prefix.
///
/// Simulation builds set `simulateSourceId` (`PUBLIC_SIMULATE_SOURCE_ID`, set
/// solely by the `simulate-failures` flake apps) and run the mock CLI against
/// the harness's `/tmp` config, so they also need `backendPort`. Live
/// deployments invoke the `stox` wrapper, which auto-loads prod config/secrets,
/// so the production command needs no config paths or port.
export type DeploymentContext = {
  simulateSourceId: string | null
  backendPort: string | null
}

/// Builds the CLI invocation prefix for the current deployment, or null when a
/// simulation build is missing the backend port it needs to locate its config.
///
/// Production -> the bare `stox` wrapper. Simulation -> the mock `cli` binary
/// pointed at the harness's `/tmp/st0x-simulate-failures-<port>` config/secrets.
const commandPrefix = (deployment: DeploymentContext): string | null => {
  if (deployment.simulateSourceId === null) return 'stox'

  if (deployment.backendPort === null) return null

  const basePath = `/tmp/st0x-simulate-failures-${deployment.backendPort}`
  return `nix develop --command cargo run --features mock --bin cli -- --config ${basePath}.config.toml --secrets ${basePath}.secrets.toml`
}

/// Whether a transfer status string (snake_case DTO status) is the terminal
/// `failed` state. Used to gate reconcile (terminal-only) versus the in-flight
/// recovery verbs.
const isFailedStatus = (status: string): boolean => status.toLowerCase() === 'failed'

/// Whether a transfer status is a terminal state (failed or completed); no
/// recovery commands apply to a completed transfer.
const isTerminalStatus = (status: string): boolean => {
  const lower = status.toLowerCase()
  return lower === 'failed' || lower === 'completed'
}

/// Builds the full set of recovery commands an operator could legitimately run
/// against a single transfer in its current state, with `--kind`/`--id`
/// pre-filled and the execution-mode warning attached. Returns an empty array
/// when no command applies (e.g. a completed transfer, or a simulation build
/// missing its backend port).
///
/// Gating by status:
///   - in-flight (non-terminal): `recheck`, `resume`, and `fail` -- the
///     stuck-but-not-yet-failed case the modal must surface.
///   - failed (terminal): `recheck` (the provider may have settled it after the
///     failure) and `reconcile` (book the residue as resolved).
///   - completed (terminal): none.
///
/// USDC bridges have no equity recovery verbs; only `resume` (in-flight) and
/// `reconcile` (post-burn failure only -- see `usdcBridgeRecoveryCommands`)
/// apply to them, both taking the bridge id directly. `postBurn` is the
/// `UsdcBridgeStatus::Failed` discriminator that gates the latter.
export const transferRecoveryCommands = (params: {
  deployment: DeploymentContext
  kind: TransferCategory
  id: string
  status: string
  direction?: UsdcBridgeDirection | null
  postBurn?: boolean | null
}): RecoveryCommand[] => {
  const prefix = commandPrefix(params.deployment)
  if (prefix === null) return []

  if (params.status.toLowerCase() === 'completed') return []

  if (params.kind === 'usdc_bridge') {
    return usdcBridgeRecoveryCommands(
      prefix,
      params.id,
      params.status,
      params.direction ?? null,
      params.postBurn ?? null
    )
  }

  const equityKind = equityTransferKind(params.kind)
  if (equityKind === null) return []

  return equityRecoveryCommands(prefix, equityKind, params.id, params.status)
}

/// Recovery commands for an equity mint or redemption. `recheck` is always
/// applicable while non-completed (the provider may settle it at any point);
/// `resume` and `fail` apply only while in-flight; `reconcile` only once failed.
const equityRecoveryCommands = (
  prefix: string,
  kind: 'mint' | 'redemption',
  id: string,
  status: string
): RecoveryCommand[] => {
  const commands: RecoveryCommand[] = [
    {
      command: `${prefix} transfer recheck --kind ${kind} --id ${id}`,
      label: 'Recheck',
      description:
        'Ask the running bot to re-poll the provider and complete the transfer if it settled.',
      mode: 'requires-bot'
    }
  ]

  if (!isTerminalStatus(status)) {
    commands.push({
      command: `${prefix} transfer resume --kind equity`,
      label: 'Resume (all equity)',
      description:
        'Re-drive ALL interrupted mints and redemptions via the bot (no id; best-effort per ' +
        'transfer, each succeeds or fails independently and failures are reported as counts).',
      mode: 'requires-bot'
    })

    commands.push({
      command: `${prefix} transfer fail --kind ${kind} --id ${id} -r "<reason>"`,
      label: 'Fail',
      description:
        'Force this stuck transfer into the terminal Failed state. Use when it is permanently stuck.',
      mode: 'direct-db'
    })
  }

  if (isFailedStatus(status)) {
    commands.push({
      command: `${prefix} transfer reconcile --kind ${kind} --id ${id} -r "<reason>"`,
      label: 'Reconcile',
      description:
        'Mark a Failed transfer as Reconciled once its residue was handled out-of-band (bookkeeping).',
      mode: 'direct-db'
    })
  }

  return commands
}

/// Maps a `UsdcBridgeDirection` DTO value to the CLI's `--direction` flag
/// vocabulary. The two namespaces deliberately differ: the DTO names the
/// venue-to-venue flow (`alpaca_to_base` / `base_to_alpaca`) while the CLI names
/// the Raindex-relative leg (`to-raindex` / `to-alpaca`), so this is a real
/// translation, not a casing change. Returns null for an unexpected value so the
/// caller can fall back to the operator-editable placeholder.
const usdcDirectionToCliFlag = (direction: UsdcBridgeDirection | null): string | null => {
  switch (direction) {
    case 'alpaca_to_base':
      return 'to-raindex'
    case 'base_to_alpaca':
      return 'to-alpaca'
    case null:
      return null
  }
}

/// Recovery commands for a USDC bridge, gated by status:
///   - failed (terminal): `reconcile`, but ONLY for a post-burn failure. The CLI
///     accepts `transfer reconcile --kind usdc` only when USDC was actually
///     stranded on-chain (`DepositFailed`, a post-burn `BridgingFailed`, or a
///     `BaseToAlpaca ConversionFailed`) and rejects pre-burn failures, which
///     strand nothing. The `postBurn` discriminator on `UsdcBridgeStatus::Failed`
///     tells the two apart; when it is not `true` we surface nothing rather than
///     a false affordance the CLI would reject.
///   - completed (terminal): none.
///   - in-flight: `resume`.
const usdcBridgeRecoveryCommands = (
  prefix: string,
  id: string,
  status: string,
  direction: UsdcBridgeDirection | null,
  postBurn: boolean | null
): RecoveryCommand[] => {
  if (isFailedStatus(status)) {
    if (postBurn !== true) return []

    return [
      {
        command: `${prefix} transfer reconcile --kind usdc --id ${id} -r "<reason>"`,
        label: 'Reconcile',
        description:
          'Mark this post-burn failed USDC bridge as Reconciled once its stranded USDC was ' +
          'recovered out-of-band (bookkeeping).',
        mode: 'direct-db'
      }
    ]
  }

  if (isTerminalStatus(status)) return []

  // The CLI requires --direction and rejects a mismatch against the persisted
  // value, so fill the bridge's known direction (translated into the CLI's flag
  // vocabulary) when we have it rather than leaving a placeholder.
  const directionArg = usdcDirectionToCliFlag(direction) ?? '<to-raindex|to-alpaca>'
  return [
    {
      command: `${prefix} transfer resume --kind usdc --id ${id} --direction ${directionArg}`,
      label: 'Resume',
      description:
        'Re-drive this USDC bridge whose CLI invocation was interrupted after the burn. Drives ' +
        'the on-chain flow against a live RPC provider.',
      mode: 'direct-db-live-rpc'
    }
  ]
}

/// Builds the position/trade recovery commands applicable to a given symbol,
/// pre-filling `-s <symbol>` and (for `process-tx`) the originating tx hash.
///
/// `txHash` is the onchain fill's transaction hash when known (raindex trades
/// carry a `txHash:logIndex` id); pass null for offchain venues with no tx.
/// `release-hedge` needs the pending offchain order id, which the dashboard
/// does not have, so it is left as a `<order-id>` placeholder for the operator.
export const tradeRecoveryCommands = (params: {
  deployment: DeploymentContext
  symbol: string
  txHash: string | null
}): RecoveryCommand[] => {
  const prefix = commandPrefix(params.deployment)
  if (prefix === null) return []

  const { symbol, txHash } = params

  const commands: RecoveryCommand[] = []

  if (txHash !== null) {
    commands.push({
      command: `${prefix} process-tx --tx-hash ${txHash}`,
      label: 'Process tx',
      description:
        'Re-account an onchain fill that the bot missed: records the trade and places the offsetting hedge. Runs in the CLI process against its own RPC provider and broker -- does not need the bot.',
      mode: 'direct-db-live-rpc'
    })
  }

  commands.push({
    command: `${prefix} position release-hedge -s ${symbol} -o <order-id> -r "<reason>"`,
    label: 'Release hedge',
    description:
      "Clear a position's stuck pending offchain order so normal hedging can retry. Needs the order id.",
    mode: 'direct-db'
  })

  commands.push({
    command: `${prefix} position set -s ${symbol} (--zero | --long <N> | --short <N>) [--price <USDC_PER_SHARE>] -r "<reason>"`,
    label: 'Set position',
    description:
      'Override the net exposure after a manual correction. Pick exactly one target. ' +
      '--price is required for a nonzero target unless the position already has a last price.',
    mode: 'direct-db'
  })

  commands.push({
    command: `${prefix} view rebuild -a position --id ${symbol}`,
    label: 'Rebuild view',
    description: 'Replay all events to reconstruct a corrupted position view.',
    mode: 'direct-db'
  })

  return commands
}

/// A documented recovery command for the static CLI recovery guide.
export type GuideCommand = {
  command: string
  description: string
  whenToUse: string
  appliesTo: string
  mode: RecoveryMode
}

/// A group of recovery commands sharing an object.
export type GuideObject = 'transfer' | 'position' | 'view' | 'cctp' | 'trade'

export type GuideGroup = {
  object: GuideObject
  commands: GuideCommand[]
}

/// The static CLI recovery guide: every recovery command grouped by object,
/// mirroring the verb glossary in `docs/domain.md`. Commands are shown with
/// `<...>` placeholders since the guide is a general reference, not bound to a
/// specific object. The `stox ` prefix shown is the production wrapper; in a
/// simulation build the modals render the mock-cli prefix instead.
export const RECOVERY_GUIDE: GuideGroup[] = [
  {
    object: 'transfer',
    commands: [
      {
        command: 'stox transfer recheck --kind <mint|redemption> --id <id>',
        description: 'Re-poll the provider and complete the transfer if it has settled.',
        whenToUse: 'A mint/redemption is stuck or failed but the provider may have settled it.',
        appliesTo: 'Equity mint / redemption (any non-completed state)',
        mode: 'requires-bot'
      },
      {
        command: 'stox transfer resume --kind equity',
        description:
          'Re-drive ALL interrupted mints and redemptions (no id; best-effort per transfer, ' +
          'failures reported as counts).',
        whenToUse: 'Equity transfers were interrupted mid-flight and need re-driving via the bot.',
        appliesTo: 'All in-flight equity transfers',
        mode: 'requires-bot'
      },
      {
        command: 'stox transfer resume --kind usdc --id <id> --direction <to-raindex|to-alpaca>',
        description:
          'Re-drive a single USDC bridge interrupted after its burn (against a live RPC provider).',
        whenToUse: 'A USDC bridge CLI invocation was interrupted after the burn went through.',
        appliesTo: 'USDC bridge (in-flight)',
        mode: 'direct-db-live-rpc'
      },
      {
        command: 'stox transfer fail --kind <mint|redemption> --id <id> -r "<reason>"',
        description: 'Force a stuck transfer into the terminal Failed state.',
        whenToUse: 'A mint/redemption is permanently stuck and unrecoverable.',
        appliesTo: 'Equity mint / redemption (non-terminal)',
        mode: 'direct-db'
      },
      {
        command: 'stox transfer reconcile --kind <usdc|mint|redemption> --id <id> -r "<reason>"',
        description:
          'Mark a terminally-failed transfer Reconciled after handling residue manually.',
        whenToUse:
          'A transfer is in a terminal failure and its residue was settled out-of-band (bookkeeping).',
        appliesTo:
          'Failed equity transfer / post-burn USDC failure (DepositFailed, post-burn ' +
          'BridgingFailed, or BaseToAlpaca ConversionFailed)',
        mode: 'direct-db'
      }
    ]
  },
  {
    object: 'position',
    commands: [
      {
        command: 'stox position release-hedge -s <symbol> -o <order-id> -r "<reason>"',
        description: "Clear a position's pending offchain order pointer so hedging can retry.",
        whenToUse: 'A position is wedged on a hedge order that never resolved.',
        appliesTo: 'Position with a stuck pending offchain order',
        mode: 'direct-db'
      },
      {
        command:
          'stox position set -s <symbol> (--zero | --long <N> | --short <N>) ' +
          '[--price <USDC_PER_SHARE>] -r "<reason>"',
        description: 'Override a position’s net exposure after a manual correction.',
        whenToUse: 'The recorded net exposure has drifted from reality and must be set explicitly.',
        appliesTo: 'Any position',
        mode: 'direct-db'
      }
    ]
  },
  {
    object: 'view',
    commands: [
      {
        command:
          'stox view rebuild -a <position|offchain-order|vault-registry> (--id <id> | --all)',
        description: 'Replay all events to reconstruct a corrupted materialized view.',
        whenToUse: 'A view became corrupted (e.g. lost updates from optimistic-lock conflicts).',
        appliesTo: 'Position / offchain-order / vault-registry views',
        mode: 'direct-db'
      }
    ]
  },
  {
    object: 'cctp',
    commands: [
      {
        command: 'stox cctp complete-mint --burn-tx <hash> --source-chain <ethereum|base>',
        description:
          'Complete the destination-chain mint of a stuck CCTP transfer (live RPC only; touches ' +
          'no database state).',
        whenToUse: 'A CCTP burn succeeded but attestation polling was interrupted before the mint.',
        appliesTo: 'CCTP cross-chain USDC transfer',
        mode: 'live-rpc-only'
      }
    ]
  },
  {
    object: 'trade',
    commands: [
      {
        command: 'stox process-tx --tx-hash <hash>',
        description:
          'Re-account a missed onchain fill: record the trade and place the hedge. Runs in the ' +
          'CLI process (own RPC + broker) -- does not need the bot.',
        whenToUse: 'The bot missed an onchain fill and the position/hedge was never updated.',
        appliesTo: 'Onchain (Raindex) fills',
        mode: 'direct-db-live-rpc'
      }
    ]
  }
]

/// Human label for an execution mode, used for the inline warning badge.
export const recoveryModeLabel = (mode: RecoveryMode): string => {
  switch (mode) {
    case 'requires-bot':
      return 'REST — requires the running bot'
    case 'live-rpc-only':
      return 'live RPC — ensure the bot is not driving this same on-chain action'
    case 'direct-db-live-rpc':
      return 'direct DB + live RPC — stop the bot / ensure it is not driving this id'
    case 'direct-db':
      return 'direct DB — stop the bot / ensure it is not driving this id'
  }
}
