import type { TransferOperation } from './api/TransferOperation'
import type { TransferWarning } from './api/TransferWarning'
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

export const transferWarningText = (warning: TransferWarning): string => {
  switch (warning.kind) {
    case 'mint_category_unavailable':
      return 'Mint transfer data is unavailable.'
    case 'redemption_category_unavailable':
      return 'Redemption transfer data is unavailable.'
    case 'bridge_category_unavailable':
      return 'USDC bridge data is unavailable.'
    case 'trade_history_unavailable':
      return 'Trade history is unavailable; transfer links may be incomplete.'
    case 'mint_replay_failed':
      return `Mint ${warning.id} could not be loaded.`
    case 'redemption_replay_failed':
      return `Redemption ${warning.id} could not be loaded.`
    case 'bridge_replay_failed':
      return `USDC bridge ${warning.id} could not be loaded.`
    case 'mint_lifecycle_failed':
      return `Mint ${warning.id} has an invalid lifecycle.`
    case 'redemption_lifecycle_failed':
      return `Redemption ${warning.id} has an invalid lifecycle.`
    case 'bridge_lifecycle_failed':
      return `USDC bridge ${warning.id} has an invalid lifecycle.`
  }
}

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

// Routing the Record through `UsdcBridgeDirection` makes a new direction variant
// fail compilation here, mirroring performance-panel.svelte's direction labels.
const USDC_BRIDGE_DIRECTION_LABELS: Record<UsdcBridgeDirection, string> = {
  alpaca_to_base: 'Alpaca → Raindex',
  base_to_alpaca: 'Raindex → Alpaca'
}

/// Row/detail label for a transfer's type. A USDC bridge spells out its
/// direction ("Alpaca → Raindex" / "Raindex → Alpaca") since the asset column
/// already reads "USDC"; the bare `kindLabel` ("USDC Bridge") still backs the
/// kind filter, where direction is not a selectable dimension.
export const transferTypeLabel = (transfer: {
  kind: string
  direction?: UsdcBridgeDirection
}): string => {
  if (transfer.kind === 'usdc_bridge' && transfer.direction !== undefined) {
    return USDC_BRIDGE_DIRECTION_LABELS[transfer.direction]
  }

  return kindLabel(transfer.kind)
}

/// Maps a status string to colour classes.  Works for both DTO statuses
/// (snake_case, used in the table) and raw event names (PascalCase,
/// used in the detail modal timeline).
///
/// Note: `lower.includes('reconciled')` also matches the raw event name
/// `OperatorReconciled` from the timeline -- this is intentional since that
/// event IS a reconciliation step and should render amber.
export const statusStyle = (status: string): StatusStyle => {
  const lower = status.toLowerCase()

  if (lower.includes('completed') || lower.includes('deposited') || lower.includes('confirmed')) {
    return {
      text: 'text-green-500',
      dot: 'bg-green-500'
    }
  }

  if (lower.includes('reconciled')) {
    return {
      text: 'text-amber-500',
      dot: 'bg-amber-500'
    }
  }

  if (lower.includes('failed') || lower.includes('rejected')) {
    return {
      text: 'text-destructive',
      dot: 'bg-destructive'
    }
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

/// Execution mode for a recovery command, from SPEC's execution-mode contracts
/// in the Operator Recovery Surface section. Every production command goes
/// through the operations client, so it is `requires-bot`; simulation builds
/// run the mock CLI, whose per-object commands are one of the two:
///   - `direct-db`: mutates local CQRS state directly; the bot must not be
///     concurrently driving the same id.
///   - `requires-bot`: dispatches through the bot's REST API and only works
///     while the bot is running.
export type RecoveryMode = 'direct-db' | 'requires-bot'

/// A single copy-pasteable recovery command applicable to one object in its
/// current state. `label` names the action, `description` says when to use it,
/// and `mode` drives the inline execution-mode warning.
export type RecoveryCommand = {
  command: string
  label: string
  description: string
  mode: RecoveryMode
}

/// Deployment context that determines which CLI the recovery commands use.
///
/// Simulation builds set `simulateSourceId` (`PUBLIC_SIMULATE_SOURCE_ID`, set
/// solely by the `simulate-failures` flake apps) and run the mock CLI against
/// the harness's `/tmp` config, so they also need `backendPort`. Live
/// deployments use the operations client, which needs no config paths or port,
/// only the environment the deployment is (`clientEnv`).
export type DeploymentContext = {
  simulateSourceId: string | null
  backendPort: string | null
  clientEnv: 'production' | 'staging' | null
}

/// The production recovery CLI: the T0 operations client
/// (`crates/liquidity-client`). It signs in with the operator's Google account
/// and calls the running bot's IAP-fronted ops API, so it needs no SSH and no
/// on-box config, and every command needs the bot running.
export const LIQUIDITY_CLIENT = 'st0x-liquidity-client --env production'

/// A client command retargeted at `clientEnv`. The static guide is spelled for
/// production; a staging deployment shows it with `--env staging`, and an
/// unknown one with a placeholder the operator must fill in.
export const forClientEnv = (
  command: string,
  clientEnv: 'production' | 'staging' | null
): string => command.replace('--env production', `--env ${clientEnv ?? '<production|staging>'}`)

/// The CLI a deployment's recovery commands run with. `client` is the
/// operations client; `mock` is the `st0x-cli` mock binary pointed at the
/// harness's `/tmp/st0x-simulate-failures-<port>` config/secrets, since the
/// harness has no IAP frontend for the client to reach. The two take
/// different syntax, so each command is spelled for both.
type RecoveryCli = { kind: 'client'; prefix: string } | { kind: 'mock'; prefix: string }

/// The deployment's recovery CLI, or null when a simulation build is missing
/// the backend port it needs to locate its config.
const recoveryCli = (deployment: DeploymentContext): RecoveryCli | null => {
  if (deployment.simulateSourceId === null)
    return { kind: 'client', prefix: forClientEnv(LIQUIDITY_CLIENT, deployment.clientEnv) }

  if (deployment.backendPort === null) return null

  const basePath = `/tmp/st0x-simulate-failures-${deployment.backendPort}`
  return {
    kind: 'mock',
    prefix: `nix develop --command cargo run -p st0x-cli --features mock -- --config ${basePath}.config.toml --secrets ${basePath}.secrets.toml`
  }
}

/// One recovery action spelled for both CLIs: the client's arguments, and the
/// mock CLI's arguments with the mode they run in.
const spell = (
  cli: RecoveryCli,
  client: string,
  mock: { args: string; mode: RecoveryMode }
): { command: string; mode: RecoveryMode } => {
  switch (cli.kind) {
    case 'client':
      return { command: `${cli.prefix} ${client}`, mode: 'requires-bot' }
    case 'mock':
      return { command: `${cli.prefix} ${mock.args}`, mode: mock.mode }
  }
}

/// Whether a transfer status string (snake_case DTO status) is the terminal
/// `failed` state. Used to gate reconcile (terminal-only) versus the in-flight
/// recovery verbs.
const isFailedStatus = (status: string): boolean => status.toLowerCase() === 'failed'

/// Whether a transfer status is a terminal state (failed, completed, or
/// reconciled); no recovery commands apply to a reconciled transfer.
export const isTerminalStatus = (status: string): boolean => {
  const lower = status.toLowerCase()
  return lower === 'failed' || lower === 'completed' || lower === 'reconciled'
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
  const cli = recoveryCli(params.deployment)
  if (cli === null) return []

  if (params.status.toLowerCase() === 'completed' || params.status.toLowerCase() === 'reconciled')
    return []

  if (params.kind === 'usdc_bridge') {
    return usdcBridgeRecoveryCommands(
      cli,
      params.id,
      params.status,
      params.direction ?? null,
      params.postBurn ?? null
    )
  }

  const equityKind = equityTransferKind(params.kind)
  if (equityKind === null) return []

  return equityRecoveryCommands(cli, equityKind, params.id, params.status)
}

/// Recovery commands for an equity mint or redemption. `recheck` is always
/// applicable while non-completed (the provider may settle it at any point);
/// `resume` and `fail` apply only while in-flight; `reconcile` only once failed.
const equityRecoveryCommands = (
  cli: RecoveryCli,
  kind: 'mint' | 'redemption',
  id: string,
  status: string
): RecoveryCommand[] => {
  const commands: RecoveryCommand[] = [
    {
      ...spell(cli, `debug recheck ${kind} ${id}`, {
        args: `transfer recheck --kind ${kind} --id ${id}`,
        mode: 'requires-bot'
      }),
      label: 'Recheck',
      description:
        'Ask the running bot to re-poll the provider and complete the transfer if it settled.'
    }
  ]

  if (!isTerminalStatus(status)) {
    commands.push({
      ...spell(cli, 'debug resume', { args: 'transfer resume --kind equity', mode: 'requires-bot' }),
      label: 'Resume (all equity)',
      description:
        'Re-drive ALL interrupted mints and redemptions via the bot (no id; best-effort per ' +
        'transfer, each succeeds or fails independently and failures are reported as counts).'
    })

    commands.push({
      ...spell(cli, `debug fail-equity-transfer ${kind} ${id} --reason "<reason>"`, {
        args: `transfer fail --kind ${kind} --id ${id} -r "<reason>"`,
        mode: 'requires-bot'
      }),
      label: 'Fail',
      description:
        'Force this stuck transfer into the terminal Failed state. Use when it is permanently stuck.'
    })
  }

  if (isFailedStatus(status)) {
    commands.push({
      ...spell(cli, `debug reconcile-equity ${kind} ${id} --reason "<reason>"`, {
        args: `transfer reconcile --kind ${kind} --id ${id} -r "<reason>"`,
        mode: 'direct-db'
      }),
      label: 'Reconcile',
      description:
        'Mark a Failed transfer as Reconciled once its residue was handled out-of-band (bookkeeping).'
    })
  }

  return commands
}

/// Maps a `UsdcBridgeDirection` DTO value to the mock CLI's `--direction` flag
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

/// Maps a `UsdcBridgeDirection` DTO value to the operations client's direction
/// argument, which names the venue-to-venue flow like the DTO.
const usdcDirectionToClientArg = (direction: UsdcBridgeDirection | null): string | null => {
  switch (direction) {
    case 'alpaca_to_base':
      return 'alpaca-to-base'
    case 'base_to_alpaca':
      return 'base-to-alpaca'
    case null:
      return null
  }
}

const USDC_FAIL_ALPACA_TO_BASE =
  'Alpaca to Base: the bot accepts this only before the burn, from a completed withdrawal ' +
  'or a burn submission with no recorded burn. The burn runs on Ethereum: verify on Ethereum ' +
  'that no CCTP burn left the bot wallet after this transfer started (its Started time) and ' +
  'that the wallet has no pending transaction (pending nonce equals latest nonce); if you ' +
  'are not certain, run resume-usdc instead. The funds left Alpaca, so the guard stays held ' +
  'until you settle them with reconcile-usdc.'

const USDC_FAIL_BASE_TO_ALPACA =
  'Base to Alpaca: the bot accepts this only while the vault withdrawal is unrecorded, ' +
  'and it does not check the chain. First confirm on Base that the bot wallet made no ' +
  "OperatorWithdraw on the inventory after the transfer's from_block, and no WithdrawV2 " +
  'on the orderbook either (a withdrawal sent before the inventory migration). Only a ' +
  'USDC withdrawal from the cash vault counts: its vaultId equals the cash vault_id in ' +
  'the bot config, and the bot wallet is the OperatorWithdraw operator or the WithdrawV2 ' +
  'sender. A WithdrawV2 with the inventory as sender does not count. A withdraw the ' +
  'network accepted but has not mined is not in the logs yet, so also wait until the ' +
  "transfer's attempt timeout has passed and confirm the wallet has no pending " +
  'transaction (no pending withdraw4, and pending nonce equals latest nonce on more than ' +
  'one RPC provider). These checks cannot prove that nothing is pending. If a withdrawal ' +
  'landed or you are not certain, run resume-usdc instead: it adopts a full withdrawal, ' +
  'and a short one ends the transfer in WithdrawalFailed, so move that USDC from the bot ' +
  'wallet back to the vault by hand. If nothing landed, the resume keeps redriving: ' +
  'repeat the checks, and once they show that no withdrawal landed and none is pending, ' +
  'take the next step. If none landed, close the nonce before you fail the transfer: ' +
  'stop the bot, send a 0-value transfer with no calldata from the bot wallet to itself ' +
  'at the latest nonce, with maxFeePerGas and maxPriorityFeePerGas well above the market ' +
  'fee, wait for its required confirmations, and check the withdrawal logs again. If a ' +
  'withdrawal mined instead, start the bot and run resume-usdc, and do not fail the ' +
  'transfer. Otherwise run the offline stox fail-usdc-transfer, or this command after ' +
  'you restart the bot. If a withdrawal for this transfer mines later anyway, move its ' +
  'USDC from the bot wallet back to the vault by hand. While a recorded withdrawal is ' +
  'still confirming, run resume-usdc. Once it has confirmed the bot refuses: stop the ' +
  'bot, then confirm the transfer has no recorded burn, and on Base that no CCTP burn ' +
  'left the bot wallet after this transfer started and the wallet has no pending ' +
  'transaction. If any of that is not certain, start the bot and run resume-usdc; else ' +
  'run the offline stox fail-usdc-transfer and move the wallet USDC back by hand.'

/// A Base to Alpaca bridge stuck at `bridging` (its vault withdrawal confirmed):
/// the running bot refuses `fail-usdc-transfer` there, so the resume command
/// points to the offline path instead.
const USDC_BRIDGING_BASE_TO_ALPACA =
  ' If the burn keeps failing, the running bot refuses fail-usdc-transfer here: see ' +
  'fail-usdc-transfer in the CLI recovery guide for the offline path.'

/// An Alpaca to Base bridge at `converting` is one of two bot states: a
/// conversion the bot recorded as complete, which resume continues with the
/// Alpaca withdrawal, or one it did not, which resume fails without asking
/// Alpaca (the bot never stored the broker order id). After an unresolved
/// conversion outcome the order can still fill, so the note asks for an Alpaca
/// check first. An unknown direction gets the note too.
const USDC_CONVERTING_ALPACA_TO_BASE =
  ' Alpaca to Base: this status covers two bot states. If the bot recorded the ' +
  'conversion as complete, this resume continues with the Alpaca withdrawal. If not, the ' +
  'bot cannot re-drive the USD to USDC conversion (it did not store the broker order ' +
  'id), so this resume fails the transfer (ConversionFailed) and releases the guard, ' +
  'without checking Alpaca. So first confirm at Alpaca that no USD to USDC order for ' +
  'this transfer is still open (it is filled, cancelled or rejected), above all after a ' +
  'conversion outcome unresolved alert; if one is open, cancel it or wait for it. Do not ' +
  'move USDC by hand before you run the resume. If it ends in ConversionFailed, settle ' +
  'any converted USDC by hand at Alpaca (reconcile-usdc does not apply). A later failure ' +
  'has its own recovery.'

/// The extra resume text for a bridge's status and direction.
const usdcResumeNote = (status: string, direction: UsdcBridgeDirection | null): string => {
  if (direction === 'base_to_alpaca' && status === 'bridging') return USDC_BRIDGING_BASE_TO_ALPACA
  if (direction !== 'base_to_alpaca' && status === 'converting') return USDC_CONVERTING_ALPACA_TO_BASE
  return ''
}

/// The check to run before `fail-usdc-transfer` on an in-flight bridge, or null
/// where the running bot always refuses it. An unknown direction gives both.
const usdcFailCheck = (status: string, direction: UsdcBridgeDirection | null): string | null => {
  switch (direction) {
    case 'alpaca_to_base':
      return status === 'withdrawing' || status === 'bridging' ? USDC_FAIL_ALPACA_TO_BASE : null
    case 'base_to_alpaca':
      return status === 'withdrawing' ? USDC_FAIL_BASE_TO_ALPACA : null
    case null:
      return status === 'withdrawing' || status === 'bridging'
        ? `${USDC_FAIL_ALPACA_TO_BASE} ${USDC_FAIL_BASE_TO_ALPACA}`
        : null
  }
}

/// Recovery commands for a USDC bridge, gated by status:
///   - failed (terminal): `reconcile`, but ONLY for a reconcile-eligible
///     failure. The CLI accepts `transfer reconcile --kind usdc` when the
///     funds provably left their source venue (`DepositFailed`, a post-burn
///     `BridgingFailed`, any `AlpacaToBase BridgingFailed` -- its withdrawal
///     completed, so the funds are off Alpaca even without a burn, e.g. the
///     settlement-retry-deadline terminal -- or a `BaseToAlpaca
///     ConversionFailed`) and rejects failures whose funds never moved. The
///     `postBurn` discriminator on `UsdcBridgeStatus::Failed` carries this
///     reconcile-eligibility flag (the name is historical, kept for wire
///     compatibility); when it is not `true` we surface nothing rather than
///     a false affordance the CLI would reject.
///   - completed (terminal): none.
///   - any in-flight status: `resume`, which hands the bridge back to the
///     bot's transfer worker whatever stage it stopped at.
///   - also `fail-usdc-transfer` where the running bot can accept it (see
///     `usdcFailCheck`): Alpaca to Base at `withdrawing` (WithdrawalComplete)
///     or `bridging` (BridgingSubmitting with no recorded burn), and Base to
///     Alpaca at `withdrawing` (an unrecorded WithdrawalSubmitting). The
///     status alone does not prove the pre-burn state, so the description
///     gives the check that applies to the bridge's direction.
const usdcBridgeRecoveryCommands = (
  cli: RecoveryCli,
  id: string,
  status: string,
  direction: UsdcBridgeDirection | null,
  postBurn: boolean | null
): RecoveryCommand[] => {
  if (isFailedStatus(status)) {
    if (postBurn !== true) return []

    return [
      {
        ...spell(
          cli,
          `debug reconcile-usdc ${id} --reason <funds-moved-manually|deposit-credited-offline>`,
          { args: `transfer reconcile --kind usdc --id ${id} -r "<reason>"`, mode: 'direct-db' }
        ),
        label: 'Reconcile',
        description:
          'Mark this failed USDC bridge as Reconciled once its off-venue funds were ' +
          'settled out-of-band (bookkeeping). Verify where the funds sit first.'
      }
    ]
  }

  if (isTerminalStatus(status)) return []

  // Both CLIs require the direction and reject a mismatch against the
  // persisted value, so fill the bridge's known direction (in each CLI's
  // vocabulary) when we have it rather than leaving a placeholder.
  const clientDirection = usdcDirectionToClientArg(direction) ?? '<alpaca-to-base|base-to-alpaca>'
  const mockDirection = usdcDirectionToCliFlag(direction) ?? '<to-raindex|to-alpaca>'
  const resume: RecoveryCommand = {
    ...spell(cli, `debug resume-usdc ${clientDirection} ${id}`, {
      args: `transfer resume --kind usdc --id ${id} --direction ${mockDirection}`,
      mode: 'requires-bot'
    }),
    label: 'Resume',
    description:
      "Re-drive this USDC bridge on the bot's transfer worker from the stage it stopped at." +
      usdcResumeNote(status.toLowerCase(), direction)
  }

  const check = usdcFailCheck(status.toLowerCase(), direction)
  if (check === null) return [resume]

  return [
    resume,
    {
      ...spell(cli, `debug fail-usdc-transfer ${id} --reason "<reason>"`, {
        args: `fail-usdc-transfer --id ${id} -r "<reason>"`,
        mode: 'direct-db'
      }),
      label: 'Fail (pre-burn)',
      description: `Check the transfer first. ${check}`
    }
  ]
}

/// Builds the position/trade recovery commands applicable to a given symbol,
/// pre-filling `-s <symbol>`.
///
/// `process-tx` is deliberately NOT offered here. A trade only reaches the
/// history panel once it has already been recorded, and re-accounting a fill is
/// not guarded against re-running on an older trade: the Position's single-slot
/// `last_acknowledged_trade_id` only blocks re-applying the most recent trade,
/// so a `process-tx` on a historical fill would double-account it. `process-tx`
/// stays in the static recovery guide for the genuinely-missed-fill case.
///
/// `release-hedge` needs the pending offchain order id, which the dashboard
/// does not have, so it is left as a `<order-id>` placeholder for the operator.
export const tradeRecoveryCommands = (params: {
  deployment: DeploymentContext
  symbol: string
}): RecoveryCommand[] => {
  const cli = recoveryCli(params.deployment)
  if (cli === null) return []

  const { symbol } = params

  const commands: RecoveryCommand[] = []

  commands.push({
    ...spell(
      cli,
      `debug position release-hedge ${symbol} --order-id <order-id> --reason "<reason>"`,
      {
        args: `position release-hedge -s ${symbol} -o <order-id> -r "<reason>"`,
        mode: 'direct-db'
      }
    ),
    label: 'Release hedge',
    description:
      "Clear a position's stuck pending offchain order so normal hedging can retry. Needs the order id."
  })

  commands.push({
    ...spell(
      cli,
      `debug position set ${symbol} --target-net <N> [--price-usdc <USDC_PER_SHARE>] --reason "<reason>"`,
      {
        args: `position set -s ${symbol} (--zero | --long <N> | --short <N>) [--price <USDC_PER_SHARE>] -r "<reason>"`,
        mode: 'direct-db'
      }
    ),
    label: 'Set position',
    description:
      'Override the net exposure after a manual correction. The target is signed: negative ' +
      'is short, 0 is flat. The price is required for a nonzero target unless the position ' +
      'already has a last price.'
  })

  commands.push({
    ...spell(cli, `debug view rebuild position --id ${symbol}`, {
      args: `view rebuild -a position --id ${symbol}`,
      mode: 'direct-db'
    }),
    label: 'Rebuild view',
    description: 'Replay all events to reconstruct a corrupted position view.'
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
/// specific object. They are the production operations client's; a simulation
/// build runs the mock CLI, whose per-object commands the modals render.
export const RECOVERY_GUIDE: GuideGroup[] = [
  {
    object: 'transfer',
    commands: [
      {
        command: `${LIQUIDITY_CLIENT} debug recheck <mint|redemption|usdc> <id>`,
        description: 'Re-poll the provider and complete the transfer if it has settled.',
        whenToUse: 'A transfer is stuck or failed but the provider may have settled it.',
        appliesTo:
          'Equity mint / redemption (any non-completed state), failed Base to Alpaca USDC ' +
          'deposit with an onchain deposit transaction. A failed Alpaca to Base deposit needs ' +
          'manual settlement, then reconcile-usdc.',
        mode: 'requires-bot'
      },
      {
        command: `${LIQUIDITY_CLIENT} debug resume`,
        description:
          'Re-drive ALL interrupted mints and redemptions (no id; best-effort per transfer, ' +
          'failures reported as counts).',
        whenToUse: 'Equity transfers were interrupted mid-flight and need re-driving via the bot.',
        appliesTo: 'All in-flight equity transfers',
        mode: 'requires-bot'
      },
      {
        command: `${LIQUIDITY_CLIENT} debug resume-usdc <alpaca-to-base|base-to-alpaca> <id>`,
        description: "Re-drive a single USDC bridge on the bot's transfer worker.",
        whenToUse:
          'A USDC bridge stopped mid-flight, before or after the burn. For an Alpaca to Base ' +
          'bridge at converting, first confirm at Alpaca that no USD to USDC order for the ' +
          'transfer is still open: the resume can fail it without asking Alpaca (see its ' +
          'transfer dialog).',
        appliesTo: 'USDC bridge (in-flight)',
        mode: 'requires-bot'
      },
      {
        command: `${LIQUIDITY_CLIENT} debug fail-equity-transfer <mint|redemption> <id> --reason "<reason>"`,
        description: 'Force a stuck transfer into the terminal Failed state.',
        whenToUse: 'A mint/redemption is permanently stuck and unrecoverable.',
        appliesTo: 'Equity mint / redemption (non-terminal)',
        mode: 'requires-bot'
      },
      {
        command: `${LIQUIDITY_CLIENT} debug fail-usdc-transfer <id> --reason "<reason>"`,
        description: `Force a USDC bridge stuck before its burn into Failed. ${USDC_FAIL_ALPACA_TO_BASE} ${USDC_FAIL_BASE_TO_ALPACA}`,
        whenToUse:
          'A USDC bridge is stuck before its CCTP burn and must be terminalized. Run the check ' +
          'for its direction in the description first; if it is not certain, run resume-usdc.',
        appliesTo: 'USDC bridge (pre-burn stuck)',
        mode: 'requires-bot'
      },
      {
        command: `${LIQUIDITY_CLIENT} debug reconcile-equity <mint|redemption> <id> --reason "<reason>"`,
        description:
          'Mark a terminally-failed equity transfer Reconciled after handling residue manually.',
        whenToUse:
          'An equity transfer is in a terminal failure and its residue was settled out-of-band ' +
          '(bookkeeping).',
        appliesTo: 'Failed equity mint / redemption',
        mode: 'requires-bot'
      },
      {
        command: `${LIQUIDITY_CLIENT} debug reconcile-usdc <id> --reason <funds-moved-manually|deposit-credited-offline>`,
        description: 'Mark a failed USDC bridge Reconciled after its funds were settled manually.',
        whenToUse:
          'A USDC bridge failed after its funds left the source venue, and they were settled ' +
          'out-of-band.',
        appliesTo:
          'USDC failure with funds off the source venue (DepositFailed, any AlpacaToBase ' +
          'BridgingFailed, a post-burn BaseToAlpaca BridgingFailed, or BaseToAlpaca ' +
          'ConversionFailed)',
        mode: 'requires-bot'
      }
    ]
  },
  {
    object: 'position',
    commands: [
      {
        command: `${LIQUIDITY_CLIENT} debug position release-hedge <symbol> --order-id <order-id> --reason "<reason>"`,
        description: "Clear a position's pending offchain order pointer so hedging can retry.",
        whenToUse: 'A position is wedged on a hedge order that never resolved.',
        appliesTo: 'Position with a stuck pending offchain order',
        mode: 'requires-bot'
      },
      {
        command: `${LIQUIDITY_CLIENT} debug position set <symbol> --target-net <N> [--price-usdc <USDC_PER_SHARE>] --reason "<reason>"`,
        description:
          'Override a position’s net exposure after a manual correction. The target is ' +
          'signed: negative is short, 0 is flat.',
        whenToUse: 'The recorded net exposure has drifted from reality and must be set explicitly.',
        appliesTo: 'Any position',
        mode: 'requires-bot'
      }
    ]
  },
  {
    object: 'view',
    commands: [
      {
        command: `${LIQUIDITY_CLIENT} debug view rebuild <position|offchain-order|vault-registry> (--id <id> | --all)`,
        description: 'Replay all events to reconstruct a corrupted materialized view.',
        whenToUse: 'A view became corrupted (e.g. lost updates from optimistic-lock conflicts).',
        appliesTo: 'Position / offchain-order / vault-registry views',
        mode: 'requires-bot'
      }
    ]
  },
  {
    object: 'cctp',
    commands: [
      {
        command: `${LIQUIDITY_CLIENT} debug cctp complete-mint --burn-tx <hash> --source-chain <ethereum|base>`,
        description:
          'Complete the destination-chain mint of a stuck CCTP transfer. A burn Circle has not ' +
          'attested yet fails at once as retryable; rerun it later.',
        whenToUse: 'A CCTP burn succeeded but attestation polling was interrupted before the mint.',
        appliesTo: 'CCTP cross-chain USDC transfer',
        mode: 'requires-bot'
      }
    ]
  },
  {
    object: 'trade',
    commands: [
      {
        command: `${LIQUIDITY_CLIENT} debug process-tx <hash> [--chain <base|ethereum|hyperevm|robinhood>]`,
        description: 'Re-account a missed onchain fill: record the trade and place the hedge.',
        whenToUse: 'The bot missed an onchain fill and the position/hedge was never updated.',
        appliesTo: 'Onchain (Raindex) fills',
        mode: 'requires-bot'
      }
    ]
  }
]

/// Human label for an execution mode, used for the inline warning badge.
export const recoveryModeLabel = (mode: RecoveryMode): string => {
  switch (mode) {
    case 'requires-bot':
      return 'REST — requires the running bot'
    case 'direct-db':
      return 'direct DB — stop the bot / ensure it is not driving this id'
  }
}

/// Colour classes for an execution-mode badge: `requires-bot` is amber,
/// `direct-db` the bot-stop red. `text` is the bare foreground class (static
/// guide cell); `badge` adds the border + tint for the per-object modal pill.
export const recoveryModeColor = (mode: RecoveryMode): { text: string; badge: string } => {
  switch (mode) {
    case 'requires-bot':
      return {
        text: 'text-amber-500',
        badge: 'text-amber-500 border-amber-500/40 bg-amber-500/10'
      }

    case 'direct-db':
      return {
        text: 'text-destructive',
        badge: 'text-destructive border-destructive/40 bg-destructive/10'
      }
  }
}
