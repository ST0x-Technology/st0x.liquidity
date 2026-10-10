import { readFileSync } from 'node:fs'
import { describe, expect, it } from 'vitest'
import {
  RECOVERY_GUIDE,
  recoveryModeLabel,
  transferRecoveryCommands,
  tradeRecoveryCommands,
  type RecoveryCommand,
  type RecoveryMode,
  type TransferCategory
} from './transfer'
import type { UsdcBridgeDirection } from './api/UsdcBridgeDirection'

// The Grafana board (observability/) shows the same recovery commands as the
// SPA from its own copies: recovery-guide.json for the guide and
// recovery-commands.js for the per-row dialog. These tests fail when a copy
// drifts from this file's builders. They also cover the board's
// status-history.js, which orders a row's status entries in its dialog.

const panels = new URL('../../../observability/liquidity-panels/', import.meta.url)

type BoardCommand = { command: string; description: string; mode: RecoveryMode }
type BoardTransfer = { kind: TransferCategory; id: string; status: string; direction?: string }
type BoardBuilders = {
  tradeCommands: (client: string, symbol: string) => BoardCommand[]
  transferCommands: (client: string, transfer: BoardTransfer) => BoardCommand[]
}

// A plain script in the board, an ES module through its export line here.
const board = (await import(new URL('recovery-commands.js', panels).href)) as BoardBuilders

type StatusEntry = { time: number; kind: string; status: string; direction?: string }
type StatusHistory = {
  latest: (
    entries: StatusEntry[]
  ) => (StatusEntry & { first: number; history: StatusEntry[] }) | null
}

// A plain script in the board, an ES module through its export line here.
const statusHistory = (await import(new URL('status-history.js', panels).href)) as StatusHistory

const CLIENT = 'st0x-liquidity-client --env production'
const PROD = { simulateSourceId: null, backendPort: null, clientEnv: 'production' } as const

const withoutLabel = (commands: RecoveryCommand[]): BoardCommand[] =>
  commands.map(({ command, description, mode }) => ({ command, description, mode }))

describe('board recovery-guide.json', () => {
  it('matches RECOVERY_GUIDE and the mode labels', () => {
    const guide = JSON.parse(readFileSync(new URL('recovery-guide.json', panels), 'utf8')) as {
      modeLabels: Record<RecoveryMode, string>
      groups: unknown
    }
    expect(guide.groups).toEqual(RECOVERY_GUIDE)
    for (const mode of ['requires-bot', 'direct-db'] as const) {
      expect(guide.modeLabels[mode]).toBe(recoveryModeLabel(mode))
    }
  })
})

describe('board recovery-commands.js', () => {
  it('matches tradeRecoveryCommands', () => {
    expect(board.tradeCommands(CLIENT, 'MSTR')).toEqual(
      withoutLabel(tradeRecoveryCommands({ deployment: PROD, symbol: 'MSTR' }))
    )
  })

  it('matches transferRecoveryCommands for every equity status', () => {
    const kinds: TransferCategory[] = ['equity_mint', 'equity_redemption']
    for (const kind of kinds) {
      for (const status of ['wrapping', 'sending', 'failed', 'completed', 'reconciled']) {
        expect(board.transferCommands(CLIENT, { kind, id: 'X1', status })).toEqual(
          withoutLabel(transferRecoveryCommands({ deployment: PROD, kind, id: 'X1', status }))
        )
      }
    }
  })

  it('matches transferRecoveryCommands for every usdc bridge status and direction', () => {
    // The exporter does not log the bridge's postBurn flag, so the board
    // matches the SPA without it: a failed bridge offers no reconcile.
    const directions: (UsdcBridgeDirection | null)[] = ['alpaca_to_base', 'base_to_alpaca', null]
    for (const direction of directions) {
      for (const status of [
        'converting',
        'withdrawing',
        'bridging',
        'depositing',
        'failed',
        'completed'
      ]) {
        expect(
          board.transferCommands(CLIENT, {
            kind: 'usdc_bridge',
            id: 'B1',
            status,
            ...(direction === null ? {} : { direction })
          })
        ).toEqual(
          withoutLabel(
            transferRecoveryCommands({
              deployment: PROD,
              kind: 'usdc_bridge',
              id: 'B1',
              status,
              direction
            })
          )
        )
      }
    }
  })
})

describe('board status-history.js', () => {
  const bridge = (status: string, time: number, direction = 'alpaca_to_base'): StatusEntry => ({
    time,
    kind: 'usdc_bridge',
    status,
    direction
  })
  // Cloud Logging returns the entries newest first.
  const order = (entries: StatusEntry[]) => {
    const row = statusHistory.latest(entries)
    return {
      status: row?.status,
      first: row?.first,
      history: row?.history.map((entry) => entry.status)
    }
  }

  it('orders in-flight bridge statuses by lifecycle when their times go backwards', () => {
    // WithdrawalComplete carries confirmed_at, the BridgingSubmitting after it
    // the older initiated_at.
    expect(order([bridge('withdrawing', 200), bridge('bridging', 100)])).toEqual({
      status: 'bridging',
      first: 100,
      history: ['withdrawing', 'bridging']
    })
    expect(
      order([bridge('bridging', 100, 'base_to_alpaca'), bridge('converting', 50, 'base_to_alpaca')])
    ).toEqual({ status: 'converting', first: 50, history: ['bridging', 'converting'] })
  })

  it('orders in-flight bridge statuses that share a time by lifecycle, in either order', () => {
    for (const entries of [
      [bridge('bridging', 100), bridge('withdrawing', 100)],
      [bridge('withdrawing', 100), bridge('bridging', 100)]
    ]) {
      expect(order(entries).history).toEqual(['withdrawing', 'bridging'])
    }
  })

  it('orders a failed entry by time, so a recovered bridge shows its newer status', () => {
    expect(
      order([bridge('bridging', 300), bridge('failed', 200), bridge('bridging', 100)])
    ).toEqual({ status: 'bridging', first: 100, history: ['bridging', 'failed', 'bridging'] })
    expect(order([bridge('failed', 200), bridge('depositing', 100)]).status).toBe('failed')
  })

  it('orders other rows by time, ties newest last as Cloud Logging returned them', () => {
    const mint = (status: string, time: number): StatusEntry => ({ time, kind: 'mint', status })
    expect(
      order([mint('wrapping', 100), mint('minting', 100), mint('pending', 50)]).history
    ).toEqual(['pending', 'minting', 'wrapping'])
    expect(statusHistory.latest([])).toBeNull()
  })
})
