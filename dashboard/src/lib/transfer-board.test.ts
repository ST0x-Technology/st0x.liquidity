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
// drifts from this file's builders.

const panels = new URL('../../../observability/liquidity-panels/', import.meta.url)

type BoardCommand = { command: string; description: string; mode: RecoveryMode }
type BoardTransfer = { kind: TransferCategory; id: string; status: string; direction?: string }
type BoardBuilders = {
  tradeCommands: (client: string, symbol: string) => BoardCommand[]
  transferCommands: (client: string, transfer: BoardTransfer) => BoardCommand[]
}

// A plain script in the board, an ES module through its export line here.
const board = (await import(
  new URL('recovery-commands.js', panels).href
)) as BoardBuilders

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
    for (const mode of ['requires-bot', 'live-rpc-only', 'direct-db-live-rpc', 'direct-db'] as const) {
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
      for (const status of ['converting', 'withdrawing', 'bridging', 'depositing', 'failed', 'completed']) {
        expect(
          board.transferCommands(CLIENT, {
            kind: 'usdc_bridge',
            id: 'B1',
            status,
            ...(direction === null ? {} : { direction })
          })
        ).toEqual(
          withoutLabel(
            transferRecoveryCommands({ deployment: PROD, kind: 'usdc_bridge', id: 'B1', status, direction })
          )
        )
      }
    }
  })
})
