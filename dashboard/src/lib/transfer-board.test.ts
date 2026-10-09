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

type StatusEntry = {
  time: number
  kind: string
  status: string
  direction?: string
  event_id?: string
}
type StatusHistory = {
  latest: (
    entries: StatusEntry[]
  ) => (StatusEntry & { first: number; history: StatusEntry[] }) | null
}

// A plain script in the board, an ES module through its export line here.
const statusHistory = (await import(new URL('status-history.js', panels).href)) as StatusHistory

type Frame = { fields: { name: string; values: unknown[] }[] }
type Line = { time: number; [key: string]: unknown }
type LogLines = {
  lineEntries: (frame: Frame | undefined, id: string, project: string) => Line[]
  eventTimeline: (entries: Line[], parent: string, kind?: string) => Line[]
  timelineIncomplete: (events: Line[], rowEventId?: string) => boolean
  blockingErrors: (errors: { refId?: string }[], source: string) => { refId?: string }[]
  queryState: (
    panelData: PanelData | undefined,
    source: string
  ) => {
    listed: QueryError[]
    blocking: QueryError[]
    failed: boolean
    unsure: boolean
    eventsFailed: boolean
  }
}
type QueryError = { refId?: string; message?: string }
type PanelData = { state?: string; errors?: QueryError[]; error?: QueryError }

// A plain script in the board, an ES module through its export line here.
const logLines = (await import(new URL('log-lines.js', panels).href)) as LogLines

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

  it("orders the bot's lines by their event sequence, whatever their times", () => {
    const line = (status: string, time: number, sequence: number): StatusEntry => ({
      ...bridge(status, time),
      event_id: `UsdcRebalance:B1:${String(sequence)}`
    })
    expect(
      order([line('converting', 300, 1), line('bridging', 100, 12), line('withdrawing', 200, 4)])
    ).toEqual({
      status: 'bridging',
      first: 100,
      history: ['converting', 'withdrawing', 'bridging']
    })
  })

  it('orders other rows by time, ties newest last as Cloud Logging returned them', () => {
    const mint = (status: string, time: number): StatusEntry => ({ time, kind: 'mint', status })
    expect(
      order([mint('wrapping', 100), mint('minting', 100), mint('pending', 50)]).history
    ).toEqual(['pending', 'minting', 'wrapping'])
    expect(statusHistory.latest([])).toBeNull()
  })
})

describe('board log-lines.js', () => {
  // The plugin's log-lines frame: newest first, `labels` an object per row.
  const frame = (
    rows: { time: number; body: string; labels: Record<string, string> }[]
  ): Frame => ({
    fields: [
      { name: 'timestamp', values: rows.map((row) => row.time) },
      { name: 'body', values: rows.map((row) => row.body) },
      { name: 'labels', values: rows.map((row) => row.labels) }
    ]
  })
  const botLine = (time: number, fields: Record<string, string>, project = 't0-liquidity') => ({
    time,
    body: 'Transfer status changed',
    labels: {
      'resource.labels.project_id': project,
      'jsonPayload.target': 'liq_transfer',
      ...Object.fromEntries(
        Object.entries(fields).map(([key, value]) => [`jsonPayload.${key}`, value])
      )
    }
  })

  it("reads a bot line's fields from its labels and drops a repeated event_id", () => {
    const lines = frame([
      botLine(300, { event_id: 'UsdcRebalance:B1:3', id: 'B1', status: 'bridging', usd: '10' }),
      botLine(300, { event_id: 'UsdcRebalance:B1:3', id: 'B1', status: 'bridging', usd: '10' }),
      botLine(200, { event_id: 'UsdcRebalance:B1:2', id: 'B1', status: 'withdrawing' }),
      botLine(100, { event_id: 'UsdcRebalance:B2:1', id: 'B2', status: 'converting' })
    ])
    expect(logLines.lineEntries(lines, 'B1', 't0-liquidity')).toEqual([
      {
        time: 300,
        target: 'liq_transfer',
        event_id: 'UsdcRebalance:B1:3',
        id: 'B1',
        status: 'bridging',
        usd: '10'
      },
      {
        time: 200,
        target: 'liq_transfer',
        event_id: 'UsdcRebalance:B1:2',
        id: 'B1',
        status: 'withdrawing'
      }
    ])
  })

  it("reads an exporter entry's JSON body and drops another project's lines", () => {
    const exporter = (time: number, project: string) => ({
      time,
      body: JSON.stringify({ id: 'T1', status: 'filled' }),
      labels: { 'resource.labels.project_id': project, 'jsonPayload.id': 'T1' }
    })
    const lines = frame([exporter(200, 't0-liquidity-staging'), exporter(100, 't0-liquidity')])
    expect(logLines.lineEntries(lines, 'T1', 't0-liquidity')).toEqual([
      { time: 100, id: 'T1', status: 'filled' }
    ])
    expect(logLines.lineEntries(undefined, 'T1', 't0-liquidity')).toEqual([])
  })

  it("orders a row's events by sequence, parses their payloads and keeps the row's kind", () => {
    const event = (sequence: string, parent: string, kind: string, payload: string): Line => ({
      time: 1,
      id: 'X1',
      parent,
      kind,
      sequence,
      step: `Step${sequence}`,
      payload
    })
    const events = [
      event('10', 'transfer', 'equity_mint', '{"tx_hash":"0x1"}'),
      event('2', 'transfer', 'equity_mint', 'not json'),
      event('1', 'transfer', 'equity_redemption', '{}'),
      event('3', 'trade', '', '{}')
    ]
    expect(
      logLines
        .eventTimeline(events, 'transfer', 'equity_mint')
        .map(({ step, payload }) => ({ step, payload }))
    ).toEqual([
      { step: 'Step2', payload: {} },
      { step: 'Step10', payload: { tx_hash: '0x1' } }
    ])
    expect(logLines.eventTimeline(events, 'trade').map(({ step }) => step)).toEqual(['Step3'])
  })

  it('flags a timeline that does not start at 1, skips one or ends before the row', () => {
    const at = (...sequences: number[]): Line[] =>
      sequences.map((sequence) => ({ time: 1, sequence: String(sequence) }))
    expect(logLines.timelineIncomplete(at(1, 2, 3))).toBe(false)
    expect(logLines.timelineIncomplete(at(2, 3))).toBe(true)
    expect(logLines.timelineIncomplete(at(1, 3))).toBe(true)
    expect(logLines.timelineIncomplete([])).toBe(false)
    expect(logLines.timelineIncomplete(at(1, 2), 'UsdcRebalance:B1:2')).toBe(false)
    expect(logLines.timelineIncomplete(at(1, 2), 'UsdcRebalance:B1:3')).toBe(true)
  })

  it("blocks the dialog only on errors of the source's own queries", () => {
    const errors = [
      { refId: 'exporter-trades' },
      { refId: 'bot-trades' },
      { refId: 'bot-events' },
      {}
    ]
    expect(logLines.blockingErrors(errors, 'exporter')).toEqual([{ refId: 'exporter-trades' }, {}])
    expect(logLines.blockingErrors(errors, 'bot')).toEqual([{ refId: 'bot-trades' }, {}])
  })

  it('marks a row as possibly out of date when only errors that do not block are listed', () => {
    // Grafana keeps only the last failing query's errors, so a failed
    // bot-trades can hide behind a failed bot-events.
    const onlyEvents = { state: 'Error', errors: [{ refId: 'bot-events', message: 'quota' }] }
    expect(logLines.queryState(onlyEvents, 'bot')).toEqual({
      listed: [{ refId: 'bot-events', message: 'quota' }],
      blocking: [],
      failed: false,
      unsure: true,
      eventsFailed: true
    })
    const otherSource = { state: 'Error', errors: [{ refId: 'bot-trades' }] }
    expect(logLines.queryState(otherSource, 'exporter')).toMatchObject({
      failed: false,
      unsure: true,
      eventsFailed: false
    })
  })

  it('fails on a blocking error or an Error state without errors, and reads a single error', () => {
    const blocking = { state: 'Error', errors: [{ refId: 'bot-events' }, { refId: 'bot-trades' }] }
    expect(logLines.queryState(blocking, 'bot')).toMatchObject({
      failed: true,
      unsure: false,
      eventsFailed: true
    })
    expect(logLines.queryState({ state: 'Error' }, 'bot')).toMatchObject({
      failed: true,
      unsure: false
    })
    const single = { state: 'Error', error: { refId: 'bot-events' } }
    expect(logLines.queryState(single, 'bot')).toMatchObject({
      failed: false,
      unsure: true,
      eventsFailed: true
    })
    expect(logLines.queryState({ state: 'Done' }, 'bot')).toMatchObject({
      failed: false,
      unsure: false,
      eventsFailed: false
    })
    expect(logLines.queryState(undefined, 'bot')).toMatchObject({ failed: false, unsure: false })
  })
})
