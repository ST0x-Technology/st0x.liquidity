import { readFileSync } from 'node:fs'
import { describe, expect, it } from 'vitest'

// The board's recovery text is its own: recovery-guide.json for the guide,
// recovery-commands.js for the per-row dialog, and client-env.js for the
// client prefix. These tests pin what an operator reads there and write
// every command shown to the two files the Rust parser tests read. They also
// cover status-history.js, which orders a row's status entries in its
// dialog, and log-lines.js, which reads its log lines.

const panels = new URL('../liquidity-panels/', import.meta.url)

type RecoveryMode = 'requires-bot' | 'direct-db'
type TransferCategory = 'equity_mint' | 'equity_redemption' | 'usdc_bridge'
type BoardCommand = { command: string; description: string; mode: RecoveryMode }
type BoardTransfer = { kind: TransferCategory; id: string; status: string; direction?: string }
type BoardBuilders = {
  tradeCommands: (client: string, symbol: string) => BoardCommand[]
  transferCommands: (client: string, transfer: BoardTransfer) => BoardCommand[]
  usdcFailedNote: (direction?: string) => string
  USDC_RECONCILE_BASE_TO_ALPACA: string
}

// A plain script in the board, an ES module through its export line here.
const board = (await import(new URL('recovery-commands.js', panels).href)) as BoardBuilders

type ClientEnv = {
  LIQUIDITY_CLIENT: string
  boardEnv: (envText: string) => 'production' | 'staging'
  forClientEnv: (command: string, env: 'production' | 'staging') => string
  clientFor: (envText: string) => string
}

// A plain script in the board, an ES module through its export line here.
const clientEnv = (await import(new URL('client-env.js', panels).href)) as ClientEnv

type Guide = {
  modeLabels: Record<RecoveryMode, string>
  groups: { object: string; commands: BoardCommand[] }[]
}
const guide = JSON.parse(readFileSync(new URL('recovery-guide.json', panels), 'utf8')) as Guide

// Every status the bot's transfer lines carry.
const EQUITY_STATUSES = [
  'minting',
  'wrapping',
  'depositing',
  'sending',
  'withdrawing',
  'unwrapping',
  'pending_confirmation',
  'failed',
  'completed',
  'reconciled'
]
const USDC_STATUSES = [
  'converting',
  'withdrawing',
  'bridging',
  'depositing',
  'failed',
  'completed',
  'reconciled'
]
const USDC_DIRECTIONS = ['alpaca_to_base', 'base_to_alpaca', undefined]

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
  ) => (StatusEntry & { first: number; newest: number; history: StatusEntry[] }) | null
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

// Every command the board's dialogs show, for every row it can open.
const dialogCommands = (): BoardCommand[] => {
  const usdc = (status: string, direction: string | undefined): BoardTransfer => ({
    kind: 'usdc_bridge',
    // A USDC rebalance id is a UUID, which the stox parser checks.
    id: '00000000-0000-4000-8000-000000000001',
    status,
    ...(direction === undefined ? {} : { direction })
  })
  return [
    ...board.tradeCommands(CLIENT, 'MSTR'),
    ...(['equity_mint', 'equity_redemption'] as const).flatMap((kind) =>
      EQUITY_STATUSES.flatMap((status) =>
        board.transferCommands(CLIENT, { kind, id: 'X1', status })
      )
    ),
    ...USDC_DIRECTIONS.flatMap((direction) =>
      USDC_STATUSES.flatMap((status) => board.transferCommands(CLIENT, usdc(status, direction)))
    )
  ]
}

// The text an operator reads in the guide and the dialogs.
const recoveryText = (): string[] => [
  ...guide.groups.flatMap((group) => group.commands.map(({ description }) => description)),
  ...dialogCommands().map(({ description }) => description),
  ...USDC_DIRECTIONS.map((direction) => board.usdcFailedNote(direction))
]

describe('board dialog commands', () => {
  it('match the file snapshot for every row the dialog can open', async () => {
    // Which command each row's status gets, and how it runs, keyed by kind,
    // status and direction. A command moved to another status or another run
    // mode changes this file, which a reviewer then reads.
    const table: Record<string, { command: string; mode: RecoveryMode }[]> = {}
    const pick = (commands: BoardCommand[]) =>
      commands.map(({ command, mode }) => ({ command, mode }))
    table['trade'] = pick(board.tradeCommands(CLIENT, 'MSTR'))
    for (const kind of ['equity_mint', 'equity_redemption'] as const) {
      for (const status of EQUITY_STATUSES) {
        table[`${kind} ${status}`] = pick(
          board.transferCommands(CLIENT, { kind, id: 'X1', status })
        )
      }
    }
    for (const direction of USDC_DIRECTIONS) {
      for (const status of USDC_STATUSES) {
        table[`usdc_bridge ${status} ${direction ?? 'no-direction'}`] = pick(
          board.transferCommands(CLIENT, {
            kind: 'usdc_bridge',
            id: 'B1',
            status,
            ...(direction === undefined ? {} : { direction })
          })
        )
      }
    }
    await expect(JSON.stringify(table, null, 2) + '\n').toMatchFileSnapshot(
      '__snapshots__/dialog-commands.json'
    )
  })
})

describe('board recovery-guide.json', () => {
  it('labels each run mode', () => {
    expect(guide.modeLabels).toEqual({
      'requires-bot': 'REST — requires the running bot',
      'direct-db': 'direct DB — stop the bot / ensure it is not driving this id'
    })
  })

  it('gives every command a description and a known run mode', () => {
    expect(guide.groups.length).toBeGreaterThan(0)
    for (const { command, description, mode } of guide.groups.flatMap((group) => group.commands)) {
      expect(command.startsWith('st0x-liquidity-client ')).toBe(true)
      expect(description.length).toBeGreaterThan(0)
      expect(Object.keys(guide.modeLabels)).toContain(mode)
    }
  })
})

describe('board failed-bridge note', () => {
  it('tells the operator to check the transfer and keeps the stop-first rule', () => {
    // Pinned word for word, apart from the builders, so an edit that drops the
    // instruction fails here instead of changing the note and its check together.
    expect(board.usdcFailedNote('base_to_alpaca')).toBe(
      'Reconcile applies only when the funds left their source venue, which the logs do not ' +
        "show. Check its events with the client's read transfer-events before you reconcile it. " +
        'Base to Alpaca after its burn (not a deposit or conversion failure): unless the mint ' +
        'was reported unresolvable or funds were already moved by hand, keep the bot running ' +
        'and run resume-usdc base-to-alpaca, which re-polls Circle, then mints and sends. ' +
        'Otherwise do not mint, send or reconcile it with the bot running: the bot re-runs its ' +
        'recovery at every start and can send the USDC to Alpaca itself, with no check for an ' +
        'earlier send. Follow docs/cli-ops.md, "Settling a post-burn Base to Alpaca failure by hand".'
    )
  })

  it('carries the Base to Alpaca reconcile rule only for a Base to Alpaca bridge', () => {
    // The board offers no USDC reconcile row, so its dialog note is the only
    // place an operator reads the stop-first rule there.
    for (const direction of ['base_to_alpaca', undefined]) {
      expect(board.usdcFailedNote(direction).endsWith(board.USDC_RECONCILE_BASE_TO_ALPACA)).toBe(
        true
      )
    }
    expect(
      board.usdcFailedNote('alpaca_to_base').includes(board.USDC_RECONCILE_BASE_TO_ALPACA)
    ).toBe(false)
  })
})

describe('board client-env.js', () => {
  it('runs the client against the environment the board shows', () => {
    expect(clientEnv.LIQUIDITY_CLIENT).toBe('st0x-liquidity-client --env production')
    expect(clientEnv.clientFor('production')).toBe('st0x-liquidity-client --env production')
    expect(clientEnv.clientFor('staging')).toBe('st0x-liquidity-client --env staging')
    expect(
      clientEnv.forClientEnv('st0x-liquidity-client --env production debug resume', 'staging')
    ).toBe('st0x-liquidity-client --env staging debug resume')
  })

  it('falls back to production for any other selector text', () => {
    // Grafana gives the variable's text; only staging is not production.
    expect(clientEnv.boardEnv('staging')).toBe('staging')
    expect(clientEnv.boardEnv('production')).toBe('production')
    expect(clientEnv.boardEnv('')).toBe('production')
  })
})

describe('runbook headings the recovery text names', () => {
  it('exist in docs/cli-ops.md', () => {
    // The fail gates send operators to these sections by name, so a renamed
    // heading must fail here instead of leaving every gate pointing nowhere.
    const runbook = readFileSync(new URL('../../docs/cli-ops.md', import.meta.url), 'utf8')
    const headings = runbook
      .split('\n')
      .filter((line) => line.startsWith('#'))
      .map((line) => line.replace(/^#+\s*/, ''))
    const named = new Set(
      recoveryText().flatMap((text) =>
        [...text.matchAll(/docs\/cli-ops\.md, "([^"]+)"/g)].map((match) => match[1] ?? '')
      )
    )
    expect([...named].sort()).toEqual([
      'Clearing a pre-burn guard latch',
      'Settling a post-burn Base to Alpaca failure by hand'
    ])
    for (const heading of named) expect(headings).toContain(heading)
  })
})

// A test value for each placeholder the recovery text leaves to the operator,
// so the stox parser can read the command.
const STOX_VALUES: Record<string, string> = {
  '<amount>': '1',
  '<burn>': `0x${'0'.repeat(64)}`,
  '<id>': '00000000-0000-4000-8000-000000000001'
}

// Every client command the board shows, and every offline stox command its
// text spells out, written to the files that the client crate's
// `every_shown_recovery_command_parses` test and the cli crate's stox test
// parse with the real clap parsers. A changed command fails here until the
// file is regenerated (vitest -u), and the Rust tests then check that it
// still parses.
describe('shown recovery commands', () => {
  it('match the files the parser tests read', async () => {
    const shown = new Set<string>()
    for (const { command } of [
      ...dialogCommands(),
      ...guide.groups.flatMap((group) => group.commands)
    ]) {
      if (command.startsWith('st0x-liquidity-client ')) shown.add(command)
    }
    // A stox command in the text: its words, flags and placeholders, up to
    // the punctuation or prose that follows it. A mention without a flag,
    // like "the offline stox fail-usdc-transfer", is not a command.
    const word = String.raw`(?:[a-z][\w-]*|--?[\w-]+|<[^>\s]+>)`
    const stox = new RegExp(String.raw`\bstox ${word}(?: ${word})*`, 'g')
    const offline = new Set(
      recoveryText()
        .flatMap((text) => [...text.matchAll(stox)].map(([command]) => command))
        .filter((command) => / -/.test(command))
        .map((command) =>
          command.replace(/<[^>|]+>/g, (placeholder) => STOX_VALUES[placeholder] ?? placeholder)
        )
    )
    const header = [
      '# Every st0x-liquidity-client command the board shows.',
      '# Written by observability/tests/recovery.test.ts; do not edit by hand.'
    ]
    await expect([...header, ...[...shown].sort()].join('\n') + '\n').toMatchFileSnapshot(
      '../../crates/liquidity-client/testdata/shown-commands.txt'
    )
    const offlineHeader = [
      '# Every offline stox command the board text gives; crates/cli parses each with st0x-cli.',
      '# Written by observability/tests/recovery.test.ts; do not edit by hand.'
    ]
    expect(offline.size).toBeGreaterThan(0)
    await expect([...offlineHeader, ...[...offline].sort()].join('\n') + '\n').toMatchFileSnapshot(
      '../../crates/cli/testdata/shown-stox-commands.txt'
    )
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

  it('gives the newest entry time apart from the time of the latest status', () => {
    const row = statusHistory.latest([bridge('withdrawing', 200), bridge('bridging', 100)])
    expect(row).toMatchObject({ status: 'bridging', time: 100, first: 100, newest: 200 })
    const line = (status: string, time: number, sequence: number): StatusEntry => ({
      ...bridge(status, time),
      event_id: `UsdcRebalance:B1:${String(sequence)}`
    })
    expect(
      statusHistory.latest([line('withdrawing', 300, 2), line('bridging', 250, 3)])
    ).toMatchObject({ status: 'bridging', first: 250, newest: 300 })
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
