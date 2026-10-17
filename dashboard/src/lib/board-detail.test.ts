// @vitest-environment happy-dom

import { readFileSync } from 'node:fs'
import { resolve } from 'node:path'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

// The Grafana board's row dialog, as deployed: the detail panel's afterRender
// from the generated board JSON, run the way the Business Text plugin runs
// it, with the Trades and Rebalances frames the Dashboard datasource hands
// it.

type Panel = {
  type: string
  options?: { renderMode?: string; afterRender?: string }
}

// happy-dom replaces the global URL, so the path is built with node:path.
const boardJson = readFileSync(
  resolve(import.meta.dirname, '../../../observability/dashboards/liquidity/t0-liquidity.json'),
  'utf8'
)
const board = JSON.parse(boardJson) as { panels: Panel[] }
const detailSource = board.panels.find(
  (panel) => panel.type === 'marcusolsson-dynamictext-panel' && panel.options?.renderMode === 'data'
)?.options?.afterRender

if (detailSource === undefined) throw new Error('the board has no detail panel')

// eslint-disable-next-line @typescript-eslint/no-implied-eval -- runs the deployed script as the plugin does
const renderDetail = new Function('context', detailSource) as (context: unknown) => void

type Entry = { time: number; payload: Record<string, string> }

// The plugin's log-lines frame, newest first, each entry's payload as its
// JSON body.
const frame = (refId: string, entries: Entry[]) => {
  const newestFirst = [...entries].sort((left, right) => right.time - left.time)
  return {
    refId,
    fields: [
      { name: 'timestamp', values: newestFirst.map((entry) => entry.time) },
      { name: 'body', values: newestFirst.map((entry) => JSON.stringify(entry.payload)) },
      {
        name: 'labels',
        values: newestFirst.map((entry) => ({
          'resource.labels.project_id': 't0-liquidity',
          'jsonPayload.id': entry.payload['id']
        }))
      }
    ]
  }
}

const TX_HASH = `0x${'ab'.repeat(32)}`
const OCT_6 = Date.UTC(2026, 9, 6, 12, 0, 0)
const minutes = (count: number) => OCT_6 + count * 60_000

const trades = [
  {
    time: minutes(0),
    payload: {
      id: `base:${TX_HASH}:7`,
      symbol: 'AAPL',
      direction: 'buy',
      shares: '1.5',
      venue: 'raindex',
      status: 'filled'
    }
  }
]
const transfers = [
  {
    time: minutes(1),
    payload: { id: 'M1', kind: 'equity_mint', symbol: 'TSLA', amount: '2', status: 'minting' }
  },
  {
    time: minutes(5),
    payload: { id: 'M1', kind: 'equity_mint', symbol: 'TSLA', amount: '2', status: 'completed' }
  },
  // WithdrawalComplete carries confirmed_at, the BridgingSubmitting after it
  // the older initiated_at, so the latest status is not the newest entry.
  {
    time: minutes(9),
    payload: {
      id: 'B1',
      kind: 'usdc_bridge',
      direction: 'alpaca_to_base',
      amount: '100',
      status: 'withdrawing'
    }
  },
  {
    time: minutes(3),
    payload: {
      id: 'B1',
      kind: 'usdc_bridge',
      direction: 'alpaca_to_base',
      amount: '100',
      status: 'bridging'
    }
  },
  {
    time: minutes(2),
    payload: { id: 'U1', kind: 'usdc_bridge', amount: '50', status: 'bridging' }
  }
]

// The board's newest event lines; a test sets the ones it needs.
let boardEvents: Entry[] = []

// M1's mint events, as the bot logs them.
const mintEvent = (sequence: number, step: string, time: number): Entry => ({
  time,
  payload: {
    id: 'M1',
    parent: 'transfer',
    kind: 'equity_mint',
    sequence: String(sequence),
    step,
    payload: '{}',
    event_id: `EquityMint:M1:${String(sequence)}`
  }
})

// Grafana's /api/ds/query answer for the row query: `entries` as one
// log-lines frame, or an error.
const rowQueryAnswer = (entries: Entry[], error?: string) => {
  const lines = frame('row-events', entries)
  return new Response(
    JSON.stringify({
      results: {
        'row-events':
          error !== undefined
            ? { error }
            : {
                frames: [
                  {
                    schema: { fields: lines.fields.map(({ name }) => ({ name })) },
                    data: { values: lines.fields.map(({ values }) => values) }
                  }
                ]
              }
      }
    }),
    { status: 200, headers: { 'Content-Type': 'application/json' } }
  )
}
let fetchSpy: ReturnType<typeof vi.spyOn>

const variables: Record<string, string> = {}
const partial = vi.fn()
let root: HTMLDivElement

const render = (detail: string) => {
  variables['detail'] = detail
  renderDetail({
    element: root,
    panelData: {
      state: 'Done',
      series: [
        frame('bot-trades', trades),
        frame('bot-transfers', transfers),
        frame('bot-events', boardEvents)
      ]
    },
    grafana: {
      theme: {
        isDark: true,
        colors: {
          text: { primary: '#fff', secondary: '#aaa' },
          border: { weak: '#333' },
          background: { primary: '#111', secondary: '#222' }
        }
      },
      replaceVariables: (text: string) =>
        text.replace(/\$\{([^}]+)\}/g, (_match, name: string) => variables[name] ?? ''),
      locationService: { partial }
    }
  })
  return root.querySelector('dialog')
}

// A browser closes a dialog at once and queues its close event as a task;
// happy-dom fires the event inside close(). The tests queue it on a timer,
// so a render can run before it, and vi.advanceTimersByTime(0) runs it.
const queueCloseEvents = () =>
  vi.spyOn(HTMLDialogElement.prototype, 'close').mockImplementation(function (
    this: HTMLDialogElement,
    returnValue?: string
  ) {
    const wasOpen = this.open
    this.open = false
    this.returnValue = returnValue ?? ''
    if (wasOpen) setTimeout(() => this.dispatchEvent(new Event('close')), 0)
  })
let closeSpy: ReturnType<typeof queueCloseEvents>

beforeEach(() => {
  vi.useFakeTimers()
  closeSpy = queueCloseEvents()
  Object.assign(variables, {
    env: 't0-liquidity',
    'env:text': 'production',
    'source:text': 'bot'
  })
  boardEvents = []
  fetchSpy = vi.spyOn(window, 'fetch').mockResolvedValue(rowQueryAnswer([]))
  root = document.createElement('div')
  document.body.appendChild(root)
})

afterEach(() => {
  root.remove()
  vi.runOnlyPendingTimers()
  vi.useRealTimers()
  closeSpy.mockRestore()
  fetchSpy.mockRestore()
  partial.mockClear()
})

describe('board detail.js', () => {
  it("shows a trade row's fields, explorer link and commands", () => {
    const dialog = render(`base:${TX_HASH}:7`)
    expect(dialog?.open).toBe(true)
    const html = dialog?.innerHTML ?? ''
    expect(html).toContain('AAPL')
    expect(html).toContain('1.500 wrapped shares')
    expect(html).toContain('Raindex')
    expect(html).toContain(`https://basescan.org/tx/${TX_HASH}`)
    expect(html).toContain('Filled')
    expect(html).toContain(
      'st0x-liquidity-client --env production debug position release-hedge AAPL'
    )
  })

  it("shows a transfer row's status history, newest status last", () => {
    const dialog = render('M1')
    expect(dialog?.querySelector('.det-title-line')?.textContent).toContain('Mint')
    expect(dialog?.innerHTML).toContain('Status history')
    const steps = [...(dialog?.querySelectorAll('.det-timeline .det-step') ?? [])].map((step) =>
      step.textContent.replace(/\s+/g, ' ').trim()
    )
    expect(steps).toEqual(['Minting Oct 6, 12:01:00 UTC', 'Completed Oct 6, 12:05:00 UTC'])
  })

  it('shows the newest entry time as Updated, not the latest status time', () => {
    const fields = [...(render('B1')?.querySelectorAll('.det-field') ?? [])]
    const updated = fields.find((field) => field.textContent.includes('Updated'))
    expect(updated?.textContent).toContain('Oct 6, 12:09:00 UTC')
    const status = fields.find((field) => field.textContent.includes('Status'))
    expect(status?.textContent).toContain('Bridging')
  })

  it('labels a USDC bridge without a direction as a USDC bridge', () => {
    const title = render('U1')?.querySelector('.det-title-line')?.textContent ?? ''
    expect(title).toContain('USDC Bridge')
    expect(title).not.toContain('usdc_bridge')
  })

  it('puts a Copy button beside each command that copies it', async () => {
    const writeText = vi.fn(() => Promise.resolve())
    Object.defineProperty(navigator, 'clipboard', { value: { writeText }, configurable: true })
    const dialog = render(`base:${TX_HASH}:7`)
    const commands = [...(dialog?.querySelectorAll('.det-command') ?? [])]
    expect(commands.length).toBeGreaterThan(0)
    for (const command of commands) {
      expect(command.querySelector('.liq-command-line > pre + button[data-copy]')).not.toBeNull()
    }
    const button = commands[0]?.querySelector<HTMLButtonElement>('[data-copy]')
    button?.click()
    expect(writeText).toHaveBeenCalledWith(commands[0]?.querySelector('pre')?.textContent)
    await vi.waitFor(() => {
      expect(button?.textContent).toBe('Copied')
    })
    vi.advanceTimersByTime(1500)
    expect(button?.textContent).toBe('Copy')
  })

  it("fetches a row's events over its own span when the board has none of them", async () => {
    fetchSpy.mockResolvedValue(
      rowQueryAnswer([
        mintEvent(1, 'MintRequested', minutes(1)),
        mintEvent(2, 'MintCompleted', minutes(5))
      ])
    )
    const dialog = render('M1')
    expect(dialog?.innerHTML).toContain('Loading this row')

    const [url, init] = fetchSpy.mock.calls[0] as [string, RequestInit]
    expect(url).toBe('/api/ds/query')
    const body = JSON.parse(init.body as string) as {
      queries: { queryText: string; projectId: string }[]
      from: string
      to: string
    }
    expect(body.queries[0]?.queryText).toContain('jsonPayload.id="M1"')
    expect(body.queries[0]?.projectId).toBe('t0-liquidity')
    // Its first and newest status lines, ten minutes either side.
    expect(body.from).toBe(String(minutes(1) - 600_000))
    expect(body.to).toBe(String(minutes(5) + 600_000))

    await vi.waitFor(() => {
      expect(dialog?.innerHTML).toContain('Event timeline')
    })
    const steps = [...(dialog?.querySelectorAll('.det-timeline .det-step') ?? [])].map((step) =>
      step.textContent.replace(/\s+/g, ' ').trim()
    )
    expect(steps).toEqual([
      'Mint Requested Oct 6, 12:01:00 UTC',
      'Mint Completed Oct 6, 12:05:00 UTC'
    ])
    expect(dialog?.innerHTML).not.toContain('SPA')

    // A refresh with the same row and status uses what it fetched.
    render('M1')
    expect(fetchSpy).toHaveBeenCalledTimes(1)
  })

  it("reads back once over the board's range when the row's first events are older", async () => {
    fetchSpy
      .mockResolvedValueOnce(rowQueryAnswer([mintEvent(2, 'MintCompleted', minutes(5))]))
      .mockResolvedValueOnce(
        rowQueryAnswer([
          mintEvent(1, 'MintRequested', minutes(-600)),
          mintEvent(2, 'MintCompleted', minutes(5))
        ])
      )
    const dialog = render('M1')

    await vi.waitFor(() => {
      expect(dialog?.innerHTML).toContain('Event timeline')
    })
    const froms = fetchSpy.mock.calls.map(
      (call: unknown[]) =>
        (JSON.parse((call[1] as RequestInit).body as string) as { from: string }).from
    )
    expect(froms).toEqual([
      String(minutes(1) - 600_000),
      // The board's 30 days back from the newest status, less the margin.
      String(minutes(5) - 30 * 24 * 60 * 60_000 - 600_000)
    ])
    const steps = [...(dialog?.querySelectorAll('.det-timeline .det-step') ?? [])].map((step) =>
      step.textContent.replace(/\s+/g, ' ').trim()
    )
    expect(steps).toEqual([
      'Mint Requested Oct 6, 02:00:00 UTC',
      'Mint Completed Oct 6, 12:05:00 UTC'
    ])
  })

  it('reads back over 30 days again when the row opens after that read failed', async () => {
    const DAY = 24 * 60 * 60_000
    const from = (call: unknown[]) =>
      Number((JSON.parse((call[1] as RequestInit).body as string) as { from: string }).from)
    fetchSpy
      .mockResolvedValueOnce(rowQueryAnswer([mintEvent(2, 'MintCompleted', minutes(5))]))
      .mockResolvedValueOnce(rowQueryAnswer([], 'quota exceeded'))
    const dialog = render('M1')
    await vi.waitFor(() => {
      expect(dialog?.innerHTML).toContain("This row's event query failed (quota exceeded)")
    })

    dialog?.querySelector<HTMLButtonElement>('[data-close]')?.click()
    render('')
    fetchSpy.mockResolvedValue(rowQueryAnswer([mintEvent(2, 'MintCompleted', minutes(5))]))
    render('M1')
    await vi.waitFor(() => {
      expect(fetchSpy).toHaveBeenCalledTimes(4)
    })

    const longReads = fetchSpy.mock.calls.filter((call: unknown[]) => from(call) < minutes(0) - DAY)
    expect(longReads).toHaveLength(2)
  })

  it("keeps an in-flight row's old events across its minute reads, with one 30-day read", async () => {
    const DAY = 24 * 60 * 60_000
    // B1's bridge events: the first one a day old, before the board's rows.
    const bridgeEvent = (sequence: number, step: string, time: number): Entry => ({
      time,
      payload: {
        id: 'B1',
        parent: 'transfer',
        kind: 'usdc_bridge',
        sequence: String(sequence),
        step,
        payload: '{}',
        event_id: `UsdcBridge:B1:${String(sequence)}`
      }
    })
    const from = (call: unknown[]) =>
      Number((JSON.parse((call[1] as RequestInit).body as string) as { from: string }).from)
    fetchSpy.mockImplementation((...call: unknown[]) =>
      Promise.resolve(
        rowQueryAnswer(
          from(call) < minutes(0) - DAY
            ? [
                bridgeEvent(1, 'OldestEvent', minutes(-DAY / 60_000)),
                bridgeEvent(2, 'Bridging', minutes(3))
              ]
            : [bridgeEvent(2, 'Bridging', minutes(3))]
        )
      )
    )
    vi.setSystemTime(minutes(60))
    const dialog = render('B1')
    await vi.waitFor(() => {
      expect(dialog?.innerHTML).toContain('Oldest Event')
    })
    expect(fetchSpy).toHaveBeenCalledTimes(2)

    vi.setSystemTime(minutes(61))
    const next = render('B1')
    // The kept events show while the minute's read loads.
    expect(next?.innerHTML).toContain('Oldest Event')
    await vi.waitFor(() => {
      expect(fetchSpy).toHaveBeenCalledTimes(3)
    })
    await vi.waitFor(() => {
      expect(render('B1')?.innerHTML).not.toContain('Loading this row')
    })
    const froms = fetchSpy.mock.calls.map((call: unknown[]) => from(call))
    expect(froms.filter((at: number) => at < minutes(0) - DAY)).toHaveLength(1)
  })

  it('shows the events a slow 30-day read found once the next minute read lands', async () => {
    const DAY = 24 * 60 * 60_000
    const bridgeEvent = (sequence: number, step: string, time: number): Entry => ({
      time,
      payload: {
        id: 'B1',
        parent: 'transfer',
        kind: 'usdc_bridge',
        sequence: String(sequence),
        step,
        payload: '{}',
        event_id: `UsdcBridge:B1:${String(sequence)}`
      }
    })
    const from = (call: unknown[]) =>
      Number((JSON.parse((call[1] as RequestInit).body as string) as { from: string }).from)
    const pending: { long: boolean; answer: (response: Response) => void }[] = []
    fetchSpy.mockImplementation(
      (...call: unknown[]) =>
        new Promise<Response>((answer) => {
          pending.push({ long: from(call) < minutes(0) - DAY, answer })
        })
    )
    const recent = [bridgeEvent(2, 'Bridging', minutes(3))]
    vi.setSystemTime(minutes(60))
    render('B1')
    pending[0]?.answer(rowQueryAnswer(recent))
    await vi.waitFor(() => {
      expect(pending).toHaveLength(2)
    })
    expect(pending[1]?.long).toBe(true)

    // The 30-day read is still out when the next minute's read starts.
    vi.setSystemTime(minutes(61))
    render('B1')
    expect(pending).toHaveLength(3)
    pending[1]?.answer(
      rowQueryAnswer([bridgeEvent(1, 'OldestEvent', minutes(-DAY / 60_000)), ...recent])
    )
    await vi.advanceTimersByTimeAsync(0)
    pending[2]?.answer(rowQueryAnswer(recent))

    await vi.waitFor(() => {
      expect(render('B1')?.innerHTML).not.toContain('Loading this row')
    })
    expect(render('B1')?.innerHTML).toContain('Oldest Event')
  })

  it('shows the events a slow 30-day read found when it lands after the next minute read', async () => {
    const DAY = 24 * 60 * 60_000
    const bridgeEvent = (sequence: number, step: string, time: number): Entry => ({
      time,
      payload: {
        id: 'B1',
        parent: 'transfer',
        kind: 'usdc_bridge',
        sequence: String(sequence),
        step,
        payload: '{}',
        event_id: `UsdcBridge:B1:${String(sequence)}`
      }
    })
    const from = (call: unknown[]) =>
      Number((JSON.parse((call[1] as RequestInit).body as string) as { from: string }).from)
    const pending: { long: boolean; answer: (response: Response) => void }[] = []
    fetchSpy.mockImplementation(
      (...call: unknown[]) =>
        new Promise<Response>((answer) => {
          pending.push({ long: from(call) < minutes(0) - DAY, answer })
        })
    )
    const recent = [bridgeEvent(2, 'Bridging', minutes(3))]
    vi.setSystemTime(minutes(60))
    render('B1')
    pending[0]?.answer(rowQueryAnswer(recent))
    await vi.waitFor(() => {
      expect(pending).toHaveLength(2)
    })

    // The next minute's read lands first, without the oldest event.
    vi.setSystemTime(minutes(61))
    render('B1')
    pending[2]?.answer(rowQueryAnswer(recent))
    await vi.waitFor(() => {
      expect(render('B1')?.innerHTML).not.toContain('Loading this row')
    })
    expect(render('B1')?.innerHTML).not.toContain('Oldest Event')

    pending[1]?.answer(
      rowQueryAnswer([bridgeEvent(1, 'OldestEvent', minutes(-DAY / 60_000)), ...recent])
    )
    await vi.waitFor(() => {
      expect(render('B1')?.innerHTML).toContain('Oldest Event')
    })
    expect(pending).toHaveLength(3)
  })

  it('reads a row again when it reopens after a close during its read', async () => {
    let answer: (response: Response) => void = () => undefined
    fetchSpy.mockImplementationOnce(
      () =>
        new Promise<Response>((resolve) => {
          answer = resolve
        })
    )
    const dialog = render('M1')
    dialog?.querySelector<HTMLButtonElement>('[data-close]')?.click()
    render('')
    answer(rowQueryAnswer([], 'quota exceeded'))
    await vi.advanceTimersByTimeAsync(0)

    render('M1')
    expect(fetchSpy).toHaveBeenCalledTimes(2)
  })

  it('says a 30-day read failed, and reads back again, when a newer read finished first', async () => {
    const DAY = 24 * 60 * 60_000
    const from = (call: unknown[]) =>
      Number((JSON.parse((call[1] as RequestInit).body as string) as { from: string }).from)
    const pending: { long: boolean; answer: (response: Response) => void }[] = []
    fetchSpy.mockImplementation(
      (...call: unknown[]) =>
        new Promise<Response>((answer) => {
          pending.push({ long: from(call) < minutes(0) - DAY, answer })
        })
    )
    const partial = () => rowQueryAnswer([mintEvent(2, 'MintCompleted', minutes(5))])
    const dialog = render('M1')
    pending[0]?.answer(partial())
    await vi.waitFor(() => {
      expect(pending).toHaveLength(2)
    })
    expect(pending[1]?.long).toBe(true)

    // Closed during the 30-day read and opened again: the new read finishes
    // first, without the first event.
    dialog?.querySelector<HTMLButtonElement>('[data-close]')?.click()
    render('')
    render('M1')
    expect(pending).toHaveLength(3)
    pending[2]?.answer(partial())
    await vi.waitFor(() => {
      expect(render('M1')?.innerHTML).not.toContain('Loading this row')
    })

    pending[1]?.answer(rowQueryAnswer([], 'quota exceeded'))
    await vi.waitFor(() => {
      expect(render('M1')?.innerHTML).toContain("This row's event query failed (quota exceeded)")
    })

    render('M1')?.querySelector<HTMLButtonElement>('[data-close]')?.click()
    render('')
    render('M1')
    pending[3]?.answer(partial())
    await vi.waitFor(() => {
      expect(pending.filter((read) => read.long)).toHaveLength(2)
    })
  })

  it('reads back over 30 days once for a row whose first event no line holds', async () => {
    const DAY = 24 * 60 * 60_000
    // B1's second event only: its first was never logged.
    const second: Entry = {
      time: minutes(3),
      payload: {
        id: 'B1',
        parent: 'transfer',
        kind: 'usdc_bridge',
        sequence: '2',
        step: 'BridgeSent',
        payload: '{}',
        event_id: 'UsdcBridge:B1:2'
      }
    }
    const from = (call: unknown[]) =>
      Number((JSON.parse((call[1] as RequestInit).body as string) as { from: string }).from)
    fetchSpy.mockImplementation(() => Promise.resolve(rowQueryAnswer([second])))
    vi.setSystemTime(minutes(60))
    render('B1')
    await vi.waitFor(() => {
      expect(fetchSpy).toHaveBeenCalledTimes(2)
    })

    // A minute later the row reads again, and that read settles.
    vi.setSystemTime(minutes(61))
    render('B1')
    await vi.waitFor(() => {
      expect(fetchSpy).toHaveBeenCalledTimes(3)
    })
    await vi.waitFor(() => {
      expect(render('B1')?.innerHTML).not.toContain('Loading this row')
    })

    expect(fetchSpy).toHaveBeenCalledTimes(3)
    const longReads = fetchSpy.mock.calls.filter((call: unknown[]) => from(call) < minutes(0) - DAY)
    expect(longReads).toHaveLength(1)
  })

  it('reads a row still in flight up to now, and again a minute later', () => {
    vi.setSystemTime(minutes(60))
    const dialog = render('B1')
    expect(dialog?.open).toBe(true)
    const [, init] = fetchSpy.mock.calls[0] as [string, RequestInit]
    const body = JSON.parse(init.body as string) as { to: string }
    expect(body.to).toBe(String(minutes(60) + 600_000))

    render('B1')
    expect(fetchSpy).toHaveBeenCalledTimes(1)
    vi.setSystemTime(minutes(61))
    render('B1')
    expect(fetchSpy).toHaveBeenCalledTimes(2)
  })

  // The board loads no event lines (a slow read at every load), so even
  // a frame that holds the whole row's events is not read: the row fetches.
  it("fetches a row's events when it opens, whatever event lines the board's data holds", async () => {
    boardEvents = [
      mintEvent(1, 'MintRequested', minutes(1)),
      mintEvent(2, 'MintCompleted', minutes(5))
    ]
    fetchSpy.mockResolvedValue(
      rowQueryAnswer([
        mintEvent(1, 'MintRequested', minutes(1)),
        mintEvent(2, 'MintCompleted', minutes(5))
      ])
    )
    const dialog = render('M1')
    expect(fetchSpy).toHaveBeenCalledTimes(1)
    await vi.waitFor(() => {
      expect(dialog?.innerHTML).toContain('Event timeline')
    })
  })

  it('says when the row query failed, and retries when the row opens again', async () => {
    fetchSpy.mockResolvedValue(rowQueryAnswer([], 'quota exceeded'))
    const dialog = render('M1')
    await vi.waitFor(() => {
      expect(dialog?.innerHTML).toContain("This row's event query failed (quota exceeded)")
    })
    // Its status history still shows.
    expect(dialog?.querySelectorAll('.det-timeline .det-step')).toHaveLength(2)

    dialog?.querySelector<HTMLButtonElement>('[data-close]')?.click()
    render('')
    fetchSpy.mockResolvedValue(rowQueryAnswer([]))
    render('M1')
    expect(fetchSpy).toHaveBeenCalledTimes(2)
  })

  it('keeps a row the operator closed closed until the variable moves off it', () => {
    const dialog = render('M1')
    dialog?.querySelector<HTMLButtonElement>('[data-close]')?.click()
    expect(dialog?.open).toBe(false)
    expect(partial).toHaveBeenCalledWith({ 'var-detail': '' }, true)
    // A refresh already running when the operator closed it.
    expect(render('M1')?.open).toBe(false)
  })

  it('reopens a row after the variable went empty and came back, as Back and Forward do', () => {
    expect(render('M1')?.open).toBe(true)
    expect(render('')?.open).toBe(false)
    expect(partial).not.toHaveBeenCalled()
    expect(render('M1')?.open).toBe(true)
  })

  it('keeps a reopened row open when the close event of the earlier script close runs late', () => {
    expect(render('M1')?.open).toBe(true)
    // Back closes the row; Forward brings it back before the close event of
    // that script close runs.
    expect(render('')?.open).toBe(false)
    expect(render('M1')?.open).toBe(true)
    vi.advanceTimersByTime(0)
    expect(root.querySelector('dialog')?.open).toBe(true)
    expect(partial).not.toHaveBeenCalled()
    expect(render('M1')?.open).toBe(true)
  })

  it('keeps a row the operator closed closed through a refresh before its close event', () => {
    render('M1')?.querySelector<HTMLButtonElement>('[data-close]')?.click()
    expect(partial).toHaveBeenCalledTimes(1)
    // A refresh with the variable still on the row runs before the event.
    expect(render('M1')?.open).toBe(false)
    vi.advanceTimersByTime(0)
    expect(render('M1')?.open).toBe(false)
    expect(partial).toHaveBeenCalledTimes(1)
    // The variable clears, then a click on the same row opens it again.
    expect(render('')?.open).toBe(false)
    expect(render('M1')?.open).toBe(true)
  })

  it('treats Escape (cancel) as the operator closing the row', () => {
    const dialog = render('M1')
    dialog?.dispatchEvent(new Event('cancel'))
    expect(partial).toHaveBeenCalledWith({ 'var-detail': '' }, true)
    // A refresh already running when the operator pressed Escape.
    expect(render('M1')?.open).toBe(false)
  })
})
