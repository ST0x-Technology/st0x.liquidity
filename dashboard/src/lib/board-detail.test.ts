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

// The plugin's log-lines frame, newest first; an exporter entry's body is its
// JSON payload.
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

const variables: Record<string, string> = {}
const partial = vi.fn()
let root: HTMLDivElement

const render = (detail: string) => {
  variables['detail'] = detail
  renderDetail({
    element: root,
    panelData: {
      state: 'Done',
      series: [frame('exporter-trades', trades), frame('exporter-transfers', transfers)]
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
    'source:text': 'exporter'
  })
  root = document.createElement('div')
  document.body.appendChild(root)
})

afterEach(() => {
  root.remove()
  vi.runOnlyPendingTimers()
  vi.useRealTimers()
  closeSpy.mockRestore()
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
