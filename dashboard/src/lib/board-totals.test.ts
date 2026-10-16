// @vitest-environment happy-dom

import { describe, expect, it } from 'vitest'

// Each inventory table's total after its title (observability/
// liquidity-panels/board-totals.js), against a stand-in for the dashboard
// scene: each panel's data with subscribeToState.

type Field = { name: string; values: unknown[] }
type BoardTotals = {
  showTotals: (
    doc: Document,
    scene: unknown,
    totals: Record<
      string,
      | string[]
      | {
          columns: string[]
          stamp?: string
          staleAfterSeconds?: number
          rowField?: string
          parts?: { label: string; columns: string[]; rows: string }[]
        }
    >,
    now?: () => number
  ) => void
}

// A plain script in the board, an ES module through its export line here.
// Loaded through a glob so svelte-check does not type-check the script.
const scriptPath = '../../../observability/liquidity-panels/board-totals.js'
const loadScript = import.meta.glob('../../../observability/liquidity-panels/board-totals.js')[
  scriptPath
]
if (!loadScript) throw new Error(`no ${scriptPath}`)
const { showTotals } = (await loadScript()) as BoardTotals

const panel = (title: string, fields: Field[]) => {
  const data = {
    state: { data: { series: [{ fields }] } },
    listeners: [] as (() => void)[],
    subscribeToState: (listener: () => void) => data.listeners.push(listener)
  }
  return { state: { body: { state: { title, $data: data } } } }
}

describe('board board-totals.js', () => {
  const doc = () => document.implementation.createHTMLDocument('board')
  const css = (page: Document) => page.getElementById('liq-board-totals')?.textContent ?? ''

  it("writes each table's total of its columns after its title, skipping empty cells", () => {
    const page = doc()
    const onchain = panel('USD · Onchain', [
      { name: 'venue\\col', values: ['base', 'robinhood', 'ethereum'] },
      { name: 'vault', values: [59976.74, 25380.13, null] },
      { name: 'inflight', values: [4174.27, 0, null] },
      { name: 'wallet', values: [0.001, null, 4173.73] }
    ])
    const scene = { state: { body: { state: { grid: { state: { children: [onchain] } } } } } }

    showTotals(page, scene, { 'USD · Onchain': ['vault', 'wallet'] })

    // 59976.74 + 25380.13 + 0.001 + 4173.73; in flight is not a column.
    expect(css(page)).toContain(
      'section[data-testid="data-testid Panel header USD · Onchain"] h2::after{content:" \\00B7  $89,531"'
    )
  })

  it('follows the data when it refreshes, and shows nothing without a number', () => {
    const page = doc()
    const equities = panel('Equities', [{ name: 'total_usd', values: [NaN, null] }])
    const scene = { state: { body: { state: { grid: { state: { children: [equities] } } } } } }

    showTotals(page, scene, { Equities: ['total_usd'] })
    expect(css(page)).toBe('')

    const data = equities.state.body.state.$data
    data.state = {
      data: { series: [{ fields: [{ name: 'total_usd', values: [1500.4, null, 2000] }] }] }
    }
    data.listeners.forEach((listener) => listener())
    expect(css(page)).toContain('$3,500')
  })

  it('says the total is partial when an asset has no price, not when a row has no cell', () => {
    const page = doc()
    const equities = panel('Equities', [{ name: 'total_usd', values: [1500, null, NaN, 2000] }])
    const scene = { state: { body: { state: { grid: { state: { children: [equities] } } } } } }

    showTotals(page, scene, { Equities: ['total_usd'] })
    expect(css(page)).toContain('$3,500 (some unpriced)')

    const data = equities.state.body.state.$data
    data.state = { data: { series: [{ fields: [{ name: 'total_usd', values: [1500, null] }] }] } }
    data.listeners.forEach((listener) => listener())
    expect(css(page)).toContain('$1,500"')
  })

  it('says how old the prices are once the oldest is stale', () => {
    const page = doc()
    // Oct 16, 2026 23:41:00 UTC and a minute later.
    const quoted = Date.UTC(2026, 9, 16, 23, 41) / 1000
    const equities = panel('Equities', [
      { name: 'total_usd', values: [1500, 2000] },
      { name: 'price_ts', values: [quoted + 60, quoted] }
    ])
    const scene = { state: { body: { state: { grid: { state: { children: [equities] } } } } } }
    const totals = {
      Equities: { columns: ['total_usd'], stamp: 'price_ts', staleAfterSeconds: 600 }
    }

    showTotals(page, scene, totals, () => (quoted + 300) * 1000)
    expect(css(page)).toContain('$3,500"')

    showTotals(page, scene, totals, () => (quoted + 3600) * 1000)
    expect(css(page)).toContain('$3,500 \\00B7  prices as of Oct 16, 23:41 UTC"')
  })

  it('adds the onchain and broker parts after the total, each over its own rows', () => {
    const page = doc()
    const equities = panel('Equities', [
      { name: 'row\\col', values: ['AAPL', 'AAPL/alpaca', 'AAPL/base', 'AAPL/robinhood'] },
      { name: 'total_usd', values: [1000, null, null, null] },
      { name: 'vault_usd', values: [null, 400, 300, 200] },
      { name: 'inflight_usd', values: [null, 50, 0, 50] }
    ])
    const scene = { state: { body: { state: { grid: { state: { children: [equities] } } } } } }
    const parts = [
      { label: 'onchain', columns: ['vault_usd', 'inflight_usd'], rows: '^[^/]+/(?!alpaca$)' },
      { label: 'brokers', columns: ['vault_usd', 'inflight_usd'], rows: '/alpaca$' }
    ]

    showTotals(page, scene, { Equities: { columns: ['total_usd'], rowField: 'row\\col', parts } })

    // Base and Robinhood: 300 + 200 + 50; Alpaca: 400 + 50.
    expect(css(page)).toContain(
      'content:" \\00B7  $1,000 \\00B7  onchain $550 \\00B7  brokers $450"'
    )
  })

  it('does nothing without the dashboard scene', () => {
    const page = doc()
    expect(() => showTotals(page, undefined, { Equities: ['total_usd'] })).not.toThrow()
    expect(page.getElementById('liq-board-totals')).toBeNull()
  })
})
