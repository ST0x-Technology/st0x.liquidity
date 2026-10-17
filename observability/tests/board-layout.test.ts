// @vitest-environment happy-dom

import { describe, expect, it, vi } from 'vitest'

// The Grafana board's column layout (observability/liquidity-panels/
// board-layout.js), against a stand-in for the dashboard scene: grid items
// with state and setState, and each panel's data with subscribeToState.

type Config = { fit: string[]; fill: string; split: string[] }
type BoardLayout = {
  fitColumns: (scene: unknown, config: Config, win: unknown) => void
}

// A plain script in the board, an ES module through its export line here.
// Loaded through a glob so tsc does not type-check the script.
const scriptPath = '../liquidity-panels/board-layout.js'
const loadScript = import.meta.glob('../liquidity-panels/board-layout.js')[scriptPath]
if (!loadScript) throw new Error(`no ${scriptPath}`)
const { fitColumns } = (await loadScript()) as BoardLayout

type ItemState = { y: number; height: number; body: { state: { title: string; $data: Data } } }
type Data = {
  state: { data?: { series: { length: number }[] } }
  listeners: (() => void)[]
  subscribeToState: (listener: () => void) => void
}

const data = (rows?: number): Data => {
  const source: Data = {
    state: rows === undefined ? {} : { data: { series: [{ length: rows }] } },
    listeners: [],
    subscribeToState: (listener) => source.listeners.push(listener)
  }
  return source
}

const item = (title: string, y: number, height: number, rows?: number) => {
  const found = {
    state: { y, height, body: { state: { title, $data: data(rows) } } } as ItemState,
    setState: (update: Partial<ItemState>) => {
      found.state = { ...found.state, ...update }
    }
  }
  return found
}

const CONFIG: Config = {
  fit: ['USD · Alpaca', 'USD · Onchain'],
  fill: 'Equities',
  split: ['Trades', 'Rebalances']
}

// The board's columns as it loads: 6 + 8 + 16 units on the left and
// 18 + 12 on the right, both from y = 1, so both end at 31 unless a test
// says otherwise.
// A window that keeps its resize listeners, as the browser does.
const fakeWindow = () => {
  const resize: (() => void)[] = []
  return {
    resize,
    addEventListener: (_type: string, listener: () => void) => resize.push(listener),
    removeEventListener: (_type: string, listener: () => void) => {
      const at = resize.indexOf(listener)
      if (at >= 0) resize.splice(at, 1)
    },
    setTimeout: (callback: () => void) => callback()
  }
}

// A window `innerHeight` px tall over a page whose USD · Alpaca panel, at
// grid unit 1, starts 100px down, so the grid's unit 0 is at 62px.
const pageWindow = (innerHeight: number) => {
  const doc = document.implementation.createHTMLDocument('board')
  doc.body.innerHTML = '<section data-testid="data-testid Panel header USD · Alpaca"></section>'
  const section = doc.querySelector('section')
  if (!section) throw new Error('no section')
  section.getBoundingClientRect = () => ({ top: 100 }) as DOMRect
  return { ...fakeWindow(), document: doc, innerHeight }
}

const board = (
  alpacaRows?: number,
  onchainRows?: number,
  rightHeights = [18, 12],
  win = fakeWindow()
) => {
  const [tradesHeight = 18, rebalancesHeight = 12] = rightHeights
  const items = {
    alpaca: item('USD · Alpaca', 1, 6, alpacaRows),
    onchain: item('USD · Onchain', 7, 8, onchainRows),
    equities: item('Equities', 15, 16, 20),
    trades: item('Trades', 1, tradesHeight, 50),
    rebalances: item('Rebalances', 1 + tradesHeight, rebalancesHeight, 50)
  }
  const { resize } = win
  const forceRender = vi.fn()
  const scene = {
    state: {
      body: { state: { grid: { state: { children: Object.values(items) }, forceRender } } }
    }
  }
  const fit = () => fitColumns(scene, CONFIG, win)
  const column = (...found: (typeof items)[keyof typeof items][]) =>
    found.map(({ state }) => [state.y, state.height])
  const left = () => column(items.alpaca, items.onchain, items.equities)
  const right = () => column(items.trades, items.rebalances)
  return { items, fit, left, right, resize, forceRender }
}

// Whether a :has() opens inside another :has()'s parentheses.
const nestedHas = (css: string) => {
  const open: boolean[] = []
  for (let at = 0; at < css.length; at += 1) {
    if (css.startsWith(':has(', at)) {
      if (open.includes(true)) return true
      open.push(true)
      at += ':has('.length - 1
    } else if (css[at] === '(') open.push(false)
    else if (css[at] === ')') open.pop()
  }
  return false
}

describe('board board-layout.js', () => {
  it('gives each USD table the units its rows need and Equities the rest', () => {
    const { fit, left, forceRender } = board(1, 3)

    fit()

    // One row needs 141px, 4 units (144px); three need 225px, 6 units.
    expect(left()).toEqual([
      [1, 4],
      [5, 6],
      [11, 20]
    ])
    expect(forceRender).toHaveBeenCalledTimes(1)
  })

  it('splits the right column in half, ending level with the left', () => {
    const { fit, right } = board(1, 3)

    fit()

    expect(right()).toEqual([
      [1, 15],
      [16, 15]
    ])
  })

  it('ends both columns on the lower of the two bottoms the board loaded with', () => {
    // The right column ends a row lower: 18 + 13 from y = 1 is 32.
    const { fit, left, right } = board(1, 3, [18, 13])

    fit()

    expect(left()).toEqual([
      [1, 4],
      [5, 6],
      [11, 21]
    ])
    // 31 units: the odd one goes to Rebalances.
    expect(right()).toEqual([
      [1, 15],
      [16, 16]
    ])
  })

  it('keeps a table’s height until its data arrives, then fits it', () => {
    const { fit, left, items } = board(1, undefined)

    fit()
    expect(left()).toEqual([
      [1, 4],
      [5, 8],
      [13, 18]
    ])

    items.onchain.state.body.state.$data.state = { data: { series: [{ length: 2 }] } }
    items.onchain.state.body.state.$data.listeners.forEach((listener) => listener())

    // Two rows need 183px, 5 units.
    expect(left()).toEqual([
      [1, 4],
      [5, 5],
      [10, 21]
    ])
  })

  it("replaces an old scene's resize listener, so only the new scene reflows", () => {
    const win = fakeWindow()
    const old = board(1, 3, [18, 12], win)
    old.fit()
    const current = board(1, 3, [18, 12], win)
    current.fit()
    old.forceRender.mockClear()
    current.items.trades.setState({ y: 1, height: 22 })

    win.resize.forEach((listener) => listener())

    expect(win.resize).toHaveLength(1)
    expect(old.forceRender).not.toHaveBeenCalled()
    expect(current.forceRender).toHaveBeenCalled()
  })

  it("ends both columns at the window's bottom, and follows it on a resize", () => {
    // 1336px leaves 1274px under unit 0 (with one margin back): 33 units
    // and 20px.
    const win = pageWindow(1336)
    const { fit, left, right } = board(1, 3, [18, 12], win)
    fit()

    expect(left()).toEqual([
      [1, 4],
      [5, 6],
      [11, 22]
    ])
    expect(right()).toEqual([
      [1, 16],
      [17, 16]
    ])
    const css = () => win.document.getElementById('liq-board-stretch')?.textContent ?? ''
    // 22 units less a margin, plus the 20px.
    expect(css()).toContain('Panel header Equities"]{height:848px!important}')
    expect(css()).toContain('Panel header Rebalances"]{height:620px!important}')

    // The window grows by a unit and 10px.
    win.innerHeight = 1336 + 48
    win.resize.forEach((listener) => {
      listener()
    })

    // 34 units and 30px.
    expect(left()[2]).toEqual([11, 23])
    expect(right()[1]).toEqual([17, 17])
    expect(css()).toContain('Panel header Equities"]{height:896px!important}')
  })

  it("stacks a phone's inventory tables first, without moving the grid", () => {
    const win = { ...pageWindow(848), innerWidth: 402 }
    const { fit, left, right } = board(1, undefined, [18, 12], win)
    fit()

    // The grid keeps its units, for when the window widens again.
    expect(left()).toEqual([
      [1, 6],
      [7, 8],
      [15, 16]
    ])
    expect(right()).toEqual([
      [1, 18],
      [19, 12]
    ])
    const css = win.document.getElementById('liq-board-stretch')?.textContent ?? ''
    const place = (title: string, order: number, height: number) =>
      `.react-grid-item:has(section[data-testid="data-testid Panel header ${title}"]){order:${String(order)}!important;height:${String(height)}px!important}`
    // Alpaca's one row needs 135px; Onchain keeps its 8 units until its data
    // arrives; the long tables take 600px, three quarters of the 800px under
    // Grafana's top bar.
    expect(css).toContain(place('USD · Alpaca', 1, 135))
    expect(css).toContain(place('USD · Onchain', 2, 296))
    expect(css).toContain(place('Equities', 3, 600))
    expect(css).toContain(place('Trades', 4, 600))
    expect(css).toContain(place('Rebalances', 5, 600))
    expect(css).toContain(
      '.react-grid-layout:has(> .react-grid-item section[data-testid="data-testid Panel header USD · Alpaca"]){display:flex!important;flex-direction:column!important'
    )
    // The row detail's empty item leaves no gap under the header.
    expect(css).toContain(
      ' > .react-grid-item:not(:first-child):not(:has(section[data-testid="data-testid Panel header USD · Alpaca"]))'
    )
    expect(css).toContain(
      ':not(:has(section[data-testid="data-testid Panel header Rebalances"])){height:0!important;margin:0!important'
    )
    // The page scrolls on a phone, so Grafana's controls scroll away, and the
    // panels' time ranges hide.
    expect(css).toContain('@media (max-width:768px)')
    expect(css).toContain(':has([data-testid="icon-clock-nine"]){display:none!important}')
    // A :has() inside another is invalid CSS: the browser would drop the rule.
    expect(nestedHas(css)).toBe(false)
    // A dark, thin scrollbar.
    expect(css).toContain('scrollbar-color:#3a3d45 transparent;scrollbar-width:thin')
    // Grafana's controls are one row, without the edit button.
    expect(css).toContain(
      '[data-testid="data-testid dashboard controls"]{display:flex!important;flex-flow:row nowrap!important'
    )
    expect(css).toContain(
      ':has(> [data-testid="data-testid Edit dashboard button"]){display:none!important}'
    )
  })

  it("gives a phone's short Equities only the height its rows need", () => {
    const win = { ...pageWindow(848), innerWidth: 402 }
    const { fit, items } = board(1, 3, [18, 12], win)
    items.equities.state.body.state.$data.state = { data: { series: [{ length: 4 }] } }
    fit()

    expect(win.document.getElementById('liq-board-stretch')?.textContent ?? '').toContain(
      'Panel header Equities"]){order:3!important;height:261px!important}'
    )
  })

  it('fits again after the board changes on a resize', () => {
    const { fit, left, right, items, resize } = board(1, 3)
    fit()

    items.alpaca.setState({ y: 1, height: 5 })
    items.onchain.setState({ y: 6, height: 7 })
    items.equities.setState({ y: 13, height: 25 })
    items.trades.setState({ y: 1, height: 22 })
    items.rebalances.setState({ y: 23, height: 15 })
    resize.forEach((listener) => listener())

    expect(left()).toEqual([
      [1, 4],
      [5, 6],
      [11, 27]
    ])
    expect(right()).toEqual([
      [1, 18],
      [19, 19]
    ])
  })

  it('gives Equities its minimum and lets the page scroll when the window is too short', () => {
    // Twenty Onchain rows need 25 units, which leaves Equities 1 of the 31.
    const { fit, left, right } = board(1, 20)

    fit()

    expect(left()).toEqual([
      [1, 4],
      [5, 25],
      [30, 6]
    ])
    // The right column ends level, 35 units split in two.
    expect(right()).toEqual([
      [1, 17],
      [18, 18]
    ])
  })

  it('keeps the heights of a short window the page scrolled', () => {
    const fitted = (scrolled: number) => {
      const win = pageWindow(600)
      const section = win.document.querySelector('section')
      if (!section) throw new Error('no section')
      section.getBoundingClientRect = () => ({ top: 100 - scrolled }) as DOMRect
      Object.defineProperty(win.document.body, 'scrollTop', { value: scrolled })
      const { fit, left, right } = board(1, 20, [18, 12], win)
      fit()
      return [left(), right()]
    }

    // 1000px is enough for a fit that followed the scroll to give Equities
    // 10 units instead of its minimum 6.
    expect(fitted(1000)).toEqual(fitted(0))
  })

  it('does nothing without the dashboard scene or one of its panels', () => {
    expect(() => fitColumns(undefined, CONFIG, {})).not.toThrow()
    const { left, items } = board(1, 3)
    const scene = {
      state: { body: { state: { grid: { state: { children: [items.alpaca, items.equities] } } } } }
    }

    fitColumns(scene, CONFIG, {})

    expect(left()).toEqual([
      [1, 6],
      [7, 8],
      [15, 16]
    ])
  })
})
