// @vitest-environment happy-dom

import { describe, expect, it, vi } from 'vitest'

// The inventory tables' scaling on a narrow window (observability/
// liquidity-panels/board-scale.js), against a stand-in for the dashboard
// scene: each panel's field config and onFieldConfigChange.

type Override = {
  matcher: { id: string; options: string }
  properties: { id: string; value: unknown }[]
}
type FieldConfig = { defaults: object; overrides: Override[] }
type Options = {
  id?: string
  panels: string[]
  fixedPx: number
  ratioMinPx: number
  stretch?: string
  stretchMinPx?: number
  cellFontPx?: number
  compactHide?: Record<string, string[]>
  compactColumns?: Record<string, Record<string, unknown>>
  compactFixedPx?: number
  compactRatioMinPx?: number
}
type BoardScale = {
  scaleTables: (doc: Document, scene: unknown, options: Options, win: unknown) => void
  tableScale: (width: number | undefined, fixedPx: number, ratioMinPx: number) => number
  stretchLeft: (fieldConfig: FieldConfig, width: number, stretch: string, minPx: number) => number
}

// A plain script in the board, an ES module through its export line here.
// Loaded through a glob so tsc does not type-check the script.
const scriptPath = '../liquidity-panels/board-scale.js'
const loadScript = import.meta.glob('../liquidity-panels/board-scale.js')[scriptPath]
if (!loadScript) throw new Error(`no ${scriptPath}`)
const { scaleTables, tableScale, stretchLeft } = (await loadScript()) as BoardScale

const width = (name: string, px: number): Override => ({
  matcher: { id: 'byName', options: name },
  properties: [{ id: 'custom.width', value: px }]
})
const FIELD_CONFIG: FieldConfig = {
  defaults: {},
  overrides: [
    width('Asset', 100),
    width('Total', 200),
    {
      matcher: { id: 'byName', options: 'Ratio' },
      properties: [{ id: 'custom.minWidth', value: 60 }]
    }
  ]
}

// A window that keeps its resize listeners, as the browser does, and its
// size observers: `sized` runs every observer, as a panel changing size does.
const fakeWindow = () => {
  const resize: (() => void)[] = []
  const observers: { callback: () => void; watched: Element[]; live: boolean }[] = []
  class ResizeObserver {
    entry: { callback: () => void; watched: Element[]; live: boolean }
    constructor(callback: () => void) {
      this.entry = { callback, watched: [], live: true }
      observers.push(this.entry)
    }
    observe(element: Element) {
      this.entry.watched.push(element)
    }
    disconnect() {
      this.entry.live = false
    }
  }
  const sized = () => {
    observers
      .filter((observer) => observer.live && observer.watched.length > 0)
      .forEach((observer) => {
        observer.callback()
      })
  }
  return {
    resize,
    sized,
    observers,
    ResizeObserver,
    addEventListener: (_type: string, listener: () => void) => resize.push(listener),
    removeEventListener: (_type: string, listener: () => void) => {
      const at = resize.indexOf(listener)
      if (at >= 0) resize.splice(at, 1)
    },
    setTimeout: (callback: () => void) => callback()
  }
}

// One table, `px` wide on the page, in a stand-in scene.
const board = (px: number, win = fakeWindow()) => {
  const doc = document.implementation.createHTMLDocument('board')
  doc.body.innerHTML =
    '<section data-testid="data-testid Panel header Equities"><div role="grid"></div></section>'
  const grid = doc.querySelector('[role="grid"]')
  if (!grid) throw new Error('no grid')
  Object.defineProperty(grid, 'clientWidth', { get: () => px, configurable: true })
  const dataListeners: (() => void)[] = []
  const panel = {
    state: {
      title: 'Equities',
      fieldConfig: FIELD_CONFIG,
      $data: { subscribeToState: (listener: () => void) => dataListeners.push(listener) }
    },
    onFieldConfigChange: vi.fn((fieldConfig: FieldConfig) => {
      panel.state.fieldConfig = fieldConfig
    })
  }
  const scene = {
    state: { body: { state: { grid: { state: { children: [{ state: { body: panel } }] } } } } }
  }
  const { resize } = win
  const scale = () => {
    scaleTables(doc, scene, { panels: ['Equities'], fixedPx: 300, ratioMinPx: 100 }, win)
  }
  const widths = () =>
    panel.state.fieldConfig.overrides.map((override) => [
      override.matcher.options,
      override.properties.find((property) => property.id === 'custom.width')?.value,
      override.properties.find((property) => property.id === 'custom.minWidth')?.value
    ])
  const font = () => doc.getElementById('liq-board-scale')?.textContent ?? ''
  const setWidth = (next: number) => {
    Object.defineProperty(grid, 'clientWidth', { get: () => next, configurable: true })
  }
  const dataLanded = () => {
    dataListeners.forEach((listener) => {
      listener()
    })
  }
  return { panel, scale, widths, font, resize, setWidth, dataLanded }
}

describe('board board-scale.js', () => {
  it('keeps full size while the columns and the bar fit', () => {
    expect(tableScale(400, 300, 100)).toBe(1)
    expect(tableScale(undefined, 300, 100)).toBe(1)
    const { panel, scale, font } = board(500)

    scale()

    expect(panel.onFieldConfigChange).toHaveBeenCalledWith(FIELD_CONFIG)
    expect(font()).toBe('')
  })

  it('shrinks every fixed width and the text by one factor when the table is too narrow', () => {
    const { scale, widths, font } = board(320)

    scale()

    // 320 / (300 + 100) = 0.8.
    expect(widths()).toEqual([
      ['Asset', 80, 80],
      ['Total', 160, 160],
      ['Ratio', undefined, 60]
    ])
    expect(font()).toContain('{font-size:11.2px}')
  })

  it('stops at the smallest readable scale, and grows back on a resize', () => {
    expect(tableScale(100, 300, 100)).toBe(0.6)
    const { scale, widths, font, resize, setWidth } = board(320)
    scale()

    setWidth(1000)
    resize.forEach((listener) => listener())

    expect(widths()).toEqual([
      ['Asset', 100, undefined],
      ['Total', 200, undefined],
      ['Ratio', undefined, 60]
    ])
    expect(font()).toBe('')
  })

  it('scales again when the table changes size without a window resize', () => {
    const win = fakeWindow()
    const { scale, widths, setWidth } = board(500, win)
    scale()
    expect(widths()[0]).toEqual(['Asset', 100, undefined])

    // Grafana draws the table narrower after the header ran, or the
    // sidebar opens.
    setWidth(320)
    win.sized()

    expect(widths()[0]).toEqual(['Asset', 80, 80])
    // Only the newest render's observers are live.
    scale()
    expect(win.observers.filter((observer) => observer.live)).toHaveLength(1)
  })

  it('scales once Grafana draws the table, after the header ran', async () => {
    // A real MutationObserver (happy-dom's), the panel empty at first.
    const win = { ...fakeWindow(), MutationObserver }
    const doc = document.implementation.createHTMLDocument('board')
    doc.body.innerHTML = '<section data-testid="data-testid Panel header Equities"></section>'
    const panel = {
      state: { title: 'Equities', fieldConfig: FIELD_CONFIG },
      onFieldConfigChange: vi.fn((fieldConfig: FieldConfig) => {
        panel.state.fieldConfig = fieldConfig
      })
    }
    const scene = {
      state: { body: { state: { grid: { state: { children: [{ state: { body: panel } }] } } } } }
    }
    scaleTables(doc, scene, { panels: ['Equities'], fixedPx: 300, ratioMinPx: 100 }, win)
    expect(panel.onFieldConfigChange).toHaveBeenLastCalledWith(FIELD_CONFIG)

    const table = doc.createElement('div')
    table.setAttribute('role', 'grid')
    Object.defineProperty(table, 'clientWidth', { get: () => 320 })
    doc.querySelector('section')?.appendChild(table)

    await vi.waitFor(() => {
      expect(panel.state.fieldConfig.overrides[0]?.properties).toContainEqual({
        id: 'custom.width',
        value: 80
      })
    })
  })

  it('scales once the table is drawn, when its data lands after the header ran', () => {
    const win = fakeWindow()
    const { scale, widths, setWidth, dataLanded } = board(0, win)
    scale()
    expect(widths()[0]).toEqual(['Asset', 100, undefined])

    setWidth(320)
    dataLanded()

    expect(widths()[0]).toEqual(['Asset', 80, 80])
  })

  it('keeps each group apart: its own rule, observer and listener', () => {
    const win = fakeWindow()
    const { panel, widths } = board(320, win)
    const doc = document.implementation.createHTMLDocument('board')
    doc.body.innerHTML =
      '<section data-testid="data-testid Panel header Equities"><div role="grid"></div></section>'
    const grid = doc.querySelector('[role="grid"]')
    if (!grid) throw new Error('no grid')
    Object.defineProperty(grid, 'clientWidth', { get: () => 320, configurable: true })
    const scene = {
      state: { body: { state: { grid: { state: { children: [{ state: { body: panel } }] } } } } }
    }
    for (const id of ['left', 'right']) {
      scaleTables(doc, scene, { id, panels: ['Equities'], fixedPx: 300, ratioMinPx: 100 }, win)
    }

    expect(doc.getElementById('liq-board-scale-left')?.textContent).toContain('{font-size:11.2px}')
    expect(doc.getElementById('liq-board-scale-right')?.textContent).toContain('{font-size:11.2px}')
    expect(win.resize).toHaveLength(2)
    expect(win.observers.filter((observer) => observer.live)).toHaveLength(2)
    expect(widths()[0]).toEqual(['Asset', 80, 80])
  })

  it('pins the stretching column to what the others leave, counting the 50px floor', () => {
    expect(stretchLeft(FIELD_CONFIG, 500, 'Ratio', 60)).toBe(200)
    expect(stretchLeft(FIELD_CONFIG, 320, 'Ratio', 60)).toBe(60)
    const narrow = { defaults: {}, overrides: [width('Id', 40), width('Total', 200)] }
    // Grafana draws the 40px column at 50.
    expect(stretchLeft(narrow, 400, 'Status', 60)).toBe(150)

    const { panel } = board(500)
    const doc = document.implementation.createHTMLDocument('board')
    doc.body.innerHTML =
      '<section data-testid="data-testid Panel header Equities"><div role="grid"></div></section>'
    const grid = doc.querySelector('[role="grid"]')
    if (!grid) throw new Error('no grid')
    Object.defineProperty(grid, 'clientWidth', { get: () => 500, configurable: true })
    const scene = {
      state: { body: { state: { grid: { state: { children: [{ state: { body: panel } }] } } } } }
    }
    scaleTables(
      doc,
      scene,
      { panels: ['Equities'], fixedPx: 300, ratioMinPx: 100, stretch: 'Ratio', stretchMinPx: 60 },
      fakeWindow()
    )

    const ratio = panel.state.fieldConfig.overrides.find(
      (override) => override.matcher.options === 'Ratio'
    )
    expect(ratio?.properties).toContainEqual({ id: 'custom.width', value: 200 })
  })

  it("sets a group's cell text size, scaled with its columns", () => {
    const { panel } = board(320)
    const doc = document.implementation.createHTMLDocument('board')
    doc.body.innerHTML =
      '<section data-testid="data-testid Panel header Equities"><div role="grid"></div></section>'
    const grid = doc.querySelector('[role="grid"]')
    if (!grid) throw new Error('no grid')
    Object.defineProperty(grid, 'clientWidth', { get: () => 320, configurable: true })
    const scene = {
      state: { body: { state: { grid: { state: { children: [{ state: { body: panel } }] } } } } }
    }
    scaleTables(
      doc,
      scene,
      { panels: ['Equities'], fixedPx: 300, ratioMinPx: 100, cellFontPx: 16 },
      fakeWindow()
    )

    // 16px at 0.8.
    expect(doc.getElementById('liq-board-scale')?.textContent).toContain(
      '[role="gridcell"]{font-size:12.8px}'
    )
  })

  it('hides the secondary columns on a phone and fits the rest', () => {
    const { panel, widths } = board(320)
    const doc = document.implementation.createHTMLDocument('board')
    doc.body.innerHTML =
      '<section data-testid="data-testid Panel header Equities"><div role="grid"></div></section>'
    const grid = doc.querySelector('[role="grid"]')
    if (!grid) throw new Error('no grid')
    Object.defineProperty(grid, 'clientWidth', { get: () => 320, configurable: true })
    const scene = {
      state: { body: { state: { grid: { state: { children: [{ state: { body: panel } }] } } } } }
    }
    const win = { ...fakeWindow(), innerWidth: 402 }
    scaleTables(
      doc,
      scene,
      {
        panels: ['Equities'],
        fixedPx: 300,
        ratioMinPx: 100,
        compactHide: { Equities: ['Total'] },
        compactFixedPx: 100,
        compactRatioMinPx: 100
      },
      win
    )

    const total = panel.state.fieldConfig.overrides.find(
      (override) => override.matcher.options === 'Total'
    )
    expect(total?.properties).toContainEqual({ id: 'custom.hideFrom.viz', value: true })
    // 320 fits Asset's 100 and the 100 of bar whole: no shrinking.
    expect(widths()[0]).toEqual(['Asset', 100, undefined])

    // Wider than a phone, every column is back.
    win.innerWidth = 1400
    win.resize.forEach((listener) => {
      listener()
    })
    const back = panel.state.fieldConfig.overrides.find(
      (override) => override.matcher.options === 'Total'
    )
    expect(back?.properties).not.toContainEqual({ id: 'custom.hideFrom.viz', value: true })
  })

  it("changes a phone's columns and shrinks the stretching column's minimum with the text", () => {
    // Trades on a phone: a shorter time, Status stretching.
    const fieldConfig: FieldConfig = {
      defaults: {},
      overrides: [
        {
          matcher: { id: 'byName', options: 'Last updated' },
          properties: [{ id: 'unit', value: 'time:MMM D, HH:mm:ss [UTC]' }]
        },
        width('Id', 50),
        width('Last updated', 150),
        width('Venue', 80),
        { matcher: { id: 'byName', options: 'Status' }, properties: [] }
      ]
    }
    const doc = document.implementation.createHTMLDocument('board')
    doc.body.innerHTML =
      '<section data-testid="data-testid Panel header Trades"><div role="grid"></div></section>'
    const grid = doc.querySelector('[role="grid"]')
    if (!grid) throw new Error('no grid')
    Object.defineProperty(grid, 'clientWidth', { get: () => 200, configurable: true })
    const panel = {
      state: { title: 'Trades', fieldConfig, $data: { subscribeToState: () => undefined } },
      onFieldConfigChange: vi.fn((next: FieldConfig) => {
        panel.state.fieldConfig = next
      })
    }
    const scene = {
      state: { body: { state: { grid: { state: { children: [{ state: { body: panel } }] } } } } }
    }
    scaleTables(
      doc,
      scene,
      {
        panels: ['Trades'],
        fixedPx: 280,
        ratioMinPx: 134,
        stretch: 'Status',
        stretchMinPx: 110,
        compactHide: { Trades: ['Venue'] },
        compactColumns: {
          'Last updated': { unit: 'time:MMM D HH:mm', 'custom.width': 100 }
        },
        compactFixedPx: 150,
        compactRatioMinPx: 100
      },
      { ...fakeWindow(), innerWidth: 402 }
    )

    const properties = (name: string) =>
      panel.state.fieldConfig.overrides
        .filter((override) => override.matcher.options === name)
        .flatMap((override) => override.properties)
    expect(properties('Last updated')).toContainEqual({ id: 'unit', value: 'time:MMM D HH:mm' })
    // 200 of the 250 the phone's columns want: 0.8, so 40 (drawn at 50) +
    // 80 px. The 70px left is under Status's 110px minimum, but that is 88 at
    // the smaller text.
    expect(properties('Last updated')).toContainEqual({ id: 'custom.width', value: 80 })
    expect(properties('Status')).toContainEqual({ id: 'custom.width', value: 88 })
  })

  it('goes compact on a desktop window where every column would need tiny text', () => {
    const { panel, widths } = board(220)
    const doc = document.implementation.createHTMLDocument('board')
    doc.body.innerHTML =
      '<section data-testid="data-testid Panel header Equities"><div role="grid"></div></section>'
    const grid = doc.querySelector('[role="grid"]')
    if (!grid) throw new Error('no grid')
    let px = 220
    Object.defineProperty(grid, 'clientWidth', { get: () => px, configurable: true })
    const scene = {
      state: { body: { state: { grid: { state: { children: [{ state: { body: panel } }] } } } } }
    }
    const win = { ...fakeWindow(), innerWidth: 1280 }
    const options = {
      panels: ['Equities'],
      fixedPx: 300,
      ratioMinPx: 100,
      compactHide: { Equities: ['Total'] },
      compactFixedPx: 100,
      compactRatioMinPx: 100
    }
    const totalHidden = () =>
      panel.state.fieldConfig.overrides
        .find((override) => override.matcher.options === 'Total')
        ?.properties.some((property) => property.id === 'custom.hideFrom.viz') ?? false
    scaleTables(doc, scene, options, win)

    // 220px is 0.55 of the 400 every column wants: under 0.72, so Total
    // goes and Asset keeps its whole 100px.
    expect(totalHidden()).toBe(true)
    expect(widths()[0]).toEqual(['Asset', 100, undefined])

    // At 300px (0.75, 0.74 in steps of 0.02) every column is back, smaller.
    px = 300
    win.resize.forEach((listener) => {
      listener()
    })
    expect(totalHidden()).toBe(false)
    expect(widths()[0]).toEqual(['Asset', 74, 74])
  })

  it("replaces an old scene's resize listener", () => {
    const win = fakeWindow()
    board(320, win).scale()
    board(320, win).scale()

    expect(win.resize).toHaveLength(1)
  })

  it('does nothing without the dashboard scene', () => {
    const doc = document.implementation.createHTMLDocument('board')
    expect(() =>
      scaleTables(doc, undefined, { panels: ['Equities'], fixedPx: 300, ratioMinPx: 100 }, {})
    ).not.toThrow()
  })
})
