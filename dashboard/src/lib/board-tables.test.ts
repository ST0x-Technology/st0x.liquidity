// @vitest-environment happy-dom

import { beforeEach, describe, expect, it, vi } from 'vitest'

// The Grafana board's table behaviour (observability/liquidity-panels/
// board-tables.js): instant hover text on cell lights and a locked sort on
// tree tables.

type RowTip = {
  rowFields: string[]
  flagFields: string[]
  chains: Record<string, string>
  empty: string
  columns: { name: string; states: Record<string, [string, string, string]> }[]
}

type BoardTables = {
  installBoardTables: (
    doc: Document,
    options: {
      lockedSortPanels: string[]
      alignedPanels: string[]
      rowTints?: Record<string, string>
      headerTips?: Record<string, Record<string, string>>
      treeSorts?: Record<string, { rowField: string; keys: Record<string, string> }>
      rowTips?: Record<string, RowTip>
      wrapperRatios?: Record<string, Record<string, number>>
      unitTags?: Record<
        string,
        {
          match: string
          column: string
          symbol?: string
          chains?: Record<string, string>
          tags: {
            values: string[]
            text: string
            tip: string
            convert?: { unit: string; times: boolean }
          }[]
        }
      >
      sceneOf?: () => unknown
      theme: unknown
    }
  ) => void
}

// A plain script in the board, an ES module through its export line here.
// Loaded through a glob so svelte-check does not type-check the script.
const scriptPath = '../../../observability/liquidity-panels/board-tables.js'
const loadScript = import.meta.glob('../../../observability/liquidity-panels/board-tables.js')[
  scriptPath
]
if (!loadScript) throw new Error(`no ${scriptPath}`)
const { installBoardTables } = (await loadScript()) as BoardTables

const theme = {
  colors: {
    text: { primary: '#fff' },
    border: { weak: '#333' },
    background: { secondary: '#222' }
  }
}

// Grafana's markup for a table panel: its section, a header cell with the
// column-name button and the filter button, and one cell with a light.
const panel = (title: string) => `
  <section data-testid="data-testid Panel header ${title}">
    <div role="columnheader"><div>
      <button data-name>Flags</button>
      <button aria-label="Filter Flags" data-filter></button>
    </div></div>
    <div role="gridcell"><p><abbr title="Rebalancing on this chain: paused">●</abbr></p></div>
  </section>`

describe('board board-tables.js', () => {
  let doc: Document

  beforeEach(() => {
    doc = document.implementation.createHTMLDocument('board')
    doc.body.innerHTML = panel('Equities') + panel('Trades')
    installBoardTables(doc, {
      lockedSortPanels: ['Equities'],
      alignedPanels: ['Equities'],
      rowTints: { Base: 'rgba(0,82,255,0.10)' },
      headerTips: { Equities: { Flags: 'Per chain lights' } },
      theme
    })
  })

  const tip = () => doc.querySelector<HTMLElement>('.liq-tip')
  const inPanel = (title: string, selector: string) => {
    const element = doc.querySelector<HTMLElement>(
      `section[data-testid="data-testid Panel header ${title}"] ${selector}`
    )
    if (!element) throw new Error(`no ${selector} in ${title}`)
    return element
  }
  const hover = (element: Element, type = 'mouseover') => {
    element.dispatchEvent(new MouseEvent(type, { bubbles: true }))
  }
  // Whether a click on `element` reaches the table, as Grafana's sort does.
  const reachesTable = (title: string, element: HTMLElement) => {
    const handler = vi.fn()
    inPanel(title, '[role="columnheader"]').addEventListener('click', handler)
    element.click()
    return handler.mock.calls.length === 1
  }

  it('shows a light’s title at once and hides it when the pointer leaves', () => {
    const light = inPanel('Equities', 'abbr')

    hover(light)
    expect(tip()?.hidden).toBe(false)
    expect(tip()?.textContent).toBe('Rebalancing on this chain: paused')
    // The browser's own tooltip would show the same text late.
    expect(light.hasAttribute('title')).toBe(false)

    hover(light, 'mouseout')
    expect(tip()?.hidden).toBe(true)

    hover(light)
    expect(tip()?.textContent).toBe('Rebalancing on this chain: paused')
  })

  it('keeps a click on a locked table’s column name from sorting it', () => {
    expect(reachesTable('Equities', inPanel('Equities', '[data-name]'))).toBe(false)
  })

  it('still lets a locked table filter, and other tables sort', () => {
    expect(reachesTable('Equities', inPanel('Equities', '[data-filter]'))).toBe(true)
    expect(reachesTable('Trades', inPanel('Trades', '[data-name]'))).toBe(true)
  })

  it('keeps room for a scrollbar on aligned tables only', () => {
    const css = doc.head.querySelector('style')?.textContent ?? ''

    expect(css).toContain(
      'section[data-testid="data-testid Panel header Equities"] [role="grid"]{scrollbar-gutter:stable}'
    )
    expect(css).not.toContain('Trades"] [role="grid"]')
  })

  it('stretches the tooltip triangle over its cell and hides it', () => {
    const css = doc.head.querySelector('style')?.textContent ?? ''

    expect(css).toContain(
      '[data-testid="data-testid tableng tooltip caret"]{top:0!important;left:0!important;right:0!important;bottom:0!important;width:auto!important;height:auto!important;opacity:0!important}'
    )
  })

  it("tints a row that holds a chain's mark", () => {
    const css = doc.head.querySelector('style')?.textContent ?? ''

    expect(css).toContain(
      '[role="row"]:has(img[alt^="Base"]) [role="gridcell"]{background-color:rgba(0,82,255,0.10)}'
    )
    // Not over its ratio bar: the tint shows only through the bar's track.
    expect(css).not.toContain('box-shadow:inset')
  })

  it('explains a header named in headerTips, and only in its panel', () => {
    hover(inPanel('Equities', '[data-name]'))
    expect(tip()?.hidden).toBe(false)
    expect(tip()?.textContent).toBe('Per chain lights')

    hover(inPanel('Equities', '[data-name]'), 'mouseout')
    expect(tip()?.hidden).toBe(true)

    hover(inPanel('Trades', '[data-name]'))
    expect(tip()?.hidden).toBe(true)
  })

  it("shows a chain mark's name at once", () => {
    const cell = inPanel('Equities', '[role="gridcell"]')
    cell.insertAdjacentHTML('beforeend', '<img title="Robinhood" alt="Robinhood">')
    const mark = cell.querySelector('img')
    if (!mark) throw new Error('no mark')

    hover(mark)

    expect(tip()?.hidden).toBe(false)
    expect(tip()?.textContent).toBe('Robinhood')

    // The title has moved off the mark, and the tip still hides and shows.
    hover(mark, 'mouseout')
    expect(tip()?.hidden).toBe(true)
    hover(mark)
    expect(tip()?.textContent).toBe('Robinhood')
  })

  describe('row tips', () => {
    const ROW_TIPS: Record<string, RowTip> = {
      Equities: {
        rowFields: ['row\\col', 'Asset'],
        flagFields: ['flags', 'Flags'],
        chains: { base: 'Base', robinhood: 'Robinhood' },
        empty: 'No chain flags published',
        columns: [
          {
            name: 'Counter trading',
            states: { '2': ['on', 'green', '●'], '1': ['off', 'red', '●'] }
          },
          {
            name: 'Rebalancing',
            states: { '2': ['enabled', 'green', '●'], '4': ['paused', 'amber', '●'] }
          },
          {
            name: 'Extended hours',
            states: { '2': ['on', 'green', '●'], '3': ['n/a', 'grey', '○'] }
          }
        ]
      }
    }

    // The Equities panel's data after its transformations: the flags are
    // strings, the asset and Alpaca rows have none.
    const rowTable = (flags: (string | null)[]) => {
      const series = [
        {
          fields: [
            {
              name: 'row\\col',
              config: { displayName: 'Asset' },
              values: ['AAPL', 'AAPL/alpaca', 'AAPL/base', 'AAPL/robinhood', 'TSLA', 'TSLA/base']
            },
            { name: 'flags', config: {}, values: flags }
          ]
        }
      ]
      const scene = {
        state: {
          body: {
            state: {
              grid: {
                state: {
                  children: [
                    {
                      state: {
                        body: {
                          state: { title: 'Equities', $data: { state: { data: { series } } } }
                        }
                      }
                    }
                  ]
                }
              }
            }
          }
        }
      }
      const cell = (key: string, html: string) =>
        `<div role="gridcell"><p><abbr title="row:${key}">${html}</abbr></p></div>`
      doc.body.innerHTML = `<section data-testid="data-testid Panel header Equities">${cell('AAPL', 'AAPL')}${cell('AAPL/robinhood', '<img alt="Robinhood">')}${cell('TSLA', 'TSLA')}</section><section data-testid="data-testid Panel header Trades">${cell('AAPL', 'AAPL')}</section>`
      installBoardTables(doc, {
        lockedSortPanels: [],
        alignedPanels: [],
        rowTips: ROW_TIPS,
        sceneOf: () => scene,
        theme
      })
    }
    const lines = () =>
      [...(tip()?.querySelectorAll('tr') ?? [])].map((row) =>
        [...row.children].map((cell) => cell.textContent.trim())
      )

    it("lists every chain's flags under a symbol, and one chain's on its mark", () => {
      rowTable([null, null, '222', '143', null, '213'])

      hover(inPanel('Equities', 'abbr[title="row:AAPL"]'))
      expect(tip()?.hidden).toBe(false)
      expect(tip()?.querySelector('.liq-tip-title')?.textContent).toBe('AAPL')
      expect(lines()).toEqual([
        ['', 'Counter trading', 'Rebalancing', 'Extended hours'],
        ['Base', '● on', '● enabled', '● on'],
        ['Robinhood', '● off', '● paused', '○ n/a']
      ])

      hover(inPanel('Equities', 'img').closest('abbr') ?? doc.body)
      expect(tip()?.querySelector('.liq-tip-title')?.textContent).toBe('AAPL on Robinhood')
      expect(lines().slice(1)).toEqual([['Robinhood', '● off', '● paused', '○ n/a']])
      // A digit no column names reads as unknown, not as a wrong state.
      hover(inPanel('Equities', 'abbr[title="row:TSLA"]'))
      expect(lines().slice(1)).toEqual([['Base', '● on', '? unknown', '○ n/a']])
    })

    it("lists a symbol's chain without flags as not published, so it is not one chain's tip", () => {
      rowTable([null, null, '222', null, null, '213'])

      hover(inPanel('Equities', 'abbr[title="row:AAPL"]'))
      expect(lines().slice(1)).toEqual([
        ['Base', '● on', '● enabled', '● on'],
        ['Robinhood', 'not published']
      ])
      // The chain's own tip still says nothing it does not know.
      hover(inPanel('Equities', 'img').closest('abbr') ?? doc.body)
      expect(tip()?.textContent).toBe('AAPL on RobinhoodNo chain flags published')
    })

    it('says so when no chain has flags, and shows nothing outside its panels', () => {
      rowTable([null, null, null, null, null, null])

      hover(inPanel('Equities', 'abbr[title="row:AAPL"]'))
      expect(tip()?.textContent).toBe('AAPLNo chain flags published')

      hover(inPanel('Equities', 'abbr'), 'mouseout')
      hover(inPanel('Trades', 'abbr'))
      expect(tip()?.hidden).toBe(true)
    })

    it('escapes what the data holds', () => {
      rowTable([null, null, '222', null, null, null])
      inPanel('Equities', 'abbr').setAttribute('title', 'row:<b>AAPL')

      hover(inPanel('Equities', 'abbr'))

      expect(tip()?.querySelector('b')).toBeNull()
      expect(tip()?.querySelector('.liq-tip-title')?.textContent).toBe('<b>AAPL')
    })
  })

  describe('tree sort', () => {
    type Step = { id: string; options?: unknown }
    const DEFAULT: Step[] = [
      { id: 'groupingToMatrix' },
      { id: 'sortBy', options: { fields: {}, sort: [{ field: 'counter_trading', desc: true }] } },
      { id: 'organize' }
    ]
    const sortBy = (field: string, desc: boolean) => ({
      id: 'sortBy',
      options: { fields: {}, sort: [{ field, desc }] }
    })

    // The Equities panel's data in a stand-in for the dashboard scene, and
    // its headers on the page: Asset (column 1), Flags (2), Total (7).
    const treeTable = () => {
      const data = {
        state: { transformations: DEFAULT },
        setState(update: { transformations: Step[] }) {
          data.state = { ...data.state, ...update }
        },
        reprocessTransformations: vi.fn()
      }
      const scene = {
        state: {
          body: {
            state: {
              grid: {
                state: {
                  children: [{ state: { body: { state: { title: 'Equities', $data: data } } } }]
                }
              }
            }
          }
        }
      }
      const header = (column: number, name: string) =>
        `<div role="columnheader" aria-colindex="${String(column)}"><div><button>${name}</button></div></div>`
      doc.body.innerHTML = `<section data-testid="data-testid Panel header Equities">${header(1, 'Asset')}${header(2, 'Flags')}${header(7, 'Total')}</section>`
      const install = () => {
        installBoardTables(doc, {
          lockedSortPanels: ['Equities'],
          alignedPanels: [],
          treeSorts: { Equities: { rowField: 'row\\col', keys: { Total: 'sort_total' } } },
          sceneOf: () => scene,
          theme
        })
      }
      install()
      const click = (name: string) =>
        [...doc.querySelectorAll('button')].find((button) => button.textContent === name)?.click()
      const arrow = () => doc.getElementById('liq-board-sort')?.textContent ?? ''
      return { data, click, arrow, install }
    }

    it('sorts by the key, largest first, then smallest first, then back to the default', () => {
      const { data, click, arrow, install } = treeTable()
      // A second render replaces the first one's listeners, so a click
      // still moves the sort one step.
      install()

      click('Total')
      expect(data.state.transformations).toEqual([
        { id: 'groupingToMatrix' },
        sortBy('row\\col', false),
        sortBy('sort_total', true),
        { id: 'organize' }
      ])
      expect(data.reprocessTransformations).toHaveBeenCalledTimes(1)
      expect(arrow()).toContain('[aria-colindex="7"]')
      expect(arrow()).toContain('\\25BC')

      click('Total')
      expect(data.state.transformations[2]).toEqual(sortBy('sort_total', false))
      expect(arrow()).toContain('\\25B2')

      click('Total')
      expect(data.state.transformations).toBe(DEFAULT)
      expect(arrow()).toBe('')
    })

    it('restores the default order from the first column, and ignores other headers', () => {
      const { data, click } = treeTable()

      click('Total')
      click('Flags')
      expect(data.state.transformations[2]).toEqual(sortBy('sort_total', true))

      click('Asset')
      expect(data.state.transformations).toBe(DEFAULT)
    })
  })

  it('tags each trade size wt or t, shows it in the other unit on hover, and follows a reused row', async () => {
    const cell = (column: number, text: string) =>
      `<div role="gridcell" aria-colindex="${String(column)}"><div>${text}</div></div>`
    const row = (asset: string, venue: string, size: string) =>
      `<div role="row">${cell(1, asset)}${cell(2, venue)}${cell(3, size)}</div>`
    const header = (column: number, name: string) =>
      `<div role="columnheader" aria-colindex="${String(column)}">${name}</div>`
    doc.body.innerHTML = `<section data-testid="data-testid Panel header Trades">
      <div role="row">${header(1, 'Asset')}${header(2, 'Venue')}${header(3, 'Size')}</div>
      ${row('AAPL', 'Raindex', '10')}${row('AAPL', 'Alpaca', '1,020')}${row('TSLA', 'Raindex', '2')}</section>`
    installBoardTables(doc, {
      lockedSortPanels: [],
      alignedPanels: [],
      unitTags: {
        Trades: {
          match: 'Venue',
          column: 'Size',
          symbol: 'Asset',
          chains: { base: 'Base' },
          tags: [
            {
              values: ['Raindex', 'Bebop'],
              text: 'wt',
              convert: { unit: 't', times: true },
              tip: 'Wrapped shares.'
            },
            {
              values: ['Alpaca'],
              text: 't',
              convert: { unit: 'wt', times: false },
              tip: 'Underlying shares.'
            }
          ]
        }
      },
      wrapperRatios: { AAPL: { base: 1.02 } },
      theme
    })
    const sizes = () =>
      [...doc.querySelectorAll<HTMLElement>('[role="gridcell"][aria-colindex="3"]')].map((size) => [
        size.getAttribute('data-liq-unit') ?? '',
        size.getAttribute('data-liq-tip') ?? ''
      ])
    expect(sizes()).toEqual([
      ['wt', "10 wt ≈ 10.2 t at today's wrapper ratio"],
      ['t', "1,020 t ≈ 1,000 wt at today's wrapper ratio"],
      ['wt', 'Wrapped shares. No wrapper ratio published for TSLA yet.']
    ])

    // Scrolling reuses the first row for a trade at a venue with no tag.
    const venue = doc.querySelector('[role="gridcell"][aria-colindex="2"] div')
    if (!venue) throw new Error('no venue cell')
    venue.textContent = 'DryRun'
    await vi.waitFor(() => {
      expect(sizes()[0]).toEqual(['', ''])
    })
    expect(doc.getElementById('liq-board-tables')?.textContent).toContain(
      '[role="gridcell"][data-liq-unit]::after{content:attr(data-liq-unit)'
    )
  })

  it("reads a trade size in the browser's number format", () => {
    const RealNumberFormat = Intl.NumberFormat
    const spanish = vi.spyOn(Intl, 'NumberFormat').mockImplementation(function () {
      return new RealNumberFormat('es-AR')
    } as unknown as typeof Intl.NumberFormat)
    const cell = (column: number, text: string) =>
      `<div role="gridcell" aria-colindex="${String(column)}"><div>${text}</div></div>`
    doc.body.innerHTML = `<section data-testid="data-testid Panel header Trades">
      <div role="row"><div role="columnheader" aria-colindex="1">Asset</div><div role="columnheader" aria-colindex="2">Venue</div><div role="columnheader" aria-colindex="3">Size</div></div>
      <div role="row">${cell(1, 'AAPL')}${cell(2, 'Raindex')}${cell(3, '1.020,5')}</div></section>`
    installBoardTables(doc, {
      lockedSortPanels: [],
      alignedPanels: [],
      unitTags: {
        Trades: {
          match: 'Venue',
          column: 'Size',
          symbol: 'Asset',
          chains: { base: 'Base' },
          tags: [
            {
              values: ['Raindex'],
              text: 'wt',
              convert: { unit: 't', times: true },
              tip: 'Wrapped shares.'
            }
          ]
        }
      },
      wrapperRatios: { AAPL: { base: 2 } },
      theme
    })
    spanish.mockRestore()

    expect(
      doc.querySelector('[role="gridcell"][aria-colindex="3"]')?.getAttribute('data-liq-tip')
    ).toBe("1,020.5 wt ≈ 2,041 t at today's wrapper ratio")
  })

  it('installs once, however often the header renders, with its newest rules', () => {
    installBoardTables(doc, {
      lockedSortPanels: ['Equities'],
      alignedPanels: [],
      rowTints: { Robinhood: 'rgba(204,255,0,0.14)' },
      theme
    })

    expect(doc.querySelectorAll('.liq-tip')).toHaveLength(1)
    // One element for the rules and one for the sort arrows, never more.
    expect(doc.head.querySelectorAll('#liq-board-tables')).toHaveLength(1)
    expect(doc.head.querySelectorAll('#liq-board-sort')).toHaveLength(1)
    const css = doc.getElementById('liq-board-tables')?.textContent ?? ''
    expect(css).toContain('img[alt^="Robinhood"]')
    expect(css).not.toContain('img[alt^="Base"]')
  })
})
