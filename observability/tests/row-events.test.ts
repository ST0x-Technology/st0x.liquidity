import { describe, expect, it } from 'vitest'

// The detail dialog's per-row event query (observability/liquidity-panels/
// row-events.js). board-detail.test.ts runs it through the dialog; these
// cover what the dialog's rows do not reach.

type Entry = { event_id?: string; step?: string }
type RowEvents = {
  rowEventsFilter: (eventsFilter: string, id: string) => string
  rowEventsRequest: (row: {
    datasource: unknown
    eventsFilter: string
    project: string
    id: string
    first: number
    newest: number
  }) => { queries: { maxDataPoints: number }[] }
  responseFrame: (response: unknown, refId: string) => { fields: { name: string }[] }
  mergeEntries: (board: Entry[], fetched: Entry[]) => Entry[]
}

// A plain script in the board, an ES module through its export line here.
// Loaded through a glob so tsc does not type-check the script.
const scriptPath = '../liquidity-panels/row-events.js'
const loadScript = import.meta.glob('../liquidity-panels/row-events.js')[scriptPath]
if (!loadScript) throw new Error(`no ${scriptPath}`)
const { rowEventsFilter, rowEventsRequest, responseFrame, mergeEntries } =
  (await loadScript()) as RowEvents

describe('board row-events.js', () => {
  it('quotes the id, so a quote or backslash in it cannot end the filter', () => {
    expect(rowEventsFilter('target="liq_event"', 'a"b\\c')).toBe(
      'target="liq_event" jsonPayload.id="a\\"b\\\\c"'
    )
  })

  it('asks for more lines than the 100 Cloud Logging returns by default', () => {
    const request = rowEventsRequest({
      datasource: {},
      eventsFilter: 'target="liq_event"',
      project: 't0-liquidity',
      id: 'M1',
      first: 0,
      newest: 1
    })

    expect(request.queries[0]?.maxDataPoints).toBe(1000)
  })

  it('reads an empty answer as no lines and a query error as an error', () => {
    expect(responseFrame({ results: { 'row-events': { frames: [] } } }, 'row-events')).toEqual({
      fields: []
    })
    expect(() =>
      responseFrame({ results: { 'row-events': { error: 'quota exceeded' } } }, 'row-events')
    ).toThrow('quota exceeded')
    expect(() => responseFrame({ results: {} }, 'row-events')).toThrow('no result')
  })

  it('keeps each event once when the board and the row query both have it', () => {
    const merged = mergeEntries(
      [{ event_id: 'Mint:M1:1', step: 'board' }],
      [
        { event_id: 'Mint:M1:1', step: 'fetched' },
        { event_id: 'Mint:M1:2', step: 'fetched' }
      ]
    )

    expect(merged).toEqual([
      { event_id: 'Mint:M1:1', step: 'board' },
      { event_id: 'Mint:M1:2', step: 'fetched' }
    ])
  })
})
