import { describe, expect, it } from 'vitest'

// The Grafana board's header (observability/liquidity-panels/header.js) reads
// its query rows through header-rows.js, a plain script in the board and an
// ES module through its export line here.

const panels = new URL('../../../observability/liquidity-panels/', import.meta.url)

type HeaderRow = { k: string; Time?: number; Value: unknown; [label: string]: unknown }
type HeaderRows = {
  readHeaderRows: (rows: HeaderRow[]) => {
    value: Record<string, number | null>
    commit: { sha: string; at: number } | null
    info: Record<string, unknown>
  }
}

const { readHeaderRows } = (await import(new URL('header-rows.js', panels).href)) as HeaderRows

describe('board header-rows.js', () => {
  it('keeps the newest row of a plain value', () => {
    const { value } = readHeaderRows([
      { k: 'uptime', Time: 2000, Value: 120 },
      { k: 'uptime', Time: 1000, Value: 60 },
      { k: 'up', Time: 1000, Value: 1 },
      { k: 'up', Time: 2000, Value: 0 }
    ])
    expect(value).toEqual({ uptime: 120, up: 0 })
  })

  it('reads a missing or non-numeric value as null', () => {
    const { value } = readHeaderRows([
      { k: 'trigger', Time: 1000, Value: null },
      { k: 'cash_reserved', Time: 1000, Value: 'NaN' }
    ])
    expect(value).toEqual({ trigger: null, cash_reserved: null })
  })

  it('keeps the info label set with the newest sample timestamp, whatever the row times', () => {
    // After a restart the old and new label sets arrive at the same row
    // times; only their sample timestamps tell them apart.
    const rows: HeaderRow[] = [
      { k: 'info', Time: 2000, Value: 1990, broker: 'alpaca', log_level: 'debug' },
      { k: 'info', Time: 2000, Value: 1500, broker: 'alpaca', log_level: 'info' },
      { k: 'info', Time: 3000, Value: 1500, broker: 'alpaca', log_level: 'info' }
    ]
    expect(readHeaderRows(rows).info['log_level']).toBe('debug')
    expect(readHeaderRows([...rows].reverse()).info['log_level']).toBe('debug')
  })

  it('keeps the commit with the newest sample timestamp', () => {
    const { commit } = readHeaderRows([
      { k: 'commit', Time: 3000, Value: 1000, git_commit: 'old0000000' },
      { k: 'commit', Time: 2000, Value: 1900, git_commit: 'new1111111' }
    ])
    expect(commit).toEqual({ sha: 'new1111111', at: 1900 })
  })

  it('never lets a commit or info row without a finite timestamp take the newest slot', () => {
    const untimed: HeaderRow[] = [
      { k: 'commit', Time: 3000, Value: null, git_commit: 'none000000' },
      { k: 'commit', Time: 3000, Value: 'NaN', git_commit: 'nan0000000' },
      { k: 'info', Time: 3000, Value: undefined, log_level: 'stale' }
    ]
    const rows: HeaderRow[] = [
      ...untimed,
      { k: 'commit', Time: 2000, Value: 1900, git_commit: 'new1111111' },
      { k: 'info', Time: 2000, Value: 1900, log_level: 'debug' }
    ]
    expect(readHeaderRows(rows).commit).toEqual({ sha: 'new1111111', at: 1900 })
    expect(readHeaderRows(rows).info['log_level']).toBe('debug')
    expect(readHeaderRows(untimed)).toEqual({ value: {}, commit: null, info: {} })
  })

  it('has no commit and an empty info without their rows', () => {
    expect(readHeaderRows([])).toEqual({ value: {}, commit: null, info: {} })
  })
})
