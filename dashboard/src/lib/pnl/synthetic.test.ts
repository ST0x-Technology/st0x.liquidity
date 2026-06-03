import { describe, expect, it } from 'vitest'
import { STREAM_KEYS } from './report'
import {
  generateSyntheticPnlDashboard,
  generateSyntheticPnlDataset,
} from './synthetic'

describe('generateSyntheticPnlDataset', () => {
  it('generates deterministic multi-day multi-asset PnL fixtures', () => {
    const dataset = generateSyntheticPnlDataset()

    expect(dataset.symbols).toEqual(['RKLB', 'SGOV', 'CRCL', 'BMNR'])
    expect(dataset.startAt).toBe('2026-05-04T13:30:00.000Z')
    expect(dataset.endAt).toBe('2026-05-14T13:30:00.000Z')
    expect(dataset.fills.length).toBeGreaterThan(1_800)
    expect(dataset.marks.length).toBe(360)
    expect(dataset.fills[0]?.rowid).toBe(1)
    expect(dataset.fills.at(-1)?.rowid).toBe(dataset.fills.length)
    expect(dataset.fills.some((fill) => fill.venue === 'onchain')).toBe(true)
    expect(dataset.fills.some((fill) => fill.venue === 'offchain')).toBe(true)
    expect(dataset.fills.some((fill) => fill.fillId.includes('passive-net'))).toBe(true)
  })

  it('generates dashboard-ready mock PnL windows and linked lots', () => {
    const dashboard = generateSyntheticPnlDashboard()

    expect(STREAM_KEYS).toEqual([
      'counterTradePnlUsd',
      'onchainNettingPnlUsd',
      'directionalInventoryBaselinePnlUsd',
      'directionalImbalanceExcessPnlUsd',
    ])
    expect(dashboard.windows).toHaveLength(500)
    expect(dashboard.windows[0]?.startAt).toBe('2025-01-01T13:30:00.000Z')
    expect(dashboard.windows.at(-1)?.startAt).toBe('2026-05-15T13:30:00.000Z')
    expect(dashboard.windows.filter((window) => window.isWeekend).length).toBeGreaterThan(140)
    expect(dashboard.report.symbols.map((row) => row.symbol)).toEqual([
      'BMNR',
      'CRCL',
      'RKLB',
      'SGOV',
    ])
    expect(dashboard.report.entries.some((entry) => entry.pnlBucket === 'counter_trade')).toBe(true)
    expect(dashboard.report.entries.some((entry) => entry.pnlBucket === 'onchain_netting')).toBe(true)
    expect(dashboard.report.entries.some((entry) => entry.pnlBucket === 'directional_exposure')).toBe(true)
    expect(dashboard.report.entries.some((entry) => entry.delayedCounterTrade)).toBe(true)
    expect(Number(dashboard.report.summary.totalPnlUsd)).not.toBe(0)
  })

  it('keeps baseline drift fields internally consistent', () => {
    const dashboard = generateSyntheticPnlDashboard()
    const symbolBaseline = dashboard.report.symbols.reduce(
      (total, row) => total + Number(row.directionalInventoryBaselinePnlUsd),
      0,
    )

    expect(Number(dashboard.report.summary.directionalInventoryBaselinePnlUsd)).toBeCloseTo(symbolBaseline, 2)
    expect(Number(dashboard.report.summary.inventoryDriftUsd)).toBe(
      Number(dashboard.report.summary.directionalInventoryBaselinePnlUsd),
    )

    for (const row of dashboard.report.symbols) {
      expect(Number(row.inventoryDriftUsd)).toBe(Number(row.directionalInventoryBaselinePnlUsd))
    }
  })

  it('keeps aggregate PnL identities and linked entries reconciled', () => {
    const dashboard = generateSyntheticPnlDashboard()
    const summary = dashboard.report.summary
    const entrySum = (bucket: string) => dashboard.report.entries
      .filter((entry) => entry.pnlBucket === bucket)
      .reduce((total, entry) => total + Number(entry.realizedPnlUsd), 0)

    expect(Number(summary.directionalExposurePnlUsd)).toBeCloseTo(
      Number(summary.directionalInventoryBaselinePnlUsd) + Number(summary.directionalImbalanceExcessPnlUsd),
      1,
    )
    expect(Number(summary.totalPnlUsd)).toBeCloseTo(
      Number(summary.counterTradePnlUsd)
        + Number(summary.onchainNettingPnlUsd)
        + Number(summary.directionalInventoryBaselinePnlUsd)
        + Number(summary.directionalImbalanceExcessPnlUsd),
      1,
    )
    expect(Number(summary.realizedPnlUsd)).toBeCloseTo(
      Number(summary.counterTradePnlUsd)
        + Number(summary.onchainNettingPnlUsd)
        + Number(summary.directionalImbalanceExcessPnlUsd),
      1,
    )
    expect(entrySum('counter_trade')).toBeCloseTo(Number(summary.counterTradePnlUsd), 1)
    expect(entrySum('onchain_netting')).toBeCloseTo(Number(summary.onchainNettingPnlUsd), 1)
    expect(entrySum('directional_exposure')).toBeCloseTo(Number(summary.directionalImbalanceExcessPnlUsd), 1)
  })
})
