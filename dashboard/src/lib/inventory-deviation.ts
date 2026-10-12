import type { Settings } from '$lib/api/Settings'

export type DeviationStyle = 'normal' | 'high' | 'low'

export type Deviation = { style: DeviationStyle }

export type DeviationBands = Pick<
  Settings,
  'equityTarget' | 'equityDeviation' | 'usdcTarget' | 'usdcDeviation'
>

// A missing band styles no row: equity has no chain-level target when only
// per-symbol overrides are configured, and cash has no band when no corridor
// rebalances cash. Cash never borrows the equity band, which no trigger uses.
export const ratioDeviation = (
  bands: DeviationBands | undefined,
  ratio: number,
  isCash: boolean
): Deviation | null => {
  if (!bands) return null

  const target = isCash ? bands.usdcTarget : bands.equityTarget
  const deviation = isCash ? bands.usdcDeviation : bands.equityDeviation
  if (target === null || deviation === null) return null

  const diff = ratio - target

  if (diff > deviation) return { style: 'high' }
  if (diff < -deviation) return { style: 'low' }
  return { style: 'normal' }
}
