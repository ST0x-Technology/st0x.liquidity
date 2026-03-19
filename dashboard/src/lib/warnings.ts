import type { Warning } from '$lib/api/Warning'
import { matcher } from '$lib/fp'

const categoryLabel = (category: Warning['category']): string => {
  const labels: Record<Warning['category'], string> = {
    equity_mint: 'equity mint',
    equity_redemption: 'equity redemption',
    usdc_bridge: 'USDC bridge'
  }
  return labels[category]
}

const matchWarning = matcher<Warning>()('kind')

export const warningMessage = (warning: Warning): string =>
  matchWarning(warning, {
    category_unavailable: ({ category }) =>
      `Failed to load ${categoryLabel(category)} transfers`,
    aggregate_replay_failed: ({ category, aggregate_id }) =>
      `Failed to replay ${categoryLabel(category)} transfer ${aggregate_id}`
  })
