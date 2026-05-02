export type StatusStyle = {
  text: string
  dot: string
}

export const kindLabel = (kind: string): string => {
  switch (kind) {
    case 'equity_mint': return 'Mint'
    case 'equity_redemption': return 'Redeem'
    case 'usdc_bridge': return 'USDC Bridge'
    default: return kind
  }
}

/// Maps a status string to colour classes.  Works for both DTO statuses
/// (snake_case, used in the table) and raw event names (PascalCase,
/// used in the detail modal timeline).
export const statusStyle = (status: string): StatusStyle => {
  const lower = status.toLowerCase()

  if (lower.includes('completed') || lower.includes('deposited') || lower.includes('confirmed')) {
    return { text: 'text-green-500', dot: 'bg-green-500' }
  }

  if (lower.includes('failed') || lower.includes('rejected')) {
    return { text: 'text-destructive', dot: 'bg-destructive' }
  }

  return { text: 'text-muted-foreground', dot: 'bg-muted-foreground' }
}

export const humanizeStatus = (status: string): string =>
  status
    .split('_')
    .map((word) => word.charAt(0).toUpperCase() + word.slice(1))
    .join(' ')

export const humanizeStep = (step: string): string =>
  step.replace(/([A-Z])/g, ' $1').trim()

export const isTxHash = (value: unknown): value is string =>
  typeof value === 'string' && /^0x[0-9a-fA-F]{64}$/.test(value)

const SKIP_FIELDS = new Set(['attestation'])

export const isTimestampField = (key: string): boolean => key.endsWith('_at')

export const isTransferRef = (value: unknown): value is Record<string, string> =>
  typeof value === 'object' &&
  value !== null &&
  ('AlpacaId' in value || 'OnchainTx' in value)

export const formatFieldName = (key: string): string =>
  key.replace(/_/g, ' ').replace(/\b\w/g, (char) => char.toUpperCase())

export const extractTimestamp = (payload: Record<string, unknown>): string | null => {
  for (const [key, value] of Object.entries(payload)) {
    if (isTimestampField(key) && typeof value === 'string') return value
  }
  return null
}

export const detailFields = (payload: Record<string, unknown>): Array<[string, unknown]> =>
  Object.entries(payload).filter(
    ([key]) => !isTimestampField(key) && !SKIP_FIELDS.has(key),
  )
