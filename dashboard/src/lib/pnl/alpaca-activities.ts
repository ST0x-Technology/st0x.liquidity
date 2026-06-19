import Decimal from 'decimal.js'
import type {
  PnlAccountingBucket,
  PnlAccountingEffect,
  PnlCostCategory,
  PnlCostEntry
} from './report'
import type { PnlQuery } from './api'

type AlpacaAccountActivity = {
  id: string
  activity_type: string
  activity_sub_type?: string | null
  date?: string | null
  created_at?: string | null
  net_amount?: string | null
  symbol?: string | null
  qty?: string | null
  per_share_amount?: string | null
  price?: string | null
  side?: string | null
  order_id?: string | null
  transaction_time?: string | null
  description?: string | null
  currency?: string | null
}

type AlpacaActivitiesResponse = {
  entries?: unknown
  total?: number
}

const FEE_ACTIVITY_TYPES = new Set(['FEE', 'PTC'])
const FEE_REBATE_ACTIVITY_TYPES = new Set(['PTR'])
const INTEREST_ACTIVITY_TYPES = new Set(['INT', 'INTNRA', 'INTTW'])
const DIVIDEND_ACTIVITY_TYPES = new Set([
  'DIV',
  'DIVCGL',
  'DIVCGS',
  'DIVFEE',
  'DIVFT',
  'DIVNRA',
  'DIVROC',
  'DIVTW',
  'DIVTXEX'
])

const fmtDecimal = (value: Decimal): string => {
  const fixed = value.toFixed(9)
  const trimmed = fixed.replace(/\.?0+$/u, '')
  return trimmed === '-0' || trimmed === '' ? '0' : trimmed
}

const nextUtcDate = (date: string, days: number): string => {
  const parsed = Date.parse(`${date}T00:00:00.000Z`)
  if (!Number.isFinite(parsed)) return date
  const next = new Date(parsed + days * 24 * 60 * 60 * 1000)
  return next.toISOString().slice(0, 10)
}

const alpacaActivityParams = (query: PnlQuery): URLSearchParams => {
  const params = new URLSearchParams()
  if (query.fromDate !== undefined && query.fromDate !== '') {
    params.set('after', `${query.fromDate}T00:00:00.000Z`)
  }

  if (query.toDate !== undefined && query.toDate !== '') {
    // Alpaca notes that non-trade fee activities can be created the day after
    // trade date. Pull one extra day from the broker API, then let the PnL
    // adapter filter by the activity's reported date.
    params.set('until', `${nextUtcDate(query.toDate, 2)}T00:00:00.000Z`)
  }

  return params
}

const activityTimestamp = (activity: AlpacaAccountActivity): string | null => {
  if (activity.transaction_time !== undefined && activity.transaction_time !== null) {
    return activity.transaction_time
  }

  if (activity.date !== undefined && activity.date !== null && activity.date !== '') {
    // Non-trade Alpaca activities are date-level ledger rows. Anchor them
    // inside RTH so counter-trade fees follow the counter-trading-active slice
    // rather than being misclassified as overnight by a midnight timestamp.
    return `${activity.date}T16:00:00.000Z`
  }

  return null
}

const classifyActivity = (
  activityType: string,
  signedAmount: Decimal
): {
  category: PnlCostCategory
  accountingBucket: PnlAccountingBucket
  effect: PnlAccountingEffect
} | null => {
  if (FEE_ACTIVITY_TYPES.has(activityType) || FEE_REBATE_ACTIVITY_TYPES.has(activityType)) {
    return {
      category: 'broker_fee',
      accountingBucket: 'generic',
      effect: signedAmount.isNegative() ? 'cost' : 'revenue'
    }
  }

  if (INTEREST_ACTIVITY_TYPES.has(activityType)) {
    return {
      category: 'margin_interest',
      accountingBucket: 'generic',
      effect: signedAmount.isNegative() ? 'cost' : 'revenue'
    }
  }

  if (DIVIDEND_ACTIVITY_TYPES.has(activityType)) {
    return {
      category: 'dividend_income',
      accountingBucket: signedAmount.isNegative() ? 'generic' : 'dividend_revenue',
      effect: signedAmount.isNegative() ? 'cost' : 'revenue'
    }
  }

  return null
}

const activityDetail = (activity: AlpacaAccountActivity): string => {
  const parts = [`Alpaca account activity ${activity.activity_type}`]
  if (activity.activity_sub_type !== undefined && activity.activity_sub_type !== null) {
    parts.push(`subtype ${activity.activity_sub_type}`)
  }
  if (activity.qty !== undefined && activity.qty !== null) parts.push(`qty ${activity.qty}`)
  if (activity.per_share_amount !== undefined && activity.per_share_amount !== null) {
    parts.push(`per-share ${activity.per_share_amount}`)
  }
  if (activity.order_id !== undefined && activity.order_id !== null) {
    parts.push(`order ${activity.order_id}`)
  }
  if (activity.currency !== undefined && activity.currency !== null) {
    parts.push(`currency ${activity.currency}`)
  }
  if (activity.description !== undefined && activity.description !== null) {
    parts.push(activity.description)
  }
  return parts.join('; ')
}

export const buildAlpacaActivityCostEntries = (
  activities: AlpacaAccountActivity[]
): PnlCostEntry[] => {
  const sorted = [...activities].sort((left, right) => {
    const timeComparison = (activityTimestamp(left) ?? '').localeCompare(
      activityTimestamp(right) ?? ''
    )
    if (timeComparison !== 0) return timeComparison
    return left.id.localeCompare(right.id)
  })
  const entries: PnlCostEntry[] = []

  for (const [idx, activity] of sorted.entries()) {
    const occurredAt = activityTimestamp(activity)
    if (occurredAt === null) continue

    const signedAmountText = activity.net_amount
    if (signedAmountText === undefined || signedAmountText === null || signedAmountText === '') {
      continue
    }

    let signedAmount: Decimal
    try {
      signedAmount = new Decimal(signedAmountText)
    } catch {
      continue
    }

    if (signedAmount.isZero()) continue

    const classification = classifyActivity(activity.activity_type, signedAmount)
    if (classification === null) continue

    entries.push({
      category: classification.category,
      accountingBucket: classification.accountingBucket,
      effect: classification.effect,
      amountUsd: fmtDecimal(signedAmount.abs()),
      occurredAt,
      aggregateType: 'AlpacaAccountActivity',
      aggregateId: activity.id,
      eventRowid: -1 - idx,
      symbol:
        activity.symbol !== undefined && activity.symbol !== null && activity.symbol !== ''
          ? activity.symbol
          : null,
      detail: activityDetail(activity)
    })
  }

  return entries
}

export const fetchAlpacaActivityCostEntries = async (
  baseUrl: string,
  query: PnlQuery
): Promise<PnlCostEntry[]> => {
  const params = alpacaActivityParams(query)
  const queryString = params.toString()
  const url = queryString === '' ? baseUrl : `${baseUrl}?${queryString}`
  const response = await fetch(url)

  if (!response.ok) {
    throw new Error(`Alpaca activities HTTP ${String(response.status)}`)
  }

  const body = (await response.json()) as AlpacaActivitiesResponse
  if (!Array.isArray(body.entries)) {
    throw new Error('Alpaca activities endpoint returned a response without entries')
  }

  return buildAlpacaActivityCostEntries(body.entries as AlpacaAccountActivity[])
}
