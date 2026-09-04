import { describe, expect, it } from 'vitest'
import {
  SESSION_FILTERS,
  matchesSessionFilter,
  sessionFilterLabel,
  type SessionFilter
} from './session-filter'

describe('matchesSessionFilter', () => {
  it('matches every session under "all", including legacy sessionless rows', () => {
    for (const session of ['Regular', 'Extended', 'Overnight', null, undefined]) {
      expect(matchesSessionFilter(session, 'all')).toBe(true)
    }
  })

  it('matches only the exact session under a specific filter', () => {
    expect(matchesSessionFilter('Overnight', 'Overnight')).toBe(true)
    expect(matchesSessionFilter('Extended', 'Overnight')).toBe(false)
    expect(matchesSessionFilter('Regular', 'Overnight')).toBe(false)
  })

  it('hides legacy sessionless rows under every specific filter', () => {
    // Guessing "Regular" for a pre-session row would silently mislabel
    // legacy data, so null only surfaces under "all".
    for (const filter of ['Regular', 'Extended', 'Overnight'] as SessionFilter[]) {
      expect(matchesSessionFilter(null, filter)).toBe(false)
      expect(matchesSessionFilter(undefined, filter)).toBe(false)
    }
  })
})

describe('sessionFilterLabel', () => {
  it('labels each filter for the chip row', () => {
    expect(SESSION_FILTERS.map(sessionFilterLabel)).toEqual([
      'All',
      'Regular',
      'Extended',
      'Overnight'
    ])
  })
})
