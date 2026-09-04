// Client-side market-session filtering for order and trade rows.
//
// The filter runs over rows the panel already loaded (the backend has no
// session query parameter), so it narrows the visible page rather than
// re-querying. Legacy rows carry no session and only show under "All":
// guessing "Regular" for them would silently mislabel pre-session data.

export const SESSION_FILTERS = ['all', 'Regular', 'Extended', 'Overnight'] as const

export type SessionFilter = (typeof SESSION_FILTERS)[number]

export const sessionFilterLabel = (filter: SessionFilter): string =>
  filter === 'all' ? 'All' : filter

export const matchesSessionFilter = (
  marketSession: string | null | undefined,
  filter: SessionFilter
): boolean => filter === 'all' || marketSession === filter
