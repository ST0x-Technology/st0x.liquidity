/**
 * Cross-tab handoff for "view logs for this target" click-throughs.
 *
 * The Performance tab's error matrix sets a target here before switching to
 * the Logs tab; the log panel consumes (and clears) it on mount to seed its
 * filters.
 */

import { reactive } from '$lib/frp.svelte'

export const requestedLogTarget = reactive<string | null>(null)

/** Read and clear the pending request, if any. */
export const takeRequestedLogTarget = (): string | null => {
  const target = requestedLogTarget.current
  requestedLogTarget.update(() => null)
  return target
}

export type LogFilterResolution =
  | { kind: 'category'; target: string }
  | { kind: 'crate'; target: string }
  | { kind: 'search'; target: string }

/**
 * Decide how a clicked log target maps onto the log panel's filters: known
 * categories and crates use their checkbox filters; anything else (e.g. a
 * full module path) falls back to the free-text search, which the backend
 * matches as a substring.
 */
export const resolveLogFilter = (
  target: string,
  categories: readonly string[],
  crates: readonly string[],
): LogFilterResolution => {
  if (categories.includes(target)) {
    return {
      kind: 'category',
      target,
    }
  }

  if (crates.includes(target)) {
    return {
      kind: 'crate',
      target,
    }
  }

  return {
    kind: 'search',
    target,
  }
}
