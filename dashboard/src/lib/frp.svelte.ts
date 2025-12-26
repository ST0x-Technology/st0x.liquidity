/**
 * Functional Reactive Programming (FRP) utilities for Svelte 5.
 *
 * This module provides utilities that combine functional programming patterns
 * with Svelte 5's reactivity system. Unlike direct $state usage, these utilities
 * enforce explicit mutations through function calls, making state changes
 * greppable and easier to reason about.
 *
 * For non-reactive FP utilities (Result, matcher, pipe, etc.), see $lib/fp.
 */

/**
 * Creates a reactive value with explicit mutation via update function.
 *
 * - Read: `value.current`
 * - Mutate: `value.update(v => transform(v))`
 *
 * @example
 * const count = reactive(0)
 * count.update(n => n + 1)
 * count.update(() => 0)  // reset
 * console.log(count.current)
 */
export const reactive = <T>(initial: T) => {
  let value = $state(initial)

  return {
    get current() {
      return value
    },
    update: (fn: (current: T) => T) => {
      value = fn(value)
    }
  }
}

export type Reactive<T> = ReturnType<typeof reactive<T>>
