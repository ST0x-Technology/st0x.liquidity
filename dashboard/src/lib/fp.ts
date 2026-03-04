/**
 * Lightweight functional programming helpers for type-safe error handling
 * and pattern matching on discriminated unions.
 */

/**
 * Creates a type-safe pattern matcher for a discriminated union.
 *
 * @example
 * type State =
 *   | { kind: 'loading' }
 *   | { kind: 'ready'; data: string }
 *   | { kind: 'error'; message: string }
 *
 * const matchState = matcher<State>()('kind')
 *
 * matchState(state, {
 *   loading: () => 'Loading...',
 *   ready: (s) => s.data,
 *   error: (s) => s.message
 * })
 */
export const matcher =
  <T extends Record<string, unknown>>() =>
  <K extends keyof T>(discriminant: K) =>
  <R>(value: T, handlers: { [V in T[K] & string]: (value: Extract<T, Record<K, V>>) => R }): R => {
    const tag = value[discriminant] as T[K] & string

    if (!Object.prototype.hasOwnProperty.call(handlers, tag)) {
      throw new Error(`Unknown tag "${tag}" for discriminant "${String(discriminant)}"`)
    }

    // Type safety is enforced at call sites via the mapped type constraint on handlers.
    // TypeScript can't prove the dynamic lookup is safe, but the type signature guarantees:
    // 1. handlers has a key for every possible discriminant value
    // 2. Each handler receives the correctly narrowed type for that variant
    const handler = handlers[tag] as (v: T) => R
    return handler(value)
  }

// Result type using 'tag' discriminant for compatibility with generic matcher
export type Ok<T> = { tag: 'ok'; value: T }
export type Err<E> = { tag: 'err'; error: E }
export type Result<T, E> = Ok<T> | Err<E>

export const ok = <T>(value: T): Ok<T> => ({ tag: 'ok', value })
export const err = <E>(error: E): Err<E> => ({ tag: 'err', error })

export const isOk = <T, E>(result: Result<T, E>): result is Ok<T> => result.tag === 'ok'
export const isErr = <T, E>(result: Result<T, E>): result is Err<E> => result.tag === 'err'

/**
 * Pattern match on a Result type.
 * Convenience wrapper around matcher for the common Result pattern.
 *
 * @example
 * matchResult(result, {
 *   ok: (r) => r.value,
 *   err: (r) => handleError(r.error)
 * })
 */
export const matchResult = <T, E, R>(
  result: Result<T, E>,
  handlers: { ok: (value: Ok<T>) => R; err: (value: Err<E>) => R }
): R => (result.tag === 'ok' ? handlers.ok(result) : handlers.err(result))

/**
 * Map over the success value of a Result.
 */
export const map = <T, E, U>(result: Result<T, E>, fn: (value: T) => U): Result<U, E> =>
  result.tag === 'ok' ? ok(fn(result.value)) : result

/**
 * Map over the error value of a Result.
 */
export const mapErr = <T, E, F>(result: Result<T, E>, fn: (error: E) => F): Result<T, F> =>
  result.tag === 'ok' ? result : err(fn(result.error))

/**
 * Chain Results together, short-circuiting on error.
 */
export const flatMap = <T, E, U>(
  result: Result<T, E>,
  fn: (value: T) => Result<U, E>
): Result<U, E> => (result.tag === 'ok' ? fn(result.value) : result)

/**
 * Unwrap a Result, throwing if it's an error.
 * Use sparingly - prefer pattern matching.
 */
export const unwrap = <T, E>(result: Result<T, E>): T => {
  if (result.tag === 'ok') return result.value
  throw new Error(`Called unwrap on an Err: ${String(result.error)}`)
}

/**
 * Unwrap a Result with a default value.
 */
export const unwrapOr = <T, E>(result: Result<T, E>, defaultValue: T): T =>
  result.tag === 'ok' ? result.value : defaultValue

/**
 * Try to execute a function, catching any thrown errors.
 *
 * Returns Result<T, unknown> because JavaScript allows throwing any value,
 * so we cannot guarantee the error type at compile time. Use type guards
 * or instanceof checks before accessing error-specific properties.
 */
export const tryCatch = <T>(fn: () => T): Result<T, unknown> => {
  try {
    return ok(fn())
  } catch (e: unknown) {
    return err(e)
  }
}

/**
 * Async version of tryCatch.
 *
 * Returns Result<T, unknown> because JavaScript allows throwing any value,
 * so we cannot guarantee the error type at compile time. Use type guards
 * or instanceof checks before accessing error-specific properties.
 */
export const tryCatchAsync = async <T>(fn: () => Promise<T>): Promise<Result<T, unknown>> => {
  try {
    return ok(await fn())
  } catch (e: unknown) {
    return err(e)
  }
}

/**
 * Left-to-right function composition.
 */
export interface Pipe<T> {
  pipe: <U>(fn: (v: T) => U) => Pipe<U>
  value: T
}

export const pipe = <T>(value: T): Pipe<T> => ({
  pipe: <U>(fn: (v: T) => U): Pipe<U> => pipe(fn(value)),
  value
})
