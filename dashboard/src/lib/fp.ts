/**
 * Lightweight functional programming helpers for type-safe error handling.
 * Inspired by Rust's Result type - makes errors explicit in the type system.
 */

export type Ok<T> = { ok: true; value: T }
export type Err<E> = { ok: false; error: E }
export type Result<T, E> = Ok<T> | Err<E>

export const ok = <T>(value: T): Ok<T> => ({ ok: true, value })
export const err = <E>(error: E): Err<E> => ({ ok: false, error })

export const isOk = <T, E>(result: Result<T, E>): result is Ok<T> => result.ok
export const isErr = <T, E>(result: Result<T, E>): result is Err<E> => !result.ok

/**
 * Pattern match on a Result, similar to Rust's match expression.
 */
export const match = <T, E, U>(
  result: Result<T, E>,
  handlers: { ok: (value: T) => U; err: (error: E) => U }
): U => (result.ok ? handlers.ok(result.value) : handlers.err(result.error))

/**
 * Map over the success value of a Result.
 */
export const map = <T, E, U>(result: Result<T, E>, fn: (value: T) => U): Result<U, E> =>
  result.ok ? ok(fn(result.value)) : result

/**
 * Map over the error value of a Result.
 */
export const mapErr = <T, E, F>(result: Result<T, E>, fn: (error: E) => F): Result<T, F> =>
  result.ok ? result : err(fn(result.error))

/**
 * Chain Results together, short-circuiting on error.
 */
export const flatMap = <T, E, U>(
  result: Result<T, E>,
  fn: (value: T) => Result<U, E>
): Result<U, E> => (result.ok ? fn(result.value) : result)

/**
 * Unwrap a Result, throwing if it's an error.
 * Use sparingly - prefer pattern matching.
 */
export const unwrap = <T, E>(result: Result<T, E>): T => {
  if (result.ok) return result.value
  throw new Error(`Called unwrap on an Err: ${String(result.error)}`)
}

/**
 * Unwrap a Result with a default value.
 */
export const unwrapOr = <T, E>(result: Result<T, E>, defaultValue: T): T =>
  result.ok ? result.value : defaultValue

/**
 * Try to execute a function, catching any thrown errors.
 */
export const tryCatch = <T, E = unknown>(fn: () => T): Result<T, E> => {
  try {
    return ok(fn())
  } catch (e) {
    return err(e as E)
  }
}

/**
 * Async version of tryCatch.
 */
export const tryCatchAsync = async <T, E = unknown>(
  fn: () => Promise<T>
): Promise<Result<T, E>> => {
  try {
    return ok(await fn())
  } catch (e) {
    return err(e as E)
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
