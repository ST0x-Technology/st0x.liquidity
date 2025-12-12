import { describe, expect, it } from 'vitest'
import {
  err,
  flatMap,
  isErr,
  isOk,
  map,
  mapErr,
  match,
  ok,
  pipe,
  type Result,
  tryCatch,
  tryCatchAsync,
  unwrap,
  unwrapOr
} from './fp'

describe('Result constructors', () => {
  it('ok creates a success result', () => {
    const result = ok(42)
    expect(result).toEqual({ ok: true, value: 42 })
  })

  it('err creates a failure result', () => {
    const result = err('failed')
    expect(result).toEqual({ ok: false, error: 'failed' })
  })
})

describe('type guards', () => {
  it('isOk returns true for Ok values', () => {
    expect(isOk(ok(1))).toBe(true)
    expect(isOk(err('x'))).toBe(false)
  })

  it('isErr returns true for Err values', () => {
    expect(isErr(err('x'))).toBe(true)
    expect(isErr(ok(1))).toBe(false)
  })
})

describe('match', () => {
  it('calls ok handler for success', () => {
    const result = match(ok(10), {
      ok: (v) => v * 2,
      err: () => 0
    })
    expect(result).toBe(20)
  })

  it('calls err handler for failure', () => {
    const result = match(err('oops') as Result<number, string>, {
      ok: (v) => v * 2,
      err: (e) => e.length
    })
    expect(result).toBe(4)
  })
})

describe('map', () => {
  it('transforms success value', () => {
    const result = map(ok(5), (x) => x * 3)
    expect(result).toEqual(ok(15))
  })

  it('passes through error unchanged', () => {
    const result = map(err('fail') as Result<number, string>, (x) => x * 3)
    expect(result).toEqual(err('fail'))
  })
})

describe('mapErr', () => {
  it('transforms error value', () => {
    const result = mapErr(err('fail'), (e) => e.toUpperCase())
    expect(result).toEqual(err('FAIL'))
  })

  it('passes through success unchanged', () => {
    const result = mapErr(ok(42) as Result<number, string>, (e) => e.toUpperCase())
    expect(result).toEqual(ok(42))
  })
})

describe('flatMap', () => {
  const divide = (a: number, b: number): Result<number, string> =>
    b === 0 ? err('division by zero') : ok(a / b)

  it('chains successful operations', () => {
    const result = flatMap(ok(10), (x) => divide(x, 2))
    expect(result).toEqual(ok(5))
  })

  it('short-circuits on first error', () => {
    const result = flatMap(ok(10), (x) => divide(x, 0))
    expect(result).toEqual(err('division by zero'))
  })

  it('propagates initial error', () => {
    const result = flatMap(err('initial') as Result<number, string>, (x) => divide(x, 2))
    expect(result).toEqual(err('initial'))
  })
})

describe('unwrap', () => {
  it('returns value for Ok', () => {
    expect(unwrap(ok('hello'))).toBe('hello')
  })

  it('throws for Err', () => {
    expect(() => unwrap(err('boom'))).toThrow('Called unwrap on an Err: boom')
  })
})

describe('unwrapOr', () => {
  it('returns value for Ok', () => {
    expect(unwrapOr(ok(42), 0)).toBe(42)
  })

  it('returns default for Err', () => {
    expect(unwrapOr(err('x') as Result<number, string>, 0)).toBe(0)
  })
})

describe('tryCatch', () => {
  it('catches thrown errors', () => {
    const result = tryCatch(() => {
      throw new Error('kaboom')
    })
    expect(isErr(result)).toBe(true)
    if (!result.ok) {
      expect(result.error).toBeInstanceOf(Error)
    }
  })

  it('returns Ok for successful execution', () => {
    const result = tryCatch(() => 'success')
    expect(result).toEqual(ok('success'))
  })
})

describe('tryCatchAsync', () => {
  it('catches rejected promises', async () => {
    const result = await tryCatchAsync(() => Promise.reject(new Error('async kaboom')))
    expect(isErr(result)).toBe(true)
  })

  it('returns Ok for resolved promises', async () => {
    const result = await tryCatchAsync(() => Promise.resolve('async success'))
    expect(result).toEqual(ok('async success'))
  })
})

describe('pipe', () => {
  it('chains transformations left to right', () => {
    const result = pipe(5)
      .pipe((x) => x * 2)
      .pipe((x) => x + 1)
      .pipe((x) => `result: ${String(x)}`).value

    expect(result).toBe('result: 11')
  })

  it('works with single transformation', () => {
    const result = pipe('hello').pipe((s) => s.toUpperCase()).value
    expect(result).toBe('HELLO')
  })
})
