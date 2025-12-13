import { describe, expect, it } from 'vitest'
import {
  err,
  flatMap,
  isErr,
  isOk,
  map,
  mapErr,
  matcher,
  matchResult,
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
    expect(result).toEqual({ tag: 'ok', value: 42 })
  })

  it('err creates a failure result', () => {
    const result = err('failed')
    expect(result).toEqual({ tag: 'err', error: 'failed' })
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

describe('matchResult', () => {
  it('calls ok handler for success Result', () => {
    const result = matchResult(ok(10), {
      ok: (r) => r.value * 2,
      err: () => 0
    })
    expect(result).toBe(20)
  })

  it('calls err handler for failure Result', () => {
    const result = matchResult(err('oops') as Result<number, string>, {
      ok: (r) => r.value * 2,
      err: (r) => r.error.length
    })
    expect(result).toBe(4)
  })
})

describe('matcher', () => {
  it('works with custom discriminated unions', () => {
    type State =
      | { kind: 'loading' }
      | { kind: 'ready'; data: string }
      | { kind: 'error'; message: string }

    const matchState = matcher<State>()('kind')

    const loading: State = { kind: 'loading' }
    const ready: State = { kind: 'ready', data: 'hello' }
    const error: State = { kind: 'error', message: 'failed' }

    expect(
      matchState(loading, {
        loading: () => 'loading...',
        ready: (s) => s.data,
        error: (s) => s.message
      })
    ).toBe('loading...')

    expect(
      matchState(ready, {
        loading: () => 'loading...',
        ready: (s) => s.data,
        error: (s) => s.message
      })
    ).toBe('hello')

    expect(
      matchState(error, {
        loading: () => 'loading...',
        ready: (s) => s.data,
        error: (s) => s.message
      })
    ).toBe('failed')
  })

  it('provides correctly typed values to handlers', () => {
    type Event =
      | { type: 'click'; x: number; y: number }
      | { type: 'keypress'; key: string }
      | { type: 'scroll'; delta: number }

    const matchEvent = matcher<Event>()('type')

    const click: Event = { type: 'click', x: 10, y: 20 }
    const keypress: Event = { type: 'keypress', key: 'Enter' }

    const clickResult = matchEvent(click, {
      click: (e) => `clicked at ${String(e.x)},${String(e.y)}`,
      keypress: (e) => `pressed ${e.key}`,
      scroll: (e) => `scrolled ${String(e.delta)}`
    })
    expect(clickResult).toBe('clicked at 10,20')

    const keypressResult = matchEvent(keypress, {
      click: (e) => `clicked at ${String(e.x)},${String(e.y)}`,
      keypress: (e) => `pressed ${e.key}`,
      scroll: (e) => `scrolled ${String(e.delta)}`
    })
    expect(keypressResult).toBe('pressed Enter')
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
    if (result.tag === 'err') {
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
