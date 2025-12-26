import { describe, it, expect } from 'vitest'
import { reactive } from './frp.svelte'

describe('reactive', () => {
  it('initializes with provided value', () => {
    const count = reactive(42)
    expect(count.current).toBe(42)
  })

  it('updates value via update function', () => {
    const count = reactive(0)
    count.update(n => n + 1)
    expect(count.current).toBe(1)
  })

  it('replaces value by ignoring previous', () => {
    const count = reactive(100)
    count.update(() => 0)
    expect(count.current).toBe(0)
  })

  it('works with object types', () => {
    type State = { name: string; count: number }
    const state = reactive<State>({ name: 'test', count: 0 })

    state.update(s => ({ ...s, count: s.count + 1 }))

    expect(state.current).toEqual({ name: 'test', count: 1 })
  })

  it('works with nullable types', () => {
    const value = reactive<string | null>(null)

    expect(value.current).toBeNull()

    value.update(() => 'hello')
    expect(value.current).toBe('hello')

    value.update(() => null)
    expect(value.current).toBeNull()
  })

  it('chains multiple updates', () => {
    const count = reactive(0)

    count.update(n => n + 1)
    count.update(n => n * 2)
    count.update(n => n + 3)

    expect(count.current).toBe(5)
  })
})
