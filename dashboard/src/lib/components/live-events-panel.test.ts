import { describe, it, expect } from 'vitest'
import { truncateId } from './live-events-panel'

describe('truncateId', () => {
  it('returns original if shorter than maxLen', () => {
    expect(truncateId('abc', 16)).toBe('abc')
    expect(truncateId('short', 10)).toBe('short')
  })

  it('returns original if equal to maxLen', () => {
    expect(truncateId('exactly16chars!!', 16)).toBe('exactly16chars!!')
  })

  it('truncates with ellipsis in middle', () => {
    expect(truncateId('abcdefghijklmnopqrstuvwxyz', 10)).toBe('abcd...xyz')
  })

  it('handles odd visibleChars by putting extra char on left', () => {
    expect(truncateId('abcdefghij', 8)).toBe('abc...ij')
  })

  it('returns original if maxLen < 5', () => {
    expect(truncateId('abcdefgh', 4)).toBe('abcdefgh')
    expect(truncateId('abcdefgh', 3)).toBe('abcdefgh')
    expect(truncateId('abcdefgh', 0)).toBe('abcdefgh')
    expect(truncateId('abcdefgh', -1)).toBe('abcdefgh')
  })

  it('works at minimum valid maxLen of 5', () => {
    expect(truncateId('abcdef', 5)).toBe('a...f')
  })

  it('uses default maxLen of 16', () => {
    const longId = 'abcdefghijklmnopqrstuvwxyz'
    expect(truncateId(longId)).toBe('abcdefg...uvwxyz')
  })
})
