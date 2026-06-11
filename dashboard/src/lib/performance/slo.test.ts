import { describe, expect, it } from 'vitest'

import { classifySlo, formatDurationMs, worstStatus } from './slo'

describe('classifySlo', () => {
  const thresholds = {
    good: 100,
    warning: 500,
  }

  it('classifies values at or below the good bound as good', () => {
    expect(classifySlo(0, thresholds)).toBe('good')
    expect(classifySlo(100, thresholds)).toBe('good')
  })

  it('classifies values between the bounds as warning', () => {
    expect(classifySlo(101, thresholds)).toBe('warning')
    expect(classifySlo(500, thresholds)).toBe('warning')
  })

  it('classifies values above the warning bound as critical', () => {
    expect(classifySlo(501, thresholds)).toBe('critical')
  })
})

describe('formatDurationMs', () => {
  it('renders sub-second durations in milliseconds', () => {
    expect(formatDurationMs(850)).toBe('850ms')
  })

  it('renders sub-minute durations in seconds', () => {
    expect(formatDurationMs(12_400)).toBe('12.4s')
  })

  it('renders sub-hour durations as minutes and seconds', () => {
    expect(formatDurationMs(200_000)).toBe('3m 20s')
  })

  it('renders hours with zero-padded minutes', () => {
    expect(formatDurationMs(7_500_000)).toBe('2h 05m')
  })

  it('renders negative durations with a sign', () => {
    expect(formatDurationMs(-1_500)).toBe('-1.5s')
  })

  it('pins the millisecond-to-second boundary', () => {
    expect(formatDurationMs(1_000)).toBe('1.0s')
  })

  it('pins the second-to-minute boundary', () => {
    expect(formatDurationMs(60_000)).toBe('1m 00s')
  })

  it('carries seconds that round to a full minute', () => {
    expect(formatDurationMs(59_950)).toBe('1m 00s')
    expect(formatDurationMs(179_500)).toBe('3m 00s')
  })

  it('carries minutes that round to a full hour', () => {
    expect(formatDurationMs(3_599_500)).toBe('1h 00m')
  })
})

describe('worstStatus', () => {
  it('returns the most severe status', () => {
    expect(worstStatus(['good', 'critical', 'warning'])).toBe('critical')
    expect(worstStatus(['good', 'warning'])).toBe('warning')
    expect(worstStatus(['good'])).toBe('good')
    expect(worstStatus([])).toBe('unknown')
  })
})
