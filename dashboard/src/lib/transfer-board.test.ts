import { readFileSync } from 'node:fs'
import { describe, expect, it } from 'vitest'
import { RECOVERY_GUIDE, recoveryModeLabel, type RecoveryMode } from './transfer'

// The Grafana board (observability/) shows the same recovery commands as the
// SPA from its own copy: recovery-guide.json for the guide. These tests fail
// when the copy drifts from this file's builders.

const panels = new URL('../../../observability/liquidity-panels/', import.meta.url)

describe('board recovery-guide.json', () => {
  it('matches RECOVERY_GUIDE and the mode labels', () => {
    const guide = JSON.parse(readFileSync(new URL('recovery-guide.json', panels), 'utf8')) as {
      modeLabels: Record<RecoveryMode, string>
      groups: unknown
    }
    expect(guide.groups).toEqual(RECOVERY_GUIDE)
    for (const mode of ['requires-bot', 'direct-db'] as const) {
      expect(guide.modeLabels[mode]).toBe(recoveryModeLabel(mode))
    }
  })
})
