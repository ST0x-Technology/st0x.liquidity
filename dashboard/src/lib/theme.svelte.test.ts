// @vitest-environment happy-dom

import { beforeEach, describe, expect, it, vi } from 'vitest'

import { nextThemeMode, resolveTheme, THEME_MODE_STORAGE_KEY } from './theme.svelte'

// The persisted store is a module-level singleton, so tests that exercise
// storage interplay import a fresh module instance after seeding storage.
const setupThemeModule = async (storedValue?: string) => {
  vi.resetModules()
  localStorage.clear()
  if (storedValue !== undefined) {
    localStorage.setItem(THEME_MODE_STORAGE_KEY, storedValue)
  }
  return import('./theme.svelte')
}

describe('resolveTheme', () => {
  it('follows the OS preference in system mode', () => {
    expect(resolveTheme('system', true)).toBe('dark')
    expect(resolveTheme('system', false)).toBe('light')
  })

  it('ignores the OS preference in explicit modes', () => {
    expect(resolveTheme('light', true)).toBe('light')
    expect(resolveTheme('dark', false)).toBe('dark')
  })
})

describe('nextThemeMode', () => {
  it('cycles system -> light -> dark -> system', () => {
    expect(nextThemeMode('system')).toBe('light')
    expect(nextThemeMode('light')).toBe('dark')
    expect(nextThemeMode('dark')).toBe('system')
  })
})

describe('themeMode', () => {
  beforeEach(() => {
    localStorage.clear()
  })

  it('defaults to system when nothing is stored', async () => {
    const { themeMode } = await setupThemeModule()

    expect(themeMode.current).toBe('system')
  })

  it('restores a stored mode', async () => {
    const { themeMode } = await setupThemeModule(JSON.stringify('dark'))

    expect(themeMode.current).toBe('dark')
  })

  it('falls back to system on corrupted storage content', async () => {
    const { themeMode } = await setupThemeModule('not-json{')

    expect(themeMode.current).toBe('system')
  })

  it('falls back to system on valid JSON that is not a theme mode', async () => {
    const { themeMode } = await setupThemeModule(JSON.stringify('purple'))

    expect(themeMode.current).toBe('system')
  })

  it('persists cycled modes to storage as JSON', async () => {
    const { themeMode } = await setupThemeModule()

    themeMode.cycle()

    expect(themeMode.current).toBe('light')
    expect(localStorage.getItem(THEME_MODE_STORAGE_KEY)).toBe(JSON.stringify('light'))

    themeMode.cycle()

    expect(themeMode.current).toBe('dark')
    expect(localStorage.getItem(THEME_MODE_STORAGE_KEY)).toBe(JSON.stringify('dark'))
  })
})

describe('resolvedTheme', () => {
  it('resolves an explicit stored mode without consulting the OS', async () => {
    const { resolvedTheme } = await setupThemeModule(JSON.stringify('light'))

    expect(resolvedTheme.current).toBe('light')
  })
})
