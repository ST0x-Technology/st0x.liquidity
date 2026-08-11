// Theme mode selection (light / dark / system) persisted in localStorage.
//
// The stored key and JSON encoding must stay in sync with the pre-paint
// inline script in src/app.html, which applies the `.dark` class before
// SvelteKit hydrates to avoid a flash of the wrong theme.

import { MediaQuery } from 'svelte/reactivity'
import { PersistedState } from 'runed'

import { tryCatch, unwrapOr } from '$lib/fp'

export type ThemeMode = 'light' | 'dark' | 'system'

export type ResolvedTheme = 'light' | 'dark'

export const THEME_MODE_STORAGE_KEY = 'theme-mode'

export const resolveTheme = (mode: ThemeMode, prefersDark: boolean): ResolvedTheme => {
  if (mode === 'system') {
    return prefersDark ? 'dark' : 'light'
  }

  return mode
}

const NEXT_THEME_MODE: Record<ThemeMode, ThemeMode> = {
  system: 'light',
  light: 'dark',
  dark: 'system'
}

export const nextThemeMode = (mode: ThemeMode): ThemeMode => NEXT_THEME_MODE[mode]

const isThemeMode = (value: unknown): value is ThemeMode =>
  value === 'light' || value === 'dark' || value === 'system'

// Falls back to system mode on corrupted or foreign storage content instead
// of trusting it. PersistedState assigns whatever deserialize returns, so the
// fallback must be a valid mode rather than undefined.
const deserializeThemeMode = (raw: string): ThemeMode => {
  const parsed = unwrapOr(
    tryCatch((): unknown => JSON.parse(raw)),
    undefined
  )
  return isThemeMode(parsed) ? parsed : 'system'
}

const storedMode = new PersistedState<ThemeMode>(THEME_MODE_STORAGE_KEY, 'system', {
  serializer: {
    serialize: JSON.stringify,
    deserialize: deserializeThemeMode
  }
})

const prefersDark = new MediaQuery('(prefers-color-scheme: dark)')

export const themeMode = {
  get current(): ThemeMode {
    return storedMode.current
  },
  cycle(): void {
    storedMode.current = nextThemeMode(storedMode.current)
  }
}

export const resolvedTheme = {
  get current(): ResolvedTheme {
    return resolveTheme(storedMode.current, prefersDark.current)
  }
}
