// @vitest-environment happy-dom

import { describe, expect, it, vi } from 'vitest'

import { THEME_MODE_STORAGE_KEY } from '$lib/theme.svelte'

// theme-switcher renders from the module-level theme singleton, so each test
// seeds storage and imports a fresh module graph before mounting. The svelte
// runtime must come from the same fresh graph as the component, otherwise
// mount() and the component disagree on the active effect context.
const mountSwitcher = async (storedValue?: string) => {
  vi.resetModules()
  localStorage.clear()
  if (storedValue !== undefined) {
    localStorage.setItem(THEME_MODE_STORAGE_KEY, storedValue)
  }
  const { mount, tick, unmount } = await import('svelte')
  const { default: ThemeSwitcher } = await import('./theme-switcher.svelte')
  const target = document.createElement('div')
  document.body.appendChild(target)
  const component = mount(ThemeSwitcher, { target })
  return {
    target,
    tick,
    dispose: () => {
      void unmount(component)
    }
  }
}

describe('ThemeSwitcher', () => {
  it('labels the button with the system mode by default', async () => {
    const { target, dispose } = await mountSwitcher()
    try {
      const button = target.querySelector('button')
      expect(button?.getAttribute('aria-label')).toBe('Theme: system')
    } finally {
      dispose()
    }
  })

  it('renders the icon of a stored explicit mode', async () => {
    const { target, dispose } = await mountSwitcher(JSON.stringify('dark'))
    try {
      expect(target.querySelector('button')?.getAttribute('aria-label')).toBe('Theme: dark')
      expect(target.innerHTML).toContain('lucide-moon')
      expect(target.innerHTML).not.toContain('lucide-sun')
    } finally {
      dispose()
    }
  })

  it('cycles system -> light on click and persists the choice', async () => {
    const { target, tick, dispose } = await mountSwitcher()
    try {
      expect(target.innerHTML).toContain('lucide-sun-moon')

      target.querySelector('button')?.click()
      await tick()

      expect(target.querySelector('button')?.getAttribute('aria-label')).toBe('Theme: light')
      expect(target.innerHTML).toContain('lucide-sun')
      expect(target.innerHTML).not.toContain('lucide-moon')
      expect(localStorage.getItem(THEME_MODE_STORAGE_KEY)).toBe(JSON.stringify('light'))
    } finally {
      dispose()
    }
  })
})
