// @vitest-environment happy-dom

import { readFileSync } from 'node:fs'
import { resolve } from 'node:path'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

// The Grafana board's header as deployed: the header panel's afterRender
// from the generated board JSON, run the way the Business Text plugin runs
// it.

type Panel = {
  type: string
  options?: { renderMode?: string; afterRender?: string; styles?: string }
}

// happy-dom replaces the global URL, so the path is built with node:path.
const boardJson = readFileSync(
  resolve(import.meta.dirname, '../../../observability/dashboards/liquidity/t0-liquidity.json'),
  'utf8'
)
const headerSource = (JSON.parse(boardJson) as { panels: Panel[] }).panels.find(
  (panel) =>
    panel.type === 'marcusolsson-dynamictext-panel' && panel.options?.renderMode === 'allRows'
)?.options?.afterRender

if (headerSource === undefined) throw new Error('the board has no header panel')

// eslint-disable-next-line @typescript-eslint/no-implied-eval -- runs the deployed script as the plugin does
const renderHeader = new Function('context', headerSource) as (context: unknown) => void

// A browser closes a dialog at once and queues its close event as a task;
// happy-dom fires the event inside close(). The tests queue it on a timer,
// so a render can run before it, and vi.advanceTimersByTime(0) runs it.
const queueCloseEvents = () =>
  vi.spyOn(HTMLDialogElement.prototype, 'close').mockImplementation(function (
    this: HTMLDialogElement,
    returnValue?: string
  ) {
    const wasOpen = this.open
    this.open = false
    this.returnValue = returnValue ?? ''
    if (wasOpen) setTimeout(() => this.dispatchEvent(new Event('close')), 0)
  })

const START_PATH = window.location.pathname
// Two of the board's one-minute refreshes and more.
const PAST_REOPEN_WINDOW_MS = 3 * 60_000

describe('board header.js', () => {
  let root: HTMLDivElement
  let closeSpy: ReturnType<typeof queueCloseEvents>

  const render = (up: number) => {
    renderHeader({
      element: root,
      data: [
        { k: 'up', Time: 1000, Value: up },
        { k: 'uptime', Time: 1000, Value: 3600 },
        { k: 'info', Time: 1000, Value: 990, broker: 'alpaca' }
      ],
      grafana: {
        theme: {
          isDark: true,
          colors: {
            text: { primary: '#fff', secondary: '#aaa' },
            border: { weak: '#333' },
            background: { primary: '#111', secondary: '#222' }
          }
        },
        replaceVariables: () => 'production'
      }
    })
  }
  const dialog = (name: string) => root.querySelector<HTMLDialogElement>(`[data-dialog="${name}"]`)
  const guideBody = () => dialog('guide')?.querySelector<HTMLElement>('.hdr-dialog-body')
  // The plugin shows its default content when the query returns no data.
  const renderNoData = () => {
    root.innerHTML = 'No bot data.'
  }

  const openDialog = (name: string) => {
    root.querySelector<HTMLButtonElement>(`[data-open="${name}"]`)?.click()
  }
  const scrollGuide = (scrollTop: number) => {
    const body = guideBody()
    if (!body) throw new Error('no guide body')
    body.scrollTop = scrollTop
    body.dispatchEvent(new Event('scroll'))
  }

  beforeEach(() => {
    vi.useFakeTimers()
    closeSpy = queueCloseEvents()
    Reflect.deleteProperty(window, '__liqHeaderDialog')
    root = document.createElement('div')
    document.body.appendChild(root)
  })

  afterEach(() => {
    root.remove()
    document.querySelectorAll('dialog').forEach((other) => {
      other.remove()
    })
    window.history.replaceState(null, '', START_PATH)
    vi.runOnlyPendingTimers()
    vi.useRealTimers()
    closeSpy.mockRestore()
  })

  it('reopens the guide at its scroll position after a refresh without data', () => {
    render(1)
    openDialog('guide')
    scrollGuide(120)
    renderNoData()
    expect(dialog('guide')).toBeNull()
    render(1)
    expect(dialog('guide')?.open).toBe(true)
    expect(guideBody()?.scrollTop).toBe(120)
  })

  it('does not reopen a dialog the operator closed', () => {
    render(1)
    openDialog('config')
    dialog('config')?.querySelector<HTMLButtonElement>('[data-close]')?.click()
    // A refresh runs before the close event.
    render(1)
    expect(dialog('config')?.open).toBe(false)
    vi.advanceTimersByTime(0)
    renderNoData()
    render(1)
    expect(dialog('config')?.open).toBe(false)
  })

  it('reopens the guide at the scroll it kept when the operator opened it again', () => {
    render(1)
    openDialog('guide')
    scrollGuide(120)
    dialog('guide')?.querySelector<HTMLButtonElement>('[data-close]')?.click()
    openDialog('guide')
    vi.advanceTimersByTime(0)
    render(1)
    expect(dialog('guide')?.open).toBe(true)
    expect(guideBody()?.scrollTop).toBe(120)
  })

  it('reopens the guide after a short stretch without data, not after a long one', () => {
    render(1)
    openDialog('guide')
    renderNoData()
    vi.setSystemTime(Date.now() + 2 * 60_000)
    render(1)
    expect(dialog('guide')?.open).toBe(true)
    renderNoData()
    vi.setSystemTime(Date.now() + PAST_REOPEN_WINDOW_MS)
    render(1)
    expect(dialog('guide')?.open).toBe(false)
  })

  it('keeps the guide open on a refresh with data however long the operator has been reading it', () => {
    render(1)
    openDialog('guide')
    const body = guideBody()
    if (!body) throw new Error('no guide body')
    // A scroll position the scroll handler never saw.
    body.scrollTop = 200
    vi.setSystemTime(Date.now() + PAST_REOPEN_WINDOW_MS)
    render(1)
    expect(dialog('guide')?.open).toBe(true)
    expect(guideBody()?.scrollTop).toBe(200)
  })

  it('does not reopen the guide over another dialog the operator opened', () => {
    render(1)
    openDialog('guide')
    renderNoData()
    const rowDialog = document.createElement('dialog')
    document.body.appendChild(rowDialog)
    rowDialog.showModal()
    render(1)
    expect(dialog('guide')?.open).toBe(false)
    // Nor once the other dialog is closed.
    rowDialog.close()
    render(1)
    expect(dialog('guide')?.open).toBe(false)
  })

  it('ignores a scroll of the old guide that runs after the operator went to another board', () => {
    render(1)
    openDialog('guide')
    const oldBody = guideBody()
    if (!oldBody) throw new Error('no guide body')
    renderNoData()
    window.history.pushState(null, '', '/d/other-board')
    oldBody.dispatchEvent(new Event('scroll'))
    render(1)
    expect(dialog('guide')?.open).toBe(false)
  })

  it('forgets a dialog the operator dismissed with Escape before its close event runs', () => {
    // A browser queues the close event, so a refresh can replace the markup
    // before it runs; the dismissal is recorded on cancel instead.
    render(1)
    root.querySelector<HTMLButtonElement>('[data-open="config"]')?.click()
    dialog('config')?.dispatchEvent(new Event('cancel'))
    render(1)
    expect(dialog('config')?.open).toBe(false)
  })

  it('keeps a dialog open when a close event runs that the operator did not ask for', () => {
    render(1)
    root.querySelector<HTMLButtonElement>('[data-open="guide"]')?.click()
    dialog('guide')?.dispatchEvent(new Event('close'))
    render(1)
    expect(dialog('guide')?.open).toBe(true)
  })

  it('keeps the Config button outside the clipped pill group', () => {
    render(1)
    const config = root.querySelector('[data-open="config"]')
    expect(config?.parentElement?.classList.contains('hdr')).toBe(true)
    expect(root.querySelector('.hdr-pills [data-open="config"]')).toBeNull()
    expect(root.querySelector('.hdr-pills')?.textContent).toContain('alpaca')
  })

  // happy-dom does no layout, so this checks the markup and the flex rules
  // that keep the controls in view on a narrow panel.
  it('lets only the pills and the clock group shrink on a narrow panel', () => {
    render(1)
    const items = Array.from(root.querySelector('.hdr')?.children ?? []).map(
      (item) => item.getAttribute('data-open') ?? item.classList[0]
    )
    expect(items).toEqual(['hdr-pills', 'config', 'guide', 'hdr-info', 'hdr-badge'])
    expect(root.querySelector('.hdr-info [data-clock]')).not.toBeNull()
    expect(root.querySelector('.hdr-info [title="Bot uptime"]')).not.toBeNull()

    const styles = (JSON.parse(boardJson) as { panels: Panel[] }).panels.find(
      (panel) => panel.options?.afterRender === headerSource
    )?.options?.styles
    if (styles === undefined) throw new Error('the header panel has no styles')
    const rule = (selector: string) => {
      const start = styles.indexOf(`\n${selector} {`)
      if (start < 0) throw new Error(`no rule for ${selector}`)
      return styles.slice(start, styles.indexOf('}', start))
    }
    expect(rule('.hdr-pills')).toMatch(/flex: 0 1 auto;[\s\S]*min-width: 0;[\s\S]*overflow: hidden;/)
    expect(rule('.hdr-info')).toMatch(
      /flex: 0 1 auto;[\s\S]*min-width: 0;[\s\S]*overflow: hidden;[\s\S]*text-overflow: ellipsis;/
    )
    expect(rule('.hdr-button,\n.hdr-guide,\n.hdr-badge')).toContain('flex: 0 0 auto;')
  })

  it('keeps an open dialog open across a refresh with data', () => {
    render(1)
    root.querySelector<HTMLButtonElement>('[data-open="config"]')?.click()
    render(0)
    expect(dialog('config')?.open).toBe(true)
  })

  it('shows uptime only while the bot is up', () => {
    render(1)
    expect(root.querySelector('[title="Bot uptime"]')?.textContent).toBe('up 1h 0m')
    render(0)
    expect(root.querySelector('[title="Bot uptime"]')).toBeNull()
    expect(root.querySelector('.hdr-badge')?.textContent).toBe('Disconnected')
  })

  it('puts a Copy button beside each guide command that copies it', async () => {
    const writeText = vi.fn(() => Promise.resolve())
    Object.defineProperty(navigator, 'clipboard', { value: { writeText }, configurable: true })
    render(1)
    const commands = [...root.querySelectorAll('.hdr-command')]
    expect(commands.length).toBeGreaterThan(0)
    for (const command of commands) {
      expect(command.querySelector('.liq-command-line > pre + button[data-copy]')).not.toBeNull()
    }
    const button = commands[0]?.querySelector<HTMLButtonElement>('[data-copy]')
    button?.click()
    const copied = commands[0]?.querySelector('pre')?.textContent
    expect(copied).toContain('st0x-liquidity-client --env production')
    expect(writeText).toHaveBeenCalledWith(copied)
    await vi.waitFor(() => {
      expect(button?.textContent).toBe('Copied')
    })
  })
})
