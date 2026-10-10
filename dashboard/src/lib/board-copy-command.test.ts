// @vitest-environment happy-dom

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

// The Copy button of the Grafana board's recovery commands
// (observability/liquidity-panels/copy-command.js), a plain script in the
// board and an ES module through its export line here.

type CopyCommand = {
  commandLine: (commandHtml: string, preClass?: string) => string
  wireCopyButtons: (container: ParentNode) => void
}

// A plain script in the board, an ES module through its export line here.
// Loaded through a glob so svelte-check does not type-check the script.
const scriptPath = '../../../observability/liquidity-panels/copy-command.js'
const loadScript = import.meta.glob('../../../observability/liquidity-panels/copy-command.js')[
  scriptPath
]
if (!loadScript) throw new Error(`no ${scriptPath}`)
const { commandLine, wireCopyButtons } = (await loadScript()) as CopyCommand

const setClipboard = (clipboard: unknown) => {
  Object.defineProperty(navigator, 'clipboard', { value: clipboard, configurable: true })
}

describe('board copy-command.js', () => {
  let root: HTMLDivElement

  // One wired command line; `commandHtml` is already escaped.
  const copyButton = (commandHtml: string) => {
    root.innerHTML = commandLine(commandHtml)
    wireCopyButtons(root)
    const button = root.querySelector<HTMLButtonElement>('[data-copy]')
    if (!button) throw new Error('no Copy button')
    return button
  }

  beforeEach(() => {
    vi.useFakeTimers()
    root = document.createElement('div')
    document.body.appendChild(root)
  })

  afterEach(() => {
    root.remove()
    vi.runOnlyPendingTimers()
    vi.useRealTimers()
  })

  it('puts the command in a pre with its class and a Copy button after it', () => {
    root.innerHTML = commandLine('stox a &amp; b', 'hdr-mono')
    expect(root.querySelector('.liq-command-line > pre.hdr-mono')?.textContent).toBe('stox a & b')
    expect(root.querySelector('pre + button[data-copy]')?.textContent).toBe('Copy')
    root.innerHTML = commandLine('stox')
    expect(root.querySelector('pre')?.hasAttribute('class')).toBe(false)
  })

  it('says Copied after the command is copied, then Copy again', async () => {
    const writeText = vi.fn(() => Promise.resolve())
    setClipboard({ writeText })
    const button = copyButton('stox a &amp; b')
    button.click()
    expect(writeText).toHaveBeenCalledWith('stox a & b')
    await vi.waitFor(() => {
      expect(button.textContent).toBe('Copied')
    })
    vi.advanceTimersByTime(1500)
    expect(button.textContent).toBe('Copy')
  })

  it('says Copy failed without a clipboard, then Copy again', () => {
    setClipboard(undefined)
    const button = copyButton('stox')
    button.click()
    expect(button.textContent).toBe('Copy failed')
    vi.advanceTimersByTime(1500)
    expect(button.textContent).toBe('Copy')
  })

  it('says Copy failed when the clipboard refuses the write, then Copy again', async () => {
    setClipboard({ writeText: vi.fn(() => Promise.reject(new Error('denied'))) })
    const button = copyButton('stox')
    button.click()
    await vi.waitFor(() => {
      expect(button.textContent).toBe('Copy failed')
    })
    vi.advanceTimersByTime(1500)
    expect(button.textContent).toBe('Copy')
  })
})
