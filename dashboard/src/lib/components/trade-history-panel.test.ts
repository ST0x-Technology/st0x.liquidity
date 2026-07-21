// @vitest-environment happy-dom

import { QueryClient } from '@tanstack/svelte-query'
import { mount, tick, unmount } from 'svelte'
import { afterEach, describe, expect, it, vi } from 'vitest'
import type { LegacyTrade } from '$lib/api/LegacyTrade'
import type { Statement } from '$lib/api/Statement'
import type { Trade } from '$lib/api/Trade'
import { createWebSocket } from '$lib/websocket'
import TradeHistoryPanelHarness from './trade-history-panel.test-harness.svelte'

vi.mock('$env/dynamic/public', () => ({ env: {} }))

const staleTrade: Trade = {
  id: 'counter-trade-1',
  occurredAt: '2026-01-01T00:00:00Z',
  venue: 'alpaca',
  direction: 'buy',
  symbol: 'SPCX',
  shares: '1',
  outcome: { status: 'filled' }
}

const failedTrade = (id: string, overrides: Partial<Trade> = {}): Trade => ({
  id,
  occurredAt: '2026-01-01T00:00:01Z',
  venue: 'alpaca',
  direction: 'buy',
  symbol: 'SPCX',
  shares: '1',
  outcome: {
    status: 'failed',
    error: 'broker rejected remainder',
    acceptedShares: '1',
    filledShares: '0.25',
    remainingShares: '0.75',
    excessShares: '0'
  },
  ...overrides
})

const setupTestWebSocket = () => {
  const instances: TestWebSocket[] = []

  class TestWebSocket {
    onopen: (() => void) | null = null
    onclose: ((event: CloseEvent) => void) | null = null
    onmessage: ((event: { data: string }) => void) | null = null
    onerror: (() => void) | null = null

    constructor(_url: string) {
      instances.push(this)
    }

    close() {
      this.onclose?.({ code: 1000, reason: '' } as CloseEvent)
    }

    open() {
      this.onopen?.()
    }

    receive(statement: Statement) {
      this.onmessage?.({ data: JSON.stringify(statement) })
    }
  }

  vi.stubGlobal('WebSocket', TestWebSocket)

  return () => {
    const instance = instances.at(-1)
    if (instance === undefined) throw new Error('WebSocket was not constructed')

    return instance
  }
}

const tradeResponse = (
  entries: Array<Trade | LegacyTrade>,
  total: number,
  hasMore = false
): Response =>
  new Response(JSON.stringify({ entries, total, hasMore }), {
    status: 200,
    headers: { 'content-type': 'application/json' }
  })

const mountPanel = () => {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false } }
  })
  const target = document.createElement('div')
  document.body.append(target)
  const component = mount(TradeHistoryPanelHarness, {
    target,
    props: { client: queryClient }
  })

  return { queryClient, target, component }
}

describe('TradeHistoryPanel', () => {
  afterEach(() => {
    vi.restoreAllMocks()
    vi.unstubAllGlobals()
  })

  it('normalizes legacy REST fills from an old server', async () => {
    const legacyTrade: LegacyTrade = {
      id: 'legacy-rest-fill',
      filledAt: '2026-01-01T00:00:00.123456789Z',
      venue: 'alpaca',
      direction: 'sell',
      symbol: 'TSLA',
      shares: '2'
    }
    vi.stubGlobal(
      'fetch',
      vi.fn(() => Promise.resolve(tradeResponse([legacyTrade], 1)))
    )

    const { target, component } = mountPanel()

    await vi.waitFor(() => {
      expect(target.textContent).toContain('TSLA')
      expect(target.textContent).toContain('Filled')
      expect(target.textContent).toContain('1 of 1')
    })

    await unmount(component)
    target.remove()
  })

  it('retries trade history with v1 when the previous backend rejects v2', async () => {
    const fetchMock = vi
      .fn<() => Promise<Response>>()
      .mockResolvedValueOnce(new Response(null, { status: 400 }))
      .mockResolvedValueOnce(tradeResponse([staleTrade], 1))
    vi.stubGlobal('fetch', fetchMock)

    const { target, component } = mountPanel()

    await vi.waitFor(() => expect(target.textContent).toContain('1 of 1'))
    expect(fetchMock).toHaveBeenNthCalledWith(
      1,
      expect.stringContaining('trade_protocol=terminal_outcomes_v2'),
      expect.any(Object)
    )
    expect(fetchMock).toHaveBeenNthCalledWith(
      2,
      expect.stringContaining('trade_protocol=terminal_outcomes_v1'),
      expect.any(Object)
    )

    await unmount(component)
    target.remove()
  })

  it('labels a fallback v1 failure quantity without claiming request provenance', async () => {
    const fetchMock = vi
      .fn<() => Promise<Response>>()
      .mockResolvedValueOnce(new Response(null, { status: 400 }))
      .mockResolvedValueOnce(tradeResponse([failedTrade('v1-failure')], 1))
    vi.stubGlobal('fetch', fetchMock)

    const { target, component } = mountPanel()

    await vi.waitFor(() => expect(target.textContent).toContain('1 of 1'))
    const quantityTooltip = Array.from(target.querySelectorAll('button')).find((button) =>
      button.getAttribute('aria-label')?.startsWith('Order quantity.') === true
    )
    expect(quantityTooltip).toBeDefined()
    expect(quantityTooltip?.getAttribute('aria-label')).not.toContain('Requested quantity')

    await unmount(component)
    target.remove()
  })

  it('re-probes v2 on the next refresh after a v1 fallback', async () => {
    let poll: (() => void) | undefined
    vi.spyOn(globalThis, 'setInterval').mockImplementation((handler) => {
      poll = handler as () => void
      return {} as ReturnType<typeof setInterval>
    })
    const fetchMock = vi
      .fn<() => Promise<Response>>()
      .mockResolvedValueOnce(new Response(null, { status: 400 }))
      .mockResolvedValueOnce(tradeResponse([failedTrade('v1-failure')], 1))
      .mockResolvedValueOnce(
        tradeResponse(
          [
            failedTrade('v2-failure', {
              symbol: 'UPGRADED',
              outcome: {
                status: 'failed',
                error: 'broker rejected remainder',
                acceptedShares: '0.5',
                filledShares: '0.25',
                remainingShares: '0.25',
                excessShares: '0'
              }
            })
          ],
          1
        )
      )
    vi.stubGlobal('fetch', fetchMock)

    const { target, component } = mountPanel()
    await vi.waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(2))

    poll?.()

    await vi.waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(3))
    expect(fetchMock).toHaveBeenNthCalledWith(
      3,
      expect.stringContaining('trade_protocol=terminal_outcomes_v2'),
      expect.any(Object)
    )
    await vi.waitFor(() => expect(target.textContent).toContain('UPGRADED'))

    await unmount(component)
    target.remove()
  })

  it('retries each concurrent v2 history request independently', async () => {
    let poll: (() => void) | undefined
    vi.spyOn(globalThis, 'setInterval').mockImplementation((handler) => {
      poll = handler as () => void
      return {} as ReturnType<typeof setInterval>
    })
    const pendingRequests: Array<{
      url: string
      resolve: (response: Response) => void
    }> = []
    const fetchMock = vi.fn<(url: string) => Promise<Response>>(
      (url) =>
        new Promise<Response>((resolve) => {
          pendingRequests.push({ url, resolve })
        })
    )
    vi.stubGlobal('fetch', fetchMock)

    const { target, component } = mountPanel()
    await vi.waitFor(() => expect(pendingRequests).toHaveLength(1))

    poll?.()
    await vi.waitFor(() => expect(pendingRequests).toHaveLength(2))

    expect(pendingRequests[0]?.url).toContain('trade_protocol=terminal_outcomes_v2')
    expect(pendingRequests[1]?.url).toContain('trade_protocol=terminal_outcomes_v2')

    pendingRequests[0]?.resolve(new Response(null, { status: 400 }))
    await vi.waitFor(() => expect(pendingRequests).toHaveLength(3))
    expect(pendingRequests[2]?.url).toContain('trade_protocol=terminal_outcomes_v1')
    pendingRequests[2]?.resolve(tradeResponse([staleTrade], 1))

    pendingRequests[1]?.resolve(new Response(null, { status: 400 }))
    await vi.waitFor(() => expect(pendingRequests).toHaveLength(4))
    expect(pendingRequests[3]?.url).toContain('trade_protocol=terminal_outcomes_v1')
    pendingRequests[3]?.resolve(
      tradeResponse([failedTrade('current-request', { symbol: 'CURR' })], 1)
    )

    await vi.waitFor(() => {
      expect(target.textContent).toContain('CURR')
      expect(target.textContent).toContain('1 of 1')
    })

    await unmount(component)
    target.remove()
  })

  it('renders initial failures and replaces live rows with an authoritative total', async () => {
    const socket = setupTestWebSocket()
    const fetchMock = vi.fn(() =>
      Promise.resolve(
        tradeResponse(
          [
            staleTrade,
            failedTrade('initial-failure', {
              outcome: {
                status: 'failed',
                error: 'initial broker failure',
                acceptedShares: '1',
                filledShares: '0',
                remainingShares: '1',
                excessShares: '0'
              }
            })
          ],
          150,
          true
        )
      )
    )
    vi.stubGlobal('fetch', fetchMock)

    const { queryClient, target, component } = mountPanel()

    await vi.waitFor(() => {
      expect(target.textContent).toContain('initial broker failure')
      expect(target.textContent).toContain('Failed')
    })

    const connection = createWebSocket('ws://localhost/api/ws', queryClient)
    connection.connect()
    socket().open()
    for (const trade of [
      failedTrade('counter-trade-1'),
      failedTrade('live-failure', {
        outcome: {
          status: 'failed',
          error: 'broker overfilled before rejecting',
          acceptedShares: '0.5',
          filledShares: '1',
          remainingShares: '0',
          excessShares: '0.5'
        }
      })
    ]) {
      socket().receive({ type: 'trade_update', data: trade })
    }
    await tick()

    await vi.waitFor(() => {
      expect(target.textContent).toContain('broker rejected remainder')
      expect(target.textContent).toContain('Filled 0.25')
      expect(target.textContent).toContain('Unfilled 0.75')
      expect(target.textContent).toContain('broker overfilled before rejecting')
      expect(target.textContent).toContain('Excess fill 0.5')
      expect(target.textContent).toContain('3 of 151')
    })

    expect(fetchMock).toHaveBeenCalledWith(expect.stringContaining('limit=100'), expect.any(Object))
    expect(fetchMock).toHaveBeenCalledWith(
      expect.stringContaining('trade_protocol=terminal_outcomes_v2'),
      expect.any(Object)
    )
    expect(fetchMock).toHaveBeenCalledTimes(1)

    connection.disconnect()
    await unmount(component)
    target.remove()
  })

  it('does not roll back a live total when an older refresh completes', async () => {
    const socket = setupTestWebSocket()
    let resolveRefresh: ((response: Response) => void) | undefined
    const refreshResponse = new Promise<Response>((resolve) => {
      resolveRefresh = resolve
    })
    const fetchMock = vi
      .fn<() => Promise<Response>>()
      .mockResolvedValueOnce(tradeResponse([staleTrade], 150, true))
      .mockReturnValueOnce(refreshResponse)
    vi.stubGlobal('fetch', fetchMock)

    const { queryClient, target, component } = mountPanel()
    await vi.waitFor(() => expect(target.textContent).toContain('1 of 150'))

    const connection = createWebSocket('ws://localhost/api/ws', queryClient)
    connection.connect()
    socket().open()
    socket().receive({
      type: 'trade_update',
      data: failedTrade('live-during-refresh')
    })
    await vi.waitFor(() => expect(target.textContent).toContain('2 of 151'))

    const refresh = [...target.querySelectorAll('button')].find(
      (button) => button.textContent.trim() === 'Refresh'
    )
    expect(refresh).toBeDefined()
    refresh?.click()
    await vi.waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(2))

    socket().receive({
      type: 'trade_update',
      data: failedTrade('newer-than-refresh')
    })
    await vi.waitFor(() => expect(target.textContent).toContain('3 of 152'))

    resolveRefresh?.(tradeResponse([staleTrade], 150, true))
    await vi.waitFor(() => {
      expect(target.textContent).toContain('Refresh')
      expect(target.textContent).toContain('3 of 152')
    })

    connection.disconnect()
    await unmount(component)
    target.remove()
  })

  it('reports an invalid refresh without replacing known-good history', async () => {
    const invalidResponse = new Response(
      JSON.stringify({
        entries: [failedTrade('invalid', { shares: '0' })],
        total: 1,
        hasMore: false
      }),
      {
        status: 200,
        headers: { 'content-type': 'application/json' }
      }
    )
    const fetchMock = vi
      .fn<() => Promise<Response>>()
      .mockResolvedValueOnce(tradeResponse([staleTrade], 1))
      .mockResolvedValueOnce(invalidResponse)
    vi.stubGlobal('fetch', fetchMock)

    const { target, component } = mountPanel()
    await vi.waitFor(() => {
      expect(target.textContent).toContain('SPCX')
      expect(target.textContent).toContain('1 of 1')
    })

    const refresh = [...target.querySelectorAll('button')].find(
      (button) => button.textContent.trim() === 'Refresh'
    )
    expect(refresh).toBeDefined()
    refresh?.click()

    await vi.waitFor(() => {
      expect(target.textContent).toContain(
        'Failed to load trades: Invalid trade payload at entries[0].shares'
      )
      expect(target.textContent).toContain('SPCX')
      expect(target.textContent).toContain('1 of 1')
    })

    await unmount(component)
    target.remove()
  })

  it('retries the same page after a failed append', async () => {
    const fetchMock = vi
      .fn<(url: string, init?: RequestInit) => Promise<Response>>()
      .mockResolvedValueOnce(tradeResponse([staleTrade], 201, true))
      .mockResolvedValueOnce(new Response(null, { status: 500 }))
      .mockResolvedValueOnce(tradeResponse([failedTrade('older-trade')], 201, true))
    vi.stubGlobal('fetch', fetchMock)

    const { target, component } = mountPanel()
    await vi.waitFor(() => expect(target.textContent).toContain('1 of 201'))

    const findLoadMore = (): HTMLButtonElement | undefined =>
      [...target.querySelectorAll('button')].find(
        (button) => button.textContent.trim() === 'Load older trades'
      )

    findLoadMore()?.click()
    await vi.waitFor(() => {
      expect(target.textContent).toContain('Failed to load trades: HTTP 500')
      expect(fetchMock).toHaveBeenCalledTimes(2)
    })

    findLoadMore()?.click()
    await vi.waitFor(() => {
      expect(target.textContent).toContain('2 of 201')
      expect(fetchMock).toHaveBeenCalledTimes(3)
    })

    const requestedUrls = fetchMock.mock.calls.map(([url]) => url)
    expect(requestedUrls[1]).toContain('offset=100')
    expect(requestedUrls[2]).toContain('offset=100')

    await unmount(component)
    target.remove()
  })

  it('does not let polling supersede an in-flight page load', async () => {
    let poll: (() => void) | undefined
    vi.spyOn(globalThis, 'setInterval').mockImplementation((handler) => {
      poll = handler as () => void
      return {} as ReturnType<typeof setInterval>
    })
    let resolveOlder: ((response: Response) => void) | undefined
    const fetchMock = vi
      .fn<() => Promise<Response>>()
      .mockResolvedValueOnce(tradeResponse([staleTrade], 201, true))
      .mockImplementationOnce(
        () =>
          new Promise<Response>((resolve) => {
            resolveOlder = resolve
          })
      )
    vi.stubGlobal('fetch', fetchMock)

    const { target, component } = mountPanel()
    await vi.waitFor(() => expect(target.textContent).toContain('1 of 201'))

    const loadMore = [...target.querySelectorAll('button')].find(
      (button) => button.textContent.trim() === 'Load older trades'
    )
    loadMore?.click()
    await vi.waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(2))

    poll?.()
    expect(fetchMock).toHaveBeenCalledTimes(2)

    resolveOlder?.(tradeResponse([failedTrade('older-trade')], 201, true))
    await vi.waitFor(() => expect(target.textContent).toContain('2 of 201'))

    await unmount(component)
    target.remove()
  })

  it('clears request state when the final venue is deselected', async () => {
    const pendingResponses: Array<(response: Response) => void> = []
    const fetchMock = vi
      .fn<() => Promise<Response>>()
      .mockResolvedValueOnce(tradeResponse([staleTrade], 150, true))
      .mockImplementation(
        () =>
          new Promise<Response>((resolve) => {
            pendingResponses.push(resolve)
          })
      )
    vi.stubGlobal('fetch', fetchMock)

    const { target, component } = mountPanel()
    await vi.waitFor(() => expect(target.textContent).toContain('1 of 150'))

    const loadMore = [...target.querySelectorAll('button')].find(
      (button) => button.textContent.trim() === 'Load older trades'
    )
    expect(loadMore).toBeDefined()
    loadMore?.click()
    await vi.waitFor(() => {
      expect(fetchMock).toHaveBeenCalledTimes(2)
      expect(target.textContent).toContain('Loading...')
    })

    const venues = [...target.querySelectorAll('button')].find((button) =>
      button.textContent.includes('Venues')
    )
    expect(venues).toBeDefined()
    venues?.click()
    await tick()

    for (const label of ['Raindex', 'Alpaca', 'DryRun']) {
      const option = [...target.querySelectorAll('button')].find((button) =>
        button.textContent.trim().endsWith(label)
      )
      expect(option).toBeDefined()
      option?.click()
      await tick()
    }

    await vi.waitFor(() => {
      expect(target.textContent).toContain('Venues (none)')
      expect(target.textContent).toContain('No trades yet')
      expect(target.textContent).toContain('0 of 0')
      expect(target.textContent).not.toContain('Loading...')
      expect(fetchMock).toHaveBeenCalledTimes(4)
    })

    for (const resolve of pendingResponses) {
      resolve(tradeResponse([failedTrade('stale-completion')], 999, true))
    }
    await vi.waitFor(() => {
      expect(target.textContent).toContain('No trades yet')
      expect(target.textContent).toContain('0 of 0')
      expect(target.textContent).not.toContain('stale-completion')
      expect(target.textContent).not.toContain('Load older trades')
    })

    await unmount(component)
    target.remove()
  })
})
