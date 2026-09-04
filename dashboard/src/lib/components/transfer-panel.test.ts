// @vitest-environment happy-dom

import { QueryClient } from '@tanstack/svelte-query'
import { mount, unmount } from 'svelte'
import { afterEach, describe, expect, it, vi } from 'vitest'
import type { TransferWarning } from '$lib/api/TransferWarning'
import TransferPanelHarness from './transfer-panel.test-harness.svelte'

vi.mock('$env/dynamic/public', () => ({ env: {} }))

const mountedPanels: ReturnType<typeof mount>[] = []

afterEach(async () => {
  while (mountedPanels.length > 0) {
    const component = mountedPanels.pop()
    if (component) await unmount(component)
  }

  vi.restoreAllMocks()
  vi.unstubAllGlobals()
})

describe('TransferPanel', () => {
  it('renders lifecycle warnings seeded by the initial WebSocket state', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn(() =>
        Promise.resolve(
          new Response(JSON.stringify({ entries: [], total: 0, hasMore: false }), {
            status: 200,
            headers: { 'content-type': 'application/json' }
          })
        )
      )
    )
    const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } })
    queryClient.setQueryData<TransferWarning[]>(
      ['transfers', 'warnings'],
      [
        { kind: 'mint_lifecycle_failed', id: 'mint-1' },
        { kind: 'redemption_lifecycle_failed', id: 'redemption-1' },
        { kind: 'bridge_lifecycle_failed', id: 'bridge-1' }
      ]
    )
    const target = document.createElement('div')
    document.body.append(target)
    mountedPanels.push(mount(TransferPanelHarness, { target, props: { client: queryClient } }))

    await vi.waitFor(() => {
      expect(target.textContent).toContain('Mint mint-1 has an invalid lifecycle.')
      expect(target.textContent).toContain('Redemption redemption-1 has an invalid lifecycle.')
      expect(target.textContent).toContain('USDC bridge bridge-1 has an invalid lifecycle.')
    })

    target.remove()
  })
})
