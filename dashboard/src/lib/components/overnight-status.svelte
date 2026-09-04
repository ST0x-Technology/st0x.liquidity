<script lang="ts">
  import { onMount } from 'svelte'
  import * as Card from '$lib/components/ui/card'
  import HoverTooltip from '$lib/components/hover-tooltip.svelte'
  import { reactive } from '$lib/frp.svelte'
  import { getApiBaseUrl } from '$lib/env'
  import { formatUtc, FETCH_TIMEOUT_MS } from '$lib/time'

  // Hand-written mirror of the server-local response struct in
  // `src/api.rs` (`OvernightEligibilityEntry`), following the
  // pending-orders precedent for endpoints outside the st0x-dto surface.
  type EligibilityEntry = {
    symbol: string
    synced_at: string | null
    overnight_tradable: boolean | null
    overnight_halted: boolean | null
    fractionable: boolean | null
    fractional_eh_enabled: boolean | null
    whole_share_verdict: string
    fractional_verdict: string
  }

  const POLL_INTERVAL_MS = 30_000

  const entries = reactive<EligibilityEntry[]>([])
  const error = reactive<string | null>(null)

  const fetchEligibility = async () => {
    try {
      const baseUrl = getApiBaseUrl()
      const response = await fetch(`${baseUrl}/overnight/eligibility`, {
        signal: AbortSignal.timeout(FETCH_TIMEOUT_MS)
      })

      if (!response.ok) {
        error.update(() => `HTTP ${String(response.status)}`)
        return
      }

      const data: EligibilityEntry[] = (await response.json()) as EligibilityEntry[]
      entries.update(() => data)
      error.update(() => null)
    } catch (fetchError) {
      error.update(() => (fetchError instanceof Error ? fetchError.message : 'Unknown error'))
    }
  }

  onMount(() => {
    void fetchEligibility()
    const interval = setInterval(() => {
      void fetchEligibility()
    }, POLL_INTERVAL_MS)
    return () => {
      clearInterval(interval)
    }
  })

  const verdictDot = (verdict: string): string =>
    verdict === 'eligible' ? 'bg-green-500' : 'bg-red-500'
</script>

<Card.Root class="flex h-full flex-col overflow-hidden border-l-4 border-l-indigo-500/50">
  <Card.Header class="shrink-0 pb-3">
    <Card.Title class="flex items-center gap-1.5">
      Overnight Eligibility
      <span class="group relative cursor-help text-muted-foreground">
        <svg
          xmlns="http://www.w3.org/2000/svg"
          viewBox="0 0 20 20"
          fill="currentColor"
          class="h-3.5 w-3.5"
          ><path
            fill-rule="evenodd"
            d="M18 10a8 8 0 1 1-16 0 8 8 0 0 1 16 0Zm-7-4a1 1 0 1 1-2 0 1 1 0 0 1 2 0ZM9 9a.75.75 0 0 0 0 1.5h.253a.25.25 0 0 1 .244.304l-.459 2.066A1.75 1.75 0 0 0 10.747 15H11a.75.75 0 0 0 0-1.5h-.253a.25.25 0 0 1-.244-.304l.459-2.066A1.75 1.75 0 0 0 9.253 9H9Z"
            clip-rule="evenodd"
          /></svg
        >
        <span
          class="pointer-events-none absolute left-0 top-full z-50 mt-1 hidden w-64 rounded bg-popover px-3 py-2 text-xs font-normal text-popover-foreground shadow-lg group-hover:block"
        >
          Per-asset verdicts for Alpaca's 24/5 overnight session, from the daily 19:55 ET
          eligibility sync. Whole = whole-share orders, Frac = fractional orders. A red light
          names the failing gate on hover.
        </span>
      </span>
    </Card.Title>
  </Card.Header>
  <Card.Content class="min-h-0 flex-1 overflow-auto px-6 pt-0">
    {#if error.current}
      <div class="text-xs text-destructive">{error.current}</div>
    {:else if entries.current.length === 0}
      <div class="text-xs text-muted-foreground">No configured equities</div>
    {:else}
      <div class="space-y-1">
        {#each entries.current as entry, idx (entry.symbol)}
          <div
            class="flex items-center gap-3 rounded px-2 py-1 text-xs {idx % 2 === 0
              ? 'bg-muted/40'
              : ''}"
          >
            <span class="w-14 font-mono font-medium">{entry.symbol}</span>

            <HoverTooltip tooltip={entry.whole_share_verdict}>
              <span class="inline-flex cursor-help items-center gap-1">
                <span class="inline-block h-1.5 w-1.5 rounded-full {verdictDot(entry.whole_share_verdict)}"
                ></span>
                Whole
              </span>
            </HoverTooltip>

            <HoverTooltip tooltip={entry.fractional_verdict}>
              <span class="inline-flex cursor-help items-center gap-1">
                <span class="inline-block h-1.5 w-1.5 rounded-full {verdictDot(entry.fractional_verdict)}"
                ></span>
                Frac
              </span>
            </HoverTooltip>

            {#if entry.overnight_halted === true}
              <span class="rounded bg-red-500/15 px-1.5 text-red-500">halted</span>
            {/if}

            <span class="ml-auto text-muted-foreground">
              {entry.synced_at ? `synced ${formatUtc(entry.synced_at)}` : 'never synced'}
            </span>
          </div>
        {/each}
      </div>
    {/if}
  </Card.Content>
</Card.Root>
