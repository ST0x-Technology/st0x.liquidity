<script lang="ts">
  import { createQuery } from '@tanstack/svelte-query'
  import type { Settings } from '$lib/api/Settings'

  const settingsQuery = createQuery<Settings>(() => ({
    queryKey: ['settings'],
    enabled: false
  }))

  const settings = $derived(settingsQuery.data)

  let dialogEl: HTMLDialogElement | undefined = $state()

  const fmtPct = (value: number): string => `${(value * 100).toFixed(0)}%`

  const equityBounds = $derived(
    settings
      ? `${fmtPct(settings.equityTarget - settings.equityDeviation)}–${fmtPct(settings.equityTarget + settings.equityDeviation)}`
      : ''
  )

  const usdcBounds = $derived(
    settings?.usdcTarget != null && settings.usdcDeviation != null
      ? `${fmtPct(settings.usdcTarget - settings.usdcDeviation)}–${fmtPct(settings.usdcTarget + settings.usdcDeviation)}`
      : ''
  )
</script>

{#if settings}
  <div class="shrink-0 border-b bg-card/30 px-2 py-1.5 md:px-4">
    <div class="flex flex-wrap items-center gap-2">
      <span class="inline-flex items-center gap-1.5 rounded-full bg-muted px-2.5 py-0.5 text-xs">
        <span class="h-1.5 w-1.5 rounded-full {settings.tradingMode === 'rebalancing' ? 'bg-green-500' : 'bg-yellow-500'}"></span>
        <span class="font-mono">{settings.tradingMode}</span>
      </span>

      <span class="inline-flex items-center gap-1.5 rounded-full bg-muted px-2.5 py-0.5 text-xs">
        <span class="font-mono">{settings.broker}</span>
      </span>

      <span class="inline-flex items-center gap-1.5 rounded-full bg-muted px-2.5 py-0.5 text-xs">
        Equity <span class="font-mono font-semibold">{fmtPct(settings.equityTarget)}</span>
        <span class="text-muted-foreground">({equityBounds})</span>
      </span>

      {#if settings.usdcTarget != null}
        <span class="inline-flex items-center gap-1.5 rounded-full bg-muted px-2.5 py-0.5 text-xs">
          USDC <span class="font-mono font-semibold">{fmtPct(settings.usdcTarget)}</span>
          <span class="text-muted-foreground">({usdcBounds})</span>
        </span>
      {/if}

      <span class="inline-flex items-center gap-1.5 rounded-full bg-muted px-2.5 py-0.5 text-xs">
        Trigger <span class="font-mono font-semibold">{settings.executionThreshold}</span>
      </span>

      {#if settings.cashReserved != null}
        <span class="inline-flex items-center gap-1.5 rounded-full bg-muted px-2.5 py-0.5 text-xs">
          Reserve <span class="font-mono font-semibold">${settings.cashReserved}</span>
        </span>
      {/if}

      <button
        class="inline-flex items-center rounded-full bg-muted px-2.5 py-0.5 text-xs text-muted-foreground hover:text-foreground"
        onclick={() => dialogEl?.showModal()}
      >
        Config
      </button>
    </div>
  </div>

  <!-- Native dialog renders in the top layer, unaffected by parent overflow -->
  <dialog
    bind:this={dialogEl}
    class="w-full max-w-md rounded-lg border bg-card p-6 text-foreground shadow-lg backdrop:bg-black/50"
    onclick={(event) => { if (event.target === dialogEl) dialogEl.close() }}
  >
    <div class="mb-4 flex items-center justify-between">
      <h3 class="text-sm font-semibold text-foreground">Configuration</h3>
      <button
        class="text-lg leading-none text-muted-foreground hover:text-foreground"
        onclick={() => dialogEl?.close()}
      >
        &times;
      </button>
    </div>

    <div class="space-y-2 font-mono text-xs">
      <div class="flex justify-between"><span class="text-muted-foreground">Log level</span><span>{settings.logLevel}</span></div>
      <div class="flex justify-between"><span class="text-muted-foreground">Server port</span><span>{String(settings.serverPort)}</span></div>
      <div class="flex justify-between"><span class="text-muted-foreground">Deployment block</span><span>{String(settings.deploymentBlock)}</span></div>
      <div class="flex justify-between"><span class="text-muted-foreground">Order polling</span><span>{String(settings.orderPollingInterval)}s</span></div>
      <div class="flex justify-between"><span class="text-muted-foreground">Inventory polling</span><span>{String(settings.inventoryPollInterval)}s</span></div>
      <div class="flex justify-between"><span class="text-muted-foreground">Cash reserve</span><span>{settings.cashReserved != null ? `$${settings.cashReserved}` : 'none'}</span></div>
      <div class="flex justify-between"><span class="text-muted-foreground">Orderbook</span><span class="ml-4 truncate">{settings.orderbook}</span></div>
    </div>
  </dialog>
{/if}
