<script lang="ts">
  import { createQuery } from '@tanstack/svelte-query'
  import AvailableInventory from '$lib/components/available-inventory.svelte'
  import * as Card from '$lib/components/ui/card'
  import type { Inventory } from '$lib/api/Inventory'
  import type { Position } from '$lib/api/Position'
  import type { Settings } from '$lib/api/Settings'

  const inventoryQuery = createQuery<Inventory>(() => ({
    queryKey: ['inventory'],
    enabled: false
  }))

  const positionsQuery = createQuery<Position[]>(() => ({
    queryKey: ['positions'],
    enabled: false
  }))

  const settingsQuery = createQuery<Settings>(() => ({
    queryKey: ['settings'],
    enabled: false
  }))

  const inventory = $derived(inventoryQuery.data)
  const symbols = $derived(inventory?.perSymbol ?? [])
  const usdc = $derived(inventory?.usdc)
  const positions = $derived(positionsQuery.data ?? [])
  const settings = $derived(settingsQuery.data)

  let glossaryDialogEl: HTMLDialogElement | undefined = $state()

  const openGlossary = () => {
    glossaryDialogEl?.showModal()
  }

  type GlossaryEntry = { name: string; def: string }

  const cashEntries: GlossaryEntry[] = [
    { name: 'Asset', def: 'Always "Cash" for the USDC row.' },
    { name: 'Raindex', def: 'USDC available in the Raindex vaults to settle takers.' },
    { name: 'Inflight', def: 'USDC the bot has already committed to transfers or settlements between venues. Included in Total, but not in Ratio.' },
    { name: 'Alpaca Total', def: 'Alpaca USD cash used for inventory math: gross broker cash when available, otherwise available cash. This is USD cash at Alpaca, not wallet-observed USDC.' },
    { name: 'Total', def: 'Raindex + Inflight + Alpaca Total. Wallet-observed Eth/Base balances are excluded.' },
    { name: 'Ratio', def: 'Venue split for cash allocation: Raindex / (Raindex + Alpaca Total). Inflight and wallet-observed balances are excluded from the ratio.' },
    { name: 'Alpaca / USDC', def: 'USDC token balance held in the Alpaca account. This is separate from Alpaca USD cash and does not count toward the cash reserve.' },
    { name: 'Alpaca / Rebalanceable', def: 'Settled Alpaca cash that can leave Alpaca for USDC rebalancing after preserving the configured reserve.' },
    { name: 'Alpaca / Counter-tradeable', def: 'Alpaca cash available for buy-side equity counter-trades. The configured reserve is not subtracted from this value.' },
    { name: 'Eth', def: 'USDC observed in the Ethereum wallet while moving between Alpaca and CCTP. Not included in Total, Ratio, or rebalancing decisions.' },
    { name: 'Base', def: 'USDC observed in the Base wallet while moving between CCTP and Raindex. Not included in Total, Ratio, or rebalancing decisions.' },
  ]

  const equityEntries: GlossaryEntry[] = [
    { name: 'Asset', def: 'Equity symbol with the t/wt prefix stripped (e.g. AAPL for tAAPL / wtAAPL). Non-trading assets are dimmed.' },
    { name: 'Raindex', def: 'tSTOCK tokens available in the Raindex vault to settle takers.' },
    { name: 'Inflight', def: 'Shares the books track as in motion between venues (pending mints, redeems, transfers). Part of imbalance math.' },
    { name: 'Alpaca', def: 'Shares held at Alpaca (offchain available).' },
    { name: 'Total', def: 'Raindex + Inflight + Alpaca. Excludes wallet-observed amounts (Unwrapped / Wrapped).' },
    { name: 'Ratio', def: 'Proportion of total shares sitting on Raindex (onchain / total). Bar and percent reflect the current split; colored offset is deviation from target.' },
    { name: 'Exposure', def: 'Net directional dollar exposure from counterparty fills (Alpaca-reported net position × last price). ▲ green = long, ▼ red = short. Values under $0.01 render as ~$0.' },
    { name: 'Unwrapped', def: 'Wallet-observed tSTOCK parked on the Base wallet between a Raindex withdrawal and an Alpaca redemption transfer (or post-mint, pre-vault deposit). Out-of-band — NOT part of imbalance math.' },
    { name: 'Wrapped', def: 'Wallet-observed wtSTOCK vault shares on the Base wallet (post vault-withdraw, pre-unwrap). Out-of-band — NOT part of imbalance math.' },
  ]
</script>

<Card.Root class="flex h-full flex-col overflow-hidden border-l-4 border-l-blue-500/50">
  <Card.Header class="shrink-0 pb-3">
    <Card.Title class="flex items-center gap-1.5">
      Inventory
      <button
        type="button"
        class="cursor-pointer text-muted-foreground hover:text-foreground"
        onclick={openGlossary}
        title="Column reference"
        aria-label="Open inventory column reference"
      >
        <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 20 20" fill="currentColor" class="h-3.5 w-3.5"><path fill-rule="evenodd" d="M18 10a8 8 0 1 1-16 0 8 8 0 0 1 16 0Zm-7-4a1 1 0 1 1-2 0 1 1 0 0 1 2 0ZM9 9a.75.75 0 0 0 0 1.5h.253a.25.25 0 0 1 .244.304l-.459 2.066A1.75 1.75 0 0 0 10.747 15H11a.75.75 0 0 0 0-1.5h-.253a.25.25 0 0 1-.244-.304l.459-2.066A1.75 1.75 0 0 0 9.253 9H9Z" clip-rule="evenodd" /></svg>
      </button>
    </Card.Title>
  </Card.Header>
  <Card.Content class="relative min-h-0 flex-1 overflow-auto px-6 pt-0">
    {#if !inventory}
      <div class="flex h-full items-center justify-center text-muted-foreground">
        Waiting for inventory data…
      </div>
    {:else}
      <AvailableInventory {symbols} {usdc} {positions} {settings} />
    {/if}
  </Card.Content>
</Card.Root>

<dialog
  bind:this={glossaryDialogEl}
  class="w-full max-w-2xl rounded-lg border bg-card p-0 text-foreground shadow-lg backdrop:bg-black/50"
  onclick={(event) => { if (event.target === glossaryDialogEl) glossaryDialogEl.close() }}
>
  <div class="flex items-center justify-between border-b px-5 py-3">
    <div class="text-sm font-semibold">Inventory column reference</div>
    <button
      type="button"
      class="text-lg leading-none text-muted-foreground hover:text-foreground"
      onclick={() => glossaryDialogEl?.close()}
      aria-label="Close"
    >
      &times;
    </button>
  </div>

  <div class="max-h-[70vh] space-y-6 overflow-y-auto px-5 py-4 text-sm">
    <section>
      <h3 class="mb-2 text-xs font-semibold uppercase tracking-wider text-muted-foreground">Cash row</h3>
      <dl class="space-y-2">
        {#each cashEntries as entry (entry.name)}
          <div class="grid grid-cols-[8rem_1fr] gap-3">
            <dt class="font-mono text-sm font-medium">{entry.name}</dt>
            <dd class="text-sm text-muted-foreground">{entry.def}</dd>
          </div>
        {/each}
      </dl>
    </section>

    <section>
      <h3 class="mb-2 text-xs font-semibold uppercase tracking-wider text-muted-foreground">Equity rows</h3>
      <dl class="space-y-2">
        {#each equityEntries as entry (entry.name)}
          <div class="grid grid-cols-[8rem_1fr] gap-3">
            <dt class="font-mono text-sm font-medium">{entry.name}</dt>
            <dd class="text-sm text-muted-foreground">{entry.def}</dd>
          </div>
        {/each}
      </dl>
    </section>

    <section class="rounded border border-amber-500/30 bg-amber-500/5 px-3 py-2 text-xs text-muted-foreground">
      <strong class="text-foreground">Wallet-observed</strong> columns (Eth, Base, Unwrapped, Wrapped) come from polling the on-chain wallets directly. They are a sanity check for funds in transit between venues and are <strong>NOT</strong> included in Total, Ratio, or imbalance / rebalancing decisions.
    </section>
  </div>
</dialog>
