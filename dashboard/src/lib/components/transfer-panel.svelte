<script lang="ts">
  import { createQuery } from '@tanstack/svelte-query'
  import * as Card from '$lib/components/ui/card'
  import * as Table from '$lib/components/ui/table'
  import { Badge, type BadgeVariant } from '$lib/components/ui/badge'
  import type { TransferOperation } from '$lib/api/TransferOperation'
  import { matcher } from '$lib/fp'
  import { decimalToNumber } from '$lib/decimal'
  import { failureReason, txLinks } from './transfer-details'

  const activeQuery = createQuery<TransferOperation[]>(() => ({
    queryKey: ['transfers', 'active'],
    queryFn: () => Promise.resolve([]),
    staleTime: Infinity
  }))

  const recentQuery = createQuery<TransferOperation[]>(() => ({
    queryKey: ['transfers', 'recent'],
    queryFn: () => Promise.resolve([]),
    staleTime: Infinity
  }))

  const activeTransfers = $derived(activeQuery.data ?? [])
  const recentTransfers = $derived(recentQuery.data ?? [])
  const allTransfers = $derived.by(() => {
    const byId = new Map(recentTransfers.map((transfer) => [transfer.id, transfer]))
    for (const transfer of activeTransfers) byId.set(transfer.id, transfer)
    return [...byId.values()]
  })

  const matchKind = matcher<TransferOperation>()('kind')

  const fmtNum = (value: string): string =>
    decimalToNumber(value).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })

  const transferAsset = (transfer: TransferOperation): string =>
    matchKind(transfer, {
      equity_mint: (op) => op.symbol,
      equity_redemption: (op) => op.symbol,
      usdc_bridge: () => 'USDC'
    })

  const transferDescription = (transfer: TransferOperation): string =>
    matchKind(transfer, {
      equity_mint: (op) => `Alpaca \u2014 ${fmtNum(op.quantity)} \u2192 Raindex`,
      equity_redemption: (op) => `Raindex \u2014 ${fmtNum(op.quantity)} \u2192 Alpaca`,
      usdc_bridge: (op) =>
        op.direction === 'alpaca_to_base'
          ? `Alpaca \u2014 ${fmtNum(op.amount)} \u2192 Raindex`
          : `Raindex \u2014 ${fmtNum(op.amount)} \u2192 Alpaca`
    })

  const statusVariant = (status: string): BadgeVariant => {
    if (status === 'completed') return 'default'
    if (status === 'failed') return 'destructive'
    return 'secondary'
  }

  const fmtHash = (hash: string): string =>
    `${hash.slice(0, 6)}\u2026${hash.slice(-4)}`
</script>

<Card.Root class="flex h-full min-h-56 flex-col overflow-hidden">
  <Card.Header class="shrink-0 pb-0">
    <Card.Title class="flex items-center justify-between">
      <span>Rebalancing</span>
      {#if activeTransfers.length > 0}
        <Badge variant="secondary">{activeTransfers.length} active</Badge>
      {/if}
    </Card.Title>
  </Card.Header>
  <Card.Content class="relative min-h-0 flex-1 overflow-auto px-6 pt-0">
    {#if allTransfers.length === 0}
      <div class="flex h-full items-center justify-center text-muted-foreground">
        No transfers
      </div>
    {:else}
      <Table.Root>
        <Table.Header>
          <Table.Row>
            <Table.Head>Asset</Table.Head>
            <Table.Head>Transfer</Table.Head>
            <Table.Head>Status</Table.Head>
          </Table.Row>
        </Table.Header>
        <Table.Body>
          {#each allTransfers as transfer (transfer.id)}
            {@const reason = failureReason(transfer)}
            {@const links = txLinks(transfer)}
            <Table.Row>
              <Table.Cell class="font-mono text-xs font-medium">
                {transferAsset(transfer)}
              </Table.Cell>
              <Table.Cell class="font-mono text-xs text-muted-foreground">
                {transferDescription(transfer)}
              </Table.Cell>
              <Table.Cell>
                <div class="flex flex-col gap-0.5">
                  <Badge variant={statusVariant(transfer.status.status)}>
                    {transfer.status.status}
                  </Badge>
                  {#if reason}
                    <span class="text-[10px] leading-tight text-destructive">{reason}</span>
                  {/if}
                  {#if links.length > 0}
                    <span class="flex flex-wrap gap-1">
                      {#each links as link (link.label)}
                        <a
                          href={link.url}
                          target="_blank"
                          rel="noopener noreferrer"
                          class="text-[10px] text-muted-foreground underline decoration-dotted hover:text-foreground"
                          title={link.hash}
                        >{link.label}: {fmtHash(link.hash)}</a>
                      {/each}
                    </span>
                  {/if}
                </div>
              </Table.Cell>
            </Table.Row>
          {/each}
        </Table.Body>
      </Table.Root>
      <div
        class="pointer-events-none sticky bottom-0 h-8 bg-gradient-to-t from-card to-transparent"
      ></div>
    {/if}
  </Card.Content>
</Card.Root>
