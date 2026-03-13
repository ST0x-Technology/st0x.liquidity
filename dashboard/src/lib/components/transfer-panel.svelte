<script lang="ts">
  import { createQuery } from '@tanstack/svelte-query'
  import * as Card from '$lib/components/ui/card'
  import * as Table from '$lib/components/ui/table'
  import { Badge, type BadgeVariant } from '$lib/components/ui/badge'
  import type { TransferOperation } from '$lib/api/TransferOperation'
  import type { Warning } from '$lib/api/Warning'
  import { matcher } from '$lib/fp'
  import { formatDecimal } from '$lib/decimal'
  import { warningMessage } from '$lib/warnings'

  const activeQuery = createQuery<TransferOperation[]>(() => ({
    queryKey: ['transfers', 'active'],
    enabled: false
  }))

  const recentQuery = createQuery<TransferOperation[]>(() => ({
    queryKey: ['transfers', 'recent'],
    enabled: false
  }))

  const warningsQuery = createQuery<Warning[]>(() => ({
    queryKey: ['warnings'],
    enabled: false
  }))

  const activeTransfers = $derived(activeQuery.data ?? [])
  const recentTransfers = $derived(recentQuery.data ?? [])
  const warnings = $derived(warningsQuery.data ?? [])
  const allTransfers = $derived.by(() => {
    const byId = new Map(activeTransfers.map((transfer) => [transfer.id, transfer]))
    for (const transfer of recentTransfers) {
      if (!byId.has(transfer.id)) byId.set(transfer.id, transfer)
    }
    return [...byId.values()]
  })

  const matchKind = matcher<TransferOperation>()('kind')

  const fmtNum = (value: string): string => formatDecimal(value, 2)

  const transferAsset = (transfer: TransferOperation): string =>
    matchKind(transfer, {
      equity_mint: ({ symbol }) => symbol,
      equity_redemption: ({ symbol }) => symbol,
      usdc_bridge: () => 'USDC'
    })

  const transferDescription = (transfer: TransferOperation): string =>
    matchKind(transfer, {
      equity_mint: ({ quantity }) => `Alpaca — ${fmtNum(quantity)} → Raindex`,
      equity_redemption: ({ quantity }) => `Raindex — ${fmtNum(quantity)} → Alpaca`,
      usdc_bridge: ({ direction, amount }) =>
        direction === 'alpaca_to_base'
          ? `Alpaca — ${fmtNum(amount)} → Raindex`
          : `Raindex — ${fmtNum(amount)} → Alpaca`
    })

  const statusVariant = (status: TransferOperation['status']['status']): BadgeVariant => {
    if (status === 'completed') return 'default'
    if (status === 'failed') return 'destructive'
    return 'secondary'
  }
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
  {#if warnings.length > 0}
    <div class="mx-6 mt-2 rounded-md border border-destructive/50 bg-destructive/10 px-3 py-2 text-xs text-destructive">
      {#each warnings as warning, idx (idx)}
        <p>{warningMessage(warning)}</p>
      {/each}
    </div>
  {/if}
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
            <Table.Row>
              <Table.Cell class="font-mono text-xs font-medium">
                {transferAsset(transfer)}
              </Table.Cell>
              <Table.Cell class="font-mono text-xs text-muted-foreground">
                {transferDescription(transfer)}
              </Table.Cell>
              <Table.Cell>
                <Badge variant={statusVariant(transfer.status.status)}>
                  {transfer.status.status}
                </Badge>
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
