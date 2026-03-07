<script lang="ts">
  import { createQuery } from '@tanstack/svelte-query'
  import * as Card from '$lib/components/ui/card'
  import * as Table from '$lib/components/ui/table'
  import { Badge, type BadgeVariant } from '$lib/components/ui/badge'
  import type { TransferOperation } from '$lib/api/TransferOperation'
  import { matcher } from '$lib/fp'

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
  const allTransfers = $derived([...activeTransfers, ...recentTransfers])

  const matchKind = matcher<TransferOperation>()('kind')

  const fmtNum = (value: string): string => {
    const num = parseFloat(value)
    return num.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })
  }

  const transferAsset = (transfer: TransferOperation): string =>
    matchKind(transfer, {
      equity_mint: (op) => op.symbol,
      equity_redemption: (op) => op.symbol,
      usdc_bridge: () => 'USDC'
    })

  const transferDescription = (transfer: TransferOperation): string =>
    matchKind(transfer, {
      equity_mint: (op) => `Alpaca — ${fmtNum(op.quantity)} → Raindex`,
      equity_redemption: (op) => `Raindex — ${fmtNum(op.quantity)} → Alpaca`,
      usdc_bridge: (op) =>
        op.direction === 'alpaca_to_base'
          ? `Alpaca — ${fmtNum(op.amount)} → Raindex`
          : `Raindex — ${fmtNum(op.amount)} → Alpaca`
    })

  const statusVariant = (status: string): BadgeVariant => {
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
