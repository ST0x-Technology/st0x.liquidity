<script lang="ts">
  import { useQueryClient } from '@tanstack/svelte-query'
  import InventoryPanel from '$lib/components/inventory-panel.svelte'
  import LiveEventsPanel from '$lib/components/live-events-panel.svelte'
  import type { Inventory } from '$lib/api/Inventory'
  import type { TransferOperation } from '$lib/api/TransferOperation'
  import type { EventStoreEntry } from '$lib/api/EventStoreEntry'

  const queryClient = useQueryClient()

  const mockInventory: Inventory = {
    perSymbol: [
      {
        symbol: 'tAAPL',
        onchainAvailable: '150.25',
        onchainInflight: '0',
        offchainAvailable: '200.00',
        offchainInflight: '10.50'
      },
      {
        symbol: 'tTSLA',
        onchainAvailable: '75.00',
        onchainInflight: '25.75',
        offchainAvailable: '50.00',
        offchainInflight: '0'
      },
      {
        symbol: 'tSPYM',
        onchainAvailable: '500.00',
        onchainInflight: '0',
        offchainAvailable: '300.00',
        offchainInflight: '0'
      },
      {
        symbol: 'tMSFT',
        onchainAvailable: '320.50',
        onchainInflight: '0',
        offchainAvailable: '180.00',
        offchainInflight: '0'
      },
      {
        symbol: 'tAMZN',
        onchainAvailable: '45.00',
        onchainInflight: '0',
        offchainAvailable: '90.75',
        offchainInflight: '0'
      },
      {
        symbol: 'tNVDA',
        onchainAvailable: '210.00',
        onchainInflight: '0',
        offchainAvailable: '125.50',
        offchainInflight: '15.00'
      },
      {
        symbol: 'tGOOG',
        onchainAvailable: '88.30',
        onchainInflight: '0',
        offchainAvailable: '112.00',
        offchainInflight: '0'
      }
    ],
    usdc: {
      onchainAvailable: '125000.50',
      onchainInflight: '0',
      offchainAvailable: '80000.00',
      offchainInflight: '5000.00'
    }
  }

  const mockActiveTransfers: TransferOperation[] = [
    {
      kind: 'equity_mint',
      id: 'mint-001',
      symbol: 'tAAPL',
      quantity: '10.50',
      status: { status: 'wrapping' },
      startedAt: new Date(Date.now() - 120000).toISOString(),
      updatedAt: new Date(Date.now() - 30000).toISOString()
    },
    {
      kind: 'usdc_bridge',
      id: 'bridge-001',
      direction: 'alpaca_to_base',
      amount: '5000.00',
      status: { status: 'bridging' },
      startedAt: new Date(Date.now() - 300000).toISOString(),
      updatedAt: new Date(Date.now() - 60000).toISOString()
    },
    {
      kind: 'equity_redemption',
      id: 'redeem-003',
      symbol: 'tNVDA',
      quantity: '15.00',
      status: { status: 'sending' },
      startedAt: new Date(Date.now() - 90000).toISOString(),
      updatedAt: new Date(Date.now() - 15000).toISOString()
    }
  ]

  const mockRecentTransfers: TransferOperation[] = [
    {
      kind: 'equity_mint',
      id: 'mint-002',
      symbol: 'tSPYM',
      quantity: '100.00',
      status: { status: 'completed', completed_at: new Date(Date.now() - 400000).toISOString() },
      startedAt: new Date(Date.now() - 700000).toISOString(),
      updatedAt: new Date(Date.now() - 400000).toISOString()
    },
    {
      kind: 'equity_redemption',
      id: 'redeem-001',
      symbol: 'tAAPL',
      quantity: '50.00',
      status: { status: 'completed', completed_at: new Date(Date.now() - 600000).toISOString() },
      startedAt: new Date(Date.now() - 900000).toISOString(),
      updatedAt: new Date(Date.now() - 600000).toISOString()
    },
    {
      kind: 'equity_mint',
      id: 'mint-003',
      symbol: 'tMSFT',
      quantity: '25.00',
      status: { status: 'completed', completed_at: new Date(Date.now() - 800000).toISOString() },
      startedAt: new Date(Date.now() - 1000000).toISOString(),
      updatedAt: new Date(Date.now() - 800000).toISOString()
    },
    {
      kind: 'usdc_bridge',
      id: 'bridge-003',
      direction: 'alpaca_to_base',
      amount: '25000.00',
      status: { status: 'completed', completed_at: new Date(Date.now() - 1100000).toISOString() },
      startedAt: new Date(Date.now() - 1300000).toISOString(),
      updatedAt: new Date(Date.now() - 1100000).toISOString()
    },
    {
      kind: 'usdc_bridge',
      id: 'bridge-002',
      direction: 'base_to_alpaca',
      amount: '10000.00',
      status: { status: 'failed', failed_at: new Date(Date.now() - 1200000).toISOString() },
      startedAt: new Date(Date.now() - 1500000).toISOString(),
      updatedAt: new Date(Date.now() - 1200000).toISOString()
    },
    {
      kind: 'equity_redemption',
      id: 'redeem-002',
      symbol: 'tTSLA',
      quantity: '30.00',
      status: { status: 'completed', completed_at: new Date(Date.now() - 1800000).toISOString() },
      startedAt: new Date(Date.now() - 2100000).toISOString(),
      updatedAt: new Date(Date.now() - 1800000).toISOString()
    }
  ]

  const mockEvents: EventStoreEntry[] = [
    {
      aggregate_type: 'EquityRedemption',
      aggregate_id: 'redeem-003',
      sequence: 2,
      event_type: 'EquityRedemptionEvent::SendingStarted',
      timestamp: new Date(Date.now() - 15000).toISOString()
    },
    {
      aggregate_type: 'TokenizedEquityMint',
      aggregate_id: 'mint-001',
      sequence: 3,
      event_type: 'TokenizedEquityMintEvent::WrappingStarted',
      timestamp: new Date(Date.now() - 30000).toISOString()
    },
    {
      aggregate_type: 'UsdcRebalance',
      aggregate_id: 'bridge-001',
      sequence: 2,
      event_type: 'UsdcRebalanceEvent::BridgingStarted',
      timestamp: new Date(Date.now() - 60000).toISOString()
    },
    {
      aggregate_type: 'EquityRedemption',
      aggregate_id: 'redeem-003',
      sequence: 1,
      event_type: 'EquityRedemptionEvent::Created',
      timestamp: new Date(Date.now() - 90000).toISOString()
    },
    {
      aggregate_type: 'TokenizedEquityMint',
      aggregate_id: 'mint-001',
      sequence: 2,
      event_type: 'TokenizedEquityMintEvent::BuyOrderPlaced',
      timestamp: new Date(Date.now() - 120000).toISOString()
    },
    {
      aggregate_type: 'TokenizedEquityMint',
      aggregate_id: 'mint-001',
      sequence: 1,
      event_type: 'TokenizedEquityMintEvent::Created',
      timestamp: new Date(Date.now() - 150000).toISOString()
    },
    {
      aggregate_type: 'TokenizedEquityMint',
      aggregate_id: 'mint-002',
      sequence: 5,
      event_type: 'TokenizedEquityMintEvent::Completed',
      timestamp: new Date(Date.now() - 400000).toISOString()
    },
    {
      aggregate_type: 'EquityRedemption',
      aggregate_id: 'redeem-001',
      sequence: 5,
      event_type: 'EquityRedemptionEvent::Completed',
      timestamp: new Date(Date.now() - 600000).toISOString()
    },
    {
      aggregate_type: 'TokenizedEquityMint',
      aggregate_id: 'mint-003',
      sequence: 4,
      event_type: 'TokenizedEquityMintEvent::Completed',
      timestamp: new Date(Date.now() - 800000).toISOString()
    },
    {
      aggregate_type: 'UsdcRebalance',
      aggregate_id: 'bridge-003',
      sequence: 3,
      event_type: 'UsdcRebalanceEvent::Completed',
      timestamp: new Date(Date.now() - 1100000).toISOString()
    }
  ]

  queryClient.setQueryData(['inventory'], mockInventory)
  queryClient.setQueryData(['transfers', 'active'], mockActiveTransfers)
  queryClient.setQueryData(['transfers', 'recent'], mockRecentTransfers)
  queryClient.setQueryData(['events'], mockEvents)
</script>

<div class="flex h-screen flex-col bg-background">
  <header class="flex items-center gap-4 border-b px-4 py-2">
    <h1 class="text-lg font-semibold">Dashboard Demo</h1>
    <span class="text-sm text-muted-foreground">Mock data</span>
  </header>

  <main class="flex-1 overflow-auto p-2 md:overflow-hidden md:p-4">
    <div class="grid h-full grid-cols-1 gap-2 md:grid-cols-[3fr_2fr] md:gap-4">
      <InventoryPanel />
      <LiveEventsPanel />
    </div>
  </main>
</div>
