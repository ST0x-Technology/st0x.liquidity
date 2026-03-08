<script lang="ts">
  import { useQueryClient } from '@tanstack/svelte-query'
  import HeaderBar from '$lib/components/header-bar.svelte'
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
        onchainAvailable: '150.253847',
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
        offchainAvailable: '90.753218',
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
      },
      {
        symbol: 'tMETA',
        onchainAvailable: '62.175',
        onchainInflight: '0',
        offchainAvailable: '44.50',
        offchainInflight: '0'
      },
      {
        symbol: 'tJPM',
        onchainAvailable: '0',
        onchainInflight: '0',
        offchainAvailable: '85.25',
        offchainInflight: '0'
      },
      {
        symbol: 'tBAC',
        onchainAvailable: '1200.00',
        onchainInflight: '0',
        offchainAvailable: '0',
        offchainInflight: '0'
      },
      {
        symbol: 'tV',
        onchainAvailable: '33.891274',
        onchainInflight: '0',
        offchainAvailable: '67.50',
        offchainInflight: '0'
      },
      {
        symbol: 'tDIS',
        onchainAvailable: '175.00',
        onchainInflight: '0',
        offchainAvailable: '92.334',
        offchainInflight: '8.00'
      },
      {
        symbol: 'tCOST',
        onchainAvailable: '12.50',
        onchainInflight: '0',
        offchainAvailable: '18.75',
        offchainInflight: '0'
      },
      {
        symbol: 'tNFLX',
        onchainAvailable: '28.00',
        onchainInflight: '0',
        offchainAvailable: '14.128573',
        offchainInflight: '0'
      }
    ],
    usdc: {
      onchainAvailable: '125000.50',
      onchainInflight: '0',
      offchainAvailable: '80000.00',
      offchainInflight: '5000.00'
    },
    snapshotAt: new Date().toISOString()
  }

  const mockActiveTransfers: TransferOperation[] = [
    {
      kind: 'equity_mint',
      id: 'mint-001',
      symbol: 'tAAPL',
      quantity: '10.50',
      status: { status: 'wrapping', token: '0xa1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2' },
      startedAt: new Date(Date.now() - 120000).toISOString(),
      updatedAt: new Date(Date.now() - 30000).toISOString()
    },
    {
      kind: 'usdc_bridge',
      id: 'bridge-001',
      direction: 'alpaca_to_base',
      amount: '5000.00',
      status: { status: 'bridging', burn: '0xb2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3' },
      startedAt: new Date(Date.now() - 300000).toISOString(),
      updatedAt: new Date(Date.now() - 60000).toISOString()
    },
    {
      kind: 'equity_redemption',
      id: 'redeem-003',
      symbol: 'tNVDA',
      quantity: '15.00',
      status: { status: 'sending', raindex_withdraw: '0xc3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4' },
      startedAt: new Date(Date.now() - 90000).toISOString(),
      updatedAt: new Date(Date.now() - 15000).toISOString()
    },
    {
      kind: 'equity_mint',
      id: 'mint-008',
      symbol: 'tMETA',
      quantity: '22.375',
      status: { status: 'minting' },
      startedAt: new Date(Date.now() - 45000).toISOString(),
      updatedAt: new Date(Date.now() - 10000).toISOString()
    },
    {
      kind: 'equity_redemption',
      id: 'redeem-007',
      symbol: 'tDIS',
      quantity: '8.00',
      status: { status: 'unwrapping', raindex_withdraw: '0xd4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5', unwrap: '0xe5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6' },
      startedAt: new Date(Date.now() - 200000).toISOString(),
      updatedAt: new Date(Date.now() - 50000).toISOString()
    }
  ]

  const mockRecentTransfers: TransferOperation[] = [
    {
      kind: 'equity_mint',
      id: 'mint-002',
      symbol: 'tSPYM',
      quantity: '100.00',
      status: { status: 'completed', completed_at: new Date(Date.now() - 400000).toISOString(), token: '0x1a2b3c4d5e6f1a2b3c4d5e6f1a2b3c4d5e6f1a2b3c4d5e6f1a2b3c4d5e6f1a2b', wrap: '0x2b3c4d5e6f1a2b3c4d5e6f1a2b3c4d5e6f1a2b3c4d5e6f1a2b3c4d5e6f1a2b3c', vault_deposit: '0x3c4d5e6f1a2b3c4d5e6f1a2b3c4d5e6f1a2b3c4d5e6f1a2b3c4d5e6f1a2b3c4d' },
      startedAt: new Date(Date.now() - 700000).toISOString(),
      updatedAt: new Date(Date.now() - 400000).toISOString()
    },
    {
      kind: 'equity_redemption',
      id: 'redeem-001',
      symbol: 'tAAPL',
      quantity: '50.00',
      status: { status: 'completed', completed_at: new Date(Date.now() - 600000).toISOString(), redemption: '0x4d5e6f1a2b3c4d5e6f1a2b3c4d5e6f1a2b3c4d5e6f1a2b3c4d5e6f1a2b3c4d5e' },
      startedAt: new Date(Date.now() - 900000).toISOString(),
      updatedAt: new Date(Date.now() - 600000).toISOString()
    },
    {
      kind: 'equity_mint',
      id: 'mint-003',
      symbol: 'tMSFT',
      quantity: '25.00',
      status: { status: 'completed', completed_at: new Date(Date.now() - 800000).toISOString(), token: '0x5e6f1a2b3c4d5e6f1a2b3c4d5e6f1a2b3c4d5e6f1a2b3c4d5e6f1a2b3c4d5e6f', wrap: '0x6f1a2b3c4d5e6f1a2b3c4d5e6f1a2b3c4d5e6f1a2b3c4d5e6f1a2b3c4d5e6f1a', vault_deposit: '0x7a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b' },
      startedAt: new Date(Date.now() - 1000000).toISOString(),
      updatedAt: new Date(Date.now() - 800000).toISOString()
    },
    {
      kind: 'usdc_bridge',
      id: 'bridge-003',
      direction: 'alpaca_to_base',
      amount: '25000.00',
      status: { status: 'completed', completed_at: new Date(Date.now() - 1100000).toISOString(), burn: '0x8b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c', mint: '0x9c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d' },
      startedAt: new Date(Date.now() - 1300000).toISOString(),
      updatedAt: new Date(Date.now() - 1100000).toISOString()
    },
    {
      kind: 'usdc_bridge',
      id: 'bridge-002',
      direction: 'base_to_alpaca',
      amount: '10000.00',
      status: { status: 'failed', failed_at: new Date(Date.now() - 1200000).toISOString(), reason: 'bridge timeout', burn: '0xad4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e', mint: null },
      startedAt: new Date(Date.now() - 1500000).toISOString(),
      updatedAt: new Date(Date.now() - 1200000).toISOString()
    },
    {
      kind: 'equity_redemption',
      id: 'redeem-002',
      symbol: 'tTSLA',
      quantity: '30.00',
      status: { status: 'completed', completed_at: new Date(Date.now() - 1800000).toISOString(), redemption: '0xbe5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f' },
      startedAt: new Date(Date.now() - 2100000).toISOString(),
      updatedAt: new Date(Date.now() - 1800000).toISOString()
    },
    {
      kind: 'equity_mint',
      id: 'mint-004',
      symbol: 'tGOOG',
      quantity: '40.00',
      status: { status: 'completed', completed_at: new Date(Date.now() - 2400000).toISOString(), token: '0xcf6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a', wrap: '0xd01b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b', vault_deposit: '0xe12c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c' },
      startedAt: new Date(Date.now() - 2800000).toISOString(),
      updatedAt: new Date(Date.now() - 2400000).toISOString()
    },
    {
      kind: 'equity_redemption',
      id: 'redeem-004',
      symbol: 'tAMZN',
      quantity: '12.50',
      status: { status: 'failed', failed_at: new Date(Date.now() - 2600000).toISOString(), raindex_withdraw: '0xf23d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d', redemption: null },
      startedAt: new Date(Date.now() - 3000000).toISOString(),
      updatedAt: new Date(Date.now() - 2600000).toISOString()
    },
    {
      kind: 'usdc_bridge',
      id: 'bridge-004',
      direction: 'base_to_alpaca',
      amount: '50000.00',
      status: { status: 'completed', completed_at: new Date(Date.now() - 3200000).toISOString(), burn: '0x034e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e', mint: '0x145f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f' },
      startedAt: new Date(Date.now() - 3600000).toISOString(),
      updatedAt: new Date(Date.now() - 3200000).toISOString()
    },
    {
      kind: 'equity_mint',
      id: 'mint-005',
      symbol: 'tV',
      quantity: '67.50',
      status: { status: 'completed', completed_at: new Date(Date.now() - 3800000).toISOString(), token: '0x256a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a', wrap: '0x367b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b', vault_deposit: '0x478c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c' },
      startedAt: new Date(Date.now() - 4200000).toISOString(),
      updatedAt: new Date(Date.now() - 3800000).toISOString()
    },
    {
      kind: 'equity_mint',
      id: 'mint-006',
      symbol: 'tNFLX',
      quantity: '14.128573',
      status: { status: 'completed', completed_at: new Date(Date.now() - 4500000).toISOString(), token: '0x589d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d', wrap: '0x69ae5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e', vault_deposit: '0x7abf6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f' },
      startedAt: new Date(Date.now() - 5000000).toISOString(),
      updatedAt: new Date(Date.now() - 4500000).toISOString()
    },
    {
      kind: 'equity_redemption',
      id: 'redeem-005',
      symbol: 'tJPM',
      quantity: '20.00',
      status: { status: 'completed', completed_at: new Date(Date.now() - 5200000).toISOString(), redemption: '0x8bc01a2b3c4d5e6f1a2b3c4d5e6f1a2b3c4d5e6f1a2b3c4d5e6f1a2b3c4d5e6f' },
      startedAt: new Date(Date.now() - 5800000).toISOString(),
      updatedAt: new Date(Date.now() - 5200000).toISOString()
    },
    {
      kind: 'usdc_bridge',
      id: 'bridge-005',
      direction: 'alpaca_to_base',
      amount: '15000.00',
      status: { status: 'failed', failed_at: new Date(Date.now() - 5500000).toISOString(), reason: 'insufficient balance', burn: null, mint: null },
      startedAt: new Date(Date.now() - 6000000).toISOString(),
      updatedAt: new Date(Date.now() - 5500000).toISOString()
    },
    {
      kind: 'equity_mint',
      id: 'mint-007',
      symbol: 'tBAC',
      quantity: '500.00',
      status: { status: 'completed', completed_at: new Date(Date.now() - 6200000).toISOString(), token: '0x9cd12a3b4c5d6e7f8a9b0c1d2e3f4a5b6c7d8e9f0a1b2c3d4e5f6a7b8c9d0e1f', wrap: '0xade23b4c5d6e7f8a9b0c1d2e3f4a5b6c7d8e9f0a1b2c3d4e5f6a7b8c9d0e1f2a', vault_deposit: '0xbef34c5d6e7f8a9b0c1d2e3f4a5b6c7d8e9f0a1b2c3d4e5f6a7b8c9d0e1f2a3b' },
      startedAt: new Date(Date.now() - 6800000).toISOString(),
      updatedAt: new Date(Date.now() - 6200000).toISOString()
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
      aggregate_id: 'mint-008',
      sequence: 2,
      event_type: 'TokenizedEquityMintEvent::BuyOrderPlaced',
      timestamp: new Date(Date.now() - 10000).toISOString()
    },
    {
      aggregate_type: 'TokenizedEquityMint',
      aggregate_id: 'mint-008',
      sequence: 1,
      event_type: 'TokenizedEquityMintEvent::Created',
      timestamp: new Date(Date.now() - 12000).toISOString()
    },
    {
      aggregate_type: 'TokenizedEquityMint',
      aggregate_id: 'mint-001',
      sequence: 3,
      event_type: 'TokenizedEquityMintEvent::WrappingStarted',
      timestamp: new Date(Date.now() - 30000).toISOString()
    },
    {
      aggregate_type: 'EquityRedemption',
      aggregate_id: 'redeem-007',
      sequence: 2,
      event_type: 'EquityRedemptionEvent::UnwrappingStarted',
      timestamp: new Date(Date.now() - 50000).toISOString()
    },
    {
      aggregate_type: 'EquityRedemption',
      aggregate_id: 'redeem-007',
      sequence: 1,
      event_type: 'EquityRedemptionEvent::Created',
      timestamp: new Date(Date.now() - 55000).toISOString()
    },
    {
      aggregate_type: 'UsdcRebalance',
      aggregate_id: 'bridge-001',
      sequence: 2,
      event_type: 'UsdcRebalanceEvent::BridgingStarted',
      timestamp: new Date(Date.now() - 60000).toISOString()
    },
    {
      aggregate_type: 'UsdcRebalance',
      aggregate_id: 'bridge-001',
      sequence: 1,
      event_type: 'UsdcRebalanceEvent::Created',
      timestamp: new Date(Date.now() - 65000).toISOString()
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
      aggregate_type: 'TokenizedEquityMint',
      aggregate_id: 'mint-002',
      sequence: 4,
      event_type: 'TokenizedEquityMintEvent::WrappingCompleted',
      timestamp: new Date(Date.now() - 420000).toISOString()
    },
    {
      aggregate_type: 'EquityRedemption',
      aggregate_id: 'redeem-001',
      sequence: 5,
      event_type: 'EquityRedemptionEvent::Completed',
      timestamp: new Date(Date.now() - 600000).toISOString()
    },
    {
      aggregate_type: 'EquityRedemption',
      aggregate_id: 'redeem-001',
      sequence: 4,
      event_type: 'EquityRedemptionEvent::SellOrderFilled',
      timestamp: new Date(Date.now() - 650000).toISOString()
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
    },
    {
      aggregate_type: 'UsdcRebalance',
      aggregate_id: 'bridge-002',
      sequence: 3,
      event_type: 'UsdcRebalanceEvent::Failed',
      timestamp: new Date(Date.now() - 1200000).toISOString()
    },
    {
      aggregate_type: 'UsdcRebalance',
      aggregate_id: 'bridge-002',
      sequence: 2,
      event_type: 'UsdcRebalanceEvent::BridgingStarted',
      timestamp: new Date(Date.now() - 1250000).toISOString()
    },
    {
      aggregate_type: 'TokenizedEquityMint',
      aggregate_id: 'mint-004',
      sequence: 5,
      event_type: 'TokenizedEquityMintEvent::Completed',
      timestamp: new Date(Date.now() - 2400000).toISOString()
    },
    {
      aggregate_type: 'EquityRedemption',
      aggregate_id: 'redeem-004',
      sequence: 3,
      event_type: 'EquityRedemptionEvent::Failed',
      timestamp: new Date(Date.now() - 2600000).toISOString()
    },
    {
      aggregate_type: 'EquityRedemption',
      aggregate_id: 'redeem-004',
      sequence: 2,
      event_type: 'EquityRedemptionEvent::SendingStarted',
      timestamp: new Date(Date.now() - 2700000).toISOString()
    },
    {
      aggregate_type: 'TokenizedEquityMint',
      aggregate_id: 'mint-005',
      sequence: 4,
      event_type: 'TokenizedEquityMintEvent::Completed',
      timestamp: new Date(Date.now() - 3800000).toISOString()
    },
    {
      aggregate_type: 'TokenizedEquityMint',
      aggregate_id: 'mint-006',
      sequence: 5,
      event_type: 'TokenizedEquityMintEvent::Completed',
      timestamp: new Date(Date.now() - 4500000).toISOString()
    }
  ]

  const mockRebalancing = {
    equityOnchainRatio: '0.5',
    equityTriggerThreshold: '0.15',
    cashOnchainRatio: '0.6',
    cashTriggerThreshold: '0.1'
  }

  queryClient.setQueryData(['inventory'], mockInventory)
  queryClient.setQueryData(['transfers', 'active'], mockActiveTransfers)
  queryClient.setQueryData(['transfers', 'recent'], mockRecentTransfers)
  queryClient.setQueryData(['events'], mockEvents)
  queryClient.setQueryData(['rebalancing'], mockRebalancing)
</script>

<div class="flex h-screen flex-col bg-background">
  <HeaderBar
    connectionStatus="disconnected"
  />

  <main class="flex-1 overflow-auto p-2 md:p-4">
    <div class="grid grid-cols-1 gap-2 lg:grid-cols-[3fr_2fr] lg:gap-4">
      <InventoryPanel />
      <LiveEventsPanel />
    </div>
  </main>
</div>
