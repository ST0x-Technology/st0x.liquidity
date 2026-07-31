<script lang="ts">
  import { QueryClientProvider, type QueryClient } from '@tanstack/svelte-query'
  import TradeHistoryPanel from './trade-history-panel.svelte'

  let { client }: { client: QueryClient } = $props()
</script>

<QueryClientProvider {client}>
  <TradeHistoryPanel />
</QueryClientProvider>
