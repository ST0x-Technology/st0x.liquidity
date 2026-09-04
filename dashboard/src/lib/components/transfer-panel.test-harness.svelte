<script lang="ts">
  import { QueryClientProvider, type QueryClient } from '@tanstack/svelte-query'
  import TransferPanel from './transfer-panel.svelte'

  let { client }: { client: QueryClient } = $props()
</script>

<QueryClientProvider {client}>
  <TransferPanel />
</QueryClientProvider>
