<script lang="ts">
  import './layout.css'
  import favicon from '$lib/assets/favicon.svg'
  import type { Snippet } from 'svelte'
  import { QueryClient, QueryClientProvider } from '@tanstack/svelte-query'

  const { children }: { children: Snippet } = $props()

  const queryClient = new QueryClient()
</script>

<svelte:head><link rel="icon" href={favicon} /></svelte:head>

<QueryClientProvider client={queryClient}>
  {@render children()}
</QueryClientProvider>
