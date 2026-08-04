<script lang="ts">
  import './layout.css'
  import favicon from '$lib/assets/favicon.svg'
  import type { Snippet } from 'svelte'
  import { QueryClient, QueryClientProvider } from '@tanstack/svelte-query'

  import { resolvedTheme } from '$lib/theme.svelte'

  const { children }: { children: Snippet } = $props()

  const queryClient = new QueryClient()

  $effect(() => {
    document.documentElement.classList.toggle('dark', resolvedTheme.current === 'dark')
  })
</script>

<svelte:head><link rel="icon" href={favicon} /></svelte:head>

<QueryClientProvider client={queryClient}>
  {@render children()}
</QueryClientProvider>
