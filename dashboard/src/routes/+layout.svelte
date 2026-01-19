<script lang="ts">
  import './layout.css'
  import favicon from '$lib/assets/favicon.svg'
  import type { Snippet } from 'svelte'

  const { children }: { children: Snippet } = $props()
</script>

<svelte:head><link rel="icon" href={favicon} /></svelte:head>

{@render children()}
