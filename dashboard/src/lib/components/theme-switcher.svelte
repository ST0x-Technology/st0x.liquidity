<script lang="ts">
  import Sun from '@lucide/svelte/icons/sun'
  import Moon from '@lucide/svelte/icons/moon'
  import SunMoon from '@lucide/svelte/icons/sun-moon'

  import { Button } from '$lib/components/ui/button'
  import { themeMode, type ThemeMode } from '$lib/theme.svelte'

  const labels: Record<ThemeMode, string> = {
    light: 'Theme: light',
    dark: 'Theme: dark',
    system: 'Theme: system'
  }

  const label = $derived(labels[themeMode.current])
</script>

<Button
  variant="ghost"
  size="icon-sm"
  title={label}
  aria-label={label}
  onclick={() => {
    themeMode.cycle()
  }}
>
  {#if themeMode.current === 'light'}
    <Sun />
  {:else if themeMode.current === 'dark'}
    <Moon />
  {:else}
    <SunMoon />
  {/if}
</Button>
