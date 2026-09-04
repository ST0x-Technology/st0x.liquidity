<script lang="ts">
  import { SESSION_FILTERS, sessionFilterLabel, type SessionFilter } from '$lib/session-filter'

  let {
    selected,
    onchange
  }: {
    selected: SessionFilter
    onchange: (filter: SessionFilter) => void
  } = $props()
</script>

<div class="flex items-center gap-1" role="group" aria-label="Session filter">
  {#each SESSION_FILTERS as filter (filter)}
    <button
      class="rounded border px-1.5 py-0.5 text-xs {selected === filter
        ? 'border-primary bg-primary/10 text-foreground'
        : 'bg-background text-muted-foreground hover:bg-accent'}"
      aria-pressed={selected === filter}
      onclick={() => {
        onchange(filter)
      }}
    >
      {sessionFilterLabel(filter)}
    </button>
  {/each}
</div>
