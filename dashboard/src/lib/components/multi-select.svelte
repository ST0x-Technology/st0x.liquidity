<script lang="ts">
  type Option = { value: string; label: string }

  interface Props {
    label: string
    options: Option[]
    selected: Set<string>
    onchange: (selected: Set<string>) => void
  }

  let { label, options, selected, onchange }: Props = $props()

  let open = $state(false)

  const toggle = (value: string) => {
    const next = new Set(selected)
    if (next.has(value)) {
      next.delete(value)
    } else {
      next.add(value)
    }
    onchange(next)
  }

  const handleClickOutside = (event: MouseEvent) => {
    const target = event.target as HTMLElement
    if (!target.closest('.multi-select-root')) {
      open = false
    }
  }

  $effect(() => {
    if (!open) return

    document.addEventListener('click', handleClickOutside, true)
    return () => { document.removeEventListener('click', handleClickOutside, true) }
  })

  const activeCount = $derived(selected.size)

  const summary = $derived(
    activeCount === 0 || activeCount === options.length
      ? label
      : `${label} (${String(activeCount)})`
  )
</script>

<div class="multi-select-root relative inline-block">
  <button
    class="inline-flex items-center gap-1 rounded border bg-background px-2 py-1 text-xs hover:bg-accent"
    onclick={() => { open = !open }}
  >
    <span>{summary}</span>
    <span class="text-[0.6rem] text-muted-foreground">{open ? '\u25B2' : '\u25BC'}</span>
  </button>

  {#if open}
    <div class="absolute left-0 top-full z-20 mt-1 min-w-36 rounded border bg-background py-1 shadow-lg">
      {#each options as opt (opt.value)}
        <button
          class="flex w-full cursor-pointer items-center gap-2 px-3 py-1.5 text-left text-xs hover:bg-accent"
          onclick={() => toggle(opt.value)}
        >
          <span class="inline-flex h-3.5 w-3.5 shrink-0 items-center justify-center rounded-sm border {selected.has(opt.value) ? 'bg-primary border-primary text-primary-foreground' : 'border-muted-foreground'}">
            {#if selected.has(opt.value)}
              <span class="text-[0.5rem]">&#10003;</span>
            {/if}
          </span>
          <span>{opt.label}</span>
        </button>
      {/each}
    </div>
  {/if}
</div>
