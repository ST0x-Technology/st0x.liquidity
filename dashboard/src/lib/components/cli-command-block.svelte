<script lang="ts">
  import { onDestroy } from 'svelte'
  import { reactive } from '$lib/frp.svelte'
  import { recoveryModeColor, recoveryModeLabel, type RecoveryCommand } from '$lib/transfer'

  type Props = {
    entry: RecoveryCommand
  }

  const { entry }: Props = $props()

  const copied = reactive(false)
  let resetTimer: ReturnType<typeof setTimeout> | undefined

  // Clear the copy-feedback timer if the block unmounts before it fires, so it
  // never calls reactive.update() on a destroyed component.
  onDestroy(() => clearTimeout(resetTimer))

  const copy = async () => {
    try {
      await navigator.clipboard.writeText(entry.command)
      copied.update(() => true)
      clearTimeout(resetTimer)
      resetTimer = setTimeout(() => {
        copied.update(() => false)
      }, 1500)
    } catch (error) {
      console.error('Failed to copy command to clipboard', error)
    }
  }

  const modeClass = $derived(recoveryModeColor(entry.mode).badge)
</script>

<div class="rounded-md border bg-muted/20 px-3 py-2">
  <div class="flex items-center justify-between gap-2">
    <span class="text-xs font-medium text-foreground">{entry.label}</span>
    <span class="rounded border px-1.5 py-0.5 text-[10px] font-medium {modeClass}">
      {recoveryModeLabel(entry.mode)}
    </span>
  </div>

  <div class="mt-1 text-[11px] text-muted-foreground">{entry.description}</div>

  <div class="mt-1.5 flex items-start gap-2">
    <pre
      class="min-w-0 flex-1 overflow-x-auto whitespace-pre-wrap break-all rounded bg-background/80 p-2 font-mono text-[11px] text-foreground">{entry.command}</pre>
    <button
      class="shrink-0 rounded border bg-background px-2 py-1 text-[11px] hover:bg-accent"
      title="Copy command"
      onclick={copy}
    >
      {copied.current ? 'Copied ✓' : 'Copy'}
    </button>
  </div>
</div>
