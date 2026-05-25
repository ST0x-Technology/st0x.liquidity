<script lang="ts">
  import { tick, type Snippet } from 'svelte'

  interface Props {
    tooltip: string
    class?: string
    children?: Snippet
  }

  let { tooltip, class: className = '', children }: Props = $props()
  let triggerEl: HTMLButtonElement | undefined = $state()
  let tooltipEl: HTMLSpanElement | undefined = $state()
  let open = $state(false)
  let tooltipLeft = $state(0)
  let tooltipTop = $state(0)

  const tooltipClass =
    'pointer-events-none fixed z-50 w-max max-w-72 whitespace-pre-line rounded border border-border bg-background px-2 py-1 text-xs font-normal text-foreground opacity-100 shadow-lg'

  const portal = (node: HTMLElement) => {
    document.body.appendChild(node)

    return {
      destroy() {
        node.remove()
      }
    }
  }

  const positionTooltip = () => {
    if (!triggerEl) return

    const rect = triggerEl.getBoundingClientRect()
    const tooltipHeight = tooltipEl?.offsetHeight ?? 0
    const width = tooltipEl?.offsetWidth ?? 288
    const left = Math.min(Math.max(rect.left, 8), window.innerWidth - width - 8)
    const below = rect.bottom + 6
    const top =
      below + tooltipHeight > window.innerHeight ? Math.max(rect.top - tooltipHeight - 6, 8) : below

    tooltipLeft = left
    tooltipTop = top
  }

  const showTooltip = async () => {
    open = true
    await tick()
    positionTooltip()
  }

  const hideTooltip = () => {
    open = false
  }

  // `position: fixed` keeps the tooltip in viewport coordinates, so it would
  // drift from its trigger if the page scrolls or resizes while open.
  const onViewportChange = () => {
    if (open) positionTooltip()
  }
</script>

<svelte:window onscroll={onViewportChange} onresize={onViewportChange} />

<button
  bind:this={triggerEl}
  type="button"
  class="inline-flex appearance-none border-0 bg-transparent p-0 text-inherit {className}"
  aria-label={tooltip}
  onmouseenter={showTooltip}
  onmouseleave={hideTooltip}
  onfocus={showTooltip}
  onblur={hideTooltip}
>
  {@render children?.()}
</button>

{#if open}
  <span
    bind:this={tooltipEl}
    use:portal
    class={tooltipClass}
    style:left={`${String(tooltipLeft)}px`}
    style:top={`${String(tooltipTop)}px`}
  >
    {tooltip}
  </span>
{/if}
