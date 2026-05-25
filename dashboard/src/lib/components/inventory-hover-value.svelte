<script lang="ts">
  import HoverTooltip from '$lib/components/hover-tooltip.svelte'

  interface Props {
    display: string
    tooltip: string
    class?: string
  }

  let { display, tooltip, class: className = '' }: Props = $props()
</script>

<HoverTooltip {tooltip}>
  <span class={className}>{display}</span>
</HoverTooltip>
