<script lang="ts">
	/* eslint-disable @typescript-eslint/no-confusing-void-expression -- svelte-eslint-parser
	   represents `{#if cond}{@render Snippet()}{/if}` as a void expression inside a conditional;
	   this fires on both conditional TooltipLabel() renders below and isn't fixable without
	   removing the conditional placement they need. */
	import { cn, type WithElementRef, type WithoutChildren } from "$lib/utils.js";
	import type { HTMLAttributes } from "svelte/elements";
	import { getPayloadConfigFromPayload, useChart, type TooltipPayload } from "./chart-utils.js";
	import { getChartContext, Tooltip as TooltipPrimitive } from "layerchart";
	import type { Snippet } from "svelte";

	const defaultFormatter = (value: unknown, _payload: TooltipPayload[]) => String(value);

	let {
		ref = $bindable(null),
		class: className,
		hideLabel = false,
		indicator = "dot",
		hideIndicator = false,
		labelKey,
		label,
		labelFormatter = defaultFormatter,
		labelClassName,
		formatter,
		nameKey,
		color,
		excludeZero = false,
		...restProps
	}: WithoutChildren<WithElementRef<HTMLAttributes<HTMLDivElement>>> & {
		hideLabel?: boolean;
		label?: string;
		indicator?: "line" | "dot" | "dashed";
		nameKey?: string;
		labelKey?: string;
		hideIndicator?: boolean;
		labelClassName?: string;
		labelFormatter?:
			| ((value: unknown, payload: TooltipPayload[]) => string | number | Snippet)
			| null;
		formatter?: Snippet<
			[
				{
					value: unknown;
					name: string;
					item: TooltipPayload;
					index: number;
					payload: TooltipPayload[];
				},
			]
		>;
		// Opt-in: exclude zero-valued series from the tooltip. Only meaningful for stacked
		// stage-breakdown bar charts (waterfall/rebalance/equity) where every series is always
		// present per row and 0 means "this stage did not occur", not "no data" -- without this,
		// every zero-valued stage still renders an empty tooltip row. Must default to false so
		// single-series LineCharts (block-lag, latency percentiles, CCTP attestation) where 0 is a
		// legitimate, common value keep rendering their tooltip instead of going blank.
		excludeZero?: boolean;
	} = $props();

	const chart = useChart();
	const chartCtx = getChartContext();

	// Defined-ness matters for item-based charts like Pie/Arc where only the hovered item has
	// a value.
	const visibleSeries = $derived(
		chartCtx.tooltip.series.filter(
			(s: TooltipPayload) => s.value !== undefined && (!excludeZero || s.value !== 0)
		)
	);

	const formattedLabel = $derived.by((): string | number | Snippet | null => {
		if (hideLabel || visibleSeries.length === 0) return null;

		const item = visibleSeries[0];
		if (!item) return null;

		const tooltipData: unknown = chartCtx.tooltip.data;

		// Get the x-axis label value from the raw tooltip data (e.g. a Date or month string)
		const dataLabel: unknown = tooltipData != null ? chartCtx.x(tooltipData) : undefined;

		const key = labelKey ?? item.label;
		const itemConfig = getPayloadConfigFromPayload(
			chart.config,
			item,
			key,
			tooltipData as Record<string, unknown> | null
		);

		let value: unknown;
		if (!labelKey && typeof label === "string") {
			value = chart.config[label]?.label ?? label;
		} else if (labelKey) {
			value = itemConfig?.label ?? dataLabel;
		} else {
			value = dataLabel;
		}

		if (value === undefined) return null;
		if (!labelFormatter) return value as string | number;
		return labelFormatter(value, visibleSeries);
	});

	const nestLabel = $derived(visibleSeries.length === 1 && indicator !== "dot");
</script>

{#snippet TooltipLabel()}
	{#if formattedLabel}
		<div class={cn("font-medium", labelClassName)}>
			{#if typeof formattedLabel === "function"}
				{@render formattedLabel()}
			{:else}
				{formattedLabel}
			{/if}
		</div>
	{/if}
{/snippet}

<TooltipPrimitive.Root variant="none">
	<div
		bind:this={ref}
		class={cn(
			"border-border/50 bg-background grid min-w-[9rem] items-start gap-1.5 rounded-lg border px-2.5 py-1.5 text-xs shadow-xl",
			className
		)}
		{...restProps}
	>
		{#if !nestLabel}
			{@render TooltipLabel()}
		{/if}
		<div class="grid gap-1.5">
			{#each visibleSeries as item, i (item.key)}
				{@const key = nameKey || item.key || item.label || "value"}
				{@const itemConfig = getPayloadConfigFromPayload(
					chart.config,
					item,
					key,
					chartCtx.tooltip.data as Record<string, unknown> | null
				)}
				{@const indicatorColor = color || item.config.color || item.color}
				<div
					class={cn(
						"[&>svg]:text-muted-foreground flex w-full flex-wrap items-stretch gap-2 [&>svg]:size-2.5",
						indicator === "dot" && "items-center"
					)}
				>
					{#if formatter && item.value !== undefined && item.label}
						{@render formatter({
							value: item.value,
							name: item.label,
							item,
							index: i,
							payload: visibleSeries,
						})}
					{:else}
						{#if itemConfig?.icon}
							<itemConfig.icon />
						{:else if !hideIndicator}
							<div
								style="--color-bg: {indicatorColor}; --color-border: {indicatorColor};"
								class={cn(
									"shrink-0 rounded-[2px] border-(--color-border) bg-(--color-bg)",
									{
										"size-2.5": indicator === "dot",
										"h-full w-1": indicator === "line",
										"w-0 border-[1.5px] border-dashed bg-transparent":
											indicator === "dashed",
										"my-0.5": nestLabel && indicator === "dashed",
									}
								)}
							></div>
						{/if}
						<div
							class={cn(
								"flex flex-1 shrink-0 justify-between leading-none",
								nestLabel ? "items-end" : "items-center"
							)}
						>
							<div class="grid gap-1.5">
								{#if nestLabel}
									<!-- eslint-disable-next-line @typescript-eslint/no-confusing-void-expression -- svelte-eslint-parser represents {@render} inside {#if} as a value-position void call; not fixable without restructuring the snippet -->
									{@render TooltipLabel()}
								{/if}
								<span class="text-muted-foreground">
									{itemConfig?.label || item.label}
								</span>
							</div>
							{#if item.value !== undefined}
								<span class="text-foreground font-mono font-medium tabular-nums">
									{(item.value as number | string).toLocaleString()}
								</span>
							{/if}
						</div>
					{/if}
				</div>
			{/each}
		</div>
	</div>
</TooltipPrimitive.Root>
