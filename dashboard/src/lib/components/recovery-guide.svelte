<script lang="ts">
  import { getSimulateSourceId } from '$lib/env'
  import { RECOVERY_GUIDE, recoveryModeColor, recoveryModeLabel } from '$lib/transfer'

  let dialogEl: HTMLDialogElement | undefined = $state()

  const open = () => dialogEl?.showModal()

  // A simulation build runs the mock CLI against a /tmp harness config, so the
  // guide's hardcoded `stox` commands are not runnable as written. Steer the
  // operator to the per-object modal commands, which carry the right prefix.
  const isSimulation = getSimulateSourceId() !== null
</script>

<button
  class="rounded border bg-background px-2 py-1 text-xs hover:bg-accent"
  title="CLI recovery guide"
  onclick={open}
>
  CLI recovery guide
</button>

<dialog
  bind:this={dialogEl}
  class="w-full max-w-2xl rounded-lg border bg-card p-0 text-foreground shadow-lg backdrop:bg-black/50"
  onclick={(event) => {
    if (event.target === dialogEl) dialogEl.close()
  }}
>
  <div class="flex items-center justify-between border-b px-5 py-3">
    <div class="text-sm font-semibold">CLI recovery guide</div>
    <button
      class="text-lg leading-none text-muted-foreground hover:text-foreground"
      onclick={() => dialogEl?.close()}
    >
      &times;
    </button>
  </div>

  <div class="max-h-[70vh] overflow-y-auto px-5 py-4">
    {#if isSimulation}
      <div
        class="mb-4 rounded-md border border-amber-500/40 bg-amber-500/10 px-3 py-2 text-xs text-amber-500"
      >
        <span class="font-semibold">Simulation build.</span> The
        <span class="font-mono">stox</span> commands below are not runnable here -- this build drives
        the mock CLI against a temporary harness config. Use the per-object recovery commands in each
        transfer/trade modal instead; they carry the correct prefix.
      </div>
    {/if}

    <p class="mb-4 text-xs text-muted-foreground">
      Every recovery command, grouped by object. The <span class="font-mono">stox</span> prefix is the
      production wrapper (run on the server via Tailscale ssh). Each command notes whether it mutates
      the local CQRS state directly or dispatches through the running bot.
    </p>

    <div class="space-y-5">
      {#each RECOVERY_GUIDE as group (group.object)}
        <section>
          <h3 class="mb-2 font-mono text-sm font-semibold capitalize text-foreground">
            {group.object}
          </h3>

          <div class="space-y-3">
            {#each group.commands as entry (entry.command)}
              <div class="rounded-md border bg-muted/20 px-3 py-2">
                <pre
                  class="overflow-x-auto whitespace-pre-wrap break-all rounded bg-background/80 p-2 font-mono text-[11px] text-foreground">{entry.command}</pre>

                <div class="mt-1.5 grid grid-cols-[auto_1fr] gap-x-3 gap-y-1 text-[11px]">
                  <span class="text-muted-foreground">What</span>
                  <span>{entry.description}</span>
                  <span class="text-muted-foreground">When</span>
                  <span>{entry.whenToUse}</span>
                  <span class="text-muted-foreground">Applies to</span>
                  <span>{entry.appliesTo}</span>
                  <span class="text-muted-foreground">Mode</span>
                  <span class={recoveryModeColor(entry.mode).text}>
                    {recoveryModeLabel(entry.mode)}
                  </span>
                </div>
              </div>
            {/each}
          </div>
        </section>
      {/each}
    </div>
  </div>
</dialog>
