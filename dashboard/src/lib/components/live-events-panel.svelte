<script lang="ts">
  import { browser } from '$app/environment'
  import { createQuery } from '@tanstack/svelte-query'
  import * as Card from '$lib/components/ui/card'
  import * as Table from '$lib/components/ui/table'
  import type { EventStoreEntry } from '$lib/api/EventStoreEntry'

  // This query doesn't fetch - it's used as a reactive state store. The WebSocket
  // connection populates it via queryClient.setQueryData(['events'], ...) in
  // websocket.svelte.ts. staleTime: Infinity prevents refetching.
  const eventsQuery = createQuery<EventStoreEntry[]>(() => ({
    queryKey: ['events'],
    queryFn: () => Promise.resolve([]),
    staleTime: Infinity
  }))

  const formatTimestamp = (timestamp: string): string => {
    const date = new Date(timestamp)
    const locale = browser ? navigator.language : 'en-US'
    return date.toLocaleTimeString(locale, {
      hour12: false,
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit'
    })
  }

  const truncateId = (id: string, maxLen: number = 16): string => {
    if (id.length <= maxLen) return id
    const visibleChars = maxLen - 3
    const left = Math.ceil(visibleChars / 2)
    const right = Math.floor(visibleChars / 2)
    return `${id.slice(0, left)}...${id.slice(-right)}`
  }

  const events = $derived(eventsQuery.data ?? [])
</script>

<Card.Root class="flex h-full min-h-56 flex-col overflow-hidden">
  <Card.Header class="shrink-0 pb-0">
    <Card.Title class="flex items-center justify-between">
      <span>Live Events</span>
      <span class="text-sm font-normal text-muted-foreground">
        {events.length} new
      </span>
    </Card.Title>
  </Card.Header>
  <Card.Content class="relative min-h-0 flex-1 overflow-auto px-6 pt-0">
    {#if events.length === 0}
      <div class="flex h-full items-center justify-center text-muted-foreground">
        No events yet
      </div>
    {:else}
      <Table.Root>
        <Table.Header>
          <Table.Row>
            <Table.Head class="w-20">Time</Table.Head>
            <Table.Head>Event</Table.Head>
            <Table.Head>ID</Table.Head>
            <Table.Head class="w-12 text-right">#</Table.Head>
          </Table.Row>
        </Table.Header>
        <Table.Body>
          {#each events as event (event.aggregate_id + '-' + String(event.sequence))}
            <Table.Row>
              <Table.Cell class="font-mono text-xs text-muted-foreground">
                {formatTimestamp(event.timestamp)}
              </Table.Cell>
              <Table.Cell class="font-mono text-xs">
                {event.event_type}
              </Table.Cell>
              <Table.Cell class="font-mono text-xs" title={event.aggregate_id}>
                {truncateId(event.aggregate_id)}
              </Table.Cell>
              <Table.Cell class="text-right font-mono text-xs">
                {event.sequence}
              </Table.Cell>
            </Table.Row>
          {/each}
        </Table.Body>
      </Table.Root>
      <div
        class="pointer-events-none sticky bottom-0 h-8 bg-gradient-to-t from-card to-transparent"
      ></div>
    {/if}
  </Card.Content>
</Card.Root>
