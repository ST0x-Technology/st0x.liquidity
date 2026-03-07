<script lang="ts">
  import { createQuery } from '@tanstack/svelte-query'
  import * as Card from '$lib/components/ui/card'
  import * as Table from '$lib/components/ui/table'
  import type { EventStoreEntry } from '$lib/api/EventStoreEntry'
  import { truncateId } from './live-events-panel'

  // This query doesn't fetch - it's used as a reactive state store. The WebSocket
  // connection populates it via queryClient.setQueryData(['events'], ...) in
  // websocket.svelte.ts. staleTime: Infinity prevents refetching.
  const eventsQuery = createQuery<EventStoreEntry[]>(() => ({
    queryKey: ['events'],
    queryFn: () => Promise.resolve([]),
    staleTime: Infinity
  }))

  const events = $derived(eventsQuery.data ?? [])

  const parseEventType = (eventType: string): { type: string; event: string } => {
    const idx = eventType.indexOf('::')
    if (idx === -1) return { type: eventType, event: '' }
    return { type: eventType.slice(0, idx), event: eventType.slice(idx + 2) }
  }

  type SortDir = 'asc' | 'desc'
  type EventCol = 'aggregate' | 'type' | 'sequence' | 'event'
  type SortState = { column: EventCol; dir: SortDir } | null

  let eventSort = $state<SortState>(null)

  const toggleSort = (current: SortState, column: EventCol): SortState => {
    if (current?.column === column) {
      return current.dir === 'asc' ? { column, dir: 'desc' } : null
    }
    return { column, dir: 'asc' }
  }

  const sortIndicator = (state: SortState, column: EventCol): string => {
    if (state?.column !== column) return ''
    return state.dir === 'asc' ? ' \u25B2' : ' \u25BC'
  }

  const sortedEvents = $derived.by(() => {
    if (!eventSort) return events
    const sorted = [...events]
    const { column, dir } = eventSort
    sorted.sort((left, right) => {
      let cmp = 0
      if (column === 'aggregate') cmp = left.aggregate_id.localeCompare(right.aggregate_id)
      else if (column === 'type') cmp = parseEventType(left.event_type).type.localeCompare(parseEventType(right.event_type).type)
      else if (column === 'sequence') cmp = left.sequence - right.sequence
      else cmp = parseEventType(left.event_type).event.localeCompare(parseEventType(right.event_type).event)
      return dir === 'desc' ? -cmp : cmp
    })
    return sorted
  })
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
            <Table.Head class="cursor-pointer select-none" onclick={() => eventSort = toggleSort(eventSort, 'aggregate')}>Aggregate{sortIndicator(eventSort, 'aggregate')}</Table.Head>
            <Table.Head class="cursor-pointer select-none" onclick={() => eventSort = toggleSort(eventSort, 'type')}>Type{sortIndicator(eventSort, 'type')}</Table.Head>
            <Table.Head class="w-12 cursor-pointer select-none" onclick={() => eventSort = toggleSort(eventSort, 'sequence')}>No.{sortIndicator(eventSort, 'sequence')}</Table.Head>
            <Table.Head class="cursor-pointer select-none" onclick={() => eventSort = toggleSort(eventSort, 'event')}>Event{sortIndicator(eventSort, 'event')}</Table.Head>
          </Table.Row>
        </Table.Header>
        <Table.Body>
          {#each sortedEvents as event (event.aggregate_id + '-' + String(event.sequence))}
            {@const parsed = parseEventType(event.event_type)}
            <Table.Row>
              <Table.Cell class="font-mono text-xs" title={event.aggregate_id}>
                {truncateId(event.aggregate_id)}
              </Table.Cell>
              <Table.Cell class="font-mono text-xs opacity-90">
                {parsed.type}
              </Table.Cell>
              <Table.Cell class="font-mono text-xs opacity-90">
                {event.sequence}
              </Table.Cell>
              <Table.Cell class="font-mono text-xs">
                {parsed.event}
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
