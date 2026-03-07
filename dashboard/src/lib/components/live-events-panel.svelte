<script lang="ts">
  import { createQuery } from '@tanstack/svelte-query'
  import * as Card from '$lib/components/ui/card'
  import * as Table from '$lib/components/ui/table'
  import type { EventStoreEntry } from '$lib/api/EventStoreEntry'
  import { truncateId } from './live-events-panel'
  import { reactive } from '$lib/frp.svelte'

  const eventsQuery = createQuery<EventStoreEntry[]>(() => ({
    queryKey: ['events'],
    enabled: false
  }))

  const events = $derived(eventsQuery.data ?? [])

  const EVENT_SUFFIX = 'Event'

  const stripEventSuffix = (name: string): string =>
    name.endsWith(EVENT_SUFFIX) ? name.slice(0, -EVENT_SUFFIX.length) : name

  const parseEventType = (eventType: string): { type: string; event: string } => {
    const idx = eventType.indexOf('::')
    if (idx === -1) return { type: '', event: stripEventSuffix(eventType) }
    return { type: stripEventSuffix(eventType.slice(0, idx)), event: eventType.slice(idx + 2) }
  }

  type SortDir = 'asc' | 'desc'
  type EventCol = 'aggregate' | 'type' | 'sequence' | 'event'
  type SortState = { column: EventCol; dir: SortDir } | null

  const eventSort = reactive<SortState>(null)

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

  const sortBtnClass = [
    'w-full',
    'cursor-pointer',
    'select-none',
    'text-left',
    'focus-visible:outline-none',
    'focus-visible:ring-1',
    'focus-visible:ring-ring'
  ].join(' ')
  const sortEvent = (col: EventCol) => () => { eventSort.update((current) => toggleSort(current, col)) }

  const ariaSort = (
    state: SortState,
    column: EventCol
  ): 'ascending' | 'descending' | 'none' => {
    if (state?.column !== column) return 'none'
    return state.dir === 'asc' ? 'ascending' : 'descending'
  }

  type EventComparator = (lhs: EventStoreEntry, rhs: EventStoreEntry) => number

  const eventComparators: Record<EventCol, EventComparator> = {
    aggregate: (lhs, rhs) =>
      lhs.aggregate_id.localeCompare(rhs.aggregate_id),
    type: (lhs, rhs) =>
      parseEventType(lhs.event_type).type
        .localeCompare(parseEventType(rhs.event_type).type),
    sequence: (lhs, rhs) =>
      lhs.sequence - rhs.sequence,
    event: (lhs, rhs) =>
      parseEventType(lhs.event_type).event
        .localeCompare(parseEventType(rhs.event_type).event)
  }

  const sortedEvents = $derived.by(() => {
    if (!eventSort.current) return events
    const { column, dir } = eventSort.current
    const cmp = eventComparators[column]
    return [...events].sort((lhs, rhs) =>
      dir === 'desc' ? -cmp(lhs, rhs) : cmp(lhs, rhs)
    )
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
            <Table.Head aria-sort={ariaSort(eventSort.current, 'aggregate')}>
              <button class={sortBtnClass} onclick={sortEvent('aggregate')}>
                Aggregate{sortIndicator(eventSort.current, 'aggregate')}
              </button>
            </Table.Head>

            <Table.Head aria-sort={ariaSort(eventSort.current, 'type')}>
              <button class={sortBtnClass} onclick={sortEvent('type')}>
                Type{sortIndicator(eventSort.current, 'type')}
              </button>
            </Table.Head>

            <Table.Head class="w-12" aria-sort={ariaSort(eventSort.current, 'sequence')}>
              <button class={sortBtnClass} onclick={sortEvent('sequence')}>
                No.{sortIndicator(eventSort.current, 'sequence')}
              </button>
            </Table.Head>

            <Table.Head aria-sort={ariaSort(eventSort.current, 'event')}>
              <button class={sortBtnClass} onclick={sortEvent('event')}>
                Event{sortIndicator(eventSort.current, 'event')}
              </button>
            </Table.Head>
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
