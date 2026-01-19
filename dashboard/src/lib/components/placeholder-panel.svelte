<script lang="ts">
  import * as Card from '$lib/components/ui/card'

  type Props = {
    title: string
  }

  const { title }: Props = $props()
</script>

<Card.Root class="flex h-full min-h-56 flex-col">
  <Card.Header class="pb-0">
    <Card.Title>{title}</Card.Title>
  </Card.Header>
  <Card.Content class="flex flex-1 items-center justify-center text-muted-foreground">
    Coming soon...
  </Card.Content>
</Card.Root>
