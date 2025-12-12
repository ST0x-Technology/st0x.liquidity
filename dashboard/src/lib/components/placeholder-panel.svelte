<script lang="ts">
  import * as Card from '$lib/components/ui/card'

  type Props = {
    title: string
  }

  const { title }: Props = $props()
</script>

<Card.Root class="h-full">
  <Card.Header>
    <Card.Title>{title}</Card.Title>
  </Card.Header>
  <Card.Content class="flex items-center justify-center text-muted-foreground">
    Coming soon...
  </Card.Content>
</Card.Root>
