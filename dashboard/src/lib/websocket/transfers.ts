import type { QueryClient } from '@tanstack/svelte-query'
import type { CurrentState } from '$lib/api/CurrentState'
import type { TransferOperation } from '$lib/api/TransferOperation'
import type { TransferWarning } from '$lib/api/TransferWarning'

const MAX_RECENT = 100

export const transferKey = (transfer: TransferOperation): string =>
  `${transfer.kind}:${transfer.id}`

export const seedTransfers = (queryClient: QueryClient, state: CurrentState) => {
  queryClient.setQueryData(['transfers', 'active'], state.activeTransfers)
  queryClient.setQueryData(['transfers', 'recent'], state.recentTransfers)
  queryClient.setQueryData<TransferWarning[]>(['transfers', 'warnings'], state.warnings)
}

export const upsertTransfer = (queryClient: QueryClient, transfer: TransferOperation) => {
  const key = transferKey(transfer)

  queryClient.setQueryData<TransferOperation[]>(['transfers', 'active'], (old) => {
    const existing = old ?? []
    const index = existing.findIndex((item) => transferKey(item) === key)

    if (transfer.status.status === 'completed' || transfer.status.status === 'failed') {
      const filtered = index >= 0 ? existing.filter((_, idx) => idx !== index) : existing

      queryClient.setQueryData<TransferOperation[]>(
        ['transfers', 'recent'],
        (recent) => [transfer, ...(recent ?? []).filter((item) => transferKey(item) !== key)].slice(0, MAX_RECENT)
      )

      return filtered
    }

    if (index >= 0) {
      return existing.map((item, idx) => (idx === index ? transfer : item))
    }

    return [transfer, ...existing]
  })
}
