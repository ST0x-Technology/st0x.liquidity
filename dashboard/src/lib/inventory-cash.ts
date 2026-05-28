import type { UsdcInventory } from '$lib/api/UsdcInventory'
import { decimalAdd, decimalCompare, decimalSub } from '$lib/decimal'

export type CashInventoryAmounts = {
  alpacaUsd: string
  alpacaUsdc: string | null
  counterTradeUsd: string
  rebalanceableUsd: string | null
  reserve: string | null
  trackedTotal: string
  venueRatio: number
}

const venueRatio = (onchain: string, offchain: string): number => {
  const onchainValue = parseFloat(onchain)
  const offchainValue = parseFloat(offchain)
  const total = onchainValue + offchainValue

  if (total === 0) return 0

  return onchainValue / total
}

const nonNegativeSub = (left: string, right: string): string => {
  const difference = decimalSub(left, right)

  return decimalCompare(difference, '0') < 0 ? '0' : difference
}

export const cashInventoryAmounts = (
  usdc: UsdcInventory,
  configuredReserve: string | null = null
): CashInventoryAmounts => {
  const alpacaUsd = usdc.offchainGross ?? usdc.offchainAvailable
  const reserve =
    configuredReserve ??
    (usdc.offchainGross === null
      ? null
      : decimalSub(usdc.offchainGross, usdc.offchainAvailable))
  const rebalanceableUsd =
    usdc.withdrawableCash === null
      ? null
      : nonNegativeSub(usdc.withdrawableCash, reserve ?? '0')
  const inflight = decimalAdd(usdc.onchainInflight, usdc.offchainInflight)
  const trackedTotal = decimalAdd(decimalAdd(usdc.onchainAvailable, alpacaUsd), inflight)

  return {
    alpacaUsd,
    alpacaUsdc: usdc.alpacaUsdc,
    counterTradeUsd: alpacaUsd,
    rebalanceableUsd,
    reserve,
    trackedTotal,
    venueRatio: venueRatio(usdc.onchainAvailable, alpacaUsd)
  }
}
