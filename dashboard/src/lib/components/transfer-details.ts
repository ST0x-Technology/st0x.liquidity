import type { CompletedStage } from '$lib/api/CompletedStage'
import type { TransferOperation } from '$lib/api/TransferOperation'
import { matcher } from '$lib/fp'

export type TxLink = { label: string; hash: string; url: string }
export type StageEntry = CompletedStage

const BASE_EXPLORER = 'https://basescan.org/tx'
const ETH_EXPLORER = 'https://etherscan.io/tx'

const matchKind = matcher<TransferOperation>()('kind')

const baseTxLink = (label: string, hash: string): TxLink => ({
  label,
  hash,
  url: `${BASE_EXPLORER}/${hash}`
})

export const failureReason = (transfer: TransferOperation): string | null =>
  matchKind(transfer, {
    equity_mint: (op) =>
      op.status.status === 'failed' ? op.status.reason : null,
    equity_redemption: () => null,
    usdc_bridge: (op) =>
      op.status.status === 'failed' ? op.status.reason : null
  })

export const txLinks = (transfer: TransferOperation): TxLink[] =>
  matchKind(transfer, {
    equity_mint: (op) => equityMintLinks(op.status),
    equity_redemption: (op) => equityRedemptionLinks(op.status),
    usdc_bridge: (op) => usdcBridgeLinks(op.status, op.direction)
  })

type EquityMintStatus =
  (TransferOperation & { kind: 'equity_mint' }) extends { status: infer S } ? S : never

type EquityRedemptionStatus =
  (TransferOperation & { kind: 'equity_redemption' }) extends { status: infer S } ? S : never

type UsdcBridgeStatus =
  (TransferOperation & { kind: 'usdc_bridge' }) extends { status: infer S } ? S : never

type UsdcDirection =
  (TransferOperation & { kind: 'usdc_bridge' }) extends { direction: infer D } ? D : never

const equityMintLinks = (status: EquityMintStatus): TxLink[] => {
  switch (status.status) {
    case 'minting':
      return []
    case 'wrapping':
      return [baseTxLink('token', status.token)]
    case 'depositing':
      return [baseTxLink('token', status.token), baseTxLink('wrap', status.wrap)]
    case 'completed':
      return [
        baseTxLink('token', status.token),
        baseTxLink('wrap', status.wrap),
        baseTxLink('deposit', status.vault_deposit)
      ]
    case 'failed':
      return []
  }
}

const equityRedemptionLinks = (status: EquityRedemptionStatus): TxLink[] => {
  switch (status.status) {
    case 'withdrawing':
      return [baseTxLink('withdraw', status.raindex_withdraw)]
    case 'unwrapping':
      return [
        baseTxLink('withdraw', status.raindex_withdraw),
        baseTxLink('unwrap', status.unwrap)
      ]
    case 'sending':
      return [baseTxLink('withdraw', status.raindex_withdraw)]
    case 'pending_confirmation':
      return [baseTxLink('redeem', status.redemption)]
    case 'completed':
      return [baseTxLink('redeem', status.redemption)]
    case 'failed': {
      const links: TxLink[] = []
      if (status.raindex_withdraw) links.push(baseTxLink('withdraw', status.raindex_withdraw))
      if (status.redemption) links.push(baseTxLink('redeem', status.redemption))
      return links
    }
  }
}

export const completedStages = (transfer: TransferOperation): StageEntry[] =>
  matchKind(transfer, {
    equity_mint: (op) => op.completedStages,
    equity_redemption: (op) => op.completedStages,
    usdc_bridge: (op) => op.completedStages
  })

const usdcBridgeLinks = (status: UsdcBridgeStatus, direction: UsdcDirection): TxLink[] => {
  const burnExplorer = direction === 'alpaca_to_base' ? ETH_EXPLORER : BASE_EXPLORER
  const mintExplorer = direction === 'alpaca_to_base' ? BASE_EXPLORER : ETH_EXPLORER

  const burnLink = (hash: string): TxLink => ({ label: 'burn', hash, url: `${burnExplorer}/${hash}` })
  const mintLink = (hash: string): TxLink => ({ label: 'mint', hash, url: `${mintExplorer}/${hash}` })

  switch (status.status) {
    case 'converting':
    case 'withdrawing':
      return []
    case 'bridging':
      return [burnLink(status.burn)]
    case 'depositing':
      return [burnLink(status.burn), mintLink(status.mint)]
    case 'completed':
      return [burnLink(status.burn), mintLink(status.mint)]
    case 'failed': {
      const links: TxLink[] = []
      if (status.burn) links.push(burnLink(status.burn))
      if (status.mint) links.push(mintLink(status.mint))
      return links
    }
  }
}
