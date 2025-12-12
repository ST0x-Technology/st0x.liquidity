export type ServerMessage =
  | { type: 'initial'; data: InitialState }
  | { type: 'event'; data: EventStoreEntry }
  | { type: 'trade:onchain'; data: OnchainTrade }
  | { type: 'trade:offchain'; data: OffchainTrade }
  | { type: 'position:updated'; data: Position }
  | { type: 'inventory:updated'; data: Inventory }
  | { type: 'metrics:updated'; data: PerformanceMetrics }
  | { type: 'spread:updated'; data: SpreadUpdate }
  | { type: 'rebalance:updated'; data: RebalanceOperation }
  | { type: 'circuit_breaker:changed'; data: CircuitBreakerStatus }
  | { type: 'auth:status'; data: AuthStatus }

export type InitialState = {
  recentTrades: Trade[]
  inventory: Inventory
  metrics: PerformanceMetrics
  spreads: SpreadSummary[]
  activeRebalances: RebalanceOperation[]
  recentRebalances: RebalanceOperation[]
  authStatus: AuthStatus
  circuitBreaker: CircuitBreakerStatus
}

export type EventStoreEntry = {
  aggregate_type: string
  aggregate_id: string
  sequence: number
  event_type: string
  timestamp: string
}

export type Direction = 'buy' | 'sell'

export type OnchainTrade = {
  txHash: string
  logIndex: number
  symbol: string
  amount: number
  direction: Direction
  priceUsdc: number
  timestamp: string
}

export type OffchainTrade = {
  id: number
  symbol: string
  shares: number
  direction: Direction
  orderId: string | null
  priceCents: number | null
  status: 'pending' | 'completed' | 'failed'
  executedAt: string | null
}

export type Trade =
  | { type: 'onchain'; data: OnchainTrade }
  | { type: 'offchain'; data: OffchainTrade }

export type Position =
  | { status: 'empty'; symbol: string }
  | { status: 'active'; symbol: string; net: number; pendingExecutionId?: string }

export type SymbolInventory = {
  symbol: string
  onchain: number
  offchain: number
  net: number
}

export type Inventory = {
  perSymbol: SymbolInventory[]
  usdc: { onchain: number; offchain: number }
}

export type Timeframe = '1h' | '1d' | '1w' | '1m' | 'all'

export type TimeframeMetrics = {
  aum: number
  pnl: { absolute: number; percent: number }
  volume: number
  tradeCount: number
  sharpeRatio: number | null
  sortinoRatio: number | null
  maxDrawdown: number
  hedgeLagMs: number | null
  uptimePercent: number
}

export type PerformanceMetrics = {
  [K in Timeframe]: TimeframeMetrics
}

export type SpreadSummary = {
  symbol: string
  lastBuyPrice: number
  lastSellPrice: number
  pythPrice: number
  spreadBps: number
  updatedAt: string
}

export type SpreadUpdate = {
  symbol: string
  timestamp: string
  buyPrice?: number
  sellPrice?: number
  pythPrice: number
}

export type RebalanceStatus =
  | { status: 'in_progress'; startedAt: string }
  | { status: 'completed'; startedAt: string; completedAt: string }
  | { status: 'failed'; startedAt: string; failedAt: string; reason: string }

export type RebalanceOperation = (
  | { type: 'mint'; id: string; symbol: string; amount: number }
  | { type: 'redeem'; id: string; symbol: string; amount: number }
  | {
      type: 'usdc'
      id: string
      direction: 'alpaca_to_base' | 'base_to_alpaca'
      amount: number
    }
) &
  RebalanceStatus

export type CircuitBreakerStatus =
  | { status: 'active' }
  | { status: 'tripped'; reason: string; trippedAt: string }

export type AuthStatus =
  | { status: 'valid'; expiresAt: string }
  | { status: 'expiring_soon'; expiresAt: string }
  | { status: 'expired' }
  | { status: 'not_configured' }
