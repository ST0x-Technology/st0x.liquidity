import { describe, expect, it, vi } from 'vitest'

// The log tables' two-step load (observability/liquidity-panels/
// board-progressive.js), against a stand-in for the dashboard scene: a
// panel's query runner with its state, setState, runQueries and
// subscribeToState.

type RunnerState = { queries: unknown[]; maxDataPoints: number; data?: { state: string } }
type BoardProgressive = {
  loadInFull: (scene: unknown, panels: Record<string, number>) => void
}

// A plain script in the board, an ES module through its export line here.
// Loaded through a glob so svelte-check does not type-check the script.
const scriptPath = '../../../observability/liquidity-panels/board-progressive.js'
const loadScript = import.meta.glob('../../../observability/liquidity-panels/board-progressive.js')[
  scriptPath
]
if (!loadScript) throw new Error(`no ${scriptPath}`)
const { loadInFull } = (await loadScript()) as BoardProgressive

const board = (dataState?: string) => {
  const listeners: (() => void)[] = []
  const runner = {
    state: {
      queries: [{ refId: 'bot-trades' }],
      maxDataPoints: 100,
      ...(dataState === undefined ? {} : { data: { state: dataState } })
    } as RunnerState,
    setState: (update: Partial<RunnerState>) => {
      runner.state = { ...runner.state, ...update }
      listeners.forEach((listener) => {
        listener()
      })
    },
    runQueries: vi.fn(),
    subscribeToState: (listener: () => void) => {
      listeners.push(listener)
      return {
        unsubscribe: () => {
          listeners.splice(listeners.indexOf(listener), 1)
        }
      }
    }
  }
  // A transformer over the runner, as Grafana nests them.
  const panel = { state: { title: 'Trades', $data: { state: { $data: runner } } } }
  const scene = {
    state: { body: { state: { grid: { state: { children: [{ state: { body: panel } }] } } } } }
  }
  return { runner, scene, listeners }
}

describe('board board-progressive.js', () => {
  it('asks for the whole history once the first screen of entries is in', () => {
    const { runner, scene, listeners } = board('Loading')
    loadInFull(scene, { Trades: 500 })
    expect(runner.runQueries).not.toHaveBeenCalled()

    runner.setState({ data: { state: 'Done' } })

    expect(runner.state.maxDataPoints).toBe(500)
    expect(runner.runQueries).toHaveBeenCalledTimes(1)
    // It stops listening, so the whole history's own answer reruns nothing.
    expect(listeners).toHaveLength(0)
  })

  it('widens at once when the first entries are already in, and only once per runner', () => {
    const { runner, scene } = board('Done')
    loadInFull(scene, { Trades: 500 })
    loadInFull(scene, { Trades: 500 })

    expect(runner.state.maxDataPoints).toBe(500)
    expect(runner.runQueries).toHaveBeenCalledTimes(1)
  })

  it('does nothing without the dashboard scene or the panel', () => {
    expect(() => {
      loadInFull(undefined, { Trades: 500 })
    }).not.toThrow()
    const { runner, scene } = board('Done')
    loadInFull(scene, { Rebalances: 500 })
    expect(runner.runQueries).not.toHaveBeenCalled()
  })
})
