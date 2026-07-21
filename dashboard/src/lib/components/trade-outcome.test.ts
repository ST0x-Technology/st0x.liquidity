import { render } from 'svelte/server'
import { describe, expect, it } from 'vitest'
import TradeOutcome from './trade-outcome.svelte'

describe('TradeOutcome', () => {
  it('renders a filled trade without failure details', () => {
    const { body } = render(TradeOutcome, {
      props: { outcome: { status: 'filled' } }
    })

    expect(body).toContain('Filled')
    expect(body).toContain('text-green-500')
    expect(body).not.toContain('Unfilled')
  })

  it('renders a failed counter-trade error and partial-fill quantities', () => {
    const { body } = render(TradeOutcome, {
      props: {
        outcome: {
          status: 'failed',
          error: 'broker rejected remainder',
          acceptedShares: '1',
          filledShares: '0.25',
          remainingShares: '0.75',
          excessShares: '0'
        }
      }
    })

    expect(body).toContain('Failed')
    expect(body).toContain('broker rejected remainder')
    expect(body).toContain('Accepted 1')
    expect(body).toContain('Filled 0.25')
    expect(body).toContain('Unfilled 0.75')
  })

  it('does not render a nonzero partial fill as zero', () => {
    const { body } = render(TradeOutcome, {
      props: {
        outcome: {
          status: 'failed',
          error: 'broker rejected remainder',
          acceptedShares: '1',
          filledShares: '0.0004',
          remainingShares: '0.9996',
          excessShares: '0'
        }
      }
    })

    expect(body).toContain('Filled 0.0004')
    expect(body).not.toContain('Filled 0 ·')
  })

  it('renders broker fills beyond the accepted order quantity', () => {
    const { body } = render(TradeOutcome, {
      props: {
        outcome: {
          status: 'failed',
          error: 'broker overfilled before rejecting',
          acceptedShares: '0.5',
          filledShares: '1',
          remainingShares: '0',
          excessShares: '0.5'
        }
      }
    })

    expect(body).toContain('Filled 1')
    expect(body).toContain('Unfilled 0')
    expect(body).toContain('Excess fill 0.5')
  })

  it('renders missing provenance as unknown instead of zero', () => {
    const { body } = render(TradeOutcome, {
      props: {
        outcome: {
          status: 'failed',
          error: 'placement failed before acceptance',
          acceptedShares: null,
          filledShares: null,
          remainingShares: null,
          excessShares: null
        }
      }
    })

    expect(body).toContain('Accepted unknown')
    expect(body).toContain('Filled unknown')
    expect(body).not.toContain('Unfilled 0')
  })

  it('renders a partially-filled cancellation distinctly', () => {
    const { body } = render(TradeOutcome, {
      props: {
        outcome: {
          status: 'cancelled',
          acceptedShares: '1',
          filledShares: '0.25',
          remainingShares: '0.75',
          excessShares: '0'
        }
      }
    })

    expect(body).toContain('Cancelled')
    expect(body).toContain('text-amber-500')
    expect(body).toContain('Filled 0.25')
    expect(body).toContain('Unfilled 0.75')
  })

  it('renders an explicit zero-fill cancellation as wholly unfilled', () => {
    const { body } = render(TradeOutcome, {
      props: {
        outcome: {
          status: 'cancelled',
          acceptedShares: '1',
          filledShares: '0',
          remainingShares: '1',
          excessShares: '0'
        }
      }
    })

    expect(body).toContain('Filled 0')
    expect(body).toContain('Unfilled 1')
    expect(body).not.toContain('Filled unknown')
  })
})
