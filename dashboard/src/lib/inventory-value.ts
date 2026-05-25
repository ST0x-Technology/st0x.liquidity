import Decimal from 'decimal.js'

const toDecimal = (value: string): Decimal => new Decimal(value !== '' ? value : '0')

const withCommas = (value: string): string => value.replace(/\B(?=(\d{3})+(?!\d))/g, ',')

const formatUsd = (value: Decimal): string => {
  const abs = value.abs().toFixed(2)
  const [intPart = '0', fracPart = '00'] = abs.split('.')
  const prefix = value.isNegative() ? '-' : ''

  return `${prefix}$${withCommas(intPart)}.${fracPart}`
}

const trimTrailingZeros = (value: string): string => {
  if (!value.includes('.')) return value

  return value.replace(/0+$/, '').replace(/\.$/, '')
}

export const cashUsdTooltip = (amount: string): string =>
  `Current USD value: ${formatUsd(toDecimal(amount))}`

export const equityUsdTooltip = (shares: string, priceUsdc: string | null): string => {
  if (priceUsdc === null) {
    return 'Current USD value unavailable: missing latest price'
  }

  return `Current USD value: ${formatUsd(toDecimal(shares).mul(toDecimal(priceUsdc)))}`
}

export const positionSharesTooltip = (shares: string): string =>
  `Stock amount: ${trimTrailingZeros(toDecimal(shares).toFixed(18))} shares`
