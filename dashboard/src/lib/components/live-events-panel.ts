/**
 * Truncates a string ID with ellipsis in the middle.
 * Returns original if maxLen < 5 (minimum for "X...Y" format).
 */
export const truncateId = (id: string, maxLen: number = 16): string => {
  if (maxLen < 5) return id
  if (id.length <= maxLen) return id

  const visibleChars = maxLen - 3
  const left = Math.ceil(visibleChars / 2)
  const right = Math.floor(visibleChars / 2)

  return `${id.slice(0, left)}...${id.slice(-right)}`
}
