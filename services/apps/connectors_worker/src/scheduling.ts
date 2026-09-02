const CADENCE_JITTER_RATIO = 0.1
const SHORT_DEFER_MIN_MS = 30_000
const SHORT_DEFER_JITTER_MS = 60_000

export function cadenceRunAt(cadenceMinutes: number): Date {
  const jitterMinutes = cadenceMinutes * CADENCE_JITTER_RATIO * (Math.random() * 2 - 1)
  return new Date(Date.now() + (cadenceMinutes + jitterMinutes) * 60_000)
}

export function shortDeferRunAt(): Date {
  return new Date(Date.now() + SHORT_DEFER_MIN_MS + Math.random() * SHORT_DEFER_JITTER_MS)
}
