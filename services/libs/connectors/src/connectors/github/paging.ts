export interface GithubWatermark {
  phase: 'backfill' | 'incremental'
  since: string | null
  cursor: string | null
}

export const MAX_PAGES_PER_RUN = Number.POSITIVE_INFINITY
export const PAGE_SIZE = 100

export function readWatermark(raw: Record<string, unknown> | null): GithubWatermark {
  if (raw && (raw.phase === 'backfill' || raw.phase === 'incremental')) {
    return {
      phase: raw.phase,
      since: typeof raw.since === 'string' ? raw.since : null,
      cursor: typeof raw.cursor === 'string' ? raw.cursor : null,
    }
  }
  return { phase: 'backfill', since: null, cursor: null }
}

export function parseRepoChannel(channelName: string): { owner: string; repo: string } {
  const path = channelName.replace(/^https:\/\/github\.com\//, '').replace(/\/+$/, '')
  const parts = path.split('/')
  if (parts.length !== 2 || !parts[0] || !parts[1]) {
    throw new Error(`invalid github repo channel name: ${channelName}`)
  }
  return { owner: parts[0], repo: parts[1] }
}
