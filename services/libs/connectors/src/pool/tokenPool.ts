import type { RedisClient } from '@crowd/redis'

import type { IPooledToken } from '../http/client'
import { ProviderAuthError, RateLimitError } from '../http/errors'

interface ITokenState {
  value: string
  parkedUntil?: string
  quarantined?: boolean
}

export interface TokenPool {
  acquire(): Promise<IPooledToken>
  park(tokenId: string, resumeAt: Date): Promise<void>
  quarantine(tokenId: string): Promise<void>
  seed(tokenId: string, value: string): Promise<void>
  earliestResumeAt(): Promise<Date | null>
}

export function createTokenPool(
  redis: RedisClient,
  platform: string,
  connectionId: string,
): TokenPool {
  const tokensKey = `connectors:pool:${platform}:${connectionId}:tokens`
  const lruKey = `connectors:pool:${platform}:${connectionId}:lru`

  async function readStates(): Promise<Map<string, ITokenState>> {
    const raw = await redis.hGetAll(tokensKey)
    const states = new Map<string, ITokenState>()
    for (const [id, json] of Object.entries(raw)) {
      states.set(id, JSON.parse(json) as ITokenState)
    }
    return states
  }

  function isHealthy(state: ITokenState, nowMs: number): boolean {
    if (state.quarantined) {
      return false
    }
    if (state.parkedUntil && new Date(state.parkedUntil).getTime() > nowMs) {
      return false
    }
    return true
  }

  function earliestParkedUntil(states: Map<string, ITokenState>, nowMs: number): Date | null {
    let earliest: Date | null = null
    for (const state of states.values()) {
      if (state.quarantined || !state.parkedUntil) {
        continue
      }
      const parkedUntil = new Date(state.parkedUntil)
      if (parkedUntil.getTime() <= nowMs) {
        continue
      }
      if (!earliest || parkedUntil < earliest) {
        earliest = parkedUntil
      }
    }
    return earliest
  }

  // POC only: read-modify-write can lose a concurrent park/quarantine on the same token
  // within a ~ms window; accepted tradeoff — fix with atomic writes when productizing.
  async function updateState(tokenId: string, update: Partial<ITokenState>): Promise<void> {
    const json = await redis.hGet(tokensKey, tokenId)
    if (!json) {
      return
    }
    const state = JSON.parse(json) as ITokenState
    await redis.hSet(tokensKey, tokenId, JSON.stringify({ ...state, ...update }))
  }

  return {
    async acquire(): Promise<IPooledToken> {
      const nowMs = Date.now()
      const states = await readStates()
      const ordered = await redis.zRange(lruKey, 0, -1)
      for (const id of ordered) {
        const state = states.get(id)
        if (state && isHealthy(state, nowMs)) {
          await redis.zAdd(lruKey, { score: nowMs, value: id })
          return { id, value: state.value }
        }
      }
      const resumeAt = earliestParkedUntil(states, nowMs)
      if (resumeAt) {
        throw new RateLimitError('token pool exhausted', { resumeAt })
      }
      throw new ProviderAuthError('token pool empty')
    },

    async park(tokenId: string, resumeAt: Date): Promise<void> {
      await updateState(tokenId, { parkedUntil: resumeAt.toISOString() })
    },

    // POC only: quarantined tokens are kept for inspection and never revived automatically
    async quarantine(tokenId: string): Promise<void> {
      await updateState(tokenId, { quarantined: true })
    },

    async seed(tokenId: string, value: string): Promise<void> {
      const json = await redis.hGet(tokensKey, tokenId)
      const state = json ? (JSON.parse(json) as ITokenState) : {}
      await redis.hSet(tokensKey, tokenId, JSON.stringify({ ...state, value }))
      await redis.zAdd(lruKey, { score: 0, value: tokenId }, { NX: true })
    },

    async earliestResumeAt(): Promise<Date | null> {
      const states = await readStates()
      return earliestParkedUntil(states, Date.now())
    },
  }
}
