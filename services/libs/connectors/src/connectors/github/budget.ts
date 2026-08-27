import axios from 'axios'

import type { BudgetProbe } from '../../pool/tokenPool'

interface RateLimitResource {
  limit: number
  remaining: number
  reset: number
}

export const probeGithubBudget: BudgetProbe = async (_platform, _connectionId, token) => {
  try {
    const response = await axios.get('https://api.github.com/rate_limit', {
      headers: {
        Authorization: `Bearer ${token.value}`,
        Accept: 'application/vnd.github+json',
      },
    })
    const graphql = response.data?.resources?.graphql as RateLimitResource | undefined
    if (!graphql) {
      return null
    }
    return {
      limit: graphql.limit,
      remaining: graphql.remaining,
      resetAt: new Date(graphql.reset * 1000),
    }
  } catch {
    return null
  }
}
