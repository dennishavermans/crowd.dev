import type { ResponseInterpreter } from '../../http/client'
import { RateLimitError } from '../../http/errors'

interface GithubErrorBody {
  errors?: { type?: string }[]
  message?: string
}

export const interpretGithubResponse: ResponseInterpreter = (response) => {
  const body = response.data as GithubErrorBody | null
  if (response.status === 200) {
    // GitHub docs say RATE_LIMITED, but installation tokens return RATE_LIMIT
    if (body?.errors?.some((e) => e.type === 'RATE_LIMITED' || e.type === 'RATE_LIMIT')) {
      return new RateLimitError('github graphql rate limited')
    }
    return null
  }
  if (response.status === 403 && body?.message?.toLowerCase().includes('secondary rate limit')) {
    return new RateLimitError('github secondary rate limited')
  }
  return null
}
