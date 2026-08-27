import type { ResponseInterpreter } from '../../http/client'
import { RateLimitError } from '../../http/errors'

interface GraphqlErrorEnvelope {
  errors?: { type?: string }[]
}

export const interpretGithubResponse: ResponseInterpreter = (response) => {
  if (response.status !== 200) {
    return null
  }
  const body = response.data as GraphqlErrorEnvelope | null
  if (body?.errors?.some((e) => e.type === 'RATE_LIMITED')) {
    return new RateLimitError('github graphql rate limited')
  }
  return null
}
