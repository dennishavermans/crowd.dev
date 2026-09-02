import axios from 'axios'
import * as jwt from 'jsonwebtoken'

import {
  ConnectorError,
  ProviderAuthError,
  ProviderContractError,
  ProviderUnavailableError,
} from '../../http/errors'
import type { TokenPool } from '../../pool/tokenPool'
import type { Credential } from '../../types'

const GITHUB_API_VERSION = '2022-11-28'

export const GITHUB_REQUEST_TIMEOUT_MS = 30_000

function mintAppJwt(credential: Credential): string {
  const now = Math.floor(Date.now() / 1000)
  return jwt.sign(
    { iat: now - 60, exp: now + 600, iss: credential.data.appId },
    credential.data.privateKey,
    { algorithm: 'RS256' },
  )
}

function classifyAppApiError(err: unknown, operation: string): ConnectorError | null {
  if (!axios.isAxiosError(err)) {
    return null
  }
  const status = err.response?.status
  const body = err.response?.data as { message?: string } | undefined
  const detail = body?.message ?? err.message
  const options = { status, cause: err }
  if (status === 401) {
    return new ProviderAuthError(`github app ${operation} unauthorized: ${detail}`, options)
  }
  if (status === undefined || status >= 500) {
    return new ProviderUnavailableError(`github app ${operation} failed: ${detail}`, options)
  }
  return new ProviderContractError(
    `github app ${operation} returned status ${status}: ${detail}`,
    options,
  )
}

export async function mintInstallationToken(
  credential: Credential,
  installationId: string,
): Promise<{ token: string; expiresAt: string }> {
  try {
    // POC only: minting cannot go through ConnectorHttp — it feeds the pool the client draws from
    const response = await axios.post(
      `https://api.github.com/app/installations/${installationId}/access_tokens`,
      {},
      {
        headers: {
          Authorization: `Bearer ${mintAppJwt(credential)}`,
          Accept: 'application/vnd.github+json',
          'X-GitHub-Api-Version': GITHUB_API_VERSION,
        },
        timeout: GITHUB_REQUEST_TIMEOUT_MS,
      },
    )
    return { token: response.data.token, expiresAt: response.data.expires_at }
  } catch (err) {
    throw classifyAppApiError(err, 'installation token minting') ?? err
  }
}

// TODO(CM-1372): POC-only resolution; store the installation id per integration after the POC
export async function resolveInstallationId(credential: Credential): Promise<string> {
  const fromEnv = process.env.CROWD_GITHUB_INSTALLATION_ID
  if (fromEnv) {
    return fromEnv
  }

  let installations: { id: number }[]
  try {
    const response = await axios.get('https://api.github.com/app/installations', {
      headers: {
        Authorization: `Bearer ${mintAppJwt(credential)}`,
        Accept: 'application/vnd.github+json',
        'X-GitHub-Api-Version': GITHUB_API_VERSION,
      },
      timeout: GITHUB_REQUEST_TIMEOUT_MS,
    })
    installations = response.data as { id: number }[]
  } catch (err) {
    throw classifyAppApiError(err, 'installation resolution') ?? err
  }
  if (installations.length === 0) {
    throw new Error('github app has no installations')
  }
  return String(installations[0].id)
}

export async function seedGithubTokens(credential: Credential, pool: TokenPool): Promise<void> {
  const installationId = await resolveInstallationId(credential)
  const { token } = await mintInstallationToken(credential, installationId)
  await pool.seed(`install-${installationId}`, token)
}
