import axios from 'axios'
import * as jwt from 'jsonwebtoken'

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

export async function mintInstallationToken(
  credential: Credential,
  installationId: string,
): Promise<{ token: string; expiresAt: string }> {
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
}

// TODO(CM-1372): POC-only resolution; store the installation id per integration after the POC
export async function resolveInstallationId(credential: Credential): Promise<string> {
  const fromEnv = process.env.CROWD_GITHUB_INSTALLATION_ID
  if (fromEnv) {
    return fromEnv
  }

  const response = await axios.get('https://api.github.com/app/installations', {
    headers: {
      Authorization: `Bearer ${mintAppJwt(credential)}`,
      Accept: 'application/vnd.github+json',
      'X-GitHub-Api-Version': GITHUB_API_VERSION,
    },
    timeout: GITHUB_REQUEST_TIMEOUT_MS,
  })

  const installations = response.data as { id: number }[]
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
