import axios from 'axios'
import * as jwt from 'jsonwebtoken'

import type { TokenPool } from '../../pool/tokenPool'
import type { Credential } from '../../types'

const GITHUB_API_VERSION = '2022-11-28'

export async function mintInstallationToken(
  credential: Credential,
  installationId: string,
): Promise<{ token: string; expiresAt: string }> {
  const now = Math.floor(Date.now() / 1000)
  const appJwt = jwt.sign(
    { iat: now - 60, exp: now + 600, iss: credential.data.appId },
    credential.data.privateKey,
    { algorithm: 'RS256' },
  )

  // POC only: minting cannot go through ConnectorHttp — it feeds the pool the client draws from
  const response = await axios.post(
    `https://api.github.com/app/installations/${installationId}/access_tokens`,
    {},
    {
      headers: {
        Authorization: `Bearer ${appJwt}`,
        Accept: 'application/vnd.github+json',
        'X-GitHub-Api-Version': GITHUB_API_VERSION,
      },
    },
  )

  return { token: response.data.token, expiresAt: response.data.expires_at }
}

export function requireInstallationId(): string {
  const installationId = process.env.CROWD_GITHUB_INSTALLATION_ID
  if (!installationId) {
    throw new Error('missing CROWD_GITHUB_INSTALLATION_ID environment variable')
  }
  return installationId
}

export async function seedGithubTokens(credential: Credential, pool: TokenPool): Promise<void> {
  const installationId = requireInstallationId()
  const { token } = await mintInstallationToken(credential, installationId)
  await pool.seed(`install-${installationId}`, token)
}
