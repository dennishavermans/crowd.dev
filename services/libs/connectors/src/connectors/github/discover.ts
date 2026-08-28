import axios from 'axios'

import type { Channel, Credential } from '../../types'

import { GITHUB_REQUEST_TIMEOUT_MS, mintInstallationToken, requireInstallationId } from './appToken'

const PER_PAGE = 100

interface InstallationRepo {
  node_id: string
  html_url: string
}

export async function discoverRepos(credential: Credential): Promise<Channel[]> {
  const installationId = requireInstallationId()
  const { token } = await mintInstallationToken(credential, installationId)

  const channels: Channel[] = []
  let page = 1
  for (;;) {
    const response = await axios.get('https://api.github.com/installation/repositories', {
      params: { per_page: PER_PAGE, page },
      headers: {
        Authorization: `Bearer ${token}`,
        Accept: 'application/vnd.github+json',
      },
      timeout: GITHUB_REQUEST_TIMEOUT_MS,
    })
    const repos = (response.data?.repositories ?? []) as InstallationRepo[]
    channels.push(...repos.map((r) => ({ channelId: r.node_id, channelName: r.html_url })))
    if (repos.length < PER_PAGE) {
      return channels
    }
    page += 1
  }
}
