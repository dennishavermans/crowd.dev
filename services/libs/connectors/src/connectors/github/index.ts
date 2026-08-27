import type { Manifest } from '../../types'

import { seedGithubTokens } from './appToken'
import { probeGithubBudget } from './budget'
import { discoverRepos } from './discover'
import { interpretGithubResponse } from './interpret'

export const githubConnector: Manifest = {
  platform: 'github',
  syncs: [],
  discover: discoverRepos,
  seedTokens: seedGithubTokens,
  probeBudget: probeGithubBudget,
  interpretResponse: interpretGithubResponse,
}
