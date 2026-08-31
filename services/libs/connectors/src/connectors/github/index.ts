import type { Manifest } from '../../types'

import { seedGithubTokens } from './appToken'
import { probeGithubBudget } from './budget'
import { discoverRepos } from './discover'
import { interpretGithubResponse } from './interpret'
import { forksSync } from './syncs/forks'
import { issueCommentsSync } from './syncs/issueComments'
import { issuesSync } from './syncs/issues'
import { pullRequestCommentsSync } from './syncs/pullRequestComments'
import { pullRequestCommitsSync } from './syncs/pullRequestCommits'
import { pullRequestReviewCommentsSync } from './syncs/pullRequestReviewComments'
import { pullRequestsSync } from './syncs/pullRequests'

export const githubConnector: Manifest = {
  platform: 'github',
  syncs: [
    forksSync,
    issuesSync,
    issueCommentsSync,
    pullRequestsSync,
    pullRequestCommentsSync,
    pullRequestReviewCommentsSync,
    pullRequestCommitsSync,
  ],
  discover: discoverRepos,
  seedTokens: seedGithubTokens,
  probeBudget: probeGithubBudget,
  interpretResponse: interpretGithubResponse,
}
