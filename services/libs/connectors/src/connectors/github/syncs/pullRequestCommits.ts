import type { SyncContext, SyncDefinition } from '../../../types'
import { githubGraphql } from '../gql'
import type { PrCommitNode, PrCommitsPage } from '../graphql/pullRequestChildren'
import { PR_COMMITS_QUERY } from '../graphql/pullRequestChildren'
import { toCommit } from '../mappers/commit'
import { parseRepoChannel } from '../paging'
import { runDualPhasePrSync } from '../prWalk'
import { githubActivitySchema } from '../schemas'

const COMMITS_PAGE_SIZE = 50

async function runPullRequestCommitsSync(ctx: SyncContext): Promise<void> {
  const { owner, repo } = parseRepoChannel(ctx.channel.channelName)

  await runDualPhasePrSync(ctx, async (prs, sinceDate) => {
    for (const pullRequest of prs) {
      let cursor: string | null = null
      let hasMore = true

      while (hasMore) {
        const data = await githubGraphql<PrCommitsPage>(ctx.http, PR_COMMITS_QUERY, {
          owner,
          repo,
          prNumber: pullRequest.number,
          first: COMMITS_PAGE_SIZE,
          cursor,
        })

        const commits = data.repository.pullRequest?.commits
        if (!commits) {
          break
        }

        let reachedSince = false
        const fresh: PrCommitNode['commit'][] = []
        for (const node of commits.nodes) {
          if (!node?.commit) {
            continue
          }
          if (sinceDate && new Date(node.commit.authoredDate) < sinceDate) {
            reachedSince = true
            break
          }
          fresh.push(node.commit)
        }

        if (fresh.length > 0) {
          await ctx.emit(fresh.map((commit) => toCommit(commit, pullRequest.id)))
        }

        hasMore = !reachedSince && commits.pageInfo.hasNextPage
        cursor = commits.pageInfo.endCursor
      }
    }
  })
}

export const pullRequestCommitsSync: SyncDefinition = {
  name: 'pull-request-commits',
  cadenceMinutes: 120,
  schema: githubActivitySchema,
  run: runPullRequestCommitsSync,
}
