import type { SyncContext, SyncDefinition } from '../../../types'
import { githubGraphql } from '../gql'
import type {
  PrCommentNode,
  PrCommentsBatchPage,
  PrCommentsConnection,
} from '../graphql/pullRequestChildren'
import { COMMENTS_FOR_PRS_QUERY } from '../graphql/pullRequestChildren'
import type { PullRequestNode } from '../graphql/pullRequests'
import { toPrComment } from '../mappers/prComment'
import { runDualPhasePrSync } from '../prWalk'
import { githubActivitySchema } from '../schemas'

const PR_BATCH_SIZE = 50
const COMMENTS_PAGE_SIZE = 50

async function runPullRequestCommentsSync(ctx: SyncContext): Promise<void> {
  const emitComments = async (
    connection: PrCommentsConnection,
    pullRequest: PullRequestNode,
    sinceDate: Date | null,
  ): Promise<void> => {
    const comments = connection.edges
      .map((edge) => edge?.node)
      .filter((node): node is PrCommentNode => Boolean(node?.id))
      .filter((node) => !sinceDate || new Date(node.createdAt) >= sinceDate)
    if (comments.length > 0) {
      await ctx.emit(comments.map((comment) => toPrComment(comment, pullRequest)))
    }
  }

  const drainRemainingComments = async (
    prId: string,
    pullRequest: PullRequestNode,
    startCursor: string | null,
    sinceDate: Date | null,
  ): Promise<void> => {
    let cursor = startCursor
    let hasMore = true
    while (hasMore) {
      const data = await githubGraphql<PrCommentsBatchPage>(ctx.http, COMMENTS_FOR_PRS_QUERY, {
        ids: [prId],
        first: COMMENTS_PAGE_SIZE,
        after: cursor,
      })
      const comments = data.nodes[0]?.comments
      if (!comments?.edges) {
        return
      }
      await emitComments(comments, pullRequest, sinceDate)
      hasMore = comments.pageInfo.hasNextPage
      cursor = comments.pageInfo.endCursor
    }
  }

  await runDualPhasePrSync(ctx, async (prs, sinceDate) => {
    for (let i = 0; i < prs.length; i += PR_BATCH_SIZE) {
      const batch = prs.slice(i, i + PR_BATCH_SIZE)
      const batchData = await githubGraphql<PrCommentsBatchPage>(ctx.http, COMMENTS_FOR_PRS_QUERY, {
        ids: batch.map((pr) => pr.id),
        first: COMMENTS_PAGE_SIZE,
        after: null,
      })

      for (const node of batchData.nodes) {
        if (!node?.comments?.edges) {
          continue
        }
        const pullRequest = batch.find((candidate) => candidate.id === node.id)
        if (!pullRequest) {
          continue
        }
        await emitComments(node.comments, pullRequest, sinceDate)
        if (node.comments.pageInfo.hasNextPage) {
          await drainRemainingComments(
            node.id,
            pullRequest,
            node.comments.pageInfo.endCursor,
            sinceDate,
          )
        }
      }
    }
  })
}

export const pullRequestCommentsSync: SyncDefinition = {
  name: 'pull-request-comments',
  cadenceMinutes: 60,
  schema: githubActivitySchema,
  run: runPullRequestCommentsSync,
}
