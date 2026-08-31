import type { SyncContext, SyncDefinition } from '../../../types'
import { githubGraphql } from '../gql'
import type {
  ReviewThreadNode,
  ReviewThreadsBatchPage,
  ThreadCommentNode,
  ThreadCommentsBatchPage,
} from '../graphql/pullRequestChildren'
import {
  COMMENTS_FOR_THREADS_QUERY,
  REVIEW_THREADS_FOR_PRS_QUERY,
} from '../graphql/pullRequestChildren'
import type { PullRequestNode } from '../graphql/pullRequests'
import { toReviewThreadComment } from '../mappers/reviewThreadComment'
import { runDualPhasePrSync } from '../prWalk'
import { githubActivitySchema } from '../schemas'

const PR_BATCH_SIZE = 50
const THREADS_PAGE_SIZE = 50
const THREAD_BATCH_SIZE = 100
const COMMENTS_PAGE_SIZE = 50

function collectThreadIds(
  prByThreadId: Map<string, PullRequestNode>,
  edges: ({ node: ReviewThreadNode | null } | null)[],
  pullRequest: PullRequestNode,
): void {
  for (const edge of edges) {
    if (edge?.node?.id) {
      prByThreadId.set(edge.node.id, pullRequest)
    }
  }
}

async function drainRemainingThreads(
  ctx: SyncContext,
  prByThreadId: Map<string, PullRequestNode>,
  pullRequest: PullRequestNode,
  startCursor: string | null,
): Promise<void> {
  let cursor = startCursor
  let hasMore = true
  while (hasMore) {
    const data = await githubGraphql<ReviewThreadsBatchPage>(
      ctx.http,
      REVIEW_THREADS_FOR_PRS_QUERY,
      { ids: [pullRequest.id], first: THREADS_PAGE_SIZE, after: cursor },
    )
    const reviewThreads = data.nodes[0]?.reviewThreads
    if (!reviewThreads?.edges) {
      return
    }
    collectThreadIds(prByThreadId, reviewThreads.edges, pullRequest)
    hasMore = reviewThreads.pageInfo.hasNextPage
    cursor = reviewThreads.pageInfo.endCursor
  }
}

async function drainRemainingThreadComments(
  ctx: SyncContext,
  threadId: string,
  startCursor: string | null,
): Promise<ThreadCommentNode[]> {
  const comments: ThreadCommentNode[] = []
  let cursor = startCursor
  let hasMore = true
  while (hasMore) {
    const data = await githubGraphql<ThreadCommentsBatchPage>(
      ctx.http,
      COMMENTS_FOR_THREADS_QUERY,
      { ids: [threadId], first: COMMENTS_PAGE_SIZE, after: cursor },
    )
    const connection = data.nodes[0]?.comments
    if (!connection?.edges) {
      break
    }
    comments.push(
      ...connection.edges
        .map((edge) => edge?.node)
        .filter((node): node is ThreadCommentNode => Boolean(node?.id)),
    )
    hasMore = connection.pageInfo.hasNextPage
    cursor = connection.pageInfo.endCursor
  }
  return comments
}

async function runPullRequestReviewCommentsSync(ctx: SyncContext): Promise<void> {
  await runDualPhasePrSync(ctx, async (prs) => {
    for (let i = 0; i < prs.length; i += PR_BATCH_SIZE) {
      const batch = prs.slice(i, i + PR_BATCH_SIZE)
      const threadsData = await githubGraphql<ReviewThreadsBatchPage>(
        ctx.http,
        REVIEW_THREADS_FOR_PRS_QUERY,
        { ids: batch.map((pr) => pr.id), first: THREADS_PAGE_SIZE, after: null },
      )

      const prByThreadId = new Map<string, PullRequestNode>()
      for (const node of threadsData.nodes) {
        if (!node?.reviewThreads?.edges) {
          continue
        }
        const pullRequest = batch.find((candidate) => candidate.id === node.id)
        if (!pullRequest) {
          continue
        }
        collectThreadIds(prByThreadId, node.reviewThreads.edges, pullRequest)
        if (node.reviewThreads.pageInfo.hasNextPage) {
          await drainRemainingThreads(
            ctx,
            prByThreadId,
            pullRequest,
            node.reviewThreads.pageInfo.endCursor,
          )
        }
      }

      const threadIds = [...prByThreadId.keys()]
      for (let j = 0; j < threadIds.length; j += THREAD_BATCH_SIZE) {
        const threadBatch = threadIds.slice(j, j + THREAD_BATCH_SIZE)
        const commentsData = await githubGraphql<ThreadCommentsBatchPage>(
          ctx.http,
          COMMENTS_FOR_THREADS_QUERY,
          { ids: threadBatch, first: COMMENTS_PAGE_SIZE, after: null },
        )

        for (const thread of commentsData.nodes) {
          if (!thread?.comments?.edges) {
            continue
          }
          const pullRequest = prByThreadId.get(thread.id)
          if (!pullRequest) {
            continue
          }
          const threadNode: ReviewThreadNode = { id: thread.id, isResolved: thread.isResolved }
          const comments = thread.comments.edges
            .map((edge) => edge?.node)
            .filter((node): node is ThreadCommentNode => Boolean(node?.id))
          if (thread.comments.pageInfo.hasNextPage) {
            comments.push(
              ...(await drainRemainingThreadComments(
                ctx,
                thread.id,
                thread.comments.pageInfo.endCursor,
              )),
            )
          }
          if (comments.length > 0) {
            await ctx.emit(
              comments.map((comment) => toReviewThreadComment(comment, threadNode, pullRequest)),
            )
          }
        }
      }
    }
  })
}

export const pullRequestReviewCommentsSync: SyncDefinition = {
  name: 'pull-request-review-comments',
  cadenceMinutes: 120,
  schema: githubActivitySchema,
  run: runPullRequestReviewCommentsSync,
}
