import type { SyncContext, SyncDefinition } from '../../../types'
import { githubGraphql } from '../gql'
import type { PrTimelineItem, PrTimelinePage, PullRequestNode } from '../graphql/pullRequests'
import { PR_TIMELINE_QUERY } from '../graphql/pullRequests'
import { toPullRequestActivities } from '../mappers/pullRequest'
import { runDualPhasePrSync } from '../prWalk'
import { githubActivitySchema } from '../schemas'

const TIMELINE_BATCH_SIZE = 50

async function drainRemainingTimeline(
  ctx: SyncContext,
  prId: string,
  startCursor: string | null,
): Promise<(PrTimelineItem | null)[]> {
  const items: (PrTimelineItem | null)[] = []
  let cursor = startCursor
  let hasMore = true
  while (hasMore) {
    const data = await githubGraphql<PrTimelinePage>(ctx.http, PR_TIMELINE_QUERY, {
      ids: [prId],
      first: TIMELINE_BATCH_SIZE,
      after: cursor,
    })
    const timeline = data.nodes[0]?.timelineItems
    if (!timeline) {
      break
    }
    items.push(...timeline.nodes)
    hasMore = timeline.pageInfo.hasNextPage
    cursor = timeline.pageInfo.endCursor
  }
  return items
}

async function emitPullRequests(ctx: SyncContext, pullRequests: PullRequestNode[]): Promise<void> {
  for (let i = 0; i < pullRequests.length; i += TIMELINE_BATCH_SIZE) {
    const batch = pullRequests.slice(i, i + TIMELINE_BATCH_SIZE)
    const timelineData = await githubGraphql<PrTimelinePage>(ctx.http, PR_TIMELINE_QUERY, {
      ids: batch.map((pr) => pr.id),
      first: TIMELINE_BATCH_SIZE,
      after: null,
    })

    const timelinesByPrId = new Map<string, (PrTimelineItem | null)[]>()
    for (const node of timelineData.nodes) {
      if (!node?.id) {
        continue
      }
      const items = [...(node.timelineItems?.nodes ?? [])]
      if (node.timelineItems?.pageInfo.hasNextPage) {
        items.push(
          ...(await drainRemainingTimeline(ctx, node.id, node.timelineItems.pageInfo.endCursor)),
        )
      }
      timelinesByPrId.set(node.id, items)
    }

    await ctx.emit(
      batch.flatMap((pr) => toPullRequestActivities(pr, timelinesByPrId.get(pr.id) ?? [])),
    )
  }
}

async function runPullRequestsSync(ctx: SyncContext): Promise<void> {
  await runDualPhasePrSync(ctx, (prs) => emitPullRequests(ctx, prs))
}

export const pullRequestsSync: SyncDefinition = {
  name: 'pull-requests',
  cadenceMinutes: 60,
  schema: githubActivitySchema,
  run: runPullRequestsSync,
}
