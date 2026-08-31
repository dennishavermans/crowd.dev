import type { SyncContext } from '../../types'

import { githubGraphql } from './gql'
import type { PullRequestNode, PullRequestsPage } from './graphql/pullRequests'
import { PULL_REQUESTS_QUERY } from './graphql/pullRequests'
import { MAX_PAGES_PER_RUN, parseRepoChannel, readWatermark } from './paging'

export const PR_PAGE_SIZE = 50

export type PrPageHandler = (prs: PullRequestNode[], sinceDate: Date | null) => Promise<void>

async function runBackfill(
  ctx: SyncContext,
  owner: string,
  repo: string,
  processPrs: PrPageHandler,
): Promise<void> {
  const watermark = readWatermark(ctx.watermark)
  let cursor = watermark.cursor
  let since = watermark.since

  for (let page = 0; page < MAX_PAGES_PER_RUN; page++) {
    const data = await githubGraphql<PullRequestsPage>(ctx.http, PULL_REQUESTS_QUERY, {
      owner,
      repo,
      first: PR_PAGE_SIZE,
      cursor,
      direction: 'ASC',
    })

    const { pageInfo, nodes } = data.repository.pullRequests
    const pullRequests = nodes.filter((node): node is PullRequestNode => node !== null)

    if (pullRequests.length > 0) {
      await processPrs(pullRequests, null)
      since = pullRequests[pullRequests.length - 1].updatedAt
    }

    if (!pageInfo.hasNextPage) {
      await ctx.commitWatermark({ phase: 'incremental', since, cursor: null })
      return
    }

    cursor = pageInfo.endCursor
    await ctx.commitWatermark({ phase: 'backfill', since, cursor })
  }
}

async function runIncremental(
  ctx: SyncContext,
  owner: string,
  repo: string,
  since: string,
  processPrs: PrPageHandler,
): Promise<void> {
  const sinceDate = new Date(since)
  let cursor: string | null = null
  let newSince: string | null = null

  for (let page = 0; page < MAX_PAGES_PER_RUN; page++) {
    const data = await githubGraphql<PullRequestsPage>(ctx.http, PULL_REQUESTS_QUERY, {
      owner,
      repo,
      first: PR_PAGE_SIZE,
      cursor,
      direction: 'DESC',
    })

    const { pageInfo, nodes } = data.repository.pullRequests
    const pullRequests = nodes.filter((node): node is PullRequestNode => node !== null)

    if (newSince === null && pullRequests.length > 0) {
      newSince = pullRequests[0].updatedAt
    }

    const fresh = pullRequests.filter((pr) => new Date(pr.updatedAt) > sinceDate)
    if (fresh.length > 0) {
      await processPrs(fresh, sinceDate)
    }

    const reachedSince = fresh.length < pullRequests.length
    if (reachedSince || !pageInfo.hasNextPage) {
      await ctx.commitWatermark({ phase: 'incremental', since: newSince ?? since, cursor: null })
      return
    }

    cursor = pageInfo.endCursor
  }
}

export async function runDualPhasePrSync(
  ctx: SyncContext,
  processPrs: PrPageHandler,
): Promise<void> {
  const { owner, repo } = parseRepoChannel(ctx.channel.channelName)
  const watermark = readWatermark(ctx.watermark)

  if (watermark.phase === 'incremental' && watermark.since) {
    await runIncremental(ctx, owner, repo, watermark.since, processPrs)
    return
  }
  await runBackfill(ctx, owner, repo, processPrs)
}
