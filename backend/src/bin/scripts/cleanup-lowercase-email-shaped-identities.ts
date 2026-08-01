/**
 * Lowercase email-shaped member identity values (and matching activityRelations usernames).
 *
 * Scans live identities where value <> lower(value) and value contains '@'.
 * Uses normalizeMemberIdentityValue (isValidEmail) — non-email-shaped values are skipped.
 *
 * If normalize would collide with an existing row on lower(value):
 * - same member → soft-delete the dirty row, normalize the keeper, rewrite ARs
 * - different member → merge dirty member into the owner of the normalized identity
 *
 * Unexpected unique-index conflicts still fail the run.
 *
 * Usage:
 *   pnpm run script:cleanup-lowercase-email-shaped-identities
 *   pnpm run script:cleanup-lowercase-email-shaped-identities -- --testRun
 *
 * Cross-member merges use process.env.CROWD_LF_AGENT_USER_ID for the audit actor.
 */
import commandLineArgs from 'command-line-args'

import { generateUUIDv1, normalizeMemberIdentityValue } from '@crowd/common'
import { CommonMemberService } from '@crowd/common_services'
import { pgpQx } from '@crowd/data-access-layer'
import { getDbConnection } from '@crowd/data-access-layer/src/database'
import { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'
import { getServiceLogger } from '@crowd/logging'

import { DB_CONFIG } from '@/conf'
import { optionsQx } from '@/database/sequelizeQueryExecutor'

import SequelizeRepository from '../../database/repositories/sequelizeRepository'

const log = getServiceLogger()

const LOAD_BATCH_SIZE = 500
const AR_UPDATE_BATCH_SIZE = 5000

type CandidateIdentity = {
  id: string
  memberId: string
  platform: string
  type: string
  value: string
}

type ConflictingIdentity = {
  id: string
  memberId: string
  value: string
  verified: boolean
}

type IdentityForKeep = {
  id: string
  memberId: string
  value: string
  verified: boolean
  verifiedBy: string | null
  updatedAt: Date
}

type ArRewriteStats = {
  usernameRows: number
  objectMemberUsernameRows: number
}

type CleanupStats = {
  processed: number
  identitiesUpdated: number
  softDeleted: number
  merged: number
  skipped: number
  arUsernameUpdated: number
  arObjectUsernameUpdated: number
}

type MergeContext = {
  memberService: CommonMemberService
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  mergeCtx: any
  userId: string
}

const emptyStats = (): CleanupStats => ({
  processed: 0,
  identitiesUpdated: 0,
  softDeleted: 0,
  merged: 0,
  skipped: 0,
  arUsernameUpdated: 0,
  arObjectUsernameUpdated: 0,
})

function addStats(total: CleanupStats, next: CleanupStats): CleanupStats {
  return {
    processed: total.processed + next.processed,
    identitiesUpdated: total.identitiesUpdated + next.identitiesUpdated,
    softDeleted: total.softDeleted + next.softDeleted,
    merged: total.merged + next.merged,
    skipped: total.skipped + next.skipped,
    arUsernameUpdated: total.arUsernameUpdated + next.arUsernameUpdated,
    arObjectUsernameUpdated: total.arObjectUsernameUpdated + next.arObjectUsernameUpdated,
  }
}

function withArStats(stats: CleanupStats, ar: ArRewriteStats): CleanupStats {
  return {
    ...stats,
    arUsernameUpdated: stats.arUsernameUpdated + ar.usernameRows,
    arObjectUsernameUpdated: stats.arObjectUsernameUpdated + ar.objectMemberUsernameRows,
  }
}

/** Same keep rule as cleanup-same-member-case-variant-identities. */
function pickKeeper(rows: IdentityForKeep[]): IdentityForKeep {
  return [...rows].sort((a, b) => {
    if (a.verified !== b.verified) return a.verified ? -1 : 1
    if (Boolean(a.verifiedBy) !== Boolean(b.verifiedBy)) return a.verifiedBy ? -1 : 1
    const aUpdated = new Date(a.updatedAt).getTime()
    const bUpdated = new Date(b.updatedAt).getTime()
    if (aUpdated !== bUpdated) return bUpdated - aUpdated
    return a.id < b.id ? -1 : 1
  })[0]
}

async function rewriteActivityRelationUsernamesExact(
  qx: QueryExecutor,
  memberId: string,
  platform: string,
  toUsername: string,
  fromUsernames: string[],
): Promise<ArRewriteStats> {
  // Access path: ix_activityRelations_memberId / ix_activityRelations_objectMemberId,
  // then filter platform + username in-heap (platform_username index was dropped).
  const values = [...new Set(fromUsernames.filter((value) => value && value !== toUsername))]
  if (values.length === 0) {
    return { usernameRows: 0, objectMemberUsernameRows: 0 }
  }

  let usernameRows = 0
  let objectMemberUsernameRows = 0
  let updated: number

  do {
    updated = await qx.result(
      `
        update "activityRelations"
        set
          username = $(toUsername),
          "updatedAt" = now()
        where "activityId" in (
          select "activityId"
          from "activityRelations"
          where "memberId" = $(memberId)
            and platform = $(platform)
            and username in ($(values:csv))
          limit $(batchSize)
        )
      `,
      {
        memberId,
        platform,
        toUsername,
        values,
        batchSize: AR_UPDATE_BATCH_SIZE,
      },
    )
    usernameRows += updated
  } while (updated === AR_UPDATE_BATCH_SIZE)

  do {
    updated = await qx.result(
      `
        update "activityRelations"
        set
          "objectMemberUsername" = $(toUsername),
          "updatedAt" = now()
        where "activityId" in (
          select "activityId"
          from "activityRelations"
          where "objectMemberId" = $(memberId)
            and platform = $(platform)
            and "objectMemberUsername" in ($(values:csv))
          limit $(batchSize)
        )
      `,
      {
        memberId,
        platform,
        toUsername,
        values,
        batchSize: AR_UPDATE_BATCH_SIZE,
      },
    )
    objectMemberUsernameRows += updated
  } while (updated === AR_UPDATE_BATCH_SIZE)

  return { usernameRows, objectMemberUsernameRows }
}

async function rewriteActivityRelationUsernamesByLower(
  qx: QueryExecutor,
  memberId: string,
  platform: string,
  lowerValue: string,
): Promise<ArRewriteStats> {
  // Same memberId/objectMemberId index access path as exact rewrite.
  if (!lowerValue) {
    return { usernameRows: 0, objectMemberUsernameRows: 0 }
  }

  let usernameRows = 0
  let objectMemberUsernameRows = 0
  let updated: number

  do {
    updated = await qx.result(
      `
        update "activityRelations"
        set
          username = $(lowerValue),
          "updatedAt" = now()
        where "activityId" in (
          select "activityId"
          from "activityRelations"
          where "memberId" = $(memberId)
            and platform = $(platform)
            and lower(username) = $(lowerValue)
            and username <> $(lowerValue)
          limit $(batchSize)
        )
      `,
      {
        memberId,
        platform,
        lowerValue,
        batchSize: AR_UPDATE_BATCH_SIZE,
      },
    )
    usernameRows += updated
  } while (updated === AR_UPDATE_BATCH_SIZE)

  do {
    updated = await qx.result(
      `
        update "activityRelations"
        set
          "objectMemberUsername" = $(lowerValue),
          "updatedAt" = now()
        where "activityId" in (
          select "activityId"
          from "activityRelations"
          where "objectMemberId" = $(memberId)
            and platform = $(platform)
            and lower("objectMemberUsername") = $(lowerValue)
            and "objectMemberUsername" <> $(lowerValue)
          limit $(batchSize)
        )
      `,
      {
        memberId,
        platform,
        lowerValue,
        batchSize: AR_UPDATE_BATCH_SIZE,
      },
    )
    objectMemberUsernameRows += updated
  } while (updated === AR_UPDATE_BATCH_SIZE)

  return { usernameRows, objectMemberUsernameRows }
}

async function rewriteActivityRelationUsernamesToNormalized(
  qx: QueryExecutor,
  memberId: string,
  platform: string,
  normalizedUsername: string,
  fromUsernames: string[] = [],
): Promise<ArRewriteStats> {
  const exact = await rewriteActivityRelationUsernamesExact(
    qx,
    memberId,
    platform,
    normalizedUsername,
    fromUsernames,
  )
  const byLower = await rewriteActivityRelationUsernamesByLower(
    qx,
    memberId,
    platform,
    normalizedUsername,
  )
  return {
    usernameRows: exact.usernameRows + byLower.usernameRows,
    objectMemberUsernameRows: exact.objectMemberUsernameRows + byLower.objectMemberUsernameRows,
  }
}

async function findMixedCaseEmailIdentities(
  qx: QueryExecutor,
  afterId: string | null,
  limit: number,
): Promise<CandidateIdentity[]> {
  return qx.select(
    `
      select id, "memberId", platform, type, value
      from "memberIdentities"
      where "deletedAt" is null
        and value <> lower(value)
        and position('@' in value) > 0
        ${afterId ? 'and id > $(afterId)' : ''}
      order by id
      limit $(limit)
    `,
    { afterId, limit },
  )
}

async function findNormalizedValueConflict(
  qx: QueryExecutor,
  params: {
    excludeId: string
    platform: string
    type: string
    normalized: string
  },
): Promise<ConflictingIdentity | null> {
  return qx.selectOneOrNone(
    `
      select id, "memberId", value, verified
      from "memberIdentities"
      where "deletedAt" is null
        and platform = $(platform)
        and type = $(type)
        and id <> $(excludeId)
        and lower(value) = $(normalized)
      order by verified desc, "updatedAt" desc, id
      limit 1
    `,
    params,
  )
}

async function findSameMemberNormalizeGroup(
  qx: QueryExecutor,
  candidate: CandidateIdentity,
  normalized: string,
): Promise<IdentityForKeep[]> {
  return qx.select(
    `
      select id, "memberId", value, verified, "verifiedBy", "updatedAt"
      from "memberIdentities"
      where "deletedAt" is null
        and "memberId" = $(memberId)
        and platform = $(platform)
        and type = $(type)
        and (id = $(candidateId) or lower(value) = $(normalized))
    `,
    {
      memberId: candidate.memberId,
      platform: candidate.platform,
      type: candidate.type,
      candidateId: candidate.id,
      normalized,
    },
  )
}

async function setIdentityValue(qx: QueryExecutor, id: string, value: string): Promise<number> {
  return qx.result(
    `
      update "memberIdentities"
      set
        value = $(value),
        "updatedAt" = now()
      where id = $(id)
        and "deletedAt" is null
        and value <> $(value)
    `,
    { id, value },
  )
}

async function softDeleteIdentity(qx: QueryExecutor, id: string): Promise<number> {
  return qx.result(
    `
      update "memberIdentities"
      set
        "deletedAt" = now(),
        "updatedAt" = now()
      where id = $(id)
        and "deletedAt" is null
    `,
    { id },
  )
}

async function identityStillActive(qx: QueryExecutor, id: string): Promise<boolean> {
  const row = await qx.selectOneOrNone(
    `
      select id
      from "memberIdentities"
      where id = $(id)
        and "deletedAt" is null
    `,
    { id },
  )
  return row != null
}

async function lowercaseIdentity(
  qx: QueryExecutor,
  candidate: CandidateIdentity,
  normalized: string,
  testRun: boolean,
): Promise<CleanupStats> {
  if (testRun) {
    log.info(
      {
        memberId: candidate.memberId,
        platform: candidate.platform,
        type: candidate.type,
        from: candidate.value,
        to: normalized,
      },
      'Lowercasing email-shaped identity',
    )
  }

  return qx.tx(async (tx) => {
    const identitiesUpdated = await setIdentityValue(tx, candidate.id, normalized)
    const ar = await rewriteActivityRelationUsernamesToNormalized(
      tx,
      candidate.memberId,
      candidate.platform,
      normalized,
      [candidate.value],
    )

    return withArStats(
      {
        ...emptyStats(),
        processed: 1,
        identitiesUpdated,
      },
      ar,
    )
  })
}

async function resolveSameMemberConflict(
  qx: QueryExecutor,
  candidate: CandidateIdentity,
  normalized: string,
): Promise<CleanupStats> {
  const group = await findSameMemberNormalizeGroup(qx, candidate, normalized)
  if (group.length < 2) {
    // Conflict vanished between check and load — fall through to plain lowercase.
    return lowercaseIdentity(qx, candidate, normalized, false)
  }

  const keeper = pickKeeper(group)
  const losers = group.filter((row) => row.id !== keeper.id)

  log.info(
    {
      memberId: candidate.memberId,
      platform: candidate.platform,
      type: candidate.type,
      keep: keeper.value,
      softDelete: losers.map((row) => row.value),
      normalized,
    },
    'Same-member normalize conflict — keeping preferred identity',
  )

  return qx.tx(async (tx) => {
    let softDeleted = 0
    for (const loser of losers) {
      softDeleted += await softDeleteIdentity(tx, loser.id)
    }

    const identitiesUpdated = await setIdentityValue(tx, keeper.id, normalized)

    const ar = await rewriteActivityRelationUsernamesToNormalized(
      tx,
      candidate.memberId,
      candidate.platform,
      normalized,
      group.map((row) => row.value),
    )

    return withArStats(
      {
        ...emptyStats(),
        processed: 1,
        softDeleted,
        identitiesUpdated,
      },
      ar,
    )
  })
}

async function settleIdentityAfterMerge(
  qx: QueryExecutor,
  candidate: CandidateIdentity,
  owner: ConflictingIdentity,
  normalized: string,
): Promise<CleanupStats> {
  let softDeleted = 0
  let identitiesUpdated = 0

  if (await identityStillActive(qx, candidate.id)) {
    const sibling = await findNormalizedValueConflict(qx, {
      excludeId: candidate.id,
      platform: candidate.platform,
      type: candidate.type,
      normalized,
    })

    if (sibling) {
      softDeleted = await softDeleteIdentity(qx, candidate.id)
    } else {
      identitiesUpdated += await setIdentityValue(qx, candidate.id, normalized)
    }
  }

  // Primary may still hold mixed-case preferred casing — normalize in this pass.
  identitiesUpdated += await setIdentityValue(qx, owner.id, normalized)

  return {
    ...emptyStats(),
    softDeleted,
    identitiesUpdated,
  }
}

async function resolveCrossMemberConflict(
  qx: QueryExecutor,
  candidate: CandidateIdentity,
  owner: ConflictingIdentity,
  normalized: string,
  merge: MergeContext,
): Promise<CleanupStats> {
  log.info(
    {
      primaryMemberId: owner.memberId,
      secondaryMemberId: candidate.memberId,
      platform: candidate.platform,
      type: candidate.type,
      primaryValue: owner.value,
      secondaryValue: candidate.value,
      normalized,
      userId: merge.userId,
    },
    'Cross-member normalize conflict — merging into owner of normalized identity',
  )

  if (owner.memberId === candidate.memberId) {
    throw new Error(
      `Cross-member conflict resolved to the same memberId=${owner.memberId}; refusing merge`,
    )
  }

  try {
    await merge.memberService.merge(owner.memberId, candidate.memberId, merge.mergeCtx)
  } catch (err) {
    log.error(
      {
        err,
        primaryMemberId: owner.memberId,
        secondaryMemberId: candidate.memberId,
      },
      'Member merge failed',
    )
    throw err
  }

  // finishMemberMerging deletes the secondary async; settle the dirty identity now
  // so the cursor scan does not reprocess it.
  const settleStats = await qx.tx(async (tx) => {
    const settled = await settleIdentityAfterMerge(tx, candidate, owner, normalized)
    const arPrimary = await rewriteActivityRelationUsernamesToNormalized(
      tx,
      owner.memberId,
      candidate.platform,
      normalized,
      [candidate.value, owner.value],
    )
    // Relations may still point at the secondary until finishMemberMerging moves them.
    const arSecondary = await rewriteActivityRelationUsernamesToNormalized(
      tx,
      candidate.memberId,
      candidate.platform,
      normalized,
      [candidate.value, owner.value],
    )

    return withArStats(withArStats(settled, arPrimary), arSecondary)
  })

  return {
    ...settleStats,
    processed: 1,
    merged: 1,
  }
}

async function processCandidate(
  qx: QueryExecutor,
  candidate: CandidateIdentity,
  merge: MergeContext,
  testRun: boolean,
): Promise<CleanupStats> {
  const normalized = normalizeMemberIdentityValue(candidate.value)
  if (normalized === candidate.value) {
    return { ...emptyStats(), skipped: 1 }
  }

  const conflict = await findNormalizedValueConflict(qx, {
    excludeId: candidate.id,
    platform: candidate.platform,
    type: candidate.type,
    normalized,
  })

  if (!conflict) {
    return lowercaseIdentity(qx, candidate, normalized, testRun)
  }

  if (conflict.memberId === candidate.memberId) {
    return resolveSameMemberConflict(qx, candidate, normalized)
  }

  return resolveCrossMemberConflict(qx, candidate, conflict, normalized, merge)
}

const parameters = commandLineArgs([
  {
    name: 'testRun',
    alias: 't',
    type: Boolean,
  },
])

setImmediate(async () => {
  try {
    const testRun = parameters.testRun ?? false
    const userId = process.env.CROWD_LF_AGENT_USER_ID
    const batchSize = testRun ? 10 : LOAD_BATCH_SIZE
    const progressEvery = testRun ? 1 : 200

    if (!userId) {
      log.error('Missing CROWD_LF_AGENT_USER_ID. Needed for merge action audit trail.')
      process.exit(1)
    }

    const db = await getDbConnection({
      host: DB_CONFIG.writeHost,
      port: DB_CONFIG.port,
      database: DB_CONFIG.database,
      user: DB_CONFIG.username,
      password: DB_CONFIG.password,
    })
    const qx = pgpQx(db)

    const repoOptions = await SequelizeRepository.getDefaultIRepositoryOptions()
    repoOptions.currentUser = { id: userId }

    const merge: MergeContext = {
      userId,
      memberService: new CommonMemberService(optionsQx(repoOptions), repoOptions.temporal, log),
      mergeCtx: {
        ...repoOptions,
        requestId: generateUUIDv1(),
        userData: {
          ip: '127.0.0.1',
          userAgent: 'script',
        },
      },
    }

    log.info({ testRun, batchSize, userId }, 'Lowercasing email-shaped identity values')

    let afterId: string | null = null
    let totals = emptyStats()
    let candidates = await findMixedCaseEmailIdentities(qx, afterId, batchSize)

    while (candidates.length > 0) {
      afterId = candidates[candidates.length - 1].id

      for (const candidate of candidates) {
        totals = addStats(totals, await processCandidate(qx, candidate, merge, testRun))

        if (totals.processed > 0 && totals.processed % progressEvery === 0) {
          log.info(totals, 'Progress')
        }
      }

      if (testRun || candidates.length < batchSize) {
        if (testRun) {
          log.info('Test run — stopping after first batch')
        }
        break
      }

      candidates = await findMixedCaseEmailIdentities(qx, afterId, batchSize)
    }

    log.info(totals, 'Done')
    process.exit(0)
  } catch (err) {
    log.error(err, 'Cleanup failed')
    process.exit(1)
  }
})
