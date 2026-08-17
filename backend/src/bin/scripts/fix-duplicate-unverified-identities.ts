import commandLineArgs from 'command-line-args'
import commandLineUsage from 'command-line-usage'

import {
  prepareMemberUnmerge,
  startMemberUnmergeWorkflow,
  unmergeMember,
} from '@crowd/common_services'
import {
  QueryExecutor,
  moveActivityRelationsToAnotherMember,
  moveActivityRelationsWithIdentityToAnotherMember,
  pgpQx,
} from '@crowd/data-access-layer'
import { getDbConnection } from '@crowd/data-access-layer/src/database'
import { chunkArray } from '@crowd/data-access-layer/src/old/apps/merge_suggestions_worker/utils'
import { getServiceLogger } from '@crowd/logging'
import { Client as TemporalClient, getTemporalClient } from '@crowd/temporal'
import { MemberIdentityType, MemberUnmergeResult } from '@crowd/types'

import { DB_CONFIG, TEMPORAL_CONFIG } from '@/conf'

const log = getServiceLogger()

const TEST_RUN_BATCH_SIZE = 10
const BATCH_SIZE = 500
const IDENTITY_CONCURRENCY = 5
const DEFAULT_AFTER_MEMBER_ID = '00000000-0000-0000-0000-000000000000'

const SYSTEM_IDENTITY_VALUES = new Set(['noreply@github.com', 'noreply@example.com'])

function isSystemIdentity(value: string) {
  return SYSTEM_IDENTITY_VALUES.has(value.toLowerCase())
}

const options = [
  {
    name: 'testRun',
    alias: 't',
    type: Boolean,
    description: 'Reduced run (10 members instead of 500). Still writes.',
  },
  {
    name: 'afterMemberId',
    alias: 'a',
    type: String,
    description:
      'Resume from this member ID (exclusive). Use the afterMemberId from the failure log.',
  },
  {
    name: 'help',
    alias: 'h',
    type: Boolean,
    description: 'Print this usage guide.',
  },
]

const usage = commandLineUsage([
  {
    header: 'Fix duplicate unverified identities',
    content:
      'Consolidate duplicate unverified member identities onto one member per identity. Loops batches until done. Use --afterMemberId to resume after a failure.',
  },
  {
    header: 'Options',
    optionList: options,
  },
])

const parameters = commandLineArgs(options)

if (parameters.help) {
  process.stdout.write(`${usage}\n`)
  process.exit(0)
}

function pickOldestMember<T extends { memberId: string; memberCreatedAt: Date }>(members: T[]) {
  return members.reduce((oldest, member) => {
    const byDate =
      new Date(member.memberCreatedAt).getTime() - new Date(oldest.memberCreatedAt).getTime()
    if (byDate !== 0) {
      return byDate < 0 ? member : oldest
    }
    return member.memberId < oldest.memberId ? member : oldest
  })
}

async function fetchMembersWithDuplicateUnverifiedIdentities(
  qx: QueryExecutor,
  afterMemberId: string,
  limit: number,
) {
  const rows: { memberId: string }[] = await qx.select(
    `
      SELECT DISTINCT ON (mi."memberId") mi."memberId"
      FROM "memberIdentities" mi
      WHERE mi."deletedAt" IS NULL
        AND mi.verified = false
        AND mi."memberId" > $(afterMemberId)
        AND NOT EXISTS (
          SELECT 1
          FROM "memberIdentities" earlier
          WHERE earlier.platform = mi.platform
            AND earlier.type = mi.type
            AND lower(earlier.value) = lower(mi.value)
            AND earlier."deletedAt" IS NULL
            AND earlier.verified = false
            AND earlier."memberId" < mi."memberId"
        )
        AND EXISTS (
          SELECT 1
          FROM "memberIdentities" later
          WHERE later.platform = mi.platform
            AND later.type = mi.type
            AND lower(later.value) = lower(mi.value)
            AND later."deletedAt" IS NULL
            AND later.verified = false
            AND later."memberId" > mi."memberId"
        )
      ORDER BY mi."memberId"
      LIMIT $(limit)
    `,
    { afterMemberId, limit },
  )

  return rows.map((row) => row.memberId)
}

async function fetchDuplicateIdentitiesForMembers(qx: QueryExecutor, memberIds: string[]) {
  if (memberIds.length === 0) {
    return []
  }

  return qx.select(
    `
      SELECT DISTINCT ON (mi.platform, mi.type, lower(mi.value))
        mi.platform,
        mi.type,
        mi.value
      FROM "memberIdentities" mi
      WHERE mi."memberId" IN ($(memberIds:csv))
        AND mi."deletedAt" IS NULL
        AND mi.verified = false
        AND EXISTS (
          SELECT 1
          FROM "memberIdentities" other
          WHERE other.platform = mi.platform
            AND other.type = mi.type
            AND lower(other.value) = lower(mi.value)
            AND other."deletedAt" IS NULL
            AND other.verified = false
            AND other."memberId" <> mi."memberId"
        )
      ORDER BY mi.platform, mi.type, lower(mi.value), mi.value
    `,
    { memberIds },
  ) as Promise<{ platform: string; type: MemberIdentityType; value: string }[]>
}

async function fetchIdentityHolders(
  qx: QueryExecutor,
  identity: { platform: string; type: MemberIdentityType; value: string },
) {
  return qx.select(
    `
      SELECT
        mi.id AS "identityId",
        mi."memberId",
        mi.value,
        m."createdAt" AS "memberCreatedAt",
        NOT EXISTS (
          SELECT 1
          FROM "memberIdentities" other
          WHERE other."memberId" = mi."memberId"
            AND other."deletedAt" IS NULL
            AND NOT (
              other.platform = $(platform)
              AND other.type = $(type)
              AND lower(other.value) = lower($(value))
            )
        ) AS "onlyIdentity",
        EXISTS (
          SELECT 1
          FROM "memberIdentities" other
          WHERE other."memberId" = mi."memberId"
            AND other."deletedAt" IS NULL
            AND CASE
              WHEN $(type) = 'email' THEN
                NOT (other.type = 'email' AND lower(other.value) = lower($(value)))
              ELSE
                NOT (
                  other.platform = $(platform)
                  AND other.type = $(type)
                  AND lower(other.value) = lower($(value))
                )
            END
        ) AS "canUnmergeFrom"
      FROM "memberIdentities" mi
      JOIN members m ON m.id = mi."memberId"
      WHERE mi."deletedAt" IS NULL
        AND mi.verified = false
        AND mi.platform = $(platform)
        AND mi.type = $(type)
        AND lower(mi.value) = lower($(value))
    `,
    identity,
  ) as Promise<
    {
      identityId: string
      memberId: string
      value: string
      memberCreatedAt: Date
      onlyIdentity: boolean
      canUnmergeFrom: boolean
    }[]
  >
}

async function reassignDuplicateMemberActivities(
  qx: QueryExecutor,
  targetMemberId: string,
  singleIdentityMemberIds: string[],
  multiIdentityMembers: { memberId: string; value: string }[],
  platform: string,
) {
  for (const memberId of singleIdentityMemberIds) {
    await moveActivityRelationsToAnotherMember(qx, memberId, targetMemberId)
  }

  for (const member of multiIdentityMembers) {
    await moveActivityRelationsWithIdentityToAnotherMember(
      qx,
      member.memberId,
      targetMemberId,
      member.value,
      platform,
    )
  }
}

async function deleteMergeSuggestionsForMembers(qx: QueryExecutor, memberIds: string[]) {
  if (memberIds.length === 0) {
    return
  }

  await qx.result(
    `
      DELETE FROM "memberToMerge"
      WHERE "memberId" IN ($(memberIds:csv))
         OR "toMergeId" IN ($(memberIds:csv))
    `,
    { memberIds },
  )
  await qx.result(
    `
      DELETE FROM "memberToMergeRaw"
      WHERE "memberId" IN ($(memberIds:csv))
         OR "toMergeId" IN ($(memberIds:csv))
    `,
    { memberIds },
  )
}

async function deleteMergeSuggestionsBetweenMembers(qx: QueryExecutor, memberIds: string[]) {
  if (memberIds.length < 2) {
    return
  }

  await qx.result(
    `
      DELETE FROM "memberToMerge"
      WHERE "memberId" IN ($(memberIds:csv))
        AND "toMergeId" IN ($(memberIds:csv))
    `,
    { memberIds },
  )
  await qx.result(
    `
      DELETE FROM "memberToMergeRaw"
      WHERE "memberId" IN ($(memberIds:csv))
        AND "toMergeId" IN ($(memberIds:csv))
    `,
    { memberIds },
  )
}

async function consolidateDuplicateIdentity(
  qx: QueryExecutor,
  temporal: TemporalClient,
  identity: { platform: string; type: MemberIdentityType; value: string },
) {
  const plan = await qx.tx(async (tx) => {
    const members = await fetchIdentityHolders(tx, identity)
    if (members.length <= 1) {
      return null
    }

    const skipActivityMoves = isSystemIdentity(identity.value)
    const singleIdentityMembers = members.filter((member) => member.onlyIdentity)
    const unmergeSources = members.filter((member) => member.canUnmergeFrom)

    let targetMemberId: string
    let unmergeSourceMemberId: string | null = null
    let unmergeResult: MemberUnmergeResult | null = null

    if (singleIdentityMembers.length > 0) {
      targetMemberId = pickOldestMember(singleIdentityMembers).memberId
    } else if (!skipActivityMoves && unmergeSources.length > 0) {
      const source = pickOldestMember(unmergeSources)
      const preview = await prepareMemberUnmerge(tx, source.memberId, source.identityId)
      unmergeResult = await unmergeMember(tx, source.memberId, preview)
      targetMemberId = unmergeResult.secondary.id
      unmergeSourceMemberId = source.memberId
    } else {
      targetMemberId = pickOldestMember(members).memberId
    }

    const duplicates = members.filter(
      (member) => member.memberId !== targetMemberId && member.memberId !== unmergeSourceMemberId,
    )

    return {
      allMemberIds: members.map((member) => member.memberId),
      targetMemberId,
      unmergeResult,
      skipActivityMoves,
      duplicates,
      singleIdentityDuplicateIds: duplicates
        .filter((member) => member.onlyIdentity)
        .map((member) => member.memberId),
      multiIdentityDuplicates: duplicates
        .filter((member) => !member.onlyIdentity)
        .map((member) => ({ memberId: member.memberId, value: member.value })),
    }
  })

  if (!plan) {
    return
  }

  if (plan.unmergeResult) {
    await startMemberUnmergeWorkflow(temporal, {
      primaryId: plan.unmergeResult.primary.id,
      secondaryId: plan.unmergeResult.secondary.id,
      movedIdentities: plan.unmergeResult.movedIdentities,
      primaryDisplayName: plan.unmergeResult.primary.displayName,
      secondaryDisplayName: plan.unmergeResult.secondary.displayName,
    })
  }

  if (!plan.skipActivityMoves) {
    await reassignDuplicateMemberActivities(
      qx,
      plan.targetMemberId,
      plan.singleIdentityDuplicateIds,
      plan.multiIdentityDuplicates,
      identity.platform,
    )
  }

  await qx.tx(async (tx) => {
    const duplicateMemberIds = plan.duplicates.map((member) => member.memberId)

    if (duplicateMemberIds.length > 0) {
      await tx.result(
        `
          UPDATE "memberIdentities"
          SET "deletedAt" = NOW()
          WHERE "memberId" IN ($(memberIds:csv))
            AND platform = $(platform)
            AND type = $(type)
            AND lower(value) = lower($(value))
            AND verified = false
            AND "deletedAt" IS NULL
        `,
        {
          memberIds: duplicateMemberIds,
          platform: identity.platform,
          type: identity.type,
          value: identity.value,
        },
      )
    }

    if (plan.singleIdentityDuplicateIds.length > 0) {
      await tx.result(
        `
          UPDATE "memberOrganizations"
          SET "deletedAt" = NOW()
          WHERE "memberId" IN ($(memberIds:csv))
            AND "deletedAt" IS NULL
        `,
        { memberIds: plan.singleIdentityDuplicateIds },
      )
      await deleteMergeSuggestionsForMembers(tx, plan.singleIdentityDuplicateIds)
    }

    await deleteMergeSuggestionsBetweenMembers(tx, plan.allMemberIds)

    const membersToTouch = [
      ...plan.multiIdentityDuplicates.map((member) => member.memberId),
      plan.targetMemberId,
    ]
    if (membersToTouch.length > 0) {
      await tx.result(
        `
          UPDATE members
          SET "updatedAt" = NOW()
          WHERE id IN ($(memberIds:csv))
        `,
        { memberIds: membersToTouch },
      )
    }
  })

  log.info(
    {
      platform: identity.platform,
      type: identity.type,
      value: identity.value,
      targetMemberId: plan.targetMemberId,
      createdByUnmerge: Boolean(plan.unmergeResult),
      skipActivityMoves: plan.skipActivityMoves,
      duplicateCount: plan.duplicates.length,
    },
    'Consolidated duplicate unverified identity',
  )
}

setImmediate(async () => {
  const testRun = parameters.testRun ?? false
  const batchSize = testRun ? TEST_RUN_BATCH_SIZE : BATCH_SIZE
  let afterMemberId = parameters.afterMemberId ?? DEFAULT_AFTER_MEMBER_ID

  try {
    const db = await getDbConnection({
      host: DB_CONFIG.writeHost,
      port: DB_CONFIG.port,
      database: DB_CONFIG.database,
      user: DB_CONFIG.username,
      password: DB_CONFIG.password,
    })
    const qx = pgpQx(db)
    const temporal = await getTemporalClient(TEMPORAL_CONFIG)

    log.info({ testRun, batchSize, afterMemberId }, 'Running script')

    let hasMore = true
    while (hasMore) {
      const batchAfterMemberId = afterMemberId
      const memberIds = await fetchMembersWithDuplicateUnverifiedIdentities(
        qx,
        batchAfterMemberId,
        batchSize,
      )

      if (memberIds.length === 0) {
        log.info(
          { afterMemberId: batchAfterMemberId },
          'No more members with duplicate unverified identities found',
        )
        break
      }

      const identities = await fetchDuplicateIdentitiesForMembers(qx, memberIds)

      log.info(
        {
          memberCount: memberIds.length,
          identityCount: identities.length,
          afterMemberId: batchAfterMemberId,
        },
        'Fetched duplicate unverified identities for batch',
      )

      for (const identitiesChunk of chunkArray(identities, IDENTITY_CONCURRENCY)) {
        await Promise.all(
          identitiesChunk.map(async (identity) => {
            try {
              await consolidateDuplicateIdentity(qx, temporal, identity)
            } catch (err) {
              log.error(
                { err, identity, afterMemberId: batchAfterMemberId },
                'Failed to consolidate duplicate unverified identity',
              )
              throw err
            }
          }),
        )
      }

      afterMemberId = memberIds[memberIds.length - 1]

      const isLastBatch = testRun || memberIds.length < batchSize
      hasMore = !isLastBatch

      log.info(
        { count: memberIds.length, afterMemberId },
        isLastBatch ? 'Batch processed.' : 'Batch processed. Continuing with next batch.',
      )
    }

    process.exit(0)
  } catch (err) {
    log.error({ err, afterMemberId }, 'Script failed. Re-run with --afterMemberId to resume.')
    process.exit(1)
  }
})
