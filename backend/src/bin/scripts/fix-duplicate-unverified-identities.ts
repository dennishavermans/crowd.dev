import commandLineArgs from 'command-line-args'
import commandLineUsage from 'command-line-usage'

import {
  prepareMemberUnmerge,
  startMemberUnmergeWorkflow,
  unmergeMember,
} from '@crowd/common_services'
import { QueryExecutor, pgpQx } from '@crowd/data-access-layer'
import { getDbConnection } from '@crowd/data-access-layer/src/database'
import { chunkArray } from '@crowd/data-access-layer/src/old/apps/merge_suggestions_worker/utils'
import { getServiceLogger } from '@crowd/logging'
import { Client as TemporalClient, getTemporalClient } from '@crowd/temporal'
import { MemberIdentityType, MemberUnmergeResult } from '@crowd/types'

import { DB_CONFIG, TEMPORAL_CONFIG } from '@/conf'

const log = getServiceLogger()

const TEST_RUN_BATCH_SIZE = 10
const BATCH_SIZE = 500
const IDENTITY_CONCURRENCY = 50
const DEFAULT_AFTER_MEMBER_ID = '00000000-0000-0000-0000-000000000000'

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
    description: 'Resume after this member ID (exclusive).',
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
      'Consolidate duplicate unverified member identities onto one member per identity. One batch per run.',
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
        ) AS "onlyIdentity"
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
    }[]
  >
}

async function reassignMemberActivities(
  qx: QueryExecutor,
  memberIds: string[],
  targetMemberId: string,
) {
  if (memberIds.length === 0) {
    return
  }

  await qx.result(
    `
      UPDATE "activityRelations"
      SET "memberId" = $(targetMemberId),
          "updatedAt" = NOW()
      WHERE "memberId" IN ($(memberIds:csv))
    `,
    { targetMemberId, memberIds },
  )
  await qx.result(
    `
      UPDATE "activityRelations"
      SET "objectMemberId" = $(targetMemberId),
          "updatedAt" = NOW()
      WHERE "objectMemberId" IN ($(memberIds:csv))
    `,
    { targetMemberId, memberIds },
  )
}

async function reassignIdentityActivities(
  qx: QueryExecutor,
  memberIds: string[],
  targetMemberId: string,
  identity: { platform: string; value: string },
) {
  if (memberIds.length === 0) {
    return
  }

  await qx.result(
    `
      UPDATE "activityRelations"
      SET "memberId" = $(targetMemberId),
          "updatedAt" = NOW()
      WHERE "memberId" IN ($(memberIds:csv))
        AND platform = $(platform)
        AND lower(username) = lower($(value))
    `,
    { targetMemberId, memberIds, platform: identity.platform, value: identity.value },
  )
  await qx.result(
    `
      UPDATE "activityRelations"
      SET "objectMemberId" = $(targetMemberId),
          "updatedAt" = NOW()
      WHERE "objectMemberId" IN ($(memberIds:csv))
        AND platform = $(platform)
        AND lower("objectMemberUsername") = lower($(value))
    `,
    { targetMemberId, memberIds, platform: identity.platform, value: identity.value },
  )
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
  const unmerge = await qx.tx(async (tx) => {
    const members = await fetchIdentityHolders(tx, identity)
    if (members.length <= 1) {
      return null
    }

    const singleIdentityMembers = members.filter((member) => member.onlyIdentity)
    const multiIdentityMembers = members.filter((member) => !member.onlyIdentity)

    let targetMemberId: string
    let unmergeSourceMemberId: string | null = null
    let unmergeResult: MemberUnmergeResult | null = null

    if (singleIdentityMembers.length > 0) {
      targetMemberId = pickOldestMember(singleIdentityMembers).memberId
    } else if (multiIdentityMembers.length === 0) {
      return null
    } else {
      const source = pickOldestMember(multiIdentityMembers)
      const preview = await prepareMemberUnmerge(tx, source.memberId, source.identityId)
      unmergeResult = await unmergeMember(tx, source.memberId, preview)
      targetMemberId = unmergeResult.secondary.id
      unmergeSourceMemberId = source.memberId
    }

    const duplicates = members.filter(
      (member) => member.memberId !== targetMemberId && member.memberId !== unmergeSourceMemberId,
    )
    const duplicateMemberIds = duplicates.map((member) => member.memberId)
    const singleIdentityDuplicateIds = duplicates
      .filter((member) => member.onlyIdentity)
      .map((member) => member.memberId)
    const multiIdentityDuplicateIds = duplicates
      .filter((member) => !member.onlyIdentity)
      .map((member) => member.memberId)

    await reassignMemberActivities(tx, singleIdentityDuplicateIds, targetMemberId)
    await reassignIdentityActivities(tx, multiIdentityDuplicateIds, targetMemberId, identity)

    if (duplicateMemberIds.length > 0) {
      await tx.result(
        `
          UPDATE "memberIdentities"
          SET "deletedAt" = NOW()
          WHERE "memberId" IN ($(memberIds:csv))
            AND platform = $(platform)
            AND type = $(type)
            AND lower(value) = lower($(value))
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

    if (singleIdentityDuplicateIds.length > 0) {
      await tx.result(
        `
          UPDATE "memberOrganizations"
          SET "deletedAt" = NOW()
          WHERE "memberId" IN ($(memberIds:csv))
            AND "deletedAt" IS NULL
        `,
        { memberIds: singleIdentityDuplicateIds },
      )
      await deleteMergeSuggestionsForMembers(tx, singleIdentityDuplicateIds)
    }

    await deleteMergeSuggestionsBetweenMembers(
      tx,
      members.map((member) => member.memberId),
    )

    const membersToTouch = [...multiIdentityDuplicateIds, targetMemberId]
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

    log.info(
      {
        platform: identity.platform,
        type: identity.type,
        value: identity.value,
        targetMemberId,
        createdByUnmerge: Boolean(unmergeResult),
        duplicateCount: duplicates.length,
      },
      'Consolidated duplicate unverified identity',
    )

    return unmergeResult
  })

  if (!unmerge) {
    return
  }

  await startMemberUnmergeWorkflow(temporal, {
    primaryId: unmerge.primary.id,
    secondaryId: unmerge.secondary.id,
    movedIdentities: unmerge.movedIdentities,
    primaryDisplayName: unmerge.primary.displayName,
    secondaryDisplayName: unmerge.secondary.displayName,
  })
}

setImmediate(async () => {
  const testRun = parameters.testRun ?? false
  const batchSize = testRun ? TEST_RUN_BATCH_SIZE : BATCH_SIZE
  const afterMemberId = parameters.afterMemberId ?? DEFAULT_AFTER_MEMBER_ID

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

    const memberIds = await fetchMembersWithDuplicateUnverifiedIdentities(
      qx,
      afterMemberId,
      batchSize,
    )

    if (memberIds.length === 0) {
      log.info('No members with duplicate unverified identities found')
      process.exit(0)
    }

    const identities = await fetchDuplicateIdentitiesForMembers(qx, memberIds)

    log.info(
      { memberCount: memberIds.length, identityCount: identities.length },
      'Fetched duplicate unverified identities for batch',
    )

    for (const identitiesChunk of chunkArray(identities, IDENTITY_CONCURRENCY)) {
      await Promise.all(
        identitiesChunk.map(async (identity) => {
          try {
            await consolidateDuplicateIdentity(qx, temporal, identity)
          } catch (err) {
            log.error({ err, identity }, 'Failed to consolidate duplicate unverified identity')
            throw err
          }
        }),
      )
    }

    const lastMemberId = memberIds[memberIds.length - 1]

    log.info(
      { count: memberIds.length, lastMemberId },
      memberIds.length === batchSize
        ? 'Batch processed. Re-run with --afterMemberId to continue.'
        : 'Final batch processed.',
    )

    process.exit(0)
  } catch (err) {
    log.error({ err }, 'Script failed')
    process.exit(1)
  }
})
