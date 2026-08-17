import commandLineArgs from 'command-line-args'
import commandLineUsage from 'command-line-usage'

import { getProperDisplayName } from '@crowd/common'
import {
  QueryExecutor,
  createMember,
  moveActivityRelationsToAnotherMember,
  moveActivityRelationsWithIdentityToAnotherMember,
  moveToNewMember,
  pgpQx,
  touchMemberUpdatedAt,
} from '@crowd/data-access-layer'
import { getDbConnection } from '@crowd/data-access-layer/src/database'
import { getServiceLogger } from '@crowd/logging'
import { MemberIdentityType } from '@crowd/types'

import { DB_CONFIG } from '@/conf'

const log = getServiceLogger()

const TEST_RUN_BATCH_SIZE = 100
const BATCH_SIZE = 1000
const IDENTITY_CONCURRENCY = 100

const SYSTEM_IDENTITY_VALUES = new Set(['noreply@github.com', 'noreply@example.com'])

const options = [
  {
    name: 'testRun',
    alias: 't',
    type: Boolean,
    description: 'Process one batch (100 groups) then exit. Still writes.',
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
      'Consolidate duplicate unverified member identities onto one parking member per identity key. Re-run anytime; only groups that still have duplicates are processed.',
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

type DuplicateUnverifiedIdentity = {
  platform: string
  type: MemberIdentityType
  value: string
  holderMemberIds: string[]
}

type IdentityHolder = {
  identityId: string
  memberId: string
  value: string
  memberCreatedAt: Date
  onlyIdentity: boolean
}

function isSystemIdentity(value: string) {
  return SYSTEM_IDENTITY_VALUES.has(value.toLowerCase())
}

function canonicalIdentityValue(value: string) {
  const trimmed = value.trim()
  if (!trimmed.includes('%')) {
    return trimmed
  }

  try {
    const decoded = decodeURIComponent(trimmed)
    return decoded !== trimmed ? decoded : trimmed
  } catch {
    return trimmed
  }
}

function identityGroupKey(platform: string, type: string, value: string) {
  return `${platform}:${type}:${canonicalIdentityValue(value).toLowerCase()}`
}

function mergeDuplicateGroupsByCanonicalValue(identities: DuplicateUnverifiedIdentity[]) {
  const merged = new Map<string, DuplicateUnverifiedIdentity>()

  for (const identity of identities) {
    const key = identityGroupKey(identity.platform, identity.type, identity.value)
    const existing = merged.get(key)
    if (!existing) {
      merged.set(key, {
        platform: identity.platform,
        type: identity.type,
        value: identity.value,
        holderMemberIds: [...identity.holderMemberIds],
      })
    } else {
      existing.holderMemberIds = [
        ...new Set([...existing.holderMemberIds, ...identity.holderMemberIds]),
      ]
    }
  }

  return Array.from(merged.values())
}

function groupIdentitiesWithoutSharedMembers(identities: DuplicateUnverifiedIdentity[]) {
  const groups: DuplicateUnverifiedIdentity[][] = []
  let remaining = identities

  while (remaining.length > 0) {
    const group: DuplicateUnverifiedIdentity[] = []
    const occupiedMemberIds = new Set<string>()
    const deferred: DuplicateUnverifiedIdentity[] = []

    for (const identity of remaining) {
      const sharesMember = identity.holderMemberIds.some((memberId) =>
        occupiedMemberIds.has(memberId),
      )
      if (!sharesMember && group.length < IDENTITY_CONCURRENCY) {
        group.push(identity)
        for (const memberId of identity.holderMemberIds) {
          occupiedMemberIds.add(memberId)
        }
      } else {
        deferred.push(identity)
      }
    }

    groups.push(group)
    remaining = deferred
  }

  return groups
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

async function fetchDuplicateIdentityGroups(qx: QueryExecutor, limit: number) {
  return qx.select(
    `
      SELECT DISTINCT ON (mi.platform, mi.type, lower(mi.value))
        mi.platform,
        mi.type,
        mi.value,
        (
          SELECT array_agg(DISTINCT holders."memberId")
          FROM "memberIdentities" holders
          WHERE holders.platform = mi.platform
            AND holders.type = mi.type
            AND lower(holders.value) = lower(mi.value)
            AND holders."deletedAt" IS NULL
            AND holders.verified = false
        ) AS "holderMemberIds"
      FROM "memberIdentities" mi
      WHERE mi."deletedAt" IS NULL
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
      LIMIT $(limit)
    `,
    { limit },
  ) as Promise<DuplicateUnverifiedIdentity[]>
}

async function fetchIdentityHolders(
  qx: QueryExecutor,
  identity: { platform: string; type: MemberIdentityType; value: string },
  decodedValue: string,
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
              AND (
                lower(other.value) = lower($(value))
                OR lower(other.value) = lower($(decodedValue))
              )
            )
        ) AS "onlyIdentity"
      FROM "memberIdentities" mi
      JOIN members m ON m.id = mi."memberId"
      WHERE mi."deletedAt" IS NULL
        AND mi.verified = false
        AND mi.platform = $(platform)
        AND mi.type = $(type)
        AND (
          lower(mi.value) = lower($(value))
          OR lower(mi.value) = lower($(decodedValue))
        )
    `,
    {
      platform: identity.platform,
      type: identity.type,
      value: identity.value,
      decodedValue,
    },
  ) as Promise<IdentityHolder[]>
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
  identity: DuplicateUnverifiedIdentity,
) {
  const decodedValue = canonicalIdentityValue(identity.value)
  const skipActivityMoves =
    isSystemIdentity(identity.value) || isSystemIdentity(decodedValue)

  const plan = await qx.tx(async (tx) => {
    const members = await fetchIdentityHolders(tx, identity, decodedValue)
    if (members.length <= 1) {
      return null
    }

    const singleIdentityMembers = members.filter((member) => member.onlyIdentity)
    let parkingMemberId: string

    if (singleIdentityMembers.length > 0) {
      parkingMemberId = pickOldestMember(singleIdentityMembers).memberId
    } else {
      const source = pickOldestMember(members)
      const parkingMember = await createMember(tx, {
        displayName: getProperDisplayName(source.value),
        manuallyCreated: true,
        reach: { total: -1 },
        joinedAt: new Date().toISOString(),
      })
      parkingMemberId = parkingMember.id

      await moveToNewMember(tx, {
        oldMemberId: source.memberId,
        newMemberId: parkingMemberId,
        platform: identity.platform,
        value: source.value,
        type: identity.type,
      })
    }

    const duplicates = members.filter((member) => member.memberId !== parkingMemberId)

    return {
      allMemberIds: members.map((member) => member.memberId),
      parkingMemberId,
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

  if (!skipActivityMoves) {
    await reassignDuplicateMemberActivities(
      qx,
      plan.parkingMemberId,
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
            AND verified = false
            AND "deletedAt" IS NULL
            AND (
              lower(value) = lower($(value))
              OR lower(value) = lower($(decodedValue))
            )
        `,
        {
          memberIds: duplicateMemberIds,
          platform: identity.platform,
          type: identity.type,
          value: identity.value,
          decodedValue,
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

    for (const memberId of plan.multiIdentityDuplicates.map((member) => member.memberId)) {
      await touchMemberUpdatedAt(tx, memberId)
    }
    await touchMemberUpdatedAt(tx, plan.parkingMemberId)
  })

  log.info(
    {
      platform: identity.platform,
      type: identity.type,
      value: identity.value,
      parkingMemberId: plan.parkingMemberId,
      skipActivityMoves,
      duplicateCount: plan.duplicates.length,
    },
    'Consolidated duplicate unverified identity',
  )
}

async function consolidateIdentityOrSkip(qx: QueryExecutor, identity: DuplicateUnverifiedIdentity) {
  try {
    await consolidateDuplicateIdentity(qx, identity)
    return { consolidated: 1, skipped: 0 }
  } catch (err) {
    log.warn({ err, identity }, 'Skipped duplicate identity consolidation')
    return { consolidated: 0, skipped: 1 }
  }
}

setImmediate(async () => {
  const testRun = parameters.testRun ?? false
  const batchSize = testRun ? TEST_RUN_BATCH_SIZE : BATCH_SIZE

  let consolidated = 0
  let skipped = 0

  try {
    const db = await getDbConnection({
      host: DB_CONFIG.writeHost,
      port: DB_CONFIG.port,
      database: DB_CONFIG.database,
      user: DB_CONFIG.username,
      password: DB_CONFIG.password,
    })
    const qx = pgpQx(db)

    log.info({ testRun, batchSize }, 'Running script')

    let hasMoreDuplicateGroups = true

    while (hasMoreDuplicateGroups) {
      const fetchedGroups = await fetchDuplicateIdentityGroups(qx, batchSize)

      if (fetchedGroups.length === 0) {
        log.info('No duplicate unverified identity groups left')
        hasMoreDuplicateGroups = false
        break
      }

      const identities = mergeDuplicateGroupsByCanonicalValue(fetchedGroups)

      log.info(
        {
          fetchedGroupCount: fetchedGroups.length,
          identityCount: identities.length,
        },
        'Fetched duplicate unverified identity groups',
      )

      const identityGroups = groupIdentitiesWithoutSharedMembers(identities)

      log.info(
        {
          identityCount: identities.length,
          groupCount: identityGroups.length,
          maxGroupSize: identityGroups.reduce((max, group) => Math.max(max, group.length), 0),
        },
        'Grouped identities without shared members',
      )

      let batchConsolidated = 0
      let batchSkipped = 0

      for (const group of identityGroups) {
        const results = await Promise.all(
          group.map((identity) => consolidateIdentityOrSkip(qx, identity)),
        )
        for (const result of results) {
          batchConsolidated += result.consolidated
          batchSkipped += result.skipped
        }
      }

      consolidated += batchConsolidated
      skipped += batchSkipped

      log.info(
        {
          fetchedGroupCount: fetchedGroups.length,
          batchConsolidated,
          batchSkipped,
          consolidated,
          skipped,
        },
        'Batch processed',
      )

      if (testRun) {
        hasMoreDuplicateGroups = false
        break
      }

      if (fetchedGroups.length < batchSize) {
        hasMoreDuplicateGroups = false
        break
      }

      if (batchConsolidated === 0 && batchSkipped > 0) {
        log.warn(
          { batchSkipped },
          'Batch made no progress; stopping to avoid retrying failing groups in a loop',
        )
        hasMoreDuplicateGroups = false
        break
      }
    }

    log.info({ consolidated, skipped }, 'Script finished')
    process.exit(0)
  } catch (err) {
    log.error({ err, consolidated, skipped }, 'Script failed')
    process.exit(1)
  }
})
