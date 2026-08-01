/**
 * Soft-delete same-member identity case variants so unique indexes on lower(value) can be created.
 *
 * Keep one row per (memberId, platform, type, lower(value)); soft-delete the rest.
 * Keep rule: verified > unverified; verifiedBy set; then newer updatedAt; then id.
 * Rewrites activityRelations.username / objectMemberUsername to the keeper value.
 *
 * Cross-member verified↔verified pairs are out of scope — run
 * findAndMergeMembersWithSamePlatformIdentitiesDifferentCapitalization for those.
 *
 * Usage:
 *   pnpm run script:cleanup-same-member-case-variant-identities
 *   pnpm run script:cleanup-same-member-case-variant-identities -- --testRun
 */
import commandLineArgs from 'command-line-args'

import { pgpQx } from '@crowd/data-access-layer'
import { getDbConnection } from '@crowd/data-access-layer/src/database'
import { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'
import { getServiceLogger } from '@crowd/logging'

import { DB_CONFIG } from '@/conf'

const log = getServiceLogger()

const options = [
  {
    name: 'testRun',
    alias: 't',
    type: Boolean,
  },
]

const parameters = commandLineArgs(options)

type CaseVariantGroup = {
  memberId: string
  platform: string
  type: string
  lv: string
}

type IdentityRow = {
  id: string
  value: string
  verified: boolean
  verifiedBy: string | null
  updatedAt: Date
}

const AR_UPDATE_BATCH_SIZE = 5000

function pickKeeper(rows: IdentityRow[]): IdentityRow {
  return [...rows].sort((a, b) => {
    if (a.verified !== b.verified) return a.verified ? -1 : 1
    if (Boolean(a.verifiedBy) !== Boolean(b.verifiedBy)) return a.verifiedBy ? -1 : 1
    const aUpdated = new Date(a.updatedAt).getTime()
    const bUpdated = new Date(b.updatedAt).getTime()
    if (aUpdated !== bUpdated) return bUpdated - aUpdated
    return a.id < b.id ? -1 : 1
  })[0]
}

async function findDuplicateCaseVariantGroups(
  qx: QueryExecutor,
  limit?: number,
): Promise<CaseVariantGroup[]> {
  const baseQuery = `
      select
        "memberId",
        platform,
        type,
        lower(value) as lv
      from "memberIdentities"
      where "deletedAt" is null
      group by "memberId", platform, type, lower(value)
      having count(distinct value) > 1
      order by "memberId", platform, type, lower(value)
  `

  if (limit != null) {
    return qx.select(`${baseQuery} limit $(limit)`, { limit })
  }

  return qx.select(baseQuery)
}

async function fetchGroupIdentities(
  qx: QueryExecutor,
  group: CaseVariantGroup,
): Promise<IdentityRow[]> {
  return qx.select(
    `
      select id, value, verified, "verifiedBy", "updatedAt"
      from "memberIdentities"
      where "memberId" = $(memberId)
        and platform = $(platform)
        and type = $(type)
        and lower(value) = $(lv)
        and "deletedAt" is null
    `,
    group,
  )
}

async function softDeleteIdentities(qx: QueryExecutor, ids: string[]): Promise<number> {
  if (ids.length === 0) return 0

  return qx.result(
    `
      update "memberIdentities"
      set
        "deletedAt" = now(),
        "updatedAt" = now()
      where id in ($(ids:csv))
        and "deletedAt" is null
    `,
    { ids },
  )
}

async function rewriteActivityRelationUsernames(
  qx: QueryExecutor,
  memberId: string,
  platform: string,
  keeperUsername: string,
  deletedUsernames: string[],
): Promise<{ usernameRows: number; objectMemberUsernameRows: number }> {
  const values = deletedUsernames.filter((v) => v !== keeperUsername)
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
          username = $(keeperUsername),
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
        keeperUsername,
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
          "objectMemberUsername" = $(keeperUsername),
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
        keeperUsername,
        values,
        batchSize: AR_UPDATE_BATCH_SIZE,
      },
    )
    objectMemberUsernameRows += updated
  } while (updated === AR_UPDATE_BATCH_SIZE)

  return { usernameRows, objectMemberUsernameRows }
}

setImmediate(async () => {
  try {
    const testRun = parameters.testRun ?? false
    const PROCESS_BATCH_LOG_EVERY = testRun ? 1 : 200

    const db = await getDbConnection({
      host: DB_CONFIG.writeHost,
      port: DB_CONFIG.port,
      database: DB_CONFIG.database,
      user: DB_CONFIG.username,
      password: DB_CONFIG.password,
    })

    const qx = pgpQx(db)

    log.info({ testRun }, 'Running Pass 1 same-member case-variant cleanup!')

    const groups = await findDuplicateCaseVariantGroups(qx, testRun ? 10 : undefined)
    log.info({ groupCount: groups.length }, 'Loaded same-member case-variant groups!')

    let processed = 0
    let softDeleted = 0
    let skipped = 0
    let arUsernameUpdated = 0
    let arObjectUsernameUpdated = 0

    for (const group of groups) {
      const rows = await fetchGroupIdentities(qx, group)

      if (rows.length < 2) {
        skipped += 1
      } else {
        const keeper = pickKeeper(rows)
        const toDeleteRows = rows.filter((r) => r.id !== keeper.id)
        const toDeleteIds = toDeleteRows.map((r) => r.id)
        const deletedValues = toDeleteRows.map((r) => r.value)

        if (testRun) {
          log.info(
            {
              memberId: group.memberId,
              platform: group.platform,
              type: group.type,
              keep: keeper.value,
              softDelete: deletedValues,
            },
            'Soft-deleting case variants!',
          )
        }

        const { deletedCount, ar } = await qx.tx(async (tx) => {
          const deletedCount = await softDeleteIdentities(tx, toDeleteIds)
          const ar = await rewriteActivityRelationUsernames(
            tx,
            group.memberId,
            group.platform,
            keeper.value,
            deletedValues,
          )
          return { deletedCount, ar }
        })

        softDeleted += deletedCount
        arUsernameUpdated += ar.usernameRows
        arObjectUsernameUpdated += ar.objectMemberUsernameRows

        processed += 1

        if (processed % PROCESS_BATCH_LOG_EVERY === 0) {
          log.info(
            {
              processed,
              total: groups.length,
              softDeleted,
              skipped,
              arUsernameUpdated,
              arObjectUsernameUpdated,
            },
            'Progress!',
          )
        }
      }
    }

    log.info(
      {
        processed,
        softDeleted,
        skipped,
        arUsernameUpdated,
        arObjectUsernameUpdated,
      },
      'Done!',
    )

    process.exit(0)
  } catch (err) {
    log.error(err, 'Cleanup failed')
    process.exit(1)
  }
})
