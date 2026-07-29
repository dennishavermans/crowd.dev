import commandLineArgs from 'command-line-args'

import { normalizeMemberIdentityValue } from '@crowd/common'
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

type MixedCaseEmailIdentity = {
  id: string
  memberId: string
  platform: string
  type: string
  value: string
}

const AR_UPDATE_BATCH_SIZE = 5000
const LOAD_BATCH_SIZE = 500

async function findMixedCaseEmailIdentities(
  qx: QueryExecutor,
  afterId: string | null,
  limit: number,
): Promise<MixedCaseEmailIdentity[]> {
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

async function lowercaseIdentityValue(qx: QueryExecutor, id: string, value: string): Promise<number> {
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

async function rewriteActivityRelationUsernamesToLower(
  qx: QueryExecutor,
  memberId: string,
  platform: string,
  lowerValue: string,
): Promise<{ usernameRows: number; objectMemberUsernameRows: number }> {
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

setImmediate(async () => {
  const testRun = parameters.testRun ?? false
  const PROCESS_BATCH_LOG_EVERY = testRun ? 1 : 200
  const batchSize = testRun ? 10 : LOAD_BATCH_SIZE

  const db = await getDbConnection({
    host: DB_CONFIG.writeHost,
    port: DB_CONFIG.port,
    database: DB_CONFIG.database,
    user: DB_CONFIG.username,
    password: DB_CONFIG.password,
  })

  const qx = pgpQx(db)

  log.info({ testRun, batchSize }, 'Lowercasing email-shaped identity values!')

  let afterId: string | null = null
  let processed = 0
  let identitiesUpdated = 0
  let skipped = 0
  let arUsernameUpdated = 0
  let arObjectUsernameUpdated = 0

  for (;;) {
    const candidates = await findMixedCaseEmailIdentities(qx, afterId, batchSize)
    if (candidates.length === 0) {
      break
    }

    afterId = candidates[candidates.length - 1].id

    for (const row of candidates) {
      const normalized = normalizeMemberIdentityValue(row.value)

      if (normalized !== row.value) {
        if (testRun) {
          log.info(
            {
              memberId: row.memberId,
              platform: row.platform,
              type: row.type,
              from: row.value,
              to: normalized,
            },
            'Lowercasing email-shaped identity!',
          )
        }

        const { updatedCount, ar } = await qx.tx(async (tx) => {
          const updatedCount = await lowercaseIdentityValue(tx, row.id, normalized)
          const ar = await rewriteActivityRelationUsernamesToLower(
            tx,
            row.memberId,
            row.platform,
            normalized,
          )
          return { updatedCount, ar }
        })

        identitiesUpdated += updatedCount
        arUsernameUpdated += ar.usernameRows
        arObjectUsernameUpdated += ar.objectMemberUsernameRows
        processed += 1

        if (processed % PROCESS_BATCH_LOG_EVERY === 0) {
          log.info(
            {
              processed,
              identitiesUpdated,
              skipped,
              arUsernameUpdated,
              arObjectUsernameUpdated,
            },
            'Progress!',
          )
        }
      } else {
        skipped += 1
      }
    }

    if (testRun) {
      log.info('Test run - stopping after first batch!')
      break
    }
  }

  log.info(
    {
      processed,
      identitiesUpdated,
      skipped,
      arUsernameUpdated,
      arObjectUsernameUpdated,
    },
    'Done!',
  )

  process.exit(0)
})
