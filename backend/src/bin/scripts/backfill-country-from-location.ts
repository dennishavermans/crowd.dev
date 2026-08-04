import commandLineArgs from 'command-line-args'

import {
  getAttributeValue,
  getCountry,
  hasAttributeValue,
  setAttributesDefaultValues,
} from '@crowd/common'
import { getDbConnection } from '@crowd/data-access-layer/src/database'
import { chunkArray } from '@crowd/data-access-layer/src/old/apps/merge_suggestions_worker/utils'
import { QueryExecutor, pgpQx } from '@crowd/data-access-layer/src/queryExecutor'
import { getServiceLogger } from '@crowd/logging'
import { IAttributes } from '@crowd/types'

import { DB_CONFIG } from '@/conf'

const log = getServiceLogger()

const BATCH_SIZE = 500
const CONCURRENCY = 50
const TEST_RUN_BATCH_SIZE = 10

const options = [
  {
    name: 'testRun',
    alias: 't',
    type: Boolean,
    description: 'Process a single small batch and exit.',
  },
  {
    name: 'afterMemberId',
    alias: 'a',
    type: String,
    description: 'Resume after this member id (exclusive).',
  },
]

const parameters = commandLineArgs(options)

async function getPriorities(qx: QueryExecutor): Promise<string[]> {
  const row = await qx.selectOneOrNone(
    `SELECT ("attributeSettings" -> 'priorities') AS priorities FROM settings LIMIT 1`,
  )

  if (!row?.priorities?.length) {
    throw new Error('No attribute priority array found in settings')
  }

  return row.priorities as string[]
}

async function findCandidates(qx: QueryExecutor, limit: number, afterMemberId?: string) {
  const cursorClause = afterMemberId ? `AND m.id > $(afterMemberId)` : ''

  return qx.select(
    `
      SELECT m.id, m.attributes
      FROM members m
      WHERE m."deletedAt" IS NULL
        ${cursorClause}
        AND (
          NULLIF(TRIM(m.attributes->'location'->>'default'), '') IS NOT NULL
          OR EXISTS (
            SELECT 1
            FROM jsonb_each_text(COALESCE(m.attributes->'location', '{}'::jsonb)) loc
            WHERE loc.key <> 'default'
              AND NULLIF(TRIM(loc.value), '') IS NOT NULL
          )
        )
        AND NOT EXISTS (
          SELECT 1
          FROM jsonb_each_text(COALESCE(m.attributes->'country', '{}'::jsonb)) c
          WHERE NULLIF(TRIM(c.value), '') IS NOT NULL
        )
        AND NOT EXISTS (
          SELECT 1
          FROM "memberEnrichmentCache" mec
          WHERE mec."memberId" = m.id
            AND mec.data IS NOT NULL
        )
      ORDER BY m.id
      LIMIT $(limit)
    `,
    { afterMemberId, limit },
  )
}

async function saveCountry(
  qx: QueryExecutor,
  memberId: string,
  country: IAttributes['country'],
): Promise<boolean> {
  const rowCount = await qx.result(
    `
      UPDATE members
      SET
        attributes = jsonb_set(
          COALESCE(attributes, '{}'::jsonb),
          '{country}',
          $(country)::jsonb,
          true
        ),
        "updatedAt" = NOW()
      WHERE id = $(memberId)
        AND "deletedAt" IS NULL
        AND NOT EXISTS (
          SELECT 1
          FROM jsonb_each_text(COALESCE(attributes->'country', '{}'::jsonb)) c
          WHERE NULLIF(TRIM(c.value), '') IS NOT NULL
        )
    `,
    { memberId, country },
  )

  return rowCount > 0
}

async function backfillMember(
  qx: QueryExecutor,
  member: { id: string; attributes: IAttributes },
  priorities: string[],
  testRun: boolean,
): Promise<'updated' | 'skipped' | 'error'> {
  try {
    if (hasAttributeValue(member.attributes?.country)) {
      return 'skipped'
    }

    const location = getAttributeValue(member.attributes?.location)
    const country = getCountry(location)

    if (!country) {
      if (testRun) {
        log.info({ memberId: member.id, location }, 'No country resolved — skipping')
      }
      return 'skipped'
    }

    const withDefaults = (await setAttributesDefaultValues(
      {
        country: {
          ...member.attributes?.country,
          system: country,
        },
      },
      priorities,
    )) as IAttributes

    if (!withDefaults.country) {
      return 'skipped'
    }

    if (testRun) {
      log.info(
        {
          memberId: member.id,
          location,
          country,
          countryAttr: withDefaults.country,
        },
        'Updating member country',
      )
    }

    const saved = await saveCountry(qx, member.id, withDefaults.country)
    return saved ? 'updated' : 'skipped'
  } catch (err) {
    log.error({ memberId: member.id, err }, 'Failed to backfill country for member')
    return 'error'
  }
}

setImmediate(async () => {
  const testRun = parameters.testRun ?? false
  const pageSize = testRun ? TEST_RUN_BATCH_SIZE : BATCH_SIZE
  let afterMemberId: string | undefined = parameters.afterMemberId ?? undefined

  const db = await getDbConnection({
    host: DB_CONFIG.writeHost,
    port: DB_CONFIG.port,
    database: DB_CONFIG.database,
    user: DB_CONFIG.username,
    password: DB_CONFIG.password,
  })
  const qx = pgpQx(db)

  const priorities = await getPriorities(qx)

  log.info(
    { testRun, pageSize, afterMemberId, CONCURRENCY },
    'Starting country-from-location backfill',
  )

  let processed = 0
  let updated = 0
  let skipped = 0
  let errors = 0
  let hasMore = true

  while (hasMore) {
    const members: { id: string; attributes: IAttributes }[] = await findCandidates(
      qx,
      pageSize,
      afterMemberId,
    )

    if (members.length === 0) {
      hasMore = false
      break
    }

    for (const chunk of chunkArray(members, CONCURRENCY)) {
      const results = await Promise.all(
        chunk.map((member) => backfillMember(qx, member, priorities, testRun)),
      )

      for (const result of results) {
        if (result === 'updated') updated += 1
        else if (result === 'skipped') skipped += 1
        else errors += 1
      }
    }

    processed += members.length
    afterMemberId = members[members.length - 1].id

    log.info(
      { afterMemberId, batchSize: members.length, processed, updated, skipped, errors },
      'Batch processed',
    )

    if (testRun || members.length < pageSize) {
      hasMore = false
    }
  }

  log.info(
    { processed, updated, skipped, errors, afterMemberId },
    'Country-from-location backfill done',
  )
  process.exit(errors > 0 ? 1 : 0)
})
