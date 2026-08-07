import commandLineArgs from 'command-line-args'

import { getCountry } from '@crowd/common'
import { getDbConnection } from '@crowd/data-access-layer/src/database'
import { chunkArray } from '@crowd/data-access-layer/src/old/apps/merge_suggestions_worker/utils'
import { QueryExecutor, pgpQx } from '@crowd/data-access-layer/src/queryExecutor'
import { getServiceLogger } from '@crowd/logging'
import { OrganizationAttributeSource } from '@crowd/types'

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
]

const parameters = commandLineArgs(options)

async function findCandidates(
  qx: QueryExecutor,
  limit: number,
  afterOrgId?: string,
): Promise<{ id: string; location: string }[]> {
  return qx.select(
    `
      select o.id, o.location
      from organizations o
      where o."deletedAt" is null
        and ($(afterOrgId)::uuid is null or o.id > $(afterOrgId))
        and nullif(btrim(o.location), '') is not null
        and nullif(btrim(o.country), '') is null
        and not exists (
          select 1
          from "orgAttributes" oa
          where oa."organizationId" = o.id
            and oa.name = 'country'
        )
      order by o.id
      limit $(limit)
    `,
    { afterOrgId: afterOrgId ?? null, limit },
  )
}

async function saveCountry(
  qx: QueryExecutor,
  organizationId: string,
  country: string,
): Promise<boolean> {
  return qx.tx(async (tx) => {
    const rowCount = await tx.result(
      `
        update organizations
        set
          country = $(country),
          "updatedAt" = now()
        where id = $(organizationId)
          and "deletedAt" is null
          and nullif(btrim(country), '') is null
      `,
      { organizationId, country },
    )

    if (rowCount === 0) {
      return false
    }

    await tx.result(
      `
        insert into "orgAttributes" ("organizationId", name, source, "default", value)
        values ($(organizationId), 'country', $(source), false, $(country))
        on conflict ("organizationId", name, source, md5(value)) do nothing
      `,
      {
        organizationId,
        source: OrganizationAttributeSource.SYSTEM,
        country,
      },
    )

    await tx.result(
      `
        update "orgAttributes"
        set "default" = case
          when source = $(source) and value = $(country) then true
          else false
        end
        where "organizationId" = $(organizationId)
          and name = 'country'
      `,
      {
        organizationId,
        source: OrganizationAttributeSource.SYSTEM,
        country,
      },
    )

    return true
  })
}

setImmediate(async () => {
  const testRun = parameters.testRun ?? false
  const pageSize = testRun ? TEST_RUN_BATCH_SIZE : BATCH_SIZE

  const db = await getDbConnection({
    host: DB_CONFIG.writeHost,
    port: DB_CONFIG.port,
    database: DB_CONFIG.database,
    user: DB_CONFIG.username,
    password: DB_CONFIG.password,
  })
  const qx = pgpQx(db)

  log.info({ testRun, pageSize, CONCURRENCY }, 'Starting org country-from-location backfill')

  let afterOrgId: string | undefined
  let processed = 0
  let updated = 0
  let skipped = 0
  let errors = 0
  let hasMore = true

  while (hasMore) {
    const orgs = await findCandidates(qx, pageSize, afterOrgId)

    if (orgs.length === 0) {
      break
    }

    for (const chunk of chunkArray(orgs, CONCURRENCY)) {
      const results = await Promise.all(
        chunk.map(async (org) => {
          try {
            const country = getCountry(org.location)
            if (!country) {
              if (testRun) {
                log.info({ organizationId: org.id, location: org.location }, 'No country resolved')
              }
              return 'skipped' as const
            }

            if (testRun) {
              log.info(
                { organizationId: org.id, location: org.location, country },
                'Updating org country',
              )
            }

            const saved = await saveCountry(qx, org.id, country)
            return saved ? ('updated' as const) : ('skipped' as const)
          } catch (err) {
            log.error({ organizationId: org.id, err }, 'Failed to backfill org country')
            return 'error' as const
          }
        }),
      )

      for (const result of results) {
        if (result === 'updated') updated += 1
        else if (result === 'skipped') skipped += 1
        else errors += 1
      }
    }

    processed += orgs.length
    afterOrgId = orgs[orgs.length - 1].id

    log.info(
      { afterOrgId, batchSize: orgs.length, processed, updated, skipped, errors },
      'Batch processed',
    )

    if (testRun || orgs.length < pageSize) {
      hasMore = false
    }
  }

  log.info({ processed, updated, skipped, errors, afterOrgId }, 'Org country backfill done')
  process.exit(errors > 0 ? 1 : 0)
})
