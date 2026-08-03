import commandLineArgs from 'command-line-args'
import merge from 'lodash/merge'

import { setAttributesDefaultValues } from '@crowd/common'
import { pgpQx } from '@crowd/data-access-layer'
import { getDbConnection } from '@crowd/data-access-layer/src/database'
import { chunkArray } from '@crowd/data-access-layer/src/old/apps/merge_suggestions_worker/utils'
import { getServiceLogger } from '@crowd/logging'
import { MemberEnrichmentSource } from '@crowd/types'

import { DB_CONFIG } from '@/conf'

const log = getServiceLogger()

const options = [
  {
    name: 'testRun',
    alias: 't',
    type: Boolean,
    description: 'Actual run with a smaller batch size; stops after the first batch.',
  },
  {
    name: 'help',
    alias: 'h',
    type: Boolean,
    description: 'Print this usage guide.',
  },
]

const parameters = commandLineArgs(options)

// Same order as triggerMembersEnrichment
const SOURCES = [
  MemberEnrichmentSource.PROGAI,
  MemberEnrichmentSource.CLEARBIT,
  MemberEnrichmentSource.PROGAI_LINKEDIN_SCRAPER,
  MemberEnrichmentSource.CRUSTDATA,
]

function cleanValue(value: unknown): string | undefined {
  if (typeof value !== 'string') {
    return undefined
  }

  const trimmed = value.trim()
  return trimmed.length > 0 ? trimmed : undefined
}

setImmediate(async () => {
  const testRun = parameters.testRun ?? false
  const BATCH_SIZE = testRun ? 10 : 100
  const PARALLELISM = 10

  const db = await getDbConnection({
    host: DB_CONFIG.writeHost,
    port: DB_CONFIG.port,
    database: DB_CONFIG.database,
    user: DB_CONFIG.username,
    password: DB_CONFIG.password,
  })

  const qx = pgpQx(db)

  const prioritiesResult = await qx.selectOneOrNone(
    `SELECT ("attributeSettings" -> 'priorities') AS priorities FROM settings`,
  )
  const priorities: string[] = prioritiesResult?.priorities || []

  log.info({ testRun, BATCH_SIZE, PARALLELISM }, 'Running script with the following parameters!')

  let afterMemberId: string | undefined
  let processed = 0
  let updated = 0
  let skipped = 0
  let members: {
    id: string
    attributes: Record<string, unknown>
    caches: {
      source: MemberEnrichmentSource
      data: Record<string, unknown> | Record<string, unknown>[]
    }[]
  }[] = []

  do {
    members = await qx.select(
      `
        SELECT
          m.id,
          m.attributes,
          json_agg(
            json_build_object(
              'source', c.source,
              'data', c.data
            )
          ) AS caches
        FROM members m
        INNER JOIN "memberEnrichmentCache" c
          ON c."memberId" = m.id
         AND c.data IS NOT NULL
         AND c.source IN ($(sources:csv))
        WHERE m."deletedAt" IS NULL
          AND (
            nullif(m.attributes->'location'->>'default', '') IS NULL
            OR nullif(m.attributes->'country'->>'default', '') IS NULL
          )
          AND ($(afterMemberId) IS NULL OR m.id > $(afterMemberId))
        GROUP BY m.id
        ORDER BY m.id
        LIMIT $(limit)
      `,
      {
        limit: BATCH_SIZE,
        afterMemberId: afterMemberId ?? null,
        sources: SOURCES,
      },
    )

    if (members.length === 0) {
      break
    }

    afterMemberId = members[members.length - 1].id

    for (const chunk of chunkArray(members, PARALLELISM)) {
      const results = await Promise.all(
        chunk.map(async (member) => {
          try {
            const existingLocation = cleanValue(
              (member.attributes?.location as Record<string, unknown> | undefined)?.default,
            )
            const existingCountry = cleanValue(
              (member.attributes?.country as Record<string, unknown> | undefined)?.default,
            )

            const cachesBySource = new Map(
              (member.caches || []).map((cache) => [cache.source, cache.data]),
            )

            // Walk sources in priority order. Prefer a source with both fields;
            // otherwise use the first source that has any geo. Never mix fields
            // across sources (prod has no complementary splits).
            let picked: { location?: string; country?: string } | null = null

            for (const source of SOURCES) {
              const data = cachesBySource.get(source)

              if (data) {
                const profiles = Array.isArray(data)
                  ? data.filter((item) => item && typeof item === 'object')
                  : [data]

                for (const profile of profiles) {
                  const geoObj =
                    profile.geo && typeof profile.geo === 'object'
                      ? (profile.geo as Record<string, unknown>)
                      : {}

                  const location =
                    source === MemberEnrichmentSource.CRUSTDATA
                      ? cleanValue(profile.employee_location) || cleanValue(profile.location)
                      : cleanValue(profile.location)

                  const country =
                    source === MemberEnrichmentSource.CLEARBIT
                      ? cleanValue(geoObj.country)
                      : cleanValue(profile.country) || cleanValue(geoObj.country)

                  if (location && country) {
                    picked = { location, country }
                    break
                  }

                  if ((location || country) && !picked) {
                    picked = { location, country }
                  }
                }
              }

              if (picked?.location && picked?.country) {
                break
              }
            }

            const locationToSet = existingLocation ? undefined : picked?.location
            const countryToSet = existingCountry ? undefined : picked?.country

            if (!locationToSet && !countryToSet) {
              if (testRun) {
                log.info({ memberId: member.id }, 'No usable geo in enrichment cache, skipping!')
              }
              return 'skipped' as const
            }

            const attributePatch: Record<string, unknown> = {}
            if (locationToSet) {
              attributePatch.location = { enrichment: locationToSet }
            }
            if (countryToSet) {
              attributePatch.country = { enrichment: countryToSet }
            }

            const attributes = merge({}, member.attributes, attributePatch)
            const withDefaults = await setAttributesDefaultValues(attributes, priorities)

            await qx.result(
              `
                UPDATE members
                SET
                  attributes = $(attributes)::jsonb,
                  "updatedAt" = NOW()
                WHERE id = $(memberId)
              `,
              {
                memberId: member.id,
                attributes: withDefaults,
              },
            )

            if (testRun) {
              log.info(
                {
                  memberId: member.id,
                  location: locationToSet,
                  country: countryToSet,
                },
                'Updated member geo from enrichment cache!',
              )
            }

            return 'updated' as const
          } catch (err) {
            log.error({ memberId: member.id, err }, 'Failed to update member geo!')
            throw err
          }
        }),
      )

      processed += results.length
      updated += results.filter((r) => r === 'updated').length
      skipped += results.filter((r) => r === 'skipped').length
    }

    log.info(
      {
        afterMemberId,
        batchSize: members.length,
        processed,
        updated,
        skipped,
      },
      'Batch processed!',
    )

    if (testRun) {
      log.info('Test run - stopping after first batch!')
      break
    }
  } while (members.length > 0)

  log.info({ processed, updated, skipped }, 'Done!')
  process.exit(0)
})
