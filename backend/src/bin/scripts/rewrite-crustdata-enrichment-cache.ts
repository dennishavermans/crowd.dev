import commandLineArgs from 'command-line-args'

import { redactNullByte } from '@crowd/common'
import { pgpQx } from '@crowd/data-access-layer'
import { getDbConnection } from '@crowd/data-access-layer/src/database'
import { chunkArray } from '@crowd/data-access-layer/src/old/apps/merge_suggestions_worker/utils'
import { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'
import { getServiceLogger } from '@crowd/logging'
import { MemberEnrichmentSource } from '@crowd/types'

import { DB_CONFIG } from '@/conf'

const log = getServiceLogger()
const SOURCE = MemberEnrichmentSource.CRUSTDATA
const UPDATE_CONCURRENCY = 50

const options = [
  {
    name: 'testRun',
    alias: 't',
    type: Boolean,
    description: 'Process one small batch (10 rows) with real writes, then stop.',
  },
  {
    name: 'help',
    alias: 'h',
    type: Boolean,
    description: 'Print this usage guide.',
  },
]

const parameters = commandLineArgs(options)

function mapEmployer(employer: {
  employer_name?: string
  employer_linkedin_id?: string
  employee_title?: string
  start_date?: string
  end_date?: string
}) {
  return {
    name: employer.employer_name,
    title: employer.employee_title,
    professional_network_id: employer.employer_linkedin_id,
    start_date: employer.start_date ?? null,
    end_date: employer.end_date ?? null,
  }
}

function mapProfile(old: Record<string, any>) {
  const skills = old.skills
    ? (Array.isArray(old.skills) ? old.skills : String(old.skills).split(','))
        .map((s: string) => s.trim())
        .filter(Boolean)
    : []

  const githubLogin = old.github_profiles?.[0]?.login

  const mapped: Record<string, unknown> = {
    basic_profile: {
      name: old.name,
      current_title: old.title,
      headline: old.headline,
      summary: old.summary,
      languages: old.languages || [],
      profile_picture_permalink: old.profile_picture_permalink,
    },
    social_handles: {
      ...(old.linkedin_flagship_url
        ? {
            professional_network_identifier: {
              profile_url: old.linkedin_flagship_url,
            },
          }
        : {}),
      ...(old.twitter_handle
        ? {
            twitter_identifier: {
              slug: old.twitter_handle,
            },
          }
        : {}),
      ...(githubLogin
        ? {
            // Crustdata "dev_platform" = GitHub
            dev_platform_identifier: {
              profile_url: `https://github.com/${githubLogin}`,
            },
          }
        : {}),
    },
    professional_network: {
      connections: old.num_of_connections,
      profile_picture_url: old.profile_picture_url,
      profile_picture_permalink: old.profile_picture_permalink,
    },
    skills: {
      professional_network_skills: skills,
    },
    education: {
      schools: (old.all_schools || []).map((school: string) => ({ school })),
    },
    experience: {
      employment_details: {
        current: (old.current_employers || []).map(mapEmployer),
        past: (old.past_employers || []).map(mapEmployer),
      },
    },
  }

  if (old.metadata) {
    mapped.metadata = old.metadata
  }

  return mapped
}

// Flat old cache: top-level `name`, no `basic_profile` (object or array-of-profiles).
const FLAT_CACHE_SHAPE_SQL = `
  (
    (
      jsonb_typeof(data) = 'object'
      AND data ? 'name'
      AND NOT (data ? 'basic_profile')
    )
    OR (
      jsonb_typeof(data) = 'array'
      AND jsonb_array_length(data) > 0
      AND (data->0) ? 'name'
      AND NOT ((data->0) ? 'basic_profile')
    )
  )
`

async function countRows(qx: QueryExecutor): Promise<number> {
  const row = await qx.selectOne(
    `
      SELECT COUNT(*)::int AS count
      FROM "memberEnrichmentCache"
      WHERE source = $(source)
        AND data IS NOT NULL
        AND ${FLAT_CACHE_SHAPE_SQL}
    `,
    { source: SOURCE },
  )
  return row.count
}

async function fetchRows(
  qx: QueryExecutor,
  limit: number,
  afterMemberId?: string,
): Promise<{
  rows: Array<{ memberId: string; data: any }>
  nextAfterMemberId?: string
  exhausted: boolean
}> {
  // Index-first page via PK, then drop already-rewritten rows in SQL.
  // Putting the jsonb predicate in the base WHERE causes a seq scan.
  const scanLimit = Math.max(limit * 2, limit + 100)

  const rows = await qx.select(
    `
      WITH candidates AS MATERIALIZED (
        SELECT "memberId", data
        FROM "memberEnrichmentCache"
        WHERE source = $(source)
          AND data IS NOT NULL
          ${afterMemberId ? 'AND "memberId" > $(afterMemberId)' : ''}
        ORDER BY "memberId"
        LIMIT $(scanLimit)
      ),
      meta AS (
        SELECT COUNT(*)::int AS "candidateCount", MAX("memberId") AS "pageLastMemberId"
        FROM candidates
      )
      SELECT
        f."memberId",
        f.data,
        m."candidateCount" AS "_candidateCount",
        m."pageLastMemberId" AS "_pageLastMemberId"
      FROM meta m
      LEFT JOIN LATERAL (
        SELECT c."memberId", c.data
        FROM candidates c
        WHERE ${FLAT_CACHE_SHAPE_SQL}
        ORDER BY c."memberId"
        LIMIT $(limit)
      ) f ON true
    `,
    {
      source: SOURCE,
      limit,
      scanLimit,
      afterMemberId,
    },
  )

  const candidateCount = Number(rows[0]?._candidateCount ?? 0)
  if (candidateCount === 0) {
    return { rows: [], exhausted: true }
  }

  const pageLastMemberId = rows[0]._pageLastMemberId as string
  const flatRows = rows
    .filter((r) => r.memberId)
    .map(({ memberId, data }) => ({ memberId, data }))

  return {
    rows: flatRows,
    // Full flat page: resume after last flat. Partial/empty: advance past the whole candidate window.
    nextAfterMemberId:
      flatRows.length === limit ? flatRows[flatRows.length - 1].memberId : pageLastMemberId,
    exhausted: false,
  }
}

async function updateCacheData(qx: QueryExecutor, memberId: string, data: unknown): Promise<void> {
  await qx.selectNone(
    `
      UPDATE "memberEnrichmentCache"
      SET
        data = $(data)::jsonb,
        "updatedAt" = NOW()
      WHERE "memberId" = $(memberId)
        AND source = $(source)
    `,
    {
      memberId,
      source: SOURCE,
      data: redactNullByte(JSON.stringify(data)),
    },
  )
}

setImmediate(async () => {
  if (parameters.help) {
    log.info('Usage: pnpm run script:rewrite-crustdata-enrichment-cache [--testRun|-t]')
    process.exit(0)
  }

  const testRun = parameters.testRun ?? false
  const BATCH_SIZE = testRun ? 10 : 500

  const db = await getDbConnection({
    host: DB_CONFIG.writeHost,
    port: DB_CONFIG.port,
    database: DB_CONFIG.database,
    user: DB_CONFIG.username,
    password: DB_CONFIG.password,
  })
  const qx = pgpQx(db)

  const total = await countRows(qx)

  log.info(
    { testRun, BATCH_SIZE, total },
    'Rewriting crustdata enrichment cache to nested person_data shape!',
  )

  let afterMemberId: string | undefined
  let totalUpdated = 0
  let batch = 0
  let hasMore = true

  while (hasMore) {
    const page = await fetchRows(qx, BATCH_SIZE, afterMemberId)
    if (page.exhausted) {
      hasMore = false
    } else {
      afterMemberId = page.nextAfterMemberId

      if (page.rows.length > 0) {
        batch += 1

        for (const chunk of chunkArray(page.rows, UPDATE_CONCURRENCY)) {
          await Promise.all(
            chunk.map(async (row) => {
              const profiles = Array.isArray(row.data) ? row.data : [row.data]

              // Fail fast if a row is not the flat cache shape this script expects.
              if (profiles.some((p) => !p?.name || p.basic_profile)) {
                throw new Error(`Unexpected crustdata cache shape for member ${row.memberId}`)
              }

              await updateCacheData(qx, row.memberId, profiles.map(mapProfile))

              if (testRun) {
                log.info({ memberId: row.memberId }, 'Updated crustdata cache row!')
              }
            }),
          )
          totalUpdated += chunk.length
        }

        log.info(
          {
            batch,
            batchSize: page.rows.length,
            totalUpdated,
            total,
            afterMemberId,
          },
          'Batch processed!',
        )

        // testRun: one real-write batch, then stop.
        if (testRun) {
          hasMore = false
        }
      }
    }
  }

  log.info({ totalUpdated, total, testRun }, 'Done!')
  process.exit(0)
})
