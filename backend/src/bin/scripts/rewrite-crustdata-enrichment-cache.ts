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
    description: 'Run in test mode (limit to 1 batch and 10 members).',
  },
  {
    name: 'afterMemberId',
    alias: 'a',
    type: String,
    description: 'The member ID to start processing after.',
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

async function fetchRows(
  qx: QueryExecutor,
  limit: number,
  afterMemberId?: string,
): Promise<Array<{ memberId: string; data: any }>> {
  return qx.select(
    `
      SELECT "memberId", data
      FROM "memberEnrichmentCache"
      WHERE source = $(source)
        AND data IS NOT NULL
        ${afterMemberId ? 'AND "memberId" > $(afterMemberId)' : ''}
      ORDER BY "memberId"
      LIMIT $(limit)
    `,
    {
      source: SOURCE,
      limit,
      afterMemberId,
    },
  )
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
  const testRun = parameters.testRun ?? false
  const BATCH_SIZE = testRun ? 10 : 500
  let afterMemberId = parameters.afterMemberId ?? undefined

  const db = await getDbConnection({
    host: DB_CONFIG.writeHost,
    port: DB_CONFIG.port,
    database: DB_CONFIG.database,
    user: DB_CONFIG.username,
    password: DB_CONFIG.password,
  })

  const qx = pgpQx(db)

  log.info({ testRun, BATCH_SIZE, afterMemberId }, 'Running script with the following parameters!')

  let hasMore = true
  let totalUpdated = 0
  let totalSkipped = 0

  while (hasMore) {
    const rows = await fetchRows(qx, BATCH_SIZE, afterMemberId)

    if (rows.length > 0) {
      for (const chunk of chunkArray(rows, UPDATE_CONCURRENCY)) {
        const results = await Promise.all(
          chunk.map(async (row) => {
            const profiles = Array.isArray(row.data) ? row.data : [row.data]

            if (profiles[0]?.basic_profile) {
              return 'skipped' as const
            }

            await updateCacheData(qx, row.memberId, profiles.map(mapProfile))

            if (testRun) {
              log.info({ memberId: row.memberId }, 'Updated crustdata cache row!')
            }

            return 'updated' as const
          }),
        )

        totalUpdated += results.filter((r) => r === 'updated').length
        totalSkipped += results.filter((r) => r === 'skipped').length
      }

      afterMemberId = rows[rows.length - 1].memberId

      log.info(
        { afterMemberId, batchSize: rows.length, totalUpdated, totalSkipped },
        'Batch processed!',
      )

      if (testRun || rows.length < BATCH_SIZE) {
        hasMore = false
      }
    } else {
      hasMore = false
    }
  }

  log.info({ totalUpdated, totalSkipped, afterMemberId, testRun }, 'Done!')
  process.exit(0)
})
