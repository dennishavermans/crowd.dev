import commandLineArgs from 'command-line-args'

import { generateOrganizationNameVariants } from '@crowd/common'
import {
  type QueryExecutor,
  applyOrganizationAffiliationPolicyToMembers,
  deleteMemberSegmentAffiliations,
  pgpQx,
  updateOrganization,
} from '@crowd/data-access-layer'
import { getDbConnection } from '@crowd/data-access-layer/src/database'
import { getServiceLogger } from '@crowd/logging'
import { Client, WorkflowIdReusePolicy, getTemporalClient } from '@crowd/temporal'
import { TemporalWorkflowId } from '@crowd/types'

import { DB_CONFIG, TEMPORAL_CONFIG } from '@/conf'

const log = getServiceLogger()

const NAME_CHUNK_SIZE = 500
const TEST_RUN_LIMIT = 10

const parameters = commandLineArgs([
  {
    name: 'testRun',
    alias: 't',
    type: Boolean,
    description: 'Actually run, but only process 10 organizations.',
  },
])

type OrganizationMatch = {
  id: string
  displayName: string
}

async function fetchLfSegmentNames(qx: QueryExecutor): Promise<string[]> {
  const rows: { name: string }[] = await qx.select(`
    SELECT name
    FROM segments
    WHERE "isLF" = true
  `)

  return rows.map((row) => row.name)
}

async function findUnblockedOrgsByNames(
  qx: QueryExecutor,
  names: string[],
): Promise<OrganizationMatch[]> {
  const matches: OrganizationMatch[] = []

  for (let i = 0; i < names.length; i += NAME_CHUNK_SIZE) {
    const rows: OrganizationMatch[] = await qx.select(
      `
        SELECT id, "displayName"
        FROM organizations
        WHERE "deletedAt" IS NULL
          AND "isAffiliationBlocked" = false
          AND trim(lower("displayName")) IN ($(names:csv))
      `,
      { names: names.slice(i, i + NAME_CHUNK_SIZE) },
    )

    matches.push(...rows)
  }

  const byId = new Map(matches.map((org) => [org.id, org]))
  return [...byId.values()].sort((a, b) => a.id.localeCompare(b.id))
}

async function blockOrganization(
  qx: QueryExecutor,
  temporal: Client,
  org: OrganizationMatch,
): Promise<void> {
  log.info({ organizationId: org.id, displayName: org.displayName }, 'Blocking affiliation')

  const organizationId = await updateOrganization(qx, org.id, { isAffiliationBlocked: true })
  if (!organizationId) {
    log.warn({ organizationId: org.id }, 'Organization was not updated, skipping')
    return
  }

  await applyOrganizationAffiliationPolicyToMembers(qx, organizationId, false)
  await deleteMemberSegmentAffiliations(qx, { organizationId })

  await temporal.workflow.start('organizationUpdate', {
    taskQueue: 'profiles',
    workflowId: `${TemporalWorkflowId.ORGANIZATION_UPDATE}/${organizationId}`,
    workflowIdReusePolicy: WorkflowIdReusePolicy.WORKFLOW_ID_REUSE_POLICY_TERMINATE_IF_RUNNING,
    retry: {
      maximumAttempts: 10,
    },
    args: [
      {
        organization: { id: organizationId },
        recalculateAffiliations: true,
        syncOptions: { doSync: true },
      },
    ],
  })
}

setImmediate(async () => {
  const testRun = Boolean(parameters.testRun)

  const db = await getDbConnection({
    host: DB_CONFIG.writeHost,
    port: DB_CONFIG.port,
    database: DB_CONFIG.database,
    user: DB_CONFIG.username,
    password: DB_CONFIG.password,
  })
  const qx = pgpQx(db)
  const temporal = await getTemporalClient(TEMPORAL_CONFIG)

  const segmentNames = await fetchLfSegmentNames(qx)
  const variantNames = [
    ...new Set(segmentNames.flatMap((name) => generateOrganizationNameVariants(name))),
  ]
  const organizations = await findUnblockedOrgsByNames(qx, variantNames)
  const selected = testRun ? organizations.slice(0, TEST_RUN_LIMIT) : organizations

  log.info(
    {
      testRun,
      variants: variantNames.length,
      matched: organizations.length,
      processing: selected.length,
    },
    'Blocking unblocked orgs that match LF project names',
  )

  for (const org of selected) {
    await blockOrganization(qx, temporal, org)
  }

  process.exit(0)
})
