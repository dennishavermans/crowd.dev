/* eslint-disable no-console */

/* eslint-disable no-continue */

/**
 * DE-1017: podman-container-tools moved from github.com/containers into its own
 * dedicated org, github.com/podman-container-tools, which also introduced repos
 * we don't track yet (container-libs, image_build, podman-machine-os, automation).
 *
 * This script:
 * 1. Updates insightsProjects.github for the podman-container-tools project to the new org URL.
 * 2. Renames the 3 existing public.repositories rows (podman, buildah, skopeo) from the old
 *    containers/* URLs to the new podman-container-tools/* URLs, and resets their git
 *    processing state so the git worker re-onboards them from the new location.
 * 3. Inserts 4 new public.repositories rows for repos not tracked before (container-libs,
 *    image_build, podman-machine-os, automation), reusing the segment/integration ids of
 *    the existing sibling repos.
 *
 * Safe to re-run: renames are keyed off the old URL (no-op once already renamed), and new
 * repos are only inserted if a row with that URL doesn't already exist.
 *
 * Usage:
 *   LOG_LEVEL=debug SERVICE=script ./node_modules/.bin/tsx src/bin/scripts/insightsProjects/fix-podman-container-tools-repos.ts --dryRun
 *   LOG_LEVEL=debug SERVICE=script ./node_modules/.bin/tsx src/bin/scripts/insightsProjects/fix-podman-container-tools-repos.ts
 */
import commandLineArgs from 'command-line-args'
import commandLineUsage from 'command-line-usage'

import { databaseInit } from '@/database/databaseConnection'
import { IRepositoryOptions } from '@/database/repositories/IRepositoryOptions'
import SequelizeRepository from '@/database/repositories/sequelizeRepository'

const INSIGHTS_PROJECT_ID = 'c3460c30-f9fd-419c-8fb2-62fa4616cc16'
const OLD_ORG = 'https://github.com/containers'
const NEW_ORG = 'https://github.com/podman-container-tools'

// Repos that already exist under the old org and just need their URL renamed.
const REPOS_TO_RENAME = ['podman', 'buildah', 'skopeo']

// Repos in the new org that aren't tracked at all yet.
const REPOS_TO_ADD = ['container-libs', 'image_build', 'podman-machine-os', 'automation']

const options = [
  {
    name: 'help',
    alias: 'h',
    type: Boolean,
    description: 'Print this usage guide.',
  },
  {
    name: 'dryRun',
    alias: 'd',
    type: Boolean,
    description: 'Dry run mode. Will not write anything. Will print the changes to be made.',
  },
]

const sections = [
  {
    header: 'Fix podman-container-tools Repos (DE-1017)',
    content:
      'Moves the podman-container-tools project and its repos from github.com/containers to the new github.com/podman-container-tools org, and adds the newly split-out repos.',
  },
  {
    header: 'Options',
    optionList: options,
  },
]

async function updateProjectGithubOrg(qx, dryRun: boolean) {
  const project = await qx.result(
    `SELECT id, slug, github FROM "insightsProjects" WHERE id = $1`,
    [INSIGHTS_PROJECT_ID],
  )

  if (project.rows.length === 0) {
    throw new Error(`insightsProjects row ${INSIGHTS_PROJECT_ID} not found`)
  }

  const current = project.rows[0]
  console.log(`Found project "${current.slug}" with github = ${current.github}`)

  if (current.github === NEW_ORG) {
    console.log('Project github org already up to date, skipping')
    return
  }

  if (dryRun) {
    console.log(`Would update project github org: ${current.github} -> ${NEW_ORG}`)
    return
  }

  await qx.result(
    `UPDATE "insightsProjects" SET github = $1, "updatedAt" = NOW() WHERE id = $2`,
    [NEW_ORG, INSIGHTS_PROJECT_ID],
  )
  console.log(`Updated project github org: ${current.github} -> ${NEW_ORG}`)
}

async function renameExistingRepos(qx, dryRun: boolean) {
  let renamedCount = 0

  for (const name of REPOS_TO_RENAME) {
    const oldUrl = `${OLD_ORG}/${name}`
    const newUrl = `${NEW_ORG}/${name}`

    const existing = await qx.result(
      `SELECT id, url FROM public.repositories WHERE "insightsProjectId" = $1 AND url = $2`,
      [INSIGHTS_PROJECT_ID, oldUrl],
    )

    if (existing.rows.length === 0) {
      const alreadyRenamed = await qx.result(
        `SELECT id FROM public.repositories WHERE "insightsProjectId" = $1 AND url = $2`,
        [INSIGHTS_PROJECT_ID, newUrl],
      )
      if (alreadyRenamed.rows.length > 0) {
        console.log(`Repo already renamed, skipping: ${oldUrl} -> ${newUrl}`)
      } else {
        console.log(`No repo found at ${oldUrl}, skipping`)
      }
      continue
    }

    const repoId = existing.rows[0].id

    if (dryRun) {
      console.log(`Would rename repo: ${oldUrl} -> ${newUrl}`)
      continue
    }

    await qx.result(
      `UPDATE public.repositories
       SET url = $1, "updatedAt" = NOW()
       WHERE id = $2`,
      [newUrl, repoId],
    )

    // Reset git processing state so the worker re-onboards from the new URL.
    await qx.result(
      `UPDATE git."repositoryProcessing"
       SET
         "lastProcessedAt" = NULL,
         "lastProcessedCommit" = NULL,
         "lastMaintainerRunAt" = NULL,
         branch = NULL,
         "lockedAt" = NULL,
         state = 'pending',
         "updatedAt" = NOW()
       WHERE "repositoryId" = $1`,
      [repoId],
    )

    renamedCount++
    console.log(`Renamed repo: ${oldUrl} -> ${newUrl}`)
  }

  console.log(`Renamed ${renamedCount}/${REPOS_TO_RENAME.length} repos`)
}

async function insertNewRepos(qx, dryRun: boolean) {
  // Reuse segmentId/gitIntegrationId/sourceIntegrationId from an existing sibling repo
  // in this project - new repos belong to the same segment/integration going forward.
  const sibling = await qx.result(
    `SELECT "segmentId", "gitIntegrationId", "sourceIntegrationId"
     FROM public.repositories
     WHERE "insightsProjectId" = $1 AND "deletedAt" IS NULL
     LIMIT 1`,
    [INSIGHTS_PROJECT_ID],
  )

  if (sibling.rows.length === 0) {
    throw new Error(
      `No existing repositories row found for project ${INSIGHTS_PROJECT_ID} to source segmentId/gitIntegrationId/sourceIntegrationId from`,
    )
  }

  const { segmentId, gitIntegrationId, sourceIntegrationId } = sibling.rows[0]
  console.log(
    `Using segmentId=${segmentId}, gitIntegrationId=${gitIntegrationId}, sourceIntegrationId=${sourceIntegrationId} for new repos`,
  )

  let insertedCount = 0

  for (const name of REPOS_TO_ADD) {
    const url = `${NEW_ORG}/${name}`

    const existing = await qx.result(`SELECT id FROM public.repositories WHERE url = $1`, [url])

    if (existing.rows.length > 0) {
      console.log(`Repo already exists, skipping: ${url}`)
      continue
    }

    if (dryRun) {
      console.log(`Would insert new repo: ${url}`)
      continue
    }

    const inserted = await qx.result(
      `INSERT INTO public.repositories (
         id, url, "segmentId", "gitIntegrationId", "sourceIntegrationId",
         "insightsProjectId", archived, "forkedFrom", excluded, enabled,
         "createdAt", "updatedAt"
       ) VALUES (
         uuid_generate_v4(), $1, $2, $3, $4,
         $5, false, NULL, false, true,
         NOW(), NOW()
       )
       RETURNING id`,
      [url, segmentId, gitIntegrationId, sourceIntegrationId, INSIGHTS_PROJECT_ID],
    )

    const repoId = inserted.rows[0].id

    await qx.result(
      `INSERT INTO git."repositoryProcessing" ("repositoryId", "createdAt", "updatedAt")
       VALUES ($1, NOW(), NOW())`,
      [repoId],
    )

    insertedCount++
    console.log(`Inserted new repo: ${url}`)
  }

  console.log(`Inserted ${insertedCount}/${REPOS_TO_ADD.length} new repos`)
}

const usage = commandLineUsage(sections)
const parameters = commandLineArgs(options)

if (parameters.help) {
  console.log(usage)
} else {
  setImmediate(async () => {
    try {
      const dryRun = parameters.dryRun || false
      if (dryRun) {
        console.log('Running in dry-run mode - no changes will be made\n')
      }

      const prodDb = await databaseInit()
      const qx = SequelizeRepository.getQueryExecutor({
        database: prodDb,
      } as IRepositoryOptions)

      await updateProjectGithubOrg(qx, dryRun)
      await renameExistingRepos(qx, dryRun)
      await insertNewRepos(qx, dryRun)

      console.log('\npodman-container-tools repo fix completed successfully')
      process.exit(0)
    } catch (error) {
      console.error('Error while fixing podman-container-tools repos:', error)
      process.exit(1)
    }
  })
}
