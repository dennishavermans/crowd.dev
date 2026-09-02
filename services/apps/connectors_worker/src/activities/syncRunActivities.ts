import { Context } from '@temporalio/activity'

import {
  ConnectorError,
  createEmit,
  createHttpClient,
  createTokenPool,
  findSync,
  getCredential,
  getManifest,
  getSync,
} from '@crowd/connectors'
import type { Emitter, SyncContext } from '@crowd/connectors'
import {
  getUnitById,
  parkUnit,
  recordRunFailure,
  recordRunPartial,
  recordRunSuccess,
} from '@crowd/data-access-layer/src/connectors'
import { fetchIntegrationById } from '@crowd/data-access-layer/src/integrations'
import IntegrationStreamRepository from '@crowd/data-access-layer/src/old/apps/integration_stream_worker/integrationStream.repo'
import { dbStoreQx } from '@crowd/data-access-layer/src/queryExecutor'
import { getChildLogger } from '@crowd/logging'

import { svc } from '../main'
import { RUN_BUDGET_MS } from '../runLimits'
import { cadenceRunAt, failureRunAt, shortDeferRunAt } from '../scheduling'

const DEAD_LETTER_AFTER = 5
const HEARTBEAT_INTERVAL_MS = 10_000
const RATE_LIMIT_FALLBACK_MS = 60_000
const UNAVAILABLE_PARK_MS = 300_000

const PARKED_ERROR_CLASSES = ['provider.rate_limit', 'provider.unavailable'] as const

export async function executeSync(unitId: string): Promise<void> {
  const qx = dbStoreQx(svc.postgres.writer)

  const unit = await getUnitById(qx, unitId)
  if (!unit) {
    throw new Error(`sync unit ${unitId} not found`)
  }

  const activityContext = Context.current()
  const log = getChildLogger('syncRun', svc.log, {
    runId: activityContext.info.workflowExecution.runId,
    unitId: unit.id,
    platform: unit.platform,
    syncName: unit.syncName,
    channelName: unit.channelName,
  })

  const heartbeat = setInterval(() => {
    try {
      activityContext.heartbeat()
    } catch (err) {
      log.warn({ errMsg: (err as Error).message }, 'heartbeat failed')
    }
  }, HEARTBEAT_INTERVAL_MS)

  let emitter: Emitter | null = null
  let committedWatermark = unit.watermark
  const runDeadline = Date.now() + RUN_BUDGET_MS

  try {
    const integration = await fetchIntegrationById(qx, unit.integrationId)
    if (!integration?.segmentId) {
      throw new Error(`integration ${unit.integrationId} not found or has no segmentId`)
    }
    if (!svc.dataSinkWorkerEmitter) {
      throw new Error('data sink worker emitter not initialized')
    }

    const manifest = getManifest(unit.platform)
    const pool = createTokenPool(svc.redis, unit.platform, unit.integrationId, {
      probeBudget: manifest.probeBudget,
    })
    if (manifest.seedTokens) {
      const credential = await getCredential(qx, unit.integrationId)
      await manifest.seedTokens(credential, pool)
    }
    const http = createHttpClient({
      acquireToken: pool.acquire,
      parkToken: pool.park,
      quarantineToken: pool.quarantine,
      interpretResponse: manifest.interpretResponse,
      log,
    })

    const sync = getSync(unit.platform, unit.syncName)

    const streamRepo = new IntegrationStreamRepository(svc.postgres.writer, log)
    emitter = createEmit({
      publishResult: streamRepo.publishExternalResult.bind(streamRepo),
      sinkEmitter: svc.dataSinkWorkerEmitter,
      unit,
      segmentId: integration.segmentId,
      schema: sync.schema,
      log,
    })

    const ctx: SyncContext = {
      channel: { channelId: unit.channelId, channelName: unit.channelName },
      watermark: unit.watermark,
      emit: emitter.emit,
      commitWatermark: async (watermark) => {
        committedWatermark = watermark
      },
      hasRunBudget: () => Date.now() < runDeadline,
      http,
      log,
    }

    const outcome = await sync.run(ctx)
    const nextRunAt = outcome.complete ? cadenceRunAt(sync.cadenceMinutes) : shortDeferRunAt()

    await recordRunSuccess(
      qx,
      unitId,
      {
        watermark: committedWatermark ?? {},
        emittedCount: emitter.emittedCount(),
      },
      nextRunAt,
    )
    log.info(
      { emittedCount: emitter.emittedCount(), complete: outcome.complete, nextRunAt },
      'sync run succeeded',
    )
  } catch (err) {
    if (
      err instanceof ConnectorError &&
      (PARKED_ERROR_CLASSES as readonly string[]).includes(err.errorClass)
    ) {
      const fallbackMs =
        err.errorClass === 'provider.rate_limit' ? RATE_LIMIT_FALLBACK_MS : UNAVAILABLE_PARK_MS
      const resumeAt = err.options?.resumeAt ?? new Date(Date.now() + fallbackMs)
      if (emitter && committedWatermark) {
        await recordRunPartial(
          qx,
          unitId,
          { watermark: committedWatermark, emittedCount: emitter.emittedCount() },
          resumeAt,
          err.errorClass,
        )
      } else {
        await parkUnit(qx, unitId, resumeAt, err.errorClass)
      }
      log.info({ resumeAt, errorClass: err.errorClass }, 'sync run parked')
      return
    }
    const errorClass = err instanceof ConnectorError ? err.errorClass : 'unknown'
    const deadLetterAfter = errorClass === 'provider.auth' ? DEAD_LETTER_AFTER : null
    const consecutiveFailures = unit.consecutiveFailures + 1
    const nextRunAt = failureRunAt(
      consecutiveFailures,
      findSync(unit.platform, unit.syncName)?.cadenceMinutes ?? null,
    )
    log.error(err, { errorClass, consecutiveFailures, nextRunAt }, 'sync run failed')
    await recordRunFailure(qx, unitId, errorClass, deadLetterAfter, nextRunAt)
    throw err
  } finally {
    clearInterval(heartbeat)
  }
}
