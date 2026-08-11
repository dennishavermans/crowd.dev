import { getSync } from '@crowd/connectors'
import { claimDueUnits, rescheduleUnit } from '@crowd/data-access-layer/src/connectors'
import type { ISyncUnit } from '@crowd/data-access-layer/src/connectors'
import { dbStoreQx } from '@crowd/data-access-layer/src/queryExecutor'
import { RedisCache } from '@crowd/redis'
import { WorkflowIdConflictPolicy, WorkflowIdReusePolicy } from '@crowd/temporal'

import { svc } from '../main'
import type { StartRunResult } from '../types'

const TASK_QUEUE = 'connectors'
const HEARTBEAT_TTL_SECONDS = 300
const CADENCE_JITTER_RATIO = 0.1

export async function claimDue(limit: number): Promise<ISyncUnit[]> {
  return claimDueUnits(dbStoreQx(svc.postgres.writer), limit)
}

export async function startRun(unit: ISyncUnit): Promise<StartRunResult> {
  try {
    await svc.temporal.workflow.start('syncRun', {
      taskQueue: TASK_QUEUE,
      workflowId: `sync-run/${unit.id}`,
      workflowIdReusePolicy: WorkflowIdReusePolicy.ALLOW_DUPLICATE,
      workflowIdConflictPolicy: WorkflowIdConflictPolicy.FAIL,
      args: [unit.id],
    })
    return 'started'
  } catch (err) {
    if (err instanceof Error && err.name === 'WorkflowExecutionAlreadyStartedError') {
      return 'alreadyRunning'
    }
    throw err
  }
}

export async function reschedule(
  unitId: string,
  platform: string,
  syncName: string,
): Promise<void> {
  const { cadenceMinutes } = getSync(platform, syncName)
  const jitterMinutes = cadenceMinutes * CADENCE_JITTER_RATIO * (Math.random() * 2 - 1)
  const nextRunAt = new Date(Date.now() + (cadenceMinutes + jitterMinutes) * 60_000)

  await rescheduleUnit(dbStoreQx(svc.postgres.writer), unitId, nextRunAt)
}

export async function touchHeartbeat(): Promise<void> {
  const cache = new RedisCache('connectors', svc.redis, svc.log)
  await cache.set('dispatcherHeartbeat', new Date().toISOString(), HEARTBEAT_TTL_SECONDS)
}
