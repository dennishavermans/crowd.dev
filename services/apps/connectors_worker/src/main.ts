import { Config } from '@crowd/archetype-standard'
import { Options, ServiceWorker } from '@crowd/archetype-worker'
import { IS_DEV_ENV } from '@crowd/common'
import { registerConnector } from '@crowd/connectors'
import { dummyConnector } from '@crowd/connectors/src/testing/dummyConnector'

import { scheduleDispatcher } from './schedules/dispatcher'

const config: Config = {
  envvars: [],
  producer: {
    enabled: true,
  },
  temporal: {
    enabled: true,
  },
  redis: {
    enabled: true,
  },
}

const options: Options = {
  postgres: {
    enabled: true,
  },
  opensearch: {
    enabled: false,
  },
}

export const svc = new ServiceWorker(config, options)

// POC only: dummy connector drives the control-plane end-to-end; real
// connectors register here starting with GitHub in M4
if (IS_DEV_ENV) {
  registerConnector(dummyConnector)
}

setImmediate(async () => {
  await svc.init()

  await scheduleDispatcher()

  await svc.start()
})
