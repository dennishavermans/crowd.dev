import type { Logger } from '@crowd/logging'

export interface Channel {
  channelId: string
  channelName: string
}

export interface Credential {
  platform: string
  kind: 'github-app' | 'token'
  data: Record<string, string>
}

export interface SyncContext {
  channel: Channel
  watermark: Record<string, unknown> | null
  emit: (records: unknown[]) => Promise<void>
  commitWatermark: (watermark: Record<string, unknown>) => Promise<void>
  log: Logger
}

export interface SyncDefinition {
  name: string
  cadenceMinutes: number
  run: (ctx: SyncContext) => Promise<void>
}

export interface Manifest {
  platform: string
  syncs: SyncDefinition[]
  discover: (credential: Credential) => Promise<Channel[]>
}
