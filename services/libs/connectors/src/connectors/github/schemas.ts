import { z } from 'zod'

import { GithubActivityType } from '@crowd/integrations'

const identitySchema = z.object({
  platform: z.string(),
  value: z.string(),
  type: z.string(),
  verified: z.boolean(),
  sourceId: z.string(),
})

const memberAttributesSchema = z.object({
  isHireable: z.boolean(),
  url: z.string(),
  bio: z.string(),
  location: z.string(),
  avatarUrl: z.string(),
  company: z.string(),
  isBot: z.boolean(),
  websiteUrl: z.string().optional(),
})

const organizationIdentitySchema = z.object({
  platform: z.string(),
  type: z.string(),
  value: z.string(),
  verified: z.boolean(),
  sourceId: z.string().optional(),
})

const organizationSchema = z.object({
  displayName: z.string(),
  names: z.array(z.string()),
  description: z.string().nullable(),
  location: z.string().nullable(),
  logo: z.string().nullable(),
  source: z.string(),
  identities: z.array(organizationIdentitySchema),
})

const memberSchema = z.object({
  displayName: z.string(),
  identities: z.array(identitySchema).min(1),
  attributes: memberAttributesSchema,
  organizations: z.array(organizationSchema).optional(),
})

export const githubActivitySchema = z.object({
  type: z.nativeEnum(GithubActivityType),
  timestamp: z.string(),
  sourceId: z.string(),
  sourceParentId: z.string().optional(),
  score: z.number(),
  title: z.string().optional(),
  body: z.string().optional(),
  url: z.string().optional(),
  attributes: z.record(z.unknown()).optional(),
  member: memberSchema,
  objectMember: memberSchema.optional(),
})

export type GithubActivity = z.infer<typeof githubActivitySchema>
export type GithubMember = z.infer<typeof memberSchema>
export type GithubOrganization = z.infer<typeof organizationSchema>
