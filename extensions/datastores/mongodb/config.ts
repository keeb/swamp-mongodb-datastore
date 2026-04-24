import { z } from "npm:zod@4";

export const ConfigSchema = z.object({
  uri: z.string().describe(
    "MongoDB URI (no auth baked in), e.g. mongodb://hancock:27017/?replicaSet=rs0&authSource=admin",
  ),
  username: z.string().describe(
    "MongoDB username — passed to the driver as an auth option, not baked into the URI",
  ),
  passwordEnv: z.string().default("MONGO_PASSWORD").describe(
    "Name of the env var (loaded from <repoDir>/.env) that holds the MongoDB password",
  ),
  database: z.string().default("swamp").describe(
    "Database name shared by all tenants/repos; isolation is via collection prefix",
  ),
  tenantId: z.string().default("default").describe(
    "Tenant identifier — today usually 'default'; becomes meaningful if this extension is offered as a service",
  ),
  namespace: z.string().describe(
    "Per-repo identifier used in collection prefixing (t_<tenant>_r_<namespace>_<purpose>)",
  ),
  defaultLockTtlMs: z.number().int().positive().default(30_000),
});

export type MongoDatastoreConfig = z.infer<typeof ConfigSchema>;

export function collectionPrefix(cfg: MongoDatastoreConfig): string {
  return `t_${cfg.tenantId}_r_${cfg.namespace}`;
}

export function lockCollectionName(cfg: MongoDatastoreConfig): string {
  return `${collectionPrefix(cfg)}_locks`;
}

export function gridfsBucketName(cfg: MongoDatastoreConfig): string {
  return `${collectionPrefix(cfg)}_fs`;
}

export function fsMetaCollectionName(cfg: MongoDatastoreConfig): string {
  return `${collectionPrefix(cfg)}_fs_meta`;
}

const ENV_LINE = /^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*?)\s*$/;

export async function loadDotEnv(repoDir: string): Promise<void> {
  const path = `${repoDir}/.env`;
  let text: string;
  try {
    text = await Deno.readTextFile(path);
  } catch {
    return;
  }
  for (const raw of text.split("\n")) {
    const line = raw.trim();
    if (!line || line.startsWith("#")) continue;
    const m = line.match(ENV_LINE);
    if (!m) continue;
    const [, key, rawValue] = m;
    if (Deno.env.get(key) !== undefined) continue;
    const value = rawValue.startsWith('"') && rawValue.endsWith('"')
      ? rawValue.slice(1, -1)
      : rawValue.startsWith("'") && rawValue.endsWith("'")
      ? rawValue.slice(1, -1)
      : rawValue;
    Deno.env.set(key, value);
  }
}
