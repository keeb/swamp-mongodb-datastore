#!/usr/bin/env -S deno run --allow-read --allow-env --allow-net --allow-sys
//
// Reclaims `_blobs` docs no live `_paths` doc references.
//
// `_blobs` is append-only by design (see maintenance.ts), so tombstoned paths
// leave their bytes behind forever. This is the out-of-band sweep that gets
// them back.
//
// The sweep takes the datastore's **global lock** for its whole duration.
// That is not optional: a concurrent push inserts a blob before upserting the
// path doc pointing at it, so an unlocked sweep can delete bytes a peer is
// about to reference.
//
// Also prunes tombstoned path docs past their own (much longer) grace window —
// `_paths` accumulates them forever, and they can dwarf the live set.
//
// Usage (from a repo whose .swamp.yaml selects @keeb/mongodb-datastore):
//
//   deno task blob-gc --repo <path-to-repo> --dry-run
//   deno task blob-gc --repo <path-to-repo> --confirm
//   deno task blob-gc --repo <path> --confirm --tombstone-days 60
//   deno task blob-gc --repo <path> --confirm --skip-tombstones
//
//   deno task blob-gc --repo <path> --list-namespaces
//   deno task blob-gc --repo <path> --namespace other-repo --dry-run
//
// Always run --dry-run first against a shared cluster.

import { parseArgs } from "jsr:@std/cli@1/parse-args";
import { parse as parseYaml } from "jsr:@std/yaml@1";
import {
  blobsCollectionName,
  ConfigSchema,
  loadDotEnv,
  type MongoDatastoreConfig,
  pathsCollectionName,
} from "./config.ts";
import { createClientFactory } from "./client.ts";
import { createLock } from "./lock.ts";
import {
  type BlobDocLike,
  DEFAULT_GRACE_MS,
  DEFAULT_TOMBSTONE_GRACE_MS,
  type PathDocLike,
  sweepOrphanBlobs,
  sweepTombstones,
} from "./maintenance.ts";

function fmtBytes(n: number): string {
  const units = ["B", "KB", "MB", "GB", "TB"];
  let v = n;
  let i = 0;
  while (v >= 1024 && i < units.length - 1) {
    v /= 1024;
    i++;
  }
  return `${v.toFixed(1)} ${units[i]}`;
}

async function readDatastoreConfig(
  repoDir: string,
): Promise<MongoDatastoreConfig> {
  const raw = await Deno.readTextFile(`${repoDir}/.swamp.yaml`);
  const doc = parseYaml(raw) as {
    datastore?: { type?: string; config?: Record<string, unknown> };
  };
  const ds = doc.datastore;
  if (!ds?.config) {
    throw new Error(`${repoDir}/.swamp.yaml has no datastore.config block`);
  }
  if (ds.type !== "@keeb/mongodb-datastore") {
    throw new Error(
      `${repoDir} uses datastore "${ds.type}", not @keeb/mongodb-datastore`,
    );
  }
  return ConfigSchema.parse(ds.config);
}

async function main(): Promise<number> {
  const args = parseArgs(Deno.args, {
    string: ["repo", "grace-minutes", "tombstone-days", "namespace"],
    boolean: [
      "dry-run",
      "confirm",
      "skip-tombstones",
      "skip-blobs",
      "list-namespaces",
    ],
    default: {
      "dry-run": false,
      confirm: false,
      "skip-tombstones": false,
      "skip-blobs": false,
      "list-namespaces": false,
    },
  });
  const repoDir = args.repo ?? Deno.cwd();
  const dryRun = args["dry-run"] || !args.confirm;
  const graceMs = args["grace-minutes"] !== undefined
    ? Number(args["grace-minutes"]) * 60_000
    : DEFAULT_GRACE_MS;
  if (!Number.isFinite(graceMs) || graceMs < 0) {
    throw new Error(`--grace-minutes must be a non-negative number`);
  }
  const tombstoneMs = args["tombstone-days"] !== undefined
    ? Number(args["tombstone-days"]) * 86_400_000
    : DEFAULT_TOMBSTONE_GRACE_MS;
  if (!Number.isFinite(tombstoneMs) || tombstoneMs < 0) {
    throw new Error(`--tombstone-days must be a non-negative number`);
  }

  const base = await readDatastoreConfig(repoDir);
  await loadDotEnv(repoDir);

  // --namespace retargets the sweep at a different repo's collections using
  // this repo's connection + credentials. A shared cluster accumulates
  // namespaces whose owning checkout has moved or been retired; without this
  // there is no way to reclaim their space short of recreating the repo.
  const cfg: MongoDatastoreConfig = args.namespace !== undefined
    ? { ...base, namespace: args.namespace }
    : base;
  const getClient = createClientFactory(cfg);

  if (args["list-namespaces"]) {
    const { client } = await getClient(repoDir);
    const db = client.db(cfg.database);
    const seen = new Map<string, { paths: number; blobs: number }>();
    for (
      const c of await db.listCollections({}, { nameOnly: true }).toArray()
    ) {
      const m = c.name.match(/^t_(.+?)_r_(.+)_(paths|blobs)$/);
      if (!m) continue;
      const key = `${m[1]}/${m[2]}`;
      const entry = seen.get(key) ?? { paths: 0, blobs: 0 };
      const n = await db.collection(c.name).estimatedDocumentCount();
      if (m[3] === "paths") entry.paths = n;
      else entry.blobs = n;
      seen.set(key, entry);
    }
    for (const [k, v] of [...seen].sort()) {
      console.log(`${k}\tpaths=${v.paths}\tblobs=${v.blobs}`);
    }
    return 0;
  }

  // Exclude every peer writer for the duration of the sweep.
  const lock = createLock(cfg, getClient, repoDir, { ttlMs: 120_000 });
  console.log("acquiring global lock…");
  await lock.acquire();
  try {
    const { client } = await getClient(repoDir);
    const db = client.db(cfg.database);
    const paths = db.collection<PathDocLike>(pathsCollectionName(cfg));
    const blobs = db.collection<BlobDocLike>(blobsCollectionName(cfg));

    const mode = dryRun ? "(dry run) " : "";

    // Tombstones first: they are not blob references, so pruning them never
    // strands bytes, and it lets the blob sweep's single pass collect anything
    // they were the last trace of.
    let tombstones = null;
    if (!args["skip-tombstones"]) {
      console.log(
        `${mode}pruning tombstones older than ${
          Math.round(tombstoneMs / 86_400_000)
        }d…`,
      );
      tombstones = await sweepTombstones(paths, {
        dryRun,
        graceMs: tombstoneMs,
      });
      console.log(JSON.stringify(tombstones, null, 2));
    }

    // --skip-blobs leaves bytes alone. Worth it against a namespace that is
    // actively written AND predates `createdAt`: with no timestamps to age
    // against, every unreferenced blob looks eligible, including one a push
    // inserted a second ago. Tombstone pruning has no such race, so a
    // tombstone-only pass is the safe maintenance option there.
    if (args["skip-blobs"]) {
      console.log(`${mode}skipping blob sweep (--skip-blobs).`);
    } else {
      console.log(
        `${mode}sweeping blobs, grace ${Math.round(graceMs / 60_000)}m…`,
      );
      const res = await sweepOrphanBlobs(paths, blobs, {
        dryRun,
        graceMs,
        onProgress: (n) => console.log(`  scanned ${n} blob docs…`),
      });

      console.log(JSON.stringify(
        {
          ...res,
          bytesReclaimedHuman: fmtBytes(res.bytesReclaimed),
        },
        null,
        2,
      ));
    }
    if (dryRun) {
      console.log("\nDry run — nothing deleted. Re-run with --confirm.");
    }
  } finally {
    await lock.release();
  }
  return 0;
}

if (import.meta.main) {
  Deno.exit(await main());
}
