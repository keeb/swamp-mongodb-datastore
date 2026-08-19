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
    string: ["repo", "grace-minutes", "tombstone-days"],
    boolean: ["dry-run", "confirm", "skip-tombstones"],
    default: { "dry-run": false, confirm: false, "skip-tombstones": false },
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

  const cfg = await readDatastoreConfig(repoDir);
  await loadDotEnv(repoDir);
  const getClient = createClientFactory(cfg);

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
