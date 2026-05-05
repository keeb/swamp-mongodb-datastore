import {
  type AnyBulkWriteOperation,
  Binary,
  type Collection,
} from "npm:mongodb@6.17.0";
import type { ClientHandle } from "./client.ts";
import {
  blobsCollectionName,
  type MongoDatastoreConfig,
  pathsCollectionName,
} from "./config.ts";
import { Sidecar, type SidecarState } from "./sidecar.ts";

export interface DatastoreSyncOptions {
  signal?: AbortSignal;
  relPath?: string;
}

export interface DatastoreSyncService {
  pullChanged(options?: DatastoreSyncOptions): Promise<number>;
  pushChanged(options?: DatastoreSyncOptions): Promise<number>;
  markDirty(options?: DatastoreSyncOptions): Promise<void>;
}

const DATASTORE_SUBDIRS = [
  "definitions-evaluated",
  "workflows-evaluated",
  "data",
  "outputs",
  "workflow-runs",
  "secrets",
  "bundles",
  "vault-bundles",
  "driver-bundles",
  "report-bundles",
  "audit",
  "telemetry",
  "logs",
  "files",
] as const;

interface PathDoc {
  _id: string;
  hash: string;
  size: number;
  updatedAt: Date;
  deletedAt: Date | null;
}

interface BlobDoc {
  _id: string;
  size: number;
  data: Binary;
}

interface LocalFile {
  relPath: string;
  hash: string;
  size: number;
  bytes: Uint8Array;
}

const PUSH_BULK = 500;
const BLOB_QUERY_BATCH = 5000;

export function createSyncService(
  cfg: MongoDatastoreConfig,
  getClient: (repoDir: string) => Promise<ClientHandle>,
  repoDir: string,
  cachePath: string,
): DatastoreSyncService {
  const sidecar = new Sidecar(cachePath);
  let updatedAtIndexEnsured = false;

  async function resources(): Promise<{
    paths: Collection<PathDoc>;
    blobs: Collection<BlobDoc>;
  }> {
    const { client } = await getClient(repoDir);
    const db = client.db(cfg.database);
    const paths = db.collection<PathDoc>(pathsCollectionName(cfg));
    const blobs = db.collection<BlobDoc>(blobsCollectionName(cfg));
    if (!updatedAtIndexEnsured) {
      await paths.createIndex({ updatedAt: 1 }).catch(() => undefined);
      updatedAtIndexEnsured = true;
    }
    return { paths, blobs };
  }

  function poolConcurrency(): number {
    return parseInt(
      Deno.env.get("MONGO_DATASTORE_PULL_CONCURRENCY") ?? "32",
      10,
    );
  }

  async function pull(): Promise<number> {
    const { paths, blobs } = await resources();
    const state = await sidecar.read();

    if (state.lastPulledAt !== null) {
      const since = new Date(state.lastPulledAt);
      const probe = await paths.findOne(
        { updatedAt: { $gt: since } },
        { projection: { _id: 1 } },
      );
      if (probe === null) return 0;
    }

    const filter = state.lastPulledAt !== null
      ? { updatedAt: { $gt: new Date(state.lastPulledAt) } }
      : {};
    const coldStart = state.lastPulledAt === null;

    const pathDocs: PathDoc[] = [];
    let maxUpdatedAtMs = state.lastPulledAt !== null
      ? new Date(state.lastPulledAt).getTime()
      : 0;
    for await (const doc of paths.find(filter)) {
      pathDocs.push(doc);
      const ms = doc.updatedAt.getTime();
      if (ms > maxUpdatedAtMs) maxUpdatedAtMs = ms;
    }

    const concurrency = poolConcurrency();
    let changes = 0;

    const deletes = pathDocs.filter((d) => d.deletedAt !== null);
    await runPool(deletes, concurrency, async (doc) => {
      if (await removeSilentlyExisting(`${cachePath}/${doc._id}`)) changes++;
    });

    const needs = pathDocs.filter((d) => d.deletedAt === null);
    const pathsByHash = new Map<string, PathDoc[]>();
    if (coldStart) {
      for (const doc of needs) addToBucket(pathsByHash, doc.hash, doc);
    } else {
      await runPool(needs, concurrency, async (doc) => {
        const local = await readFileOrNull(`${cachePath}/${doc._id}`);
        if (local !== null && (await sha256Hex(local)) === doc.hash) return;
        addToBucket(pathsByHash, doc.hash, doc);
      });
    }

    const hashesNeeded = [...pathsByHash.keys()];
    for (let i = 0; i < hashesNeeded.length; i += BLOB_QUERY_BATCH) {
      const hashBatch = hashesNeeded.slice(i, i + BLOB_QUERY_BATCH);
      const writeJobs: Array<{ relPath: string; bytes: Uint8Array }> = [];
      for await (const blob of blobs.find({ _id: { $in: hashBatch } })) {
        const bytes = blob.data.buffer;
        const dependents = pathsByHash.get(blob._id) ?? [];
        for (const doc of dependents) {
          writeJobs.push({ relPath: doc._id, bytes });
        }
      }
      await runPool(writeJobs, concurrency, async ({ relPath, bytes }) => {
        await writeFileAtomic(`${cachePath}/${relPath}`, bytes);
        changes++;
      });
    }

    const watermark = maxUpdatedAtMs > 0
      ? new Date(maxUpdatedAtMs).toISOString()
      : new Date().toISOString();
    await sidecar.setLastPulledAt(watermark);
    return changes;
  }

  async function fullWalkPush(
    paths: Collection<PathDoc>,
    blobs: Collection<BlobDoc>,
    lastPulledAt: string | null,
  ): Promise<number> {
    const locals: LocalFile[] = [];
    for (const sub of DATASTORE_SUBDIRS) {
      await walkInto(`${cachePath}/${sub}`, sub, locals);
    }

    const localByHash = new Map<string, Uint8Array>();
    for (const f of locals) {
      if (!localByHash.has(f.hash)) localByHash.set(f.hash, f.bytes);
    }
    const remotePaths = new Map<string, PathDoc>();
    for await (const doc of paths.find({})) remotePaths.set(doc._id, doc);

    const localHashes = [...localByHash.keys()];
    const remoteBlobHashes = new Set<string>();
    for (let i = 0; i < localHashes.length; i += BLOB_QUERY_BATCH) {
      const batch = localHashes.slice(i, i + BLOB_QUERY_BATCH);
      for await (
        const b of blobs.find(
          { _id: { $in: batch } },
          { projection: { _id: 1 } },
        )
      ) {
        remoteBlobHashes.add(b._id);
      }
    }
    const missingHashes = localHashes.filter((h) => !remoteBlobHashes.has(h));
    let blobsPushed = 0;
    if (missingHashes.length > 0) {
      let i = 0;
      while (i < missingHashes.length) {
        const ops: { insertOne: { document: BlobDoc } }[] = [];
        let batchBytes = 0;
        while (
          i < missingHashes.length &&
          ops.length < PUSH_BULK &&
          batchBytes < 14 * 1024 * 1024
        ) {
          const h = missingHashes[i++];
          const bytes = localByHash.get(h)!;
          batchBytes += bytes.byteLength + 64;
          ops.push({
            insertOne: {
              document: {
                _id: h,
                size: bytes.byteLength,
                data: new Binary(bytes),
              },
            },
          });
        }
        try {
          const res = await blobs.bulkWrite(ops, { ordered: false });
          blobsPushed += res.insertedCount;
        } catch (err) {
          if (
            !(err instanceof Error) ||
            !("code" in err) ||
            (err as { code?: number }).code !== 11000
          ) {
            const wErr = err as {
              writeErrors?: Array<{ code: number }>;
              insertedCount?: number;
            };
            const allDup = (wErr.writeErrors ?? []).every((e) =>
              e.code === 11000
            );
            if (!allDup) throw err;
            blobsPushed += wErr.insertedCount ?? 0;
          }
        }
      }
    }
    let pathsPushed = 0;
    let pathOps: AnyBulkWriteOperation<PathDoc>[] = [];
    const flushPathOps = async () => {
      if (pathOps.length === 0) return;
      const res = await paths.bulkWrite(pathOps, { ordered: false });
      pathsPushed += (res.upsertedCount ?? 0) + (res.modifiedCount ?? 0);
      pathOps = [];
    };
    const now = new Date();
    for (const f of locals) {
      const existing = remotePaths.get(f.relPath);
      if (
        existing &&
        existing.deletedAt === null &&
        existing.hash === f.hash
      ) continue;
      pathOps.push({
        updateOne: {
          filter: { _id: f.relPath },
          update: {
            $set: {
              hash: f.hash,
              size: f.size,
              updatedAt: now,
              deletedAt: null,
            },
          },
          upsert: true,
        },
      });
      if (pathOps.length >= PUSH_BULK) await flushPathOps();
    }
    await flushPathOps();

    if (lastPulledAt !== null) {
      const watermark = new Date(lastPulledAt);
      const localPaths = new Set(locals.map((f) => f.relPath));
      const tombstoneOps: AnyBulkWriteOperation<PathDoc>[] = [];
      for (const [relPath, doc] of remotePaths) {
        if (localPaths.has(relPath) || doc.deletedAt !== null) continue;
        if (doc.updatedAt > watermark) continue;
        tombstoneOps.push({
          updateOne: {
            filter: { _id: relPath },
            update: { $set: { deletedAt: now, updatedAt: now } },
          },
        });
        pathsPushed++;
      }
      for (let i = 0; i < tombstoneOps.length; i += PUSH_BULK) {
        await paths.bulkWrite(
          tombstoneOps.slice(i, i + PUSH_BULK),
          { ordered: false },
        );
      }
    }
    return pathsPushed + blobsPushed;
  }

  async function pushOneRel(
    paths: Collection<PathDoc>,
    blobs: Collection<BlobDoc>,
    relPath: string,
    lastPulledAt: string | null,
  ): Promise<number> {
    const absPath = `${cachePath}/${relPath}`;
    let stat: Deno.FileInfo | null = null;
    try {
      stat = await Deno.stat(absPath);
    } catch (err) {
      if (!(err instanceof Deno.errors.NotFound)) throw err;
    }

    const local: LocalFile[] = [];
    if (stat?.isFile) {
      const bytes = await Deno.readFile(absPath);
      local.push({
        relPath,
        hash: await sha256Hex(bytes),
        size: bytes.byteLength,
        bytes,
      });
    } else if (stat?.isDirectory) {
      await walkInto(absPath, relPath, local);
    }

    const remoteDocs = await paths.find({
      $or: [
        { _id: relPath },
        { _id: { $regex: `^${escapeRegex(relPath)}/` } },
      ],
    }).toArray();
    const remoteByPath = new Map<string, PathDoc>();
    for (const d of remoteDocs) remoteByPath.set(d._id, d);

    const localByHash = new Map<string, Uint8Array>();
    for (const f of local) {
      if (!localByHash.has(f.hash)) localByHash.set(f.hash, f.bytes);
    }
    let changes = 0;

    const localHashes = [...localByHash.keys()];
    const remoteBlobHashes = new Set<string>();
    if (localHashes.length > 0) {
      for await (
        const b of blobs.find(
          { _id: { $in: localHashes } },
          { projection: { _id: 1 } },
        )
      ) {
        remoteBlobHashes.add(b._id);
      }
    }
    const missing = localHashes.filter((h) => !remoteBlobHashes.has(h));
    if (missing.length > 0) {
      const ops = missing.map((h) => ({
        insertOne: {
          document: {
            _id: h,
            size: localByHash.get(h)!.byteLength,
            data: new Binary(localByHash.get(h)!),
          } as BlobDoc,
        },
      }));
      try {
        const res = await blobs.bulkWrite(ops, { ordered: false });
        changes += res.insertedCount ?? 0;
      } catch (err) {
        const wErr = err as {
          writeErrors?: Array<{ code: number }>;
          insertedCount?: number;
        };
        const allDup = (wErr.writeErrors ?? []).every((e) => e.code === 11000);
        if (!allDup) throw err;
        changes += wErr.insertedCount ?? 0;
      }
    }

    const now = new Date();
    const pathOps: AnyBulkWriteOperation<PathDoc>[] = [];
    for (const f of local) {
      const existing = remoteByPath.get(f.relPath);
      if (
        existing &&
        existing.deletedAt === null &&
        existing.hash === f.hash
      ) continue;
      pathOps.push({
        updateOne: {
          filter: { _id: f.relPath },
          update: {
            $set: {
              hash: f.hash,
              size: f.size,
              updatedAt: now,
              deletedAt: null,
            },
          },
          upsert: true,
        },
      });
      changes++;
    }
    for (let i = 0; i < pathOps.length; i += PUSH_BULK) {
      await paths.bulkWrite(
        pathOps.slice(i, i + PUSH_BULK),
        { ordered: false },
      );
    }

    if (lastPulledAt !== null) {
      const watermark = new Date(lastPulledAt);
      const localPaths = new Set(local.map((f) => f.relPath));
      const tombstoneOps: AnyBulkWriteOperation<PathDoc>[] = [];
      for (const doc of remoteDocs) {
        if (localPaths.has(doc._id) || doc.deletedAt !== null) continue;
        if (doc.updatedAt > watermark) continue;
        tombstoneOps.push({
          updateOne: {
            filter: { _id: doc._id },
            update: { $set: { deletedAt: now, updatedAt: now } },
          },
        });
        changes++;
      }
      for (let i = 0; i < tombstoneOps.length; i += PUSH_BULK) {
        await paths.bulkWrite(
          tombstoneOps.slice(i, i + PUSH_BULK),
          { ordered: false },
        );
      }
    }

    return changes;
  }

  return {
    async pushChanged(): Promise<number> {
      const { paths, blobs } = await resources();
      const state = await sidecar.read();

      if (state.bulkInvalidated) {
        const changes = await fullWalkPush(paths, blobs, state.lastPulledAt);
        await sidecar.clearDirty();
        return changes;
      }

      if (state.dirtyPaths.length === 0) return 0;
      let changes = 0;
      for (const relPath of state.dirtyPaths) {
        changes += await pushOneRel(paths, blobs, relPath, state.lastPulledAt);
      }
      await sidecar.clearDirty();
      return changes;
    },

    pullChanged(): Promise<number> {
      return pull();
    },

    markDirty(options?: DatastoreSyncOptions): Promise<void> {
      return sidecar.recordDirty(options?.relPath).then(() => undefined);
    },
  };
}

function addToBucket<K, V>(map: Map<K, V[]>, key: K, value: V): void {
  const list = map.get(key);
  if (list) list.push(value);
  else map.set(key, [value]);
}

async function runPool<T>(
  items: T[],
  concurrency: number,
  worker: (item: T) => Promise<void>,
): Promise<void> {
  if (items.length === 0) return;
  let idx = 0;
  await Promise.all(
    Array.from({ length: Math.min(concurrency, items.length) }, async () => {
      while (true) {
        const i = idx++;
        if (i >= items.length) return;
        await worker(items[i]);
      }
    }),
  );
}

async function walkInto(
  root: string,
  relRoot: string,
  out: LocalFile[],
): Promise<void> {
  try {
    for await (const entry of Deno.readDir(root)) {
      if (entry.isSymlink) continue;
      const childAbs = `${root}/${entry.name}`;
      const childRel = `${relRoot}/${entry.name}`;
      if (entry.isDirectory) {
        await walkInto(childAbs, childRel, out);
        continue;
      }
      if (!entry.isFile) continue;
      let bytes: Uint8Array;
      try {
        bytes = await Deno.readFile(childAbs);
      } catch (err) {
        if (err instanceof Deno.errors.NotFound) continue;
        throw err;
      }
      out.push({
        relPath: childRel,
        hash: await sha256Hex(bytes),
        size: bytes.byteLength,
        bytes,
      });
    }
  } catch (err) {
    if (err instanceof Deno.errors.NotFound) return;
    throw err;
  }
}

async function sha256Hex(bytes: Uint8Array): Promise<string> {
  const input = new ArrayBuffer(bytes.byteLength);
  new Uint8Array(input).set(bytes);
  const digest = await crypto.subtle.digest("SHA-256", input);
  const view = new Uint8Array(digest);
  let hex = "";
  for (let i = 0; i < view.length; i++) {
    hex += view[i].toString(16).padStart(2, "0");
  }
  return hex;
}

async function removeSilentlyExisting(path: string): Promise<boolean> {
  try {
    await Deno.remove(path);
    return true;
  } catch (err) {
    if (err instanceof Deno.errors.NotFound) return false;
    throw err;
  }
}

async function readFileOrNull(path: string): Promise<Uint8Array | null> {
  try {
    return await Deno.readFile(path);
  } catch (err) {
    if (err instanceof Deno.errors.NotFound) return null;
    throw err;
  }
}

async function writeFileAtomic(
  absPath: string,
  bytes: Uint8Array,
): Promise<void> {
  const slash = absPath.lastIndexOf("/");
  const dir = slash > 0 ? absPath.slice(0, slash) : ".";
  await Deno.mkdir(dir, { recursive: true });
  const tmp = `${absPath}.tmp.${Deno.pid}.${crypto.randomUUID()}`;
  await Deno.writeFile(tmp, bytes);
  await Deno.rename(tmp, absPath);
}

function escapeRegex(s: string): string {
  return s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

export type { SidecarState };
