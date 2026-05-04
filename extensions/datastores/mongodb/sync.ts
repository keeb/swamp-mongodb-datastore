import { type Collection, GridFSBucket, ObjectId } from "npm:mongodb@6.17.0";
import type { ClientHandle } from "./client.ts";
import {
  fsMetaCollectionName,
  gridfsBucketName,
  type MongoDatastoreConfig,
} from "./config.ts";
import { Sidecar, type SidecarState } from "./sidecar.ts";

export interface DatastoreSyncOptions {
  signal?: AbortSignal;
  // Cache-relative path (forward-slash) of the file about to be written or
  // removed. Set by swamp-core on per-path markDirty calls; undefined for
  // bulk mutations (rename, collectGarbage). No defined meaning on
  // pullChanged / pushChanged — present on the shared options bag for
  // source compatibility (see swamp-club#232 rule 7).
  relPath?: string;
}

export interface DatastoreSyncService {
  pullChanged(options?: DatastoreSyncOptions): Promise<number>;
  pushChanged(options?: DatastoreSyncOptions): Promise<number>;
  markDirty(options?: DatastoreSyncOptions): Promise<void>;
}

// Mirrors swamp core's DEFAULT_DATASTORE_SUBDIRS — the subdirs under the
// cache path that belong to the datastore tier. datastore-bundles is
// intentionally absent: the loader that reads it runs before the provider
// exists, so those bytes must stay local.
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

interface FileMetaDoc {
  _id: string;
  hash: string;
  size: number;
  gridfsFileId: ObjectId;
  updatedAt: Date;
  deletedAt: Date | null;
}

interface LocalFile {
  hash: string;
  size: number;
}

export function createSyncService(
  cfg: MongoDatastoreConfig,
  getClient: (repoDir: string) => Promise<ClientHandle>,
  repoDir: string,
  cachePath: string,
): DatastoreSyncService {
  const sidecar = new Sidecar(cachePath);
  let updatedAtIndexEnsured = false;

  async function resources(): Promise<{
    meta: Collection<FileMetaDoc>;
    bucket: GridFSBucket;
  }> {
    const { client } = await getClient(repoDir);
    const db = client.db(cfg.database);
    const meta = db.collection<FileMetaDoc>(fsMetaCollectionName(cfg));
    const bucket = new GridFSBucket(db, { bucketName: gridfsBucketName(cfg) });
    if (!updatedAtIndexEnsured) {
      await meta.createIndex({ updatedAt: 1 }).catch(() => undefined);
      updatedAtIndexEnsured = true;
    }
    return { meta, bucket };
  }

  async function loadMeta(
    meta: Collection<FileMetaDoc>,
  ): Promise<Map<string, FileMetaDoc>> {
    const out = new Map<string, FileMetaDoc>();
    for await (const doc of meta.find({})) out.set(doc._id, doc);
    return out;
  }

  async function walkLocal(): Promise<Map<string, LocalFile>> {
    const out = new Map<string, LocalFile>();
    for (const sub of DATASTORE_SUBDIRS) {
      const root = `${cachePath}/${sub}`;
      await walkInto(root, sub, out);
    }
    return out;
  }

  // Push a single relPath signal. relPath can be:
  //   - a file (yaml repo writes, finalizeVersion) — push exactly that one
  //   - a directory subtree (data-name dir from save/append/allocateVersion,
  //     version dir from delete-version) — push every file under it that
  //     differs from remote; safe-tombstone files remote has under the
  //     prefix that local doesn't, subject to the lastPulledAt watermark
  //     check (same gate fullWalkPush uses to avoid clobbering another
  //     writer's data).
  //   - absent — tombstone anything remote has under the prefix (with the
  //     same watermark gate).
  async function pushOneRel(
    meta: Collection<FileMetaDoc>,
    bucket: GridFSBucket,
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

    const local = new Map<string, LocalFile>();
    if (stat?.isFile) {
      const bytes = await Deno.readFile(absPath);
      local.set(relPath, {
        hash: await sha256Hex(bytes),
        size: bytes.byteLength,
      });
    } else if (stat?.isDirectory) {
      await walkInto(absPath, relPath, local);
    }

    const remoteDocs = await meta.find({
      $or: [
        { _id: relPath },
        { _id: { $regex: `^${escapeRegex(relPath)}/` } },
      ],
    }).toArray();
    const remoteById = new Map<string, FileMetaDoc>();
    for (const doc of remoteDocs) remoteById.set(doc._id, doc);

    let changes = 0;

    for (const [rel, file] of local) {
      const existing = remoteById.get(rel);
      if (
        existing && existing.deletedAt === null && existing.hash === file.hash
      ) continue;

      const fileBytes = await Deno.readFile(`${cachePath}/${rel}`);
      const newId = await uploadBytes(bucket, rel, fileBytes);
      await meta.updateOne(
        { _id: rel },
        {
          $set: {
            hash: file.hash,
            size: file.size,
            gridfsFileId: newId,
            updatedAt: new Date(),
            deletedAt: null,
          },
        },
        { upsert: true },
      );
      if (existing?.gridfsFileId) {
        await deleteSilently(bucket, existing.gridfsFileId);
      }
      changes++;
    }

    if (lastPulledAt !== null) {
      const watermark = new Date(lastPulledAt);
      for (const doc of remoteDocs) {
        if (local.has(doc._id)) continue;
        if (doc.deletedAt !== null) continue;
        if (doc.updatedAt > watermark) continue;
        await meta.updateOne(
          { _id: doc._id },
          { $set: { deletedAt: new Date(), updatedAt: new Date() } },
        );
        await deleteSilently(bucket, doc.gridfsFileId);
        changes++;
      }
    }

    return changes;
  }

  function escapeRegex(s: string): string {
    return s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  }

  async function fullWalkPush(
    meta: Collection<FileMetaDoc>,
    bucket: GridFSBucket,
    lastPulledAt: string | null,
  ): Promise<number> {
    const local = await walkLocal();
    const remote = await loadMeta(meta);
    let changes = 0;

    for (const [relPath, file] of local) {
      const existing = remote.get(relPath);
      if (
        existing &&
        existing.deletedAt === null &&
        existing.hash === file.hash
      ) continue;

      const absPath = `${cachePath}/${relPath}`;
      let bytes: Uint8Array;
      try {
        bytes = await Deno.readFile(absPath);
      } catch (err) {
        if (err instanceof Deno.errors.NotFound) continue;
        throw err;
      }
      const newId = await uploadBytes(bucket, relPath, bytes);

      await meta.updateOne(
        { _id: relPath },
        {
          $set: {
            hash: file.hash,
            size: file.size,
            gridfsFileId: newId,
            updatedAt: new Date(),
            deletedAt: null,
          },
        },
        { upsert: true },
      );

      if (existing?.gridfsFileId) {
        await deleteSilently(bucket, existing.gridfsFileId);
      }
      changes++;
    }

    // Tombstone gate: only deletes files we KNOW we previously synced
    // and are now missing locally. Without this, a fresh-cache host on a
    // shared namespace would silently delete every other writer's data.
    //
    //   doc.updatedAt > lastPulledAt → another writer pushed after our
    //                                  last pull; not ours to tombstone.
    //   lastPulledAt === null        → cold start; we can't distinguish
    //                                  "we deleted it" from "we never
    //                                  had it." Skip the whole loop.
    //
    // Tradeoff: legitimately-deleted files during a bulk-invalidate
    // window won't be tombstoned until a per-path push arrives or the
    // post-#232 markDirty(relPath) signal flows through. Zombies in
    // Mongo are recoverable; spurious tombstones are not.
    if (lastPulledAt !== null) {
      const watermark = new Date(lastPulledAt);
      for (const [relPath, doc] of remote) {
        if (local.has(relPath) || doc.deletedAt !== null) continue;
        if (doc.updatedAt > watermark) continue;
        await meta.updateOne(
          { _id: relPath },
          { $set: { deletedAt: new Date(), updatedAt: new Date() } },
        );
        await deleteSilently(bucket, doc.gridfsFileId);
        changes++;
      }
    }

    return changes;
  }

  return {
    async pushChanged(): Promise<number> {
      const { meta, bucket } = await resources();
      const state = await sidecar.read();

      // Bulk path: full walk on cold start, on explicit bulk invalidate
      // (markDirty without relPath), or on corrupt/missing sidecar (which
      // readState already converts to bulkInvalidated=true).
      if (state.bulkInvalidated) {
        const changes = await fullWalkPush(meta, bucket, state.lastPulledAt);
        await sidecar.clearDirty();
        return changes;
      }

      // Per-path path: trust the contract that every cache write went
      // through swamp-core's markDirty(relPath). Empty dirty set means
      // nothing changed locally since the last successful push.
      if (state.dirtyPaths.length === 0) return 0;

      let changes = 0;
      for (const relPath of state.dirtyPaths) {
        changes += await pushOneRel(meta, bucket, relPath, state.lastPulledAt);
      }
      await sidecar.clearDirty();
      return changes;
    },

    async pullChanged(): Promise<number> {
      const { meta, bucket } = await resources();
      const state = await sidecar.read();

      // Fast-path: nothing in remote with updatedAt > lastPulledAt → 0.
      if (state.lastPulledAt !== null) {
        const since = new Date(state.lastPulledAt);
        const probe = await meta.findOne(
          { updatedAt: { $gt: since } },
          { projection: { _id: 1 } },
        );
        if (probe === null) return 0;
      }

      // Slow path: download everything that has moved since lastPulledAt
      // (or everything, on first pull with no sidecar history).
      const filter = state.lastPulledAt !== null
        ? { updatedAt: { $gt: new Date(state.lastPulledAt) } }
        : {};
      let changes = 0;
      let maxUpdatedAtMs = state.lastPulledAt !== null
        ? new Date(state.lastPulledAt).getTime()
        : 0;

      for await (const doc of meta.find(filter)) {
        const docMs = doc.updatedAt.getTime();
        if (docMs > maxUpdatedAtMs) maxUpdatedAtMs = docMs;

        if (doc.deletedAt !== null) {
          if (await removeSilentlyExisting(`${cachePath}/${doc._id}`)) {
            changes++;
          }
          continue;
        }

        const localBytes = await readFileOrNull(`${cachePath}/${doc._id}`);
        if (localBytes !== null) {
          const localHash = await sha256Hex(localBytes);
          if (localHash === doc.hash) continue;
        }

        const bytes = await downloadBytes(bucket, doc.gridfsFileId);
        await writeFileAtomic(`${cachePath}/${doc._id}`, bytes);
        changes++;
      }

      const watermark = maxUpdatedAtMs > 0
        ? new Date(maxUpdatedAtMs).toISOString()
        : new Date().toISOString();
      await sidecar.setLastPulledAt(watermark);
      return changes;
    },

    markDirty(options?: DatastoreSyncOptions): Promise<void> {
      return sidecar.recordDirty(options?.relPath).then(() => undefined);
    },
  };
}

async function walkInto(
  root: string,
  relRoot: string,
  out: Map<string, LocalFile>,
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
      const hash = await sha256Hex(bytes);
      out.set(childRel, { hash, size: bytes.byteLength });
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

function uploadBytes(
  bucket: GridFSBucket,
  filename: string,
  bytes: Uint8Array,
): Promise<ObjectId> {
  return new Promise((resolve, reject) => {
    const stream = bucket.openUploadStream(filename);
    stream.once("error", reject);
    stream.once("finish", () => resolve(stream.id as ObjectId));
    stream.end(bytes);
  });
}

function downloadBytes(
  bucket: GridFSBucket,
  id: ObjectId,
): Promise<Uint8Array> {
  return new Promise((resolve, reject) => {
    const stream = bucket.openDownloadStream(id);
    const chunks: Uint8Array[] = [];
    let total = 0;
    stream.on("data", (chunk: Uint8Array) => {
      chunks.push(chunk);
      total += chunk.byteLength;
    });
    stream.once("error", reject);
    stream.once("end", () => {
      const out = new Uint8Array(total);
      let offset = 0;
      for (const c of chunks) {
        out.set(c, offset);
        offset += c.byteLength;
      }
      resolve(out);
    });
  });
}

async function deleteSilently(
  bucket: GridFSBucket,
  id: ObjectId,
): Promise<void> {
  try {
    await bucket.delete(id);
  } catch {
    // Already gone — benign. Any other error would surface on next op.
  }
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

// Re-exported for tests.
export type { SidecarState };
