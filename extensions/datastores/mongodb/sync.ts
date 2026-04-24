import { type Collection, GridFSBucket, ObjectId } from "npm:mongodb@6.17.0";
import type { ClientHandle } from "./client.ts";
import {
  fsMetaCollectionName,
  gridfsBucketName,
  type MongoDatastoreConfig,
} from "./config.ts";

export interface DatastoreSyncService {
  pullChanged(): Promise<number>;
  pushChanged(): Promise<number>;
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
  async function resources(): Promise<{
    meta: Collection<FileMetaDoc>;
    bucket: GridFSBucket;
  }> {
    const { client } = await getClient(repoDir);
    const db = client.db(cfg.database);
    const meta = db.collection<FileMetaDoc>(fsMetaCollectionName(cfg));
    const bucket = new GridFSBucket(db, { bucketName: gridfsBucketName(cfg) });
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

  return {
    async pushChanged(): Promise<number> {
      const { meta, bucket } = await resources();
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
          // The walk saw this file; a concurrent write (e.g. SQLite
          // rotating a -wal file) may have removed it before upload.
          // Treat as transiently absent and let the next sync pick it up.
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

      for (const [relPath, doc] of remote) {
        if (local.has(relPath) || doc.deletedAt !== null) continue;
        await meta.updateOne(
          { _id: relPath },
          { $set: { deletedAt: new Date(), updatedAt: new Date() } },
        );
        await deleteSilently(bucket, doc.gridfsFileId);
        changes++;
      }

      return changes;
    },

    async pullChanged(): Promise<number> {
      const { meta, bucket } = await resources();
      const local = await walkLocal();
      const remote = await loadMeta(meta);
      let changes = 0;

      for (const [relPath, doc] of remote) {
        if (doc.deletedAt !== null) {
          if (local.has(relPath)) {
            await removeSilently(`${cachePath}/${relPath}`);
            changes++;
          }
          continue;
        }
        const localEntry = local.get(relPath);
        if (localEntry && localEntry.hash === doc.hash) continue;

        const bytes = await downloadBytes(bucket, doc.gridfsFileId);
        await writeFileAtomic(`${cachePath}/${relPath}`, bytes);
        changes++;
      }

      return changes;
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

async function removeSilently(path: string): Promise<void> {
  try {
    await Deno.remove(path);
  } catch (err) {
    if (!(err instanceof Deno.errors.NotFound)) throw err;
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
