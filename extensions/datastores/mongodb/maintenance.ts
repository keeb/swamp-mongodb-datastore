// Blob-store maintenance: reclaiming content-addressed bytes that no live
// path doc references any more (repo issue 012).
//
// Why this is needed: `_blobs` is append-only by design. Deduplication is the
// whole point — N agents pushing identical bytes collapse to one doc — so no
// push may ever delete a blob, since it cannot know whether some *other* path
// still points at that hash. Tombstoning a path in `_paths` therefore leaves
// its bytes behind forever. On proxmox-manager that arithmetic had run for
// months: ~21k live files against a `_blobs` collection holding 931k docs and
// 197 GB.
//
// Safety: an orphan sweep races with a concurrent push, which inserts the blob
// *before* upserting the path doc that references it. A sweep landing in that
// window sees an unreferenced hash and deletes bytes a peer is about to point
// at, leaving a live path doc whose hash resolves to nothing.
//
// The global lock is NOT sufficient on its own. Swamp core does not funnel
// every write through the global lock — a sweep of this collection that held
// it still produced a dangling reference from a concurrent push (a
// `workflow-runs/...yaml` upserted mid-sweep). So the real protection is a
// **grace window**: blobs carry `createdAt`, and anything younger than
// `graceMs` is left alone regardless of reachability, because a push that just
// wrote it may not have upserted its path doc yet. Blobs predating the field
// are definitionally old and always eligible.
//
// Take the global lock anyway — it is cheap defense-in-depth against the
// writers that do honor it — but correctness rests on the grace window.
//
// The sweep is deliberately not wired into pushChanged: it is a maintenance
// operation, not a hot-path one.

import type { Collection } from "npm:mongodb@6.17.0";

export interface BlobDocLike {
  _id: string;
  size: number;
  // Written by pushes from 2026.08.19.1 on. Absent on older docs, which are
  // therefore always past any grace window.
  createdAt?: Date;
}

export interface PathDocLike {
  _id: string;
  hash: string;
  deletedAt: Date | null;
}

export interface OrphanSweepResult {
  liveHashes: number;
  blobDocsScanned: number;
  orphanBlobs: number;
  orphanChunks: number;
  bytesReclaimed: number;
  // Unreferenced but inside the grace window, so deliberately spared. A
  // non-zero count here is normal on a busy cluster.
  skippedTooYoung: number;
  deleted: boolean;
}

// One hour: comfortably longer than the gap between a push's blob insert and
// its path upsert, even for a push moving hundreds of MB.
export const DEFAULT_GRACE_MS = 60 * 60 * 1000;

// Chunked blobs store a header under the bare hash and chunks under
// `<hash>:<n>`. A chunk is an orphan exactly when its header's hash is.
export function chunkParentHash(blobId: string): string | null {
  const colon = blobId.lastIndexOf(":");
  if (colon < 0) return null;
  return blobId.slice(0, colon);
}

const DELETE_BATCH = 1000;

// Collects blobs unreferenced by any *live* path doc.
//
// Tombstoned paths are intentionally not treated as references. A peer whose
// watermark predates the tombstone will pull the tombstone and unlink its
// local copy; it never fetches the bytes (hydrateFile refuses a doc with
// deletedAt set), so keeping them buys nothing.
//
// `dryRun` reports what would go without touching anything — always worth
// running first against a shared cluster.
export async function sweepOrphanBlobs(
  paths: Collection<PathDocLike>,
  blobs: Collection<BlobDocLike>,
  opts?: {
    dryRun?: boolean;
    graceMs?: number;
    now?: Date;
    onProgress?: (scanned: number) => void;
  },
): Promise<OrphanSweepResult> {
  const dryRun = opts?.dryRun === true;
  const graceMs = opts?.graceMs ?? DEFAULT_GRACE_MS;
  const cutoff = (opts?.now ?? new Date()).getTime() - graceMs;

  // Reference set: distinct hashes of live paths. Bounded by the working set
  // (~21k on proxmox-manager), not by the blob store.
  const liveHashes = new Set<string>();
  for await (
    const doc of paths.find(
      { deletedAt: null },
      { projection: { hash: 1 } },
    )
  ) {
    if (typeof doc.hash === "string") liveHashes.add(doc.hash);
  }

  let blobDocsScanned = 0;
  let orphanBlobs = 0;
  let orphanChunks = 0;
  let bytesReclaimed = 0;
  let skippedTooYoung = 0;
  let batch: string[] = [];

  const flush = async () => {
    if (batch.length === 0) return;
    if (!dryRun) {
      await blobs.deleteMany({ _id: { $in: batch } });
    }
    batch = [];
  };

  // Stream every blob id + size. Sizes come from the doc rather than
  // collStats so the reclaim figure is exact for the docs we actually remove.
  for await (
    const blob of blobs.find({}, {
      projection: { _id: 1, size: 1, createdAt: 1 },
    })
  ) {
    blobDocsScanned++;
    if (opts?.onProgress && blobDocsScanned % 100_000 === 0) {
      opts.onProgress(blobDocsScanned);
    }
    const parent = chunkParentHash(blob._id);
    const referenced = parent === null
      ? liveHashes.has(blob._id)
      : liveHashes.has(parent);
    if (referenced) continue;

    // Unreferenced, but possibly only because the push that wrote it has not
    // upserted its path doc yet. Docs with no `createdAt` predate the field
    // and are always past the window.
    if (blob.createdAt !== undefined && blob.createdAt.getTime() > cutoff) {
      skippedTooYoung++;
      continue;
    }

    if (parent === null) orphanBlobs++;
    else orphanChunks++;
    bytesReclaimed += blob.size ?? 0;
    batch.push(blob._id);
    if (batch.length >= DELETE_BATCH) await flush();
  }
  await flush();

  return {
    liveHashes: liveHashes.size,
    blobDocsScanned,
    orphanBlobs,
    orphanChunks,
    bytesReclaimed,
    skippedTooYoung,
    deleted: !dryRun,
  };
}
