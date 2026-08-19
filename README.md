# MongoDB Datastore

Custom swamp `DatastoreProvider` backed by MongoDB.

Built for a **mega swamp** — one shared `.swamp` that many users and agents
read/write concurrently.

Replaces the coarse per-model file lock of the filesystem/S3 backends with
finer-grained, event-driven coordination.

## Requirements

- MongoDB 4.0+ running as a replica set in any configuration - single node is
  fine.
- Swamp CLI with extension support.

## Install

Three steps. You need a swamp repo and a MongoDB replica set you can reach.

**1. Pull the extension into your swamp repo:**

```bash
swamp extension pull @keeb/mongodb-datastore
```

**2. Add it as the datastore in your repo's `.swamp.yaml`:**

```yaml
datastore:
  type: "@keeb/mongodb-datastore"
  config:
    uri: "mongodb://mongo.example.com:27017/?replicaSet=rs0&authSource=admin"
    username: "swamp-user"
    passwordEnv: "MONGO_PASSWORD"
    database: "swamp"
    tenantId: "my-org"
    namespace: "my-repo"
```

See [Configuration](#configuration) for field-by-field descriptions.

**3. Put your MongoDB password in `<repoDir>/.env` (gitignored):**

```
MONGO_PASSWORD=...
```

Swamp picks it up on the next invocation.

## Configuration

| Field              | Type   | Required | Default          | Description                                                                 |
| ------------------ | ------ | -------- | ---------------- | --------------------------------------------------------------------------- |
| `uri`              | string | yes      | —                | MongoDB URI. Must resolve to a replica set.                                 |
| `username`         | string | yes      | —                | Mongo user, passed to the driver as an auth option.                         |
| `passwordEnv`      | string | no       | `MONGO_PASSWORD` | Env var name holding the password. Loaded from `<repoDir>/.env` at startup. |
| `database`         | string | no       | `swamp`          | Shared database; per-repo isolation is by collection prefix.                |
| `tenantId`         | string | no       | `default`        | Tenant identifier; part of the collection prefix.                           |
| `namespace`        | string | yes      | —                | Per-repo identifier; part of the collection prefix.                         |
| `defaultLockTtlMs` | number | no       | `30000`          | Default lock TTL. Must exceed your longest critical section.                |

Collections are prefixed `t_<tenantId>_r_<namespace>_*` — `_locks` for lock
docs, `_paths` for the manifest, `_blobs` for content-addressed bytes.

## What it does

- **Distributed lock.** `findOneAndUpdate` on a lock doc, TTL + heartbeat
  refresh, nonce fenced on `release` and `forceRelease`. Global + per-model
  keys.
- **Manifest + content-addressed blob sync.** The datastore-tier cache tree
  (`.swamp/<cache>/{data,outputs,workflow-runs,...}`) is split across two
  collections: `_paths` holds one doc per file
  (`{_id: relPath, hash, size,
  updatedAt, deletedAt}`) and `_blobs` holds
  bytes keyed by their sha256. Pull = cursor over `_paths` since the last
  watermark + bulk `$in` over `_blobs` for the unique hashes the host doesn't
  already have. Push = hash locally, upsert any blob that's missing (idempotent
  on the hash `_id`), upsert path docs in bulk. Identical bytes pushed by N
  agents collapse to one blob server-side; renames are free; the cursor itself
  is the wire transport (no per-file roundtrips).
- **Dirty tracking via an append-only journal.** `markDirty` appends one line to
  `<cache>/.datastore-dirty.log` — no read, no parse, no rewrite. The JSON
  sidecar next to it (`.datastore-sync-state.json`) holds only scalars
  (watermarks and flags) and is rewritten only when one of them changes. On
  push, the journal is deduped and **coalesced**: a dirty directory absorbs
  every dirty path beneath it, since the push walks a dirty directory in full.
  Past `MAX_DIRTY_PATHS` (10k) tracking degrades to a single full walk, which is
  cheaper than reconciling that many roots individually.
- **Health verifier.** Rejects non-replica-set clusters and reports
  primary/secondary state, latency, and namespace.

## Maintenance

- **Blob GC.** `_blobs` is append-only by design — dedup means no push can know
  whether some other path still references a hash — so tombstoning a path leaves
  its bytes behind. Reclaim them out of band:

  ```bash
  deno task blob-gc --repo /path/to/repo --dry-run   # always first
  deno task blob-gc --repo /path/to/repo --confirm
  deno task blob-gc --repo /path/to/repo --confirm --grace-minutes 120
  ```

  A push inserts a blob _before_ upserting the path doc that references it, so a
  sweep landing between the two would delete bytes a peer is about to point at.
  Two defenses, in order of importance:

  1. **Grace window (the real one).** Blobs carry `createdAt`; anything younger
     than `--grace-minutes` (default 60) is spared regardless of reachability.
  2. **Global lock**, held for the sweep's duration — defense in depth only.
     Swamp core does not funnel every write through it. A real sweep that held
     the lock still lost one blob to a concurrent push, which is why the grace
     window exists. Blobs written before 2026.08.19.1 have no `createdAt` and
     are always eligible, so the first sweep after upgrading is the risky one:
     run it when the cluster is quiet.

  A dangling reference is not fatal — pull skips a path whose blob is missing,
  and the owning host re-uploads the bytes on its next full walk, since the push
  probes blob existence independently of the path diff. Do **not** "fix" one by
  tombstoning the path: that deletes the owning host's local copy on its next
  pull.

- **Version retention is swamp's job, not the datastore's.** The largest sync
  costs come from unbounded data versions, which this extension faithfully
  mirrors. Check `garbageCollection` on your model types' output specs and run
  `swamp data gc` — on one real repo that took `data/` from 229,598 files to
  1,258.

## Important Information

- **Vault secrets do not travel.** Swamp's `local_encryption` vault reads and
  writes `<repoDir>/.swamp/secrets/...` on local disk regardless of datastore.
  This extension excludes the `secrets/` tier from sync entirely — neither the
  symmetric `.key` files nor their `.enc` ciphertext are ever pushed to MongoDB,
  and any `secrets/*` docs left in the remote by an older version are skipped on
  pull. Vault contents stay per-host; use a non-local (KMS-backed) vault if you
  need cross-host secrets.

  > **Security note (versions ≤ 2026.05.25.1):** earlier releases listed
  > `secrets` in the synced tier, so a repo that switched to this datastore
  > pushed every vault `.key` next to its `.enc` ciphertext into the shared
  > MongoDB — anyone with read access could decrypt them (CVE-class
  > encryption-at-rest defeat). After upgrading, **rotate every secret that was
  > synced** and purge the leaked docs from MongoDB, e.g.:
  >
  > ```js
  > // hashes of the now-orphaned secret blobs, to drop after tombstoning paths
  > const hashes = db["<prefix>_paths"].find(
  >   { _id: /^secrets\// },
  >   { hash: 1 },
  > ).map((d) => d.hash);
  > db["<prefix>_paths"].deleteMany({ _id: /^secrets\// });
  > db["<prefix>_blobs"].deleteMany({ _id: { $in: hashes } });
  > ```
- **TTL must exceed your critical section.** The lock's nonce fences `release` /
  `forceRelease` only; it does not fence writes performed inside the critical
  section. If a holder pauses past TTL, another process can legitimately take
  over while the first still believes it holds the lock. Size `defaultLockTtlMs`
  with margin.
- **`swamp datastore setup` can OOM on large existing `.swamp/` trees.** Swamp
  core's migrator reads the tree into memory; at ~1 GB / ~100k files it dies.
  Purge `.swamp/` first, or use `--skip-migration` and let workflows repopulate.
- **Host-local files are never synced.** `*.db`, `*.db-wal`, `*.db-shm` (swamp's
  SQLite catalogs) and in-flight `*.tmp.<pid>.<uuid>` staging files are excluded
  on both legs. A `-shm` file is a mmap'd shared-memory region that means
  nothing off the machine that made it, and both it and `-wal` churn on every
  command — syncing them re-uploaded a blob per invocation for bytes no peer
  could correctly consume.
- **Two watermarks, not one.** `lastPulledAt` tracks hydrated content and drives
  pull; `lastReconciledAt` tracks when this cache last enumerated the complete
  remote path list and drives the push tombstone pass. They must stay separate:
  a push stamps `updatedAt = now` on every path it writes, so those docs sort
  newer than `lastPulledAt` and the tombstone pass — which skips anything newer,
  to protect a peer's concurrent writes — would refuse to ever delete them. The
  symptom is a host that cannot propagate deletion of data it pushed itself:
  `swamp data gc` prunes locally and the remote keeps every version.

## Related

[`@keeb/mongodb`](https://github.com/keeb/swamp-mongodb) — sibling extension for
querying MongoDB collections from swamp workflows. Different extension (a
_model_ , not a datastore).

## Development

Contributor notes: [CLAUDE.md](CLAUDE.md) and [SWAMP.md](SWAMP.md).

## License

MIT.
