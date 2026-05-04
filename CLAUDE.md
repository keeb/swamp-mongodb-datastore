# mongodb-swamp-datastore

Custom swamp `DatastoreProvider` backed by MongoDB. Replaces the coarse
per-model file lock of the filesystem/S3 backends with finer-grained,
event-driven coordination. Built for a **mega swamp** — one shared `.swamp` that
many users and agents read/write concurrently.

## MongoDB primitives in use

- Swamp data is already document-shaped (`attributes`, outputs, run history).
- `findAndModify` gives clean optimistic locking per model document — replaces
  the filesystem lock primitive.

## What to read first

1. **The `swamp-extension-datastore` skill** — authoritative guide for this
   project, with references for api / examples / testing / troubleshooting.
2. **The interface we implement:** `src/domain/datastore/datastore_provider.ts`
   in the swamp source tree — `DatastoreProvider` with `createLock` ,
   `createVerifier` , `createSyncService?` , `resolveDatastorePath` ,
   `resolveCachePath?` .
3. **Supporting types:**
   - `src/domain/datastore/distributed_lock.ts` — `DistributedLock` ,
     `LockOptions`
   - `src/domain/datastore/datastore_health.ts` — `DatastoreVerifier`
   - `src/domain/datastore/datastore_sync_service.ts` — `DatastoreSyncService`
   - `src/domain/datastore/datastore_type_registry.ts`
4. **Built-in filesystem datastore** — canonical reference to mirror:
   - Lock: `src/libswamp/datastores/lock.ts`
   - Setup: `src/libswamp/datastores/setup.ts`
   - Status: `src/libswamp/datastores/status.ts`
   - Sync: `src/libswamp/datastores/sync.ts`
   - Verifier: `src/infrastructure/persistence/filesystem_datastore_verifier.ts`
5. **How swamp registers datastore types:**
   `src/domain/datastore/datastore_types.ts` and `datastore_config.ts` — types
   other than `"filesystem"` are user-defined and loaded via
   `user_datastore_loader.ts` .

All paths above are within the swamp source repo ( `github.com/systeminit/swamp`
); read them against the version of swamp whose `DatastoreProvider` interface
you're targeting.

## Project conventions

- **Extension namespace:** `@keeb/mongodb-datastore`. The `type` string in the
  extension export is what consumer `.swamp.yaml` files reference.
- **Extension layout:** `extensions/datastores/mongodb/`
  - `mod.ts` — provider entry point (wires the five interface methods)
  - `client.ts` — `MongoClient` factory, cached per `repoDir`
  - `config.ts` — Zod `ConfigSchema`, collection naming, `.env` loader
  - `lock.ts` — TTL lock with heartbeat + nonce fencing
  - `sync.ts` — GridFS byte sync of the datastore tier
  - `verifier.ts` — replica-set health check

  Root `manifest.yaml` is the publishable package manifest.
- **Secrets:** the mongo password comes from `$MONGO_PASSWORD` (env var name
  overridable via `passwordEnv` in `ConfigSchema`), loaded by `loadDotEnv()`
  from `<repoDir>/.env` at client-factory time.
- **npm deps are bundled, not lockfile-tracked** (per SWAMP.md rule 7). Pin
  every `npm:` import with an explicit version (e.g. `npm:mongodb@6.17.0` ).

## Architecture decisions

1. **Lock scope.** Global + per-model keys, each keyed on a Mongo doc `_id`.
   `findOneAndUpdate` with `{expiresAt: $lte: now}` gives atomic take-over of
   expired locks. A nonce protects `release` and `forceRelease` from acting on a
   lock that was already reaped.
2. **Sync service.** Swamp core writes a local cache tree under the datastore
   tier. `sync.ts` mirrors those bytes to GridFS so other hosts can pull them
   back.
3. **Actor metadata.** No interface hook for "who." `$USER@$HOSTNAME` (plus pid)
   gets stamped onto every lock doc from the environment.
4. **Bytes vs. metadata split.** Everything lives in Mongo — metadata in normal
   collections, bytes in GridFS.

## Verification

Run before committing:

1. `deno check` — type check
2. `deno lint`
3. `deno fmt`
4. `deno test` — unit tests
5. Integration: swamp CLI against a real replica-set Mongo and at least one
   consumer repo.

## Do not

- Do not commit connection strings or credentials. Config must come from the
  extension's Zod schema plus a gitignored `.env` — not hardcoded constants, not
  committed `.swamp.yaml` values.
- Do not bypass the `DatastoreProvider` interface with side-channel
  reads/writes. Everything must flow through the provider so swamp core owns the
  lifecycle.

Swamp-specific guidance for this repo (rules, skills, getting started) lives in
[SWAMP.md](SWAMP.md).
