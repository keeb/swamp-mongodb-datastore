import { assertEquals } from "jsr:@std/assert@1";
import { Sidecar } from "./sidecar.ts";

async function withTempCache(
  fn: (cachePath: string) => Promise<void>,
): Promise<void> {
  const dir = await Deno.makeTempDir({ prefix: "mongodb-swamp-sidecar-" });
  try {
    await fn(dir);
  } finally {
    await Deno.remove(dir, { recursive: true });
  }
}

Deno.test("read on missing sidecar returns bulkInvalidated=true (cold start)", async () => {
  await withTempCache(async (cache) => {
    const sc = new Sidecar(cache);
    const state = await sc.read();
    assertEquals(state.bulkInvalidated, true);
    assertEquals(state.dirtyPaths, []);
    assertEquals(state.lastPulledAt, null);
  });
});

Deno.test("read on corrupt sidecar JSON returns bulkInvalidated=true", async () => {
  await withTempCache(async (cache) => {
    await Deno.writeTextFile(
      `${cache}/.datastore-sync-state.json`,
      "{not json",
    );
    const sc = new Sidecar(cache);
    const state = await sc.read();
    assertEquals(state.bulkInvalidated, true);
  });
});

Deno.test("read on unknown schema version returns bulkInvalidated=true", async () => {
  await withTempCache(async (cache) => {
    await Deno.writeTextFile(
      `${cache}/.datastore-sync-state.json`,
      JSON.stringify({ version: 999, dirtyPaths: ["a"] }),
    );
    const sc = new Sidecar(cache);
    const state = await sc.read();
    assertEquals(state.bulkInvalidated, true);
    assertEquals(state.dirtyPaths, []);
  });
});

Deno.test("recordDirty(undefined) flips bulkInvalidated, leaves dirtyPaths alone", async () => {
  await withTempCache(async (cache) => {
    const sc = new Sidecar(cache);
    await sc.recordDirty("data/foo");
    const state = await sc.recordDirty(undefined);
    assertEquals(state.bulkInvalidated, true);
    assertEquals(state.dirtyPaths, ["data/foo"]);
  });
});

Deno.test("recordDirty(path) adds path; dedupes on repeat", async () => {
  await withTempCache(async (cache) => {
    const sc = new Sidecar(cache);
    await sc.recordDirty("data/foo");
    await sc.recordDirty("data/foo");
    await sc.recordDirty("data/bar");
    const state = await sc.read();
    assertEquals(state.dirtyPaths.sort(), ["data/bar", "data/foo"]);
  });
});

Deno.test("clearDirty empties paths and clears bulk flag", async () => {
  await withTempCache(async (cache) => {
    const sc = new Sidecar(cache);
    await sc.recordDirty("data/foo");
    await sc.recordDirty(undefined); // bulkInvalidated = true
    const state = await sc.clearDirty();
    assertEquals(state.dirtyPaths, []);
    assertEquals(state.bulkInvalidated, false);
  });
});

Deno.test("clearDirty preserves lastPulledAt", async () => {
  await withTempCache(async (cache) => {
    const sc = new Sidecar(cache);
    await sc.setLastPulledAt("2026-05-04T12:00:00.000Z");
    await sc.recordDirty("data/foo");
    const state = await sc.clearDirty();
    assertEquals(state.lastPulledAt, "2026-05-04T12:00:00.000Z");
  });
});

Deno.test("setLastPulledAt persists across reads", async () => {
  await withTempCache(async (cache) => {
    const sc = new Sidecar(cache);
    await sc.setLastPulledAt("2026-05-04T12:00:00.000Z");
    const fresh = new Sidecar(cache);
    const state = await fresh.read();
    assertEquals(state.lastPulledAt, "2026-05-04T12:00:00.000Z");
  });
});

Deno.test("concurrent recordDirty calls serialize without losing entries", async () => {
  await withTempCache(async (cache) => {
    const sc = new Sidecar(cache);
    // Cold-start sidecar reads as bulkInvalidated; clear it first so
    // we're testing the serialization, not the cold-start behavior.
    await sc.clearDirty();
    const paths = Array.from({ length: 50 }, (_, i) => `data/file-${i}`);
    await Promise.all(paths.map((p) => sc.recordDirty(p)));
    const state = await sc.read();
    assertEquals(state.dirtyPaths.length, 50);
    assertEquals(new Set(state.dirtyPaths).size, 50);
  });
});

Deno.test("recordDirty after clearDirty preserves cleared bulk flag", async () => {
  await withTempCache(async (cache) => {
    const sc = new Sidecar(cache);
    await sc.clearDirty();
    const state = await sc.recordDirty("data/foo");
    assertEquals(state.bulkInvalidated, false);
    assertEquals(state.dirtyPaths, ["data/foo"]);
  });
});
