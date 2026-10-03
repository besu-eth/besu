# Bonsai cross-block cache and versioning

This package implements the **versioned cross-block cache** used by Bonsai world state storage when `bonsaiCrossBlockCacheEnabled` is on. The design keeps a **single shared cache** (accounts and storage slots) correct across **reorgs**, **snapshots**, and **head** reads, without tying versions to block numbers.

The implementation lives mainly in `VersionedCacheManager`; `BonsaiWorldStateKeyValueStorage` holds each storage view’s **pinned cache version** and bumps it on commit.

---

## Global monotonic version (not the block number)

`VersionedCacheManager` maintains an `AtomicLong globalVersion`, exposed as `getCurrentVersion()` / `incrementAndGetVersion()`.

- The version **increments on every successful world-state commit** (when `CachedUpdater` commits and applies staged cache updates), not when a block is “named” or finalized.
- It is **not** the chain head block number. Two different blocks could theoretically correspond to similar heights before/after a reorg; the **version counter still moves forward on each commit**, so **two different persisted histories do not “reuse” the same cache generation** in a way that would confuse readers.

**Example**

1. Start: `globalVersion = 0`.
2. Commit A (e.g. block 100 on branch 1): `globalVersion` becomes `1`.
3. Reorg: you roll back and commit B (e.g. again “at” 100 on branch 2): `globalVersion` becomes `2`.

Block height may repeat; **cache generation does not**. Entries written at version `1` are not treated as authoritative for a reader that is pinned to an older or incompatible view in the ways described below.

---

## Pinned version per storage view (head vs snapshot)

`BonsaiWorldStateKeyValueStorage` stores `cacheVersion` (see `getCurrentVersion()` on the storage instance).

- **Persisted / head storage**: after each commit, `cacheVersion` is updated to match the new `globalVersion` (increment happens as part of `CachedUpdater.commit()`).
- **Snapshot storage**: when a snapshot is created (`BonsaiSnapshotWorldStateKeyValueStorage`), it copies the parent’s `getCurrentVersion()` at creation time and **keeps that value**. The parent can commit again and advance its own `cacheVersion` and `globalVersion`; **the snapshot’s pinned version stays immutable**.

So the snapshot is tied to a **logical cache epoch** taken at snapshot time, not to “whatever the cache says later”.

**Example**

| Step | Head `cacheVersion` | Snapshot (taken at step 1) `cacheVersion` | `globalVersion` |
|------|----------------------|---------------------------------------------|-----------------|
| 1 Snapshot of head | 5 | 5 | 5 |
| 2 Head commits | 6 | **5** (unchanged) | 6 |
| 3 Head commits | 7 | **5** | 7 |

The snapshot always reads with **version 5**; the head reads with **7**. Cache rules below ensure the snapshot does not observe newer cache-only state that belongs to epochs **after** its pin.

---

## At most one cached generation per key (favor the head)

The cache maps each key (e.g. account hash, or account||slot for storage) to a single `VersionedValue`: value (or “removed”), **writer version**, and removal flag.

On each write to that key at version **V**, if the existing entry is older (`existing.version < V`), it is **replaced**. There is no parallel retention of “account A at version 2” and “account A at version 3” in the same map.

**Example**

- Commit 1: account `A` written → cache holds `A` with `version = 1`.
- Commit 2: account `A` updated → same key `A` now holds `version = 2` (version 1 is gone from the cache).

Eviction under `maximumSize` can drop entries too; correctness does not rely on keeping all history—only on **version checks** and **backing storage**.

---

## Read path: when is the cache used?

For `getFromCacheOrStorage(segment, key, version, storageGetter)`:

1. **Hit** (use cache, skip storage for the value) if there is an entry and  
   `cachedEntry.version <= readerVersion`  
   So the entry must be **at or before** the reader’s pinned epoch: the reader is allowed to see cached data that is **not newer than their view**.

2. **Miss** → load from storage (`storageGetter`).

3. **Populate cache on miss** only if  
   `readerVersion == globalVersion`  
   i.e. only the view that is still aligned with the **current** global epoch may **insert** results of a read through this path. Snapshots pinned to an older version typically fail this equality once the head has committed again, so they **read storage** but **do not warm the cache** from their misses.

Together, this **prioritizes cache validity for the moving head**: the head can still insert when it is the current global epoch; **stale pinned views do not push or overwrite head-oriented cache state** through the read path.

**Brief window after snapshot creation:** until the head commits again, a snapshot’s pinned version may still **equal** `globalVersion`. In that window, read-path inserts are still allowed for that snapshot (same epoch as the head). As soon as the head commits and bumps `globalVersion`, the snapshot stays on the old pin and **stops** populating the cache from read misses.

**Example: snapshot after head moved**

- Snapshot pinned at `5`, cache entry for key `K` was written at `7` (head activity).
- Snapshot reads `K` with `readerVersion = 5`: condition `7 <= 5` is false → **miss**, read from snapshot DB.
- On miss, `5 == globalVersion` is false if `globalVersion` is `7` → **no insert**.

**Example: head read**

- Head `cacheVersion` and `globalVersion` are both `7`.
- Miss, load from DB, then `7 == 7` → entry may be cached at version `7`.

`putInCache` / `removeFromCache` (used from `CachedUpdater.updateCache()` after commit) still apply **writer** versions explicitly; the head’s committed writes always use the new post-increment version.

---

## Reorgs and “same block height, different version”

Because the version counter advances on **every commit** along the actually executed chain state, **different committed histories do not share the same version line** in the sense of cache staleness across incompatible branches: a reader pinned to an older `cacheVersion` will not accept a cached value produced at a **newer** cache version (`cached.version <= reader` fails), and will fall back to **its** storage (e.g. snapshot KV).

---

## What segments are cached?

`VersionedCacheManager` only caches:

- `ACCOUNT_INFO_STATE`
- `ACCOUNT_STORAGE_STORAGE`

Other segments (e.g. code, trie branches) are not covered by this versioned cache.

---

## Related code

| Piece | Role |
|--------|------|
| `VersionedCacheManager` | `globalVersion`, Caffeine caches, hit/miss/insert rules |
| `BonsaiWorldStateKeyValueStorage.CachedUpdater` | `incrementCacheVersion()` on commit, `updateCache()` writes/removals at new version |
| `BonsaiSnapshotWorldStateKeyValueStorage` | Constructor passes parent `getCurrentVersion()` into `super(...)` so snapshot pins version |
| `BonsaiWorldStateKeyValueStorageCacheTest` | Examples: version progression, overwrite single slot, rollback does not bump version |

---

## Commit publish and concurrency

`CachedUpdater.commit()` goes through `FlatDbCacheManager.commitAndPublish()`, which holds the cache's publish lock, enables the commit cache bypass, commits storage, allocates the next version and hands it to the updater, which publishes its staged writes at exactly that version:

- **Serialized publishes**: concurrent updaters (e.g. snap sync pipelines) commit one at a time, so the cache version order matches the storage commit order and the head `cacheVersion` never moves backwards. Each updater publishes at the version it obtained itself.
- **Read-path inserts re-check under the key lock**: a reader inserts a value it loaded from storage only if, inside `compute`, no bypass is active and its pinned version is still `globalVersion`. A read that overlapped a commit therefore cannot shadow the published value, even if that entry was evicted in between.
- **Clearing bumps the version**: `clearCrossBlockCache()` calls `FlatDbCacheManager.invalidateAll()`, which (under the same lock) advances the version before invalidating, so a read that loaded a pre-clear value cannot repopulate the cache after the clear.

---

## Disabled during the initial sync

`BesuControllerBuilder` disables the cache while the initial (snap) sync runs and enables it on `onInitialSyncCompleted()` (disabling it again on `onInitialSyncRestart()`). While disabled, reads go straight to storage, nothing is inserted or published, and `commitAndPublish()` does not take the publish lock, so concurrent sync pipelines are never serialized. Commits still advance the version under the bypass, so a read that loaded a value around a disable → commit → enable sequence cannot insert it afterwards. Managers built directly (tests, tools) start enabled.

---

## Operational note

Cache maintenance (Caffeine cleanup) is deferred on purpose: Caffeine's drain task is queued by `ThresholdDrainExecutor` and only run by `scheduleAsyncMaintenance()` after each commit, so no eviction work runs while a block is being processed. Only new keys use Caffeine's bounded write buffer (`128 × ceilPow2(NCPU)` entries); an inserting thread performs maintenance inline only if more new keys than that are inserted between two commits.
