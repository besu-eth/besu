# Frontier Trie Migration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the I/O-bound `seekForPrev` reads in the archive migrator's checkpoint `persist()` path with fast bloom-accelerated point lookups against a dedicated frontier CF (`TRIE_BRANCH_MIGRATION`).

**Architecture:** Split the migrator's read and write substrates. The existing `TRIE_BRANCH_STORAGE_ARCHIVE` (multi-version suffix-keyed CF) remains the sole proof output, written exactly as today. A new single-version plain-key frontier CF (`TRIE_BRANCH_MIGRATION`) holds the migrator's current trie for fast point reads during `persist()`. `BonsaiArchiveMigrationTrieNodeStrategy` is rewritten to read from the frontier CF and dual-write to both CFs. `MigrationTransaction` is updated to route frontier CF writes to real storage.

**Tech Stack:** Java 21, RocksDB (via `SegmentedKeyValueStorage`), JUnit 5, Mockito, AssertJ

---

### Task 1: Write the failing test

**Files:**
- Modify: `ethereum/core/src/test/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsaiarchive/BonsaiFlatDbToArchiveMigratorTest.java`

- [ ] **Step 1: Add the import for `TRIE_BRANCH_MIGRATION`**

In `BonsaiFlatDbToArchiveMigratorTest.java`, add this import alongside the existing `TRIE_BRANCH_STORAGE_ARCHIVE` import:

```java
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_MIGRATION;
```

- [ ] **Step 2: Write the failing test**

Add this test in the `// --- trie checkpoint tests ---` section, right after `trieCheckpointWritesNodesToArchiveStorage`:

```java
@Test
public void trieCheckpointWritesNodesToFrontierCF() throws Exception {
  // Frontier CF (TRIE_BRANCH_MIGRATION) must receive plain-key trie node writes during
  // checkpoint persist() so subsequent persist() calls read via point lookup instead of
  // seekForPrev on the multi-version archive CF.
  final Hash stateRoot = computeTestAccountStateRoot();
  final Block genesis = blockchain.getBlockByNumber(0).orElseThrow();
  final Block block1 =
      blockDataGenerator.block(
          BlockDataGenerator.BlockOptions.create()
              .setParentHash(genesis.getHash())
              .setBlockNumber(1)
              .setStateRoot(stateRoot));
  blockchain.appendBlock(block1, blockDataGenerator.receipts(block1));

  // interval=2: checkpoint fires at block 1 ((1+1) % 2 == 0)
  final BonsaiFlatDbToArchiveMigrator migrator = createMigratorWithRealTrieLogs(2);
  migrator.migrate().get(MIGRATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);

  assertThat(storage.streamKeys(TRIE_BRANCH_MIGRATION).count()).isGreaterThan(0);
}
```

- [ ] **Step 3: Run the test to verify it fails**

```bash
./gradlew :ethereum:core:test \
  --tests "BonsaiFlatDbToArchiveMigratorTest.trieCheckpointWritesNodesToFrontierCF" \
  --info 2>&1 | grep -E "PASSED|FAILED|ERROR|Exception" | head -20
```

Expected: **FAILED** — `TRIE_BRANCH_MIGRATION` has 0 entries (nothing writes to it yet).

---

### Task 2: Rewrite `BonsaiArchiveMigrationTrieNodeStrategy`

**Files:**
- Modify: `ethereum/core/src/main/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/storage/flat/BonsaiArchiveMigrationTrieNodeStrategy.java`

The new strategy:
- **Reads** from `TRIE_BRANCH_MIGRATION` via point `get()` (bloom-accelerated, cache-resident, no `seekForPrev`), with fallback to `TRIE_BRANCH_STORAGE` for genesis/unchanged nodes not yet in the frontier CF.
- **Writes** each new/changed node to both `TRIE_BRANCH_MIGRATION` (plain key, overwrite-in-place) and `TRIE_BRANCH_STORAGE_ARCHIVE` (suffixed, proof output — identical to today).
- **Removes** from `TRIE_BRANCH_MIGRATION` only (archive nodes are immutable once written).

Note: `BonsaiTrieNodeStrategy(TRIE_BRANCH_MIGRATION)` already handles point read/write against a configurable segment — reuse it for both the frontier read and the fallback, rather than duplicating the key logic.

- [ ] **Step 1: Replace the file with the new implementation**

```java
/*
 * Copyright contributors to Hyperledger Besu.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat;

import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_MIGRATION;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE_ARCHIVE;
import static org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedWorldStateKeyValueStorage.ARCHIVE_PROOF_BLOCK_NUMBER_KEY;
import static org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedWorldStateKeyValueStorage.ARCHIVE_PROOF_CHECKPOINT_INTERVAL_KEY;
import static org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedWorldStateKeyValueStorage.WORLD_BLOCK_NUMBER_KEY;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache.BonsaiCachedMerkleTrieLoader;
import org.hyperledger.besu.ethereum.trie.pathbased.common.BonsaiContext;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;

import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/**
 * Trie node strategy used by the Bonsai archive flat-DB migrator during checkpoint trie rebuilds.
 *
 * <p>Reads from {@code TRIE_BRANCH_MIGRATION} (the frontier CF) via a plain point {@code get()}
 * — bloom-accelerated and cache-resident, identical in performance to a normal bonsai trie build.
 * Falls back to live {@code TRIE_BRANCH_STORAGE} for genesis/unchanged nodes not yet in the
 * frontier CF.
 *
 * <p>Writes each new/changed node to <em>both</em>:
 * <ol>
 *   <li>{@code TRIE_BRANCH_MIGRATION} — plain key, overwrite-in-place (the frontier read substrate)
 *   <li>{@code TRIE_BRANCH_STORAGE_ARCHIVE} — suffixed by the checkpoint window boundary (proof
 *       output, identical to before)
 * </ol>
 *
 * <p>The proof query path ({@link BonsaiArchiveTrieNodeStrategy}) is untouched — it continues to
 * read from {@code TRIE_BRANCH_STORAGE_ARCHIVE} via {@code getNearestBeforeMatchLength}.
 */
public class BonsaiArchiveMigrationTrieNodeStrategy implements TrieNodeStrategy {

  private final Long trieNodeCheckpointInterval;
  private final BonsaiCachedMerkleTrieLoader trieLoader;
  private final TrieNodeStrategy frontierReadStrategy;
  private final TrieNodeStrategy liveTrieFallback;
  private volatile boolean intervalSeeded = false;

  public BonsaiArchiveMigrationTrieNodeStrategy(
      final Long trieNodeCheckpointInterval, final BonsaiCachedMerkleTrieLoader trieLoader) {
    this.trieNodeCheckpointInterval = trieNodeCheckpointInterval;
    this.trieLoader = trieLoader;
    this.frontierReadStrategy = new BonsaiTrieNodeStrategy(TRIE_BRANCH_MIGRATION);
    this.liveTrieFallback = new BonsaiTrieNodeStrategy();
  }

  @Override
  public Optional<Bytes> getFlatAccountTrieNode(
      final Bytes location, final Bytes32 nodeHash, final SegmentedKeyValueStorage storage) {
    return frontierReadStrategy
        .getFlatAccountTrieNode(location, nodeHash, storage)
        .or(() -> liveTrieFallback.getFlatAccountTrieNode(location, nodeHash, storage));
  }

  @Override
  public Optional<Bytes> getFlatStorageTrieNode(
      final Hash accountHash,
      final Bytes location,
      final Bytes32 nodeHash,
      final SegmentedKeyValueStorage storage) {
    return frontierReadStrategy
        .getFlatStorageTrieNode(accountHash, location, nodeHash, storage)
        .or(
            () ->
                liveTrieFallback.getFlatStorageTrieNode(accountHash, location, nodeHash, storage));
  }

  @Override
  public void putFlatAccountTrieNode(
      final SegmentedKeyValueStorage storage,
      final SegmentedKeyValueStorageTransaction transaction,
      final Bytes location,
      final Bytes32 nodeHash,
      final Bytes node) {
    // Frontier CF: plain key, overwrite-in-place
    frontierReadStrategy.putFlatAccountTrieNode(storage, transaction, location, nodeHash, node);
    // Archive CF: suffixed for proof serving
    if (trieNodeCheckpointInterval != null) {
      ensureIntervalSeeded(storage);
      final BonsaiContext ctx = getWriteContext(storage);
      transaction.put(
          TRIE_BRANCH_STORAGE_ARCHIVE,
          BonsaiArchiveKeyUtil.calculateArchiveKeyWithMinSuffix(ctx, location.toArrayUnsafe()),
          node.toArrayUnsafe());
    }
    if (trieLoader != null) {
      trieLoader.putAccountNode(nodeHash, node);
    }
  }

  @Override
  public void putFlatStorageTrieNode(
      final SegmentedKeyValueStorage storage,
      final SegmentedKeyValueStorageTransaction transaction,
      final Hash accountHash,
      final Bytes location,
      final Bytes32 nodeHash,
      final Bytes node) {
    // Frontier CF: plain key, overwrite-in-place
    frontierReadStrategy.putFlatStorageTrieNode(
        storage, transaction, accountHash, location, nodeHash, node);
    // Archive CF: suffixed for proof serving
    if (trieNodeCheckpointInterval != null) {
      ensureIntervalSeeded(storage);
      final BonsaiContext ctx = getWriteContext(storage);
      final Bytes accountHashLocation = Bytes.concatenate(accountHash.getBytes(), location);
      transaction.put(
          TRIE_BRANCH_STORAGE_ARCHIVE,
          BonsaiArchiveKeyUtil.calculateArchiveKeyWithMinSuffix(
              ctx, accountHashLocation.toArrayUnsafe()),
          node.toArrayUnsafe());
    }
    if (trieLoader != null) {
      trieLoader.putStorageNode(nodeHash, node);
    }
  }

  @Override
  public void removeFlatAccountStateTrieNode(
      final SegmentedKeyValueStorage storage,
      final SegmentedKeyValueStorageTransaction transaction,
      final Bytes location) {
    frontierReadStrategy.removeFlatAccountStateTrieNode(storage, transaction, location);
  }

  private BonsaiContext getWriteContext(final SegmentedKeyValueStorage storage) {
    final Optional<byte[]> proofBlockNumber =
        storage.get(TRIE_BRANCH_STORAGE, ARCHIVE_PROOF_BLOCK_NUMBER_KEY);
    if (proofBlockNumber.isPresent()) {
      return new BonsaiContext(Bytes.wrap(proofBlockNumber.get()).toLong());
    }
    return storage
        .get(TRIE_BRANCH_STORAGE, WORLD_BLOCK_NUMBER_KEY)
        .map(
            bytes -> {
              final long blockNumber = Bytes.wrap(bytes).toLong();
              final long windowStart =
                  ((blockNumber + 1) / trieNodeCheckpointInterval) * trieNodeCheckpointInterval;
              return new BonsaiContext(windowStart);
            })
        .orElse(new BonsaiContext(0L));
  }

  private void ensureIntervalSeeded(final SegmentedKeyValueStorage storage) {
    if (intervalSeeded) return;
    synchronized (this) {
      if (intervalSeeded) return;
      storage
          .get(TRIE_BRANCH_STORAGE_ARCHIVE, ARCHIVE_PROOF_CHECKPOINT_INTERVAL_KEY)
          .ifPresentOrElse(
              persistedBytes -> {
                final long persisted = Bytes.wrap(persistedBytes).toLong();
                if (persisted != trieNodeCheckpointInterval) {
                  throw new RuntimeException(
                      "Checkpoint interval mismatch (DB="
                          + persisted
                          + ", config="
                          + trieNodeCheckpointInterval
                          + ")");
                }
              },
              () -> {
                final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
                tx.put(
                    TRIE_BRANCH_STORAGE_ARCHIVE,
                    ARCHIVE_PROOF_CHECKPOINT_INTERVAL_KEY,
                    Bytes.ofUnsignedLong(trieNodeCheckpointInterval).toArrayUnsafe());
                tx.commit();
              });
      intervalSeeded = true;
    }
  }
}
```

- [ ] **Step 2: Run the failing test to see if it now passes**

```bash
./gradlew :ethereum:core:test \
  --tests "BonsaiFlatDbToArchiveMigratorTest.trieCheckpointWritesNodesToFrontierCF" \
  --info 2>&1 | grep -E "PASSED|FAILED|ERROR|Exception" | head -20
```

Expected: **FAILED** — the test still fails because `MigrationTransaction.put()` drops `TRIE_BRANCH_MIGRATION` writes (they're not in the routing table yet).

---

### Task 3: Route `TRIE_BRANCH_MIGRATION` writes in `MigrationTransaction`

**Files:**
- Modify: `ethereum/core/src/main/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsaiarchive/BonsaiFlatDbToArchiveMigrator.java`

`MigrationTransaction` is a private inner class. Its `put()` currently has three cases:
1. `TRIE_BRANCH_STORAGE` → in-memory (metadata keys only)
2. `TRIE_BRANCH_STORAGE_ARCHIVE` → real tx
3. Everything else (flat account/storage) → dropped

Add `TRIE_BRANCH_MIGRATION` as a fourth case → real tx. Same pattern for `remove()`.

- [ ] **Step 1: Add the `TRIE_BRANCH_MIGRATION` static import**

At the top of `BonsaiFlatDbToArchiveMigrator.java`, add to the existing static imports:

```java
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_MIGRATION;
```

- [ ] **Step 2: Update `MigrationTransaction.put()`**

Locate the `MigrationTransaction` inner class `put()` method (around line 643):

```java
    @Override
    public void put(final SegmentIdentifier segmentId, final byte[] key, final byte[] value) {
      if (segmentId == TRIE_BRANCH_STORAGE) {
        // Metadata keys only (WORLD_*, ARCHIVE_PROOF_BLOCK_NUMBER_KEY) — kept in-memory so they
        // never touch live HEAD's TRIE_BRANCH_STORAGE.
        inMemoryTx.put(segmentId, key, value);
      } else if (segmentId == TRIE_BRANCH_STORAGE_ARCHIVE) {
        realTx.put(segmentId, key, value);
      }
      // flat account/storage writes dropped — processBlock() handles those separately
    }
```

Replace with:

```java
    @Override
    public void put(final SegmentIdentifier segmentId, final byte[] key, final byte[] value) {
      if (segmentId == TRIE_BRANCH_STORAGE) {
        // Metadata keys only (WORLD_*, ARCHIVE_PROOF_BLOCK_NUMBER_KEY) — kept in-memory so they
        // never touch live HEAD's TRIE_BRANCH_STORAGE.
        inMemoryTx.put(segmentId, key, value);
      } else if (segmentId == TRIE_BRANCH_STORAGE_ARCHIVE || segmentId == TRIE_BRANCH_MIGRATION) {
        realTx.put(segmentId, key, value);
      }
      // flat account/storage writes dropped — processBlock() handles those separately
    }
```

- [ ] **Step 3: Update `MigrationTransaction.remove()`**

Locate the `remove()` method:

```java
    @Override
    public void remove(final SegmentIdentifier segmentId, final byte[] key) {
      if (segmentId == TRIE_BRANCH_STORAGE) {
        inMemoryTx.remove(segmentId, key);
      }
      // archive removes dropped
    }
```

Replace with:

```java
    @Override
    public void remove(final SegmentIdentifier segmentId, final byte[] key) {
      if (segmentId == TRIE_BRANCH_STORAGE) {
        inMemoryTx.remove(segmentId, key);
      } else if (segmentId == TRIE_BRANCH_MIGRATION) {
        realTx.remove(segmentId, key);
      }
      // archive removes dropped
    }
```

- [ ] **Step 4: Run the failing test to verify it now passes**

```bash
./gradlew :ethereum:core:test \
  --tests "BonsaiFlatDbToArchiveMigratorTest.trieCheckpointWritesNodesToFrontierCF" \
  --info 2>&1 | grep -E "PASSED|FAILED|ERROR|Exception" | head -20
```

Expected: **PASSED**.

- [ ] **Step 5: Run all trie checkpoint tests**

```bash
./gradlew :ethereum:core:test \
  --tests "BonsaiFlatDbToArchiveMigratorTest" \
  --info 2>&1 | grep -E "PASSED|FAILED|ERROR|tests were" | head -30
```

Expected: all tests **PASSED** — in particular `trieCheckpointWritesNodesToArchiveStorage` still passes (archive CF is still written), and `secondCheckpointAfterRestart_doesNotMismatchStateRoot` still passes.

---

### Task 4: Clear the frontier CF on full resync

**Files:**
- Modify: `ethereum/core/src/main/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/storage/flat/BonsaiArchiveFlatDbStrategy.java`

`clearArchiveSegments()` already clears `TRIE_BRANCH_STORAGE_ARCHIVE` when the interval is set. Add `TRIE_BRANCH_MIGRATION` to the same guard — it's only populated when checkpoint trie is enabled.

- [ ] **Step 1: Add the `TRIE_BRANCH_MIGRATION` import**

In `BonsaiArchiveFlatDbStrategy.java`, add to the existing static imports:

```java
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_MIGRATION;
```

- [ ] **Step 2: Update `clearArchiveSegments()`**

Locate the `clearArchiveSegments` method:

```java
  private void clearArchiveSegments(final SegmentedKeyValueStorage storage) {
    storage.clear(ACCOUNT_INFO_STATE_ARCHIVE);
    storage.clear(ACCOUNT_STORAGE_ARCHIVE);
    if (trieNodeCheckpointInterval != null) {
      storage.clear(TRIE_BRANCH_STORAGE_ARCHIVE);
    }
  }
```

Replace with:

```java
  private void clearArchiveSegments(final SegmentedKeyValueStorage storage) {
    storage.clear(ACCOUNT_INFO_STATE_ARCHIVE);
    storage.clear(ACCOUNT_STORAGE_ARCHIVE);
    if (trieNodeCheckpointInterval != null) {
      storage.clear(TRIE_BRANCH_STORAGE_ARCHIVE);
      storage.clear(TRIE_BRANCH_MIGRATION);
    }
  }
```

---

### Task 5: Run the full test suite and commit

- [ ] **Step 1: Run all affected tests**

```bash
./gradlew :ethereum:core:test \
  --tests "BonsaiFlatDbToArchiveMigratorTest" \
  --tests "BonsaiArchiveProofsIntegrationTest" \
  --tests "BonsaiArchiveFlatDbStrategyTest" \
  --info 2>&1 | grep -E "PASSED|FAILED|ERROR|tests were" | head -40
```

Expected: all tests **PASSED**.

- [ ] **Step 2: Spotless**

```bash
./gradlew :ethereum:core:spotlessApply
```

- [ ] **Step 3: Check LSP diagnostics**

After the spotless pass, check for any compilation errors:

```bash
./gradlew :ethereum:core:compileJava 2>&1 | grep -E "error:|warning:" | head -20
```

Expected: no errors.

- [ ] **Step 4: Commit**

```bash
git add \
  ethereum/core/src/main/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/storage/flat/BonsaiArchiveMigrationTrieNodeStrategy.java \
  ethereum/core/src/main/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsaiarchive/BonsaiFlatDbToArchiveMigrator.java \
  ethereum/core/src/main/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/storage/flat/BonsaiArchiveFlatDbStrategy.java \
  ethereum/core/src/test/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsaiarchive/BonsaiFlatDbToArchiveMigratorTest.java
git commit -m "perf(bonsai-archive): frontier trie CF eliminates seekForPrev from checkpoint persist

Replace TRIE_BRANCH_STORAGE_ARCHIVE seekForPrev reads in the archive migrator's
persist() path with point lookups against a dedicated frontier CF
(TRIE_BRANCH_MIGRATION). The frontier CF holds one plain-key version of the
migrator's current trie — cache-resident and bloom-accelerated. Writes go
dual: plain to TRIE_BRANCH_MIGRATION + suffixed to TRIE_BRANCH_STORAGE_ARCHIVE.
The proof query path is untouched."
```
