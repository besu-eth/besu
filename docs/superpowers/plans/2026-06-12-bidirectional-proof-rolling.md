# Bidirectional Archive Proof Rolling Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Halve average archive-proof reconstruction latency by picking whichever checkpoint (floor or ceiling) is nearer to the target block and rolling in the matching direction.

**Architecture:** `getCheckpointStateStartBlock` computes both floor and ceiling checkpoints and selects the one requiring fewer trie-log steps. `rollArchiveProofWorldStateToBlockHash` detects direction from checkpoint-vs-target block number: if checkpoint < target it iterates forward via `rollForward`; if checkpoint >= target it uses the existing `rollBack` path. Both directions call `resetStorageRootsToCheckpointForArchiveProof` before `commit()` — after rolling in either direction the accumulator's `updated.storageRoot` is the target block's root (not in the archive CF), so we reset it to `prior.storageRoot` (checkpoint root, whose nodes ARE in the archive CF) to give `persist()` a valid trie base.

**Spec reference:** Design doc at `~/code/projects/bonsai_archive/phase3/2026-05-21-proof-reconstruction-latency-design.md`, Option 1: Nearest-Direction Selection.

---

## Files

- **Modify:** `ethereum/core/src/main/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/BonsaiArchiveWorldStateProvider.java`
  - `getCheckpointStateStartBlock()` lines 169–183: add floor/ceiling distance comparison
  - `rollArchiveProofWorldStateToBlockHash()` lines 185–279: add roll-forward branch
- **Modify (tests):** `ethereum/core/src/test/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/BonsaiArchiveWorldStateProviderTest.java`

---

## Task 1: Write a failing test that drives floor-checkpoint selection

**Files:**
- Modify: `ethereum/core/src/test/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/BonsaiArchiveWorldStateProviderTest.java`

The test verifies which checkpoint block number is looked up by `getCheckpointStateStartBlock`. Because `getBlockHeaderSafe(long)` is only called from that method (roll-back/forward uses `getBlockHeaderSafe(Hash)` or `getBlockHeader(long)`), Mockito `verify` gives a clean signal. The `getWorldState` call will throw `MerkleTrieException` since there is no real trie state in the unit test; the try-catch absorbs it so the `verify` assertions can run.

- [ ] **Step 1: Add imports to the test file**

Add these imports alongside the existing `import static org.mockito.Mockito.mock;` and `import static org.mockito.Mockito.when;`:

```java
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
```

Also add the MerkleTrieException import:

```java
import org.hyperledger.besu.ethereum.trie.MerkleTrieException;
```

- [ ] **Step 2: Add three new test methods**

Insert after the existing `proofsDisabled_historicalBlock_usesArchiveFlatDbPath` test (before the `createProvider` helper methods):

```java
@Test
void proofsEnabled_floorCheckpointCloser_requestsFloorBlockNumber() {
  // interval=100, target=110: floor=99 (dist=11), ceiling=199 (dist=89) → floor selected
  final long interval = 100L;
  final BonsaiArchiveWorldStateProvider proofsProvider =
      createProviderWithProofs(true, true, interval);
  proofsProvider.setArchiveMigrationProgressSupplier(() -> CHAIN_HEAD);

  final long targetNumber = 110L;
  final BlockHeader targetHeader = new BlockHeaderTestFixture().number(targetNumber).buildHeader();
  final BlockHeader floorHeader = new BlockHeaderTestFixture().number(99L).buildHeader();

  when(blockchain.getBlockHeader(targetHeader.getHash())).thenReturn(Optional.of(targetHeader));
  when(blockchain.getBlockHeaderSafe(99L)).thenReturn(Optional.of(floorHeader));
  // ceiling=199 not mocked — returns Optional.empty() by Mockito default

  try {
    proofsProvider.getWorldState(
        WorldStateQueryParams.withBlockHeaderAndNoUpdateNodeHead(targetHeader));
  } catch (final MerkleTrieException ignored) {
    // Expected: unit test has no real trie state, so rolling throws.
    // The checkpoint-selection verify below is the assertion under test.
  }

  verify(blockchain, atLeastOnce()).getBlockHeaderSafe(99L);   // floor was chosen
  verify(blockchain, never()).getBlockHeaderSafe(199L);        // ceiling was NOT requested
}

@Test
void proofsEnabled_ceilingCheckpointCloser_requestsCeilingBlockNumber() {
  // interval=100, target=180: floor=99 (dist=81), ceiling=199 (dist=19) → ceiling selected
  final long interval = 100L;
  final BonsaiArchiveWorldStateProvider proofsProvider =
      createProviderWithProofs(true, true, interval);
  proofsProvider.setArchiveMigrationProgressSupplier(() -> CHAIN_HEAD);

  final long targetNumber = 180L;
  final BlockHeader targetHeader = new BlockHeaderTestFixture().number(targetNumber).buildHeader();
  final BlockHeader ceilingHeader = new BlockHeaderTestFixture().number(199L).buildHeader();

  when(blockchain.getBlockHeader(targetHeader.getHash())).thenReturn(Optional.of(targetHeader));
  when(blockchain.getBlockHeaderSafe(199L)).thenReturn(Optional.of(ceilingHeader));
  // floor=99 not mocked — returns Optional.empty() by Mockito default

  try {
    proofsProvider.getWorldState(
        WorldStateQueryParams.withBlockHeaderAndNoUpdateNodeHead(targetHeader));
  } catch (final MerkleTrieException ignored) {}

  verify(blockchain, never()).getBlockHeaderSafe(99L);          // floor was NOT requested
  verify(blockchain, atLeastOnce()).getBlockHeaderSafe(199L);   // ceiling was chosen
}

@Test
void proofsEnabled_targetInFirstWindow_alwaysRequestsCeiling() {
  // interval=100, target=50: floor=(-1) does not exist → ceiling=99 always selected
  final long interval = 100L;
  final BonsaiArchiveWorldStateProvider proofsProvider =
      createProviderWithProofs(true, true, interval);
  proofsProvider.setArchiveMigrationProgressSupplier(() -> CHAIN_HEAD);

  final long targetNumber = 50L;
  final BlockHeader targetHeader = new BlockHeaderTestFixture().number(targetNumber).buildHeader();
  final BlockHeader ceilingHeader = new BlockHeaderTestFixture().number(99L).buildHeader();

  when(blockchain.getBlockHeader(targetHeader.getHash())).thenReturn(Optional.of(targetHeader));
  when(blockchain.getBlockHeaderSafe(99L)).thenReturn(Optional.of(ceilingHeader));

  try {
    proofsProvider.getWorldState(
        WorldStateQueryParams.withBlockHeaderAndNoUpdateNodeHead(targetHeader));
  } catch (final MerkleTrieException ignored) {}

  verify(blockchain, atLeastOnce()).getBlockHeaderSafe(99L);  // ceiling is the only checkpoint
}
```

- [ ] **Step 3: Run the three tests to observe baseline failure**

```bash
./gradlew :ethereum:core:test \
  --tests "org.hyperledger.besu.ethereum.trie.pathbased.bonsai.BonsaiArchiveWorldStateProviderTest.proofsEnabled_floorCheckpointCloser_requestsFloorBlockNumber" \
  --tests "org.hyperledger.besu.ethereum.trie.pathbased.bonsai.BonsaiArchiveWorldStateProviderTest.proofsEnabled_ceilingCheckpointCloser_requestsCeilingBlockNumber" \
  --tests "org.hyperledger.besu.ethereum.trie.pathbased.bonsai.BonsaiArchiveWorldStateProviderTest.proofsEnabled_targetInFirstWindow_alwaysRequestsCeiling" \
  2>&1 | tail -30
```

Expected: `proofsEnabled_floorCheckpointCloser_requestsFloorBlockNumber` FAILS (current code always picks ceiling 199; `getBlockHeaderSafe(99L)` is never called). The ceiling and first-window tests already pass because the current code always picks ceiling.

---

## Task 2: Implement floor/ceiling distance comparison in `getCheckpointStateStartBlock`

**Files:**
- Modify: `ethereum/core/src/main/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/BonsaiArchiveWorldStateProvider.java`

- [ ] **Step 1: Replace `getCheckpointStateStartBlock` body (lines 169–183)**

```java
// BEFORE:
  private Optional<BlockHeader> getCheckpointStateStartBlock(final Hash targetHash) {
    return blockchain
        .getBlockHeader(targetHash)
        .map(BlockHeader::getNumber)
        .flatMap(
            targetNumber -> {
              long nearestCheckpoint =
                  (((targetNumber + trieNodeCheckpointInterval) / trieNodeCheckpointInterval)
                          * trieNodeCheckpointInterval)
                      - 1;
              return blockchain
                  .getBlockHeaderSafe(nearestCheckpoint)
                  .or(() -> blockchain.getBlockHeaderSafe(blockchain.getChainHeadHash()));
            });
  }
```

```java
// AFTER:
  private Optional<BlockHeader> getCheckpointStateStartBlock(final Hash targetHash) {
    return blockchain
        .getBlockHeader(targetHash)
        .map(BlockHeader::getNumber)
        .flatMap(
            targetNumber -> {
              final long ceilingCheckpoint =
                  (((targetNumber + trieNodeCheckpointInterval) / trieNodeCheckpointInterval)
                          * trieNodeCheckpointInterval)
                      - 1;
              final long floorCheckpoint =
                  (targetNumber / trieNodeCheckpointInterval) * trieNodeCheckpointInterval - 1;
              final long chosenCheckpoint;
              if (floorCheckpoint >= 0) {
                final long distanceToCeiling = ceilingCheckpoint - targetNumber;
                final long distanceToFloor = targetNumber - floorCheckpoint;
                chosenCheckpoint =
                    distanceToFloor <= distanceToCeiling ? floorCheckpoint : ceilingCheckpoint;
              } else {
                chosenCheckpoint = ceilingCheckpoint;
              }
              return blockchain
                  .getBlockHeaderSafe(chosenCheckpoint)
                  .or(() -> blockchain.getBlockHeaderSafe(blockchain.getChainHeadHash()));
            });
  }
```

- [ ] **Step 2: Run the three checkpoint-selection tests**

```bash
./gradlew :ethereum:core:test \
  --tests "org.hyperledger.besu.ethereum.trie.pathbased.bonsai.BonsaiArchiveWorldStateProviderTest.proofsEnabled_floorCheckpointCloser_requestsFloorBlockNumber" \
  --tests "org.hyperledger.besu.ethereum.trie.pathbased.bonsai.BonsaiArchiveWorldStateProviderTest.proofsEnabled_ceilingCheckpointCloser_requestsCeilingBlockNumber" \
  --tests "org.hyperledger.besu.ethereum.trie.pathbased.bonsai.BonsaiArchiveWorldStateProviderTest.proofsEnabled_targetInFirstWindow_alwaysRequestsCeiling" \
  2>&1 | tail -20
```

Expected: all three PASS.

- [ ] **Step 3: Run the full provider test suite to check for regressions**

```bash
./gradlew :ethereum:core:test \
  --tests "org.hyperledger.besu.ethereum.trie.pathbased.bonsai.BonsaiArchiveWorldStateProviderTest" \
  2>&1 | tail -20
```

Expected: all tests pass.

---

## Task 3: Add roll-forward path to `rollArchiveProofWorldStateToBlockHash`

**Files:**
- Modify: `ethereum/core/src/main/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/BonsaiArchiveWorldStateProvider.java`

The current method always collects trie logs for rolling backward. We need to branch on `checkpointBlock.getNumber() < targetHeader.getNumber()` and add a forward path.

- [ ] **Step 1: Replace the entire `rollArchiveProofWorldStateToBlockHash` method (lines 185–279)**

```java
  private Optional<MutableWorldState> rollArchiveProofWorldStateToBlockHash(
      final PathBasedWorldState mutableState,
      final BlockHeader checkpointBlock,
      final Hash targetBlockHash) {

    ((BonsaiWorldState) mutableState).resetWorldStateToCheckpoint(checkpointBlock);

    if (targetBlockHash.equals(mutableState.blockHash())) {
      return Optional.of(mutableState);
    }

    try {
      final BlockHeader targetHeader =
          blockchain
              .getBlockHeaderSafe(targetBlockHash)
              .orElseThrow(
                  () ->
                      new MerkleTrieException("target block header not found: " + targetBlockHash));

      final PathBasedWorldStateUpdateAccumulator<?> diffBasedUpdater =
          (PathBasedWorldStateUpdateAccumulator<?>) mutableState.updater();
      try {
        if (checkpointBlock.getNumber() < targetHeader.getNumber()) {
          // Roll forward: checkpoint is before target; apply each block's trie log in sequence.
          for (long blockNum = checkpointBlock.getNumber() + 1;
              blockNum <= targetHeader.getNumber();
              blockNum++) {
            final long n = blockNum;
            final Hash blockHash =
                blockchain
                    .getBlockHeader(blockNum)
                    .orElseThrow(() -> new MerkleTrieException("missing block header at " + n))
                    .getBlockHash();
            LOG.debug("Roll forward {}", blockHash);
            diffBasedUpdater.rollForward(
                trieLogManager
                    .getTrieLogLayer(blockHash)
                    .orElseThrow(
                        () -> new MerkleTrieException("missing trie log for " + blockHash)));
          }
        } else {
          // Roll backward: checkpoint is after target; existing path.
          final Optional<BlockHeader> maybePersistedHeader =
              blockchain.getBlockHeaderSafe(mutableState.blockHash()).map(BlockHeader.class::cast);

          final List<TrieLog> rollBacks = new ArrayList<>();
          if (maybePersistedHeader.isEmpty()) {
            trieLogManager.getTrieLogLayer(mutableState.blockHash()).ifPresent(rollBacks::add);
          } else {
            BlockHeader persistedHeader = maybePersistedHeader.get();
            Hash persistedBlockHash = persistedHeader.getBlockHash();
            while (persistedHeader.getNumber() > targetHeader.getNumber()) {
              LOG.debug("Rollback {}", persistedBlockHash);
              final Hash blockHashForLog = persistedBlockHash;
              rollBacks.add(
                  trieLogManager
                      .getTrieLogLayer(persistedBlockHash)
                      .orElseThrow(
                          () ->
                              new MerkleTrieException("missing trie log for " + blockHashForLog)));
              final Hash parentHash = persistedHeader.getParentHash();
              persistedHeader =
                  blockchain
                      .getBlockHeaderSafe(parentHash)
                      .orElseThrow(
                          () ->
                              new MerkleTrieException(
                                  "missing parent header for " + parentHash));
              persistedBlockHash = persistedHeader.getBlockHash();
            }
          }
          for (final TrieLog rollBack : rollBacks) {
            LOG.debug("Attempting rollback of {}", rollBack.getBlockHash());
            diffBasedUpdater.rollBack(rollBack);
          }
        }

        // After rolling in either direction the accumulator's updated.storageRoot is the
        // target block's storage root, whose trie nodes are NOT in the archive CF. Reset
        // each account's storageRoot to its prior (checkpoint) value so persist() rebuilds
        // the storage trie from the checkpoint's archived nodes and derives the target root
        // from the slot diffs in storagesToUpdate.
        if (diffBasedUpdater instanceof BonsaiWorldStateUpdateAccumulator bonsaiUpdater) {
          bonsaiUpdater.resetStorageRootsToCheckpointForArchiveProof();
        }
        diffBasedUpdater.commit();

        // overrides suffix selection in putFlatAccountTrieNode/putFlatStorageTrieNode
        final SegmentedKeyValueStorageTransaction tx =
            mutableState.getWorldStateStorage().getComposedWorldStateStorage().startTransaction();
        tx.put(
            TRIE_BRANCH_STORAGE,
            ARCHIVE_PROOF_BLOCK_NUMBER_KEY,
            Bytes.ofUnsignedLong(targetHeader.getNumber()).toArrayUnsafe());
        tx.commit();

        mutableState.persist(targetHeader);

        return Optional.of(mutableState);
      } catch (final MerkleTrieException re) {
        throw re;
      } catch (final Exception e) {
        diffBasedUpdater.reset();
        LOG.atInfo()
            .setMessage("Archive proof state rolling failed on {} for block hash {}: {}")
            .addArgument(mutableState.getWorldStateStorage().getClass().getSimpleName())
            .addArgument(targetBlockHash)
            .addArgument(e)
            .log();
        return Optional.empty();
      }
    } catch (final RuntimeException re) {
      LOG.info("Archive proof rolling failed for block hash {}", targetBlockHash, re);
      if (re instanceof MerkleTrieException) {
        throw re;
      }
      throw new MerkleTrieException("invalid archive proof rollback for " + targetBlockHash);
    }
  }
```

- [ ] **Step 2: Verify compilation**

```bash
./gradlew :ethereum:core:compileJava 2>&1 | grep -E "error:" | head -20
```

Expected: no errors.

- [ ] **Step 3: Run the full provider test suite**

```bash
./gradlew :ethereum:core:test \
  --tests "org.hyperledger.besu.ethereum.trie.pathbased.bonsai.BonsaiArchiveWorldStateProviderTest" \
  2>&1 | tail -20
```

Expected: all tests pass (including the three new direction-selection tests).

- [ ] **Step 4: Run migrator and proof integration tests**

```bash
./gradlew :ethereum:core:test \
  --tests "org.hyperledger.besu.ethereum.trie.pathbased.bonsaiarchive.BonsaiFlatDbToArchiveMigratorTest" \
  --tests "org.hyperledger.besu.ethereum.trie.pathbased.bonsai.BonsaiArchiveStorageProofIntegrationTest" \
  --tests "org.hyperledger.besu.ethereum.trie.pathbased.bonsai.BonsaiArchiveProofsIntegrationTest" \
  2>&1 | tail -30
```

Expected: all pass.

- [ ] **Step 5: Apply spotless and verify build**

```bash
./gradlew :ethereum:core:spotlessApply && \
./gradlew :ethereum:core:build -x test 2>&1 | tail -10
```

Expected: BUILD SUCCESSFUL.

- [ ] **Step 6: Commit**

```bash
git add \
  ethereum/core/src/main/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/BonsaiArchiveWorldStateProvider.java \
  ethereum/core/src/test/java/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/BonsaiArchiveWorldStateProviderTest.java \
  docs/superpowers/plans/2026-06-12-bidirectional-proof-rolling.md
git commit -m "perf(bonsai-archive): bidirectional proof rolling reduces average latency 2x"
```
