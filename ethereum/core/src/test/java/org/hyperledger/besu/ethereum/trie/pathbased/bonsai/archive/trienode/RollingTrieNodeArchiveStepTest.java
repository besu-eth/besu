/*
 * Copyright contributors to Besu.
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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE_ARCHIVE;
import static org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage.WORLD_BLOCK_HASH_KEY;
import static org.hyperledger.besu.ethereum.worldstate.ExtraStorageConfiguration.Unstable.DEFAULT_BONSAI_ARCHIVE_DEEP_CHECKPOINT_INTERVAL;
import static org.hyperledger.besu.ethereum.worldstate.ExtraStorageConfiguration.Unstable.DEFAULT_BONSAI_ARCHIVE_SHALLOW_CHECKPOINT_INTERVAL;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode.ArchiveCoverageTracker.Coverage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode.TrieNodeArchiveStep.ArchiveGapException;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.trienode.TrieNodeStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.trielog.TrieLogManager;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.plugin.services.trielogs.TrieLog;
import org.hyperledger.besu.services.kvstore.SegmentedInMemoryKeyValueStorage;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class RollingTrieNodeArchiveStepTest {

  private static final Bytes NODE_KEY = Bytes.of(1, 2);

  @Mock private BonsaiWorldState state;
  @Mock private BonsaiWorldStateKeyValueStorage stateStorage;
  @Mock private TrieLogManager trieLogManager;

  private final ExecutorService capturePool = Executors.newFixedThreadPool(2);
  private final SegmentedInMemoryKeyValueStorage canonical =
      new SegmentedInMemoryKeyValueStorage(
          List.of(TRIE_BRANCH_STORAGE, TRIE_BRANCH_STORAGE_ARCHIVE));
  private final SegmentedInMemoryKeyValueStorage layer =
      new SegmentedInMemoryKeyValueStorage(List.of(TRIE_BRANCH_STORAGE));
  private final ArchiveCoverageTracker coverage = new ArchiveCoverageTracker(canonical);
  private final List<Long> rolledTo = new ArrayList<>();
  private final List<Boolean> installedAtRoll = new ArrayList<>();
  private TrieNodeStrategy installed;
  private MutableBlockchain blockchain;
  private long layerBlock;
  private long skipHookAt = -1;
  private long throwAt = -1;

  @BeforeEach
  void setUp() {
    final BlockDataGenerator generator = new BlockDataGenerator();
    final Block genesis = generator.genesisBlock();
    blockchain = InMemoryKeyValueStorageProvider.createInMemoryBlockchain(genesis);
    for (final Block block : generator.blockSequence(genesis, 5)) {
      blockchain.appendBlock(block, generator.receipts(block));
    }
    final SegmentedKeyValueStorageTransaction tx = canonical.startTransaction();
    coverage.record(tx, 0L, hashOf(0));
    tx.commit();

    when(state.getWorldStateStorage()).thenReturn(stateStorage);
    doAnswer(invocation -> installed = invocation.getArgument(0))
        .when(stateStorage)
        .setTrieNodeStrategy(any());
    when(trieLogManager.getTrieLogLayer(any())).thenReturn(Optional.of(mock(TrieLog.class)));
  }

  @AfterEach
  void tearDown() {
    capturePool.shutdownNow();
  }

  // Rolls the layer to the target; a forward step of one block persists it through the
  // installed strategy, as PathBasedWorldState.persist does.
  private void roll(final Hash target) {
    final BlockHeader header = blockchain.getBlockHeader(target).orElseThrow();
    rolledTo.add(header.getNumber());
    installedAtRoll.add(installed != null);
    if (installed != null
        && header.getNumber() == layerBlock + 1
        && header.getNumber() != skipHookAt) {
      final SegmentedKeyValueStorageTransaction tx = layer.startTransaction();
      installed.onBlockPersist(layer, tx, header);
      final Bytes node = Bytes.ofUnsignedLong(header.getNumber());
      installed.putTrieNode(layer, tx, NODE_KEY, Bytes32.wrap(Hash.hash(node).getBytes()), node);
      if (header.getNumber() == throwAt) {
        installed.onRollback(tx);
        tx.rollback();
        throw new IllegalStateException("boom");
      }
      installed.onBeforeCommit(layer, tx);
      tx.commit();
    }
    moveLayerTo(header.getNumber());
  }

  private void moveLayerTo(final long block) {
    layerBlock = block;
    final SegmentedKeyValueStorageTransaction tx = layer.startTransaction();
    tx.put(TRIE_BRANCH_STORAGE, WORLD_BLOCK_HASH_KEY, hashOf(block).getBytes().toArrayUnsafe());
    tx.commit();
  }

  private RollingTrieNodeArchiveStep step() {
    return new RollingTrieNodeArchiveStep(
        () -> {
          installed = null;
          moveLayerTo(5);
          return state;
        },
        (s, hash) -> roll(hash),
        blockchain,
        trieLogManager,
        coverage,
        () ->
            new ArchiveTrieNodeWriter(
                new ArchiveNodeHistoryStore(canonical),
                coverage,
                capturePool,
                DEFAULT_BONSAI_ARCHIVE_SHALLOW_CHECKPOINT_INTERVAL,
                DEFAULT_BONSAI_ARCHIVE_DEEP_CHECKPOINT_INTERVAL,
                Optional.of(canonical)));
  }

  private Hash hashOf(final long block) {
    return blockchain.getBlockHeader(block).orElseThrow().getHash();
  }

  private boolean hasEntryAt(final long block) {
    return canonical
        .get(
            TRIE_BRANCH_STORAGE_ARCHIVE,
            ArchiveNodeKey.historyKey(ArchiveNodeKey.of(NODE_KEY), block).toArrayUnsafe())
        .isPresent();
  }

  private long coverageEnd() {
    return coverage.getCoverage().map(Coverage::lastBlock).orElseThrow();
  }

  @Test
  void archivesEachBlockAndCommitsCoverage() {
    step().archiveRange(0, 3, () -> true);

    assertThat(coverage.getCoverage()).contains(new Coverage(0, 3, hashOf(3)));
    assertThat(hasEntryAt(1)).isTrue();
    assertThat(hasEntryAt(2)).isTrue();
    assertThat(hasEntryAt(3)).isTrue();
    assertThat(hasEntryAt(4)).isFalse();
    assertThat(rolledTo).containsExactly(0L, 1L, 2L, 3L);
  }

  @Test
  void installsStrategyOnlyAfterPositioning() {
    step().archiveRange(0, 1, () -> true);

    assertThat(rolledTo).containsExactly(0L, 1L);
    assertThat(installedAtRoll).containsExactly(false, true);
    assertThat(installed).isInstanceOf(ArchiveTrieNodeStrategy.class);
  }

  @Test
  void stopsBetweenBlocksWhenKeepGoingTurnsFalse() {
    final AtomicInteger checks = new AtomicInteger();
    step().archiveRange(0, 3, () -> checks.incrementAndGet() <= 1);

    assertThat(coverageEnd()).isEqualTo(1);
    assertThat(rolledTo).containsExactly(0L, 1L);
    verify(state).close();
  }

  @Test
  void failedRollArchivesNothing() {
    throwAt = 2;

    assertThatThrownBy(() -> step().archiveRange(0, 3, () -> true))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("boom");

    assertThat(hasEntryAt(2)).isFalse();
    assertThat(coverage.getCoverage()).contains(new Coverage(0, 1, hashOf(1)));
    assertThat(rolledTo).containsExactly(0L, 1L, 2L);
    verify(state).close();
  }

  @Test
  void stopsRangeWhenCoverageDidNotAdvance() {
    skipHookAt = 1;

    step().archiveRange(0, 3, () -> true);

    assertThat(coverageEnd()).isZero();
    assertThat(rolledTo).containsExactly(0L, 1L);
    verify(state).close();
  }

  @Test
  void missingTrieLogThrowsGap() {
    when(trieLogManager.getTrieLogLayer(hashOf(2))).thenReturn(Optional.empty());

    assertThatThrownBy(() -> step().archiveRange(0, 3, () -> true))
        .isInstanceOf(ArchiveGapException.class);

    assertThat(coverageEnd()).isEqualTo(1);
    assertThat(rolledTo).containsExactly(0L, 1L);
    verify(state).close();
  }

  @Test
  void closesStateEveryRun() {
    final RollingTrieNodeArchiveStep step = step();
    step.archiveRange(0, 2, () -> true);
    verify(state, times(1)).close();

    step.archiveRange(2, 4, () -> true);
    verify(state, times(2)).close();
    assertThat(coverageEnd()).isEqualTo(4);
    assertThat(rolledTo).containsExactly(0L, 1L, 2L, 2L, 3L, 4L);
  }
}
