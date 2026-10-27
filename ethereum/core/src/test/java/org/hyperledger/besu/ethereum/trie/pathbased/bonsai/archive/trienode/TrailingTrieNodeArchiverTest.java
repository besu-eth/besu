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
import static org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider.createInMemoryBlockchain;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE_ARCHIVE;

import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.services.kvstore.SegmentedInMemoryKeyValueStorage;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.function.BooleanSupplier;

import com.google.common.util.concurrent.MoreExecutors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TrailingTrieNodeArchiverTest {

  private static final long MAX_LAYERS = 512;

  private final BlockDataGenerator gen = new BlockDataGenerator();
  private MutableBlockchain blockchain;
  private SegmentedKeyValueStorage storage;
  private ArchiveNodeHistoryStore historyStore;
  private FakeStep step;
  private ExecutorService executor;
  private TrailingTrieNodeArchiver archiver;

  @BeforeEach
  void setUp() {
    blockchain = createInMemoryBlockchain(gen.genesisBlock());
    storage = new SegmentedInMemoryKeyValueStorage(List.of(TRIE_BRANCH_STORAGE_ARCHIVE));
    historyStore = new ArchiveNodeHistoryStore(storage);
    step = new FakeStep();
    executor = MoreExecutors.newDirectExecutorService();
  }

  @AfterEach
  void tearDown() {
    if (archiver != null) {
      archiver.close();
    }
  }

  @Test
  void targetWithFinalityTrailsByMaxLayers() {
    assertThat(TrailingTrieNodeArchiver.targetFor(1000, true, 512)).isEqualTo(488);
    assertThat(TrailingTrieNodeArchiver.targetFor(100, true, 512)).isEqualTo(0);
  }

  @Test
  void targetWithoutFinalityIsOneBehindHead() {
    assertThat(TrailingTrieNodeArchiver.targetFor(1000, false, 512)).isEqualTo(999);
    assertThat(TrailingTrieNodeArchiver.targetFor(1, false, 512)).isEqualTo(0);
    assertThat(TrailingTrieNodeArchiver.targetFor(0, false, 512)).isEqualTo(0);
  }

  @Test
  void newHeadTriggersCatchUpToTarget() {
    recordCoverage(0);
    start(MAX_LAYERS);

    appendBlocks(5);

    assertThat(step.calls).hasSize(4);
    assertThat(step.calls.getLast()).isEqualTo(new Call(3, 4));
    assertThat(lastCovered()).isEqualTo(4);
  }

  @Test
  void startCatchesUpToCurrentHead() {
    recordCoverage(0);
    appendBlocks(5);

    start(MAX_LAYERS);

    assertThat(step.calls).containsExactly(new Call(0, 4));
    assertThat(lastCovered()).isEqualTo(4);
  }

  @Test
  void finalityTrailsTargetBehindHead() {
    recordCoverage(0);
    blockchain.setFinalized(blockchain.getGenesisBlock().getHash());
    start(2);

    appendBlocks(5);

    assertThat(step.calls.getLast().toInclusive()).isEqualTo(3);
    assertThat(lastCovered()).isEqualTo(3);
  }

  @Test
  void rangeIsCappedAtCanonicalHeadAfterRewind() {
    recordCoverage(0);
    step.makeProgress = false;
    start(MAX_LAYERS);
    appendBlocks(5);
    blockchain.rewindToBlock(2);
    step.makeProgress = true;
    step.calls.clear();

    appendBlocks(1);

    assertThat(step.calls).containsExactly(new Call(0, 2));
    assertThat(lastCovered()).isEqualTo(2);
  }

  @Test
  void doesNothingWithoutCoverage() {
    start(MAX_LAYERS);

    appendBlocks(3);

    assertThat(step.calls).isEmpty();
  }

  @Test
  void doesNotResubmitWithoutProgress() {
    recordCoverage(0);
    step.makeProgress = false;
    start(MAX_LAYERS);

    appendBlocks(4);

    assertThat(step.calls).containsExactly(new Call(0, 1), new Call(0, 2), new Call(0, 3));
  }

  @Test
  void resubmitsWhenTargetRoseDuringRunWithoutProgress() {
    recordCoverage(0);
    appendBlocks(2);
    step.makeProgress = false;
    step.onCall =
        () -> {
          step.onCall = null;
          appendBlocks(1);
        };

    start(MAX_LAYERS);

    assertThat(step.calls).containsExactly(new Call(0, 1), new Call(0, 2));
  }

  @Test
  void gapHaltsUntilRestart() {
    recordCoverage(0);
    step.failure = new TrieNodeArchiveStep.ArchiveGapException("missing trie log");
    start(MAX_LAYERS);

    appendBlocks(2);
    assertThat(archiver.isHalted()).isTrue();
    step.failure = null;
    appendBlocks(2);

    assertThat(step.calls).containsExactly(new Call(0, 1));

    archiver.close();
    executor = MoreExecutors.newDirectExecutorService();
    start(MAX_LAYERS);
    assertThat(archiver.isHalted()).isFalse();
    assertThat(lastCovered()).isEqualTo(3);
  }

  @Test
  void failureRetriesOnNextBlock() {
    recordCoverage(0);
    step.failure = new IllegalStateException("boom");
    start(MAX_LAYERS);

    appendBlocks(2);
    step.failure = null;
    appendBlocks(1);

    assertThat(archiver.isHalted()).isFalse();
    assertThat(step.calls).containsExactly(new Call(0, 1), new Call(0, 2));
    assertThat(lastCovered()).isEqualTo(2);
  }

  @Test
  void closeStopsObservingAndRunning() {
    recordCoverage(0);
    start(MAX_LAYERS);
    appendBlocks(2);

    archiver.close();
    appendBlocks(2);

    assertThat(step.calls).containsExactly(new Call(0, 1));
    assertThat(step.lastKeepGoing.getAsBoolean()).isFalse();
    assertThat(executor.isShutdown()).isTrue();
  }

  private void start(final long maxLayers) {
    archiver = new TrailingTrieNodeArchiver(blockchain, historyStore, step, maxLayers, executor);
    archiver.start();
  }

  private void appendBlocks(final int count) {
    final Block head = blockchain.getChainHeadBlock();
    for (final Block block : gen.blockSequence(head, count)) {
      blockchain.appendBlock(block, gen.receipts(block));
    }
  }

  private void recordCoverage(final long block) {
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    historyStore.saveArchiveProgress(
        tx, block, blockchain.getBlockHeader(block).orElseThrow().getHash());
    tx.commit();
  }

  private long lastCovered() {
    return historyStore.getArchivedBlockNumber();
  }

  private record Call(long fromExclusive, long toInclusive) {}

  private final class FakeStep implements TrieNodeArchiveStep {
    final List<Call> calls = new ArrayList<>();
    boolean makeProgress = true;
    RuntimeException failure;
    BooleanSupplier lastKeepGoing;
    Runnable onCall;

    @Override
    public void archiveRange(
        final long fromExclusive, final long toInclusive, final BooleanSupplier keepGoing) {
      calls.add(new Call(fromExclusive, toInclusive));
      lastKeepGoing = keepGoing;
      if (onCall != null) {
        onCall.run();
      }
      if (failure != null) {
        throw failure;
      }
      if (makeProgress) {
        for (long b = fromExclusive + 1; b <= toInclusive && keepGoing.getAsBoolean(); b++) {
          recordCoverage(b);
        }
      }
    }
  }
}
