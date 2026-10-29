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

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode.ArchiveCoverageTracker.Coverage;
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
  private ArchiveCoverageTracker coverage;
  private FakeStep step;
  private ExecutorService executor;
  private TrailingTrieNodeArchiver archiver;

  @BeforeEach
  void setUp() {
    blockchain = createInMemoryBlockchain(gen.genesisBlock());
    storage = new SegmentedInMemoryKeyValueStorage(List.of(TRIE_BRANCH_STORAGE_ARCHIVE));
    coverage = new ArchiveCoverageTracker(storage);
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
  void targetWithFinalitySitsOneBatchInsideLiveWindow() {
    assertThat(archiverWith(512).targetFor(1000, true)).isEqualTo(528);
    assertThat(archiverWith(512).targetFor(100, true)).isEqualTo(0);
    assertThat(archiverWith(4).targetFor(10, true)).isEqualTo(8);
  }

  @Test
  void targetWithoutFinalityIsOneBehindHead() {
    assertThat(archiverWith(512).targetFor(1000, false)).isEqualTo(999);
    assertThat(archiverWith(512).targetFor(1, false)).isEqualTo(0);
    assertThat(archiverWith(512).targetFor(0, false)).isEqualTo(0);
  }

  @Test
  void minBatchIsThirtyTwoWithFinalityCappedAtHalfMaxLayers() {
    assertThat(archiverWith(512).minBatchFor(true)).isEqualTo(32);
    assertThat(archiverWith(64).minBatchFor(true)).isEqualTo(32);
    assertThat(archiverWith(4).minBatchFor(true)).isEqualTo(2);
    assertThat(archiverWith(1).minBatchFor(true)).isEqualTo(1);
  }

  @Test
  void minBatchIsOneWithoutFinality() {
    assertThat(archiverWith(512).minBatchFor(false)).isEqualTo(1);
    assertThat(archiverWith(4).minBatchFor(false)).isEqualTo(1);
  }

  @Test
  void finalityWaitsForAFullBatch() {
    recordCoverage(0);
    blockchain.setFinalized(blockchain.getGenesisBlock().getHash());
    start(64);

    appendBlocks(55);
    assertThat(step.calls).isEmpty();

    appendBlocks(1);
    assertThat(step.calls).containsExactly(new Call(0, 32));

    appendBlocks(31);
    assertThat(step.calls).hasSize(1);

    appendBlocks(1);
    assertThat(step.calls).containsExactly(new Call(0, 32), new Call(32, 64));
    assertThat(lastCovered()).isEqualTo(64);
  }

  @Test
  void noHoleWithSynchronousPasses() {
    assertThat(holesWithPassLatency(64, 0)).isEmpty();
  }

  @Test
  void noHoleWithSynchronousPassesAndSmallWindow() {
    assertThat(holesWithPassLatency(4, 0)).isEmpty();
  }

  @Test
  void noHoleWhileBlocksArriveDuringAPassWithinSlack() {
    // maxLayers 64: batch 32, slack 8
    assertThat(holesWithPassLatency(64, 8)).isEmpty();
  }

  @Test
  void blocksArrivingBeyondSlackDuringAPassLeaveAHole() {
    assertThat(holesWithPassLatency(64, 9)).isNotEmpty();
  }

  /**
   * Runs heads up to 200 with finality, where each pass sees {@code latency} more blocks arrive
   * before it records coverage, and returns the heads at which the block that just left the live
   * window (blocks less than {@code maxLayers} behind the head) was not archived.
   */
  private List<Long> holesWithPassLatency(final long maxLayers, final int latency) {
    recordCoverage(0);
    blockchain.setFinalized(blockchain.getGenesisBlock().getHash());
    final List<Long> holes = new ArrayList<>();
    blockchain.observeBlockAdded(
        event -> {
          final long head = event.getHeader().getNumber();
          if (lastCovered() < head - maxLayers) {
            holes.add(head);
          }
        });
    step.onCall = () -> appendBlocks(latency);
    start(maxLayers);

    while (blockchain.getChainHeadBlockNumber() < 200) {
      appendBlocks(1);
    }
    assertThat(archiver.isHalted()).isFalse();
    assertThat(step.calls).isNotEmpty();
    return holes;
  }

  @Test
  void nonCanonicalCoverageTipHalts() {
    appendBlocks(3);
    recordCoverage(0);
    recordCoverage(1, Hash.ZERO);

    start(MAX_LAYERS);
    appendBlocks(1);

    assertThat(archiver.isHalted()).isTrue();
    assertThat(step.calls).isEmpty();
  }

  @Test
  void depthBeyondTwiceMaxLayersHalts() {
    recordCoverage(0);
    appendBlocks(9);

    start(4);
    appendBlocks(1);

    assertThat(archiver.isHalted()).isTrue();
    assertThat(step.calls).isEmpty();
  }

  @Test
  void depthOfTwiceMaxLayersStillArchives() {
    recordCoverage(0);
    appendBlocks(8);

    start(4);

    assertThat(archiver.isHalted()).isFalse();
    assertThat(step.calls).containsExactly(new Call(0, 7));
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

    // maxLayers 2 gives a batch of 1, so the target is head - 2 + 1
    assertThat(step.calls.getLast().toInclusive()).isEqualTo(4);
    assertThat(lastCovered()).isEqualTo(4);
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
    archiver = archiverWith(maxLayers);
    archiver.start();
  }

  private TrailingTrieNodeArchiver archiverWith(final long maxLayers) {
    return new TrailingTrieNodeArchiver(blockchain, coverage, step, maxLayers, executor);
  }

  private void appendBlocks(final int count) {
    final Block head = blockchain.getChainHeadBlock();
    for (final Block block : gen.blockSequence(head, count)) {
      blockchain.appendBlock(block, gen.receipts(block));
    }
  }

  private void recordCoverage(final long block) {
    recordCoverage(block, blockchain.getBlockHeader(block).orElseThrow().getHash());
  }

  private void recordCoverage(final long block, final Hash hash) {
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    coverage.record(tx, block, hash);
    tx.commit();
  }

  private long lastCovered() {
    return coverage.getCoverage().map(Coverage::lastBlock).orElse(-1L);
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
