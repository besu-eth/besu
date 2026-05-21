/*
 * Copyright contributors to Hyperledger Besu.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package org.hyperledger.besu.ethereum.eth.sync.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.metrics.BesuMetricCategory;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.metrics.Counter;
import org.hyperledger.besu.plugin.services.metrics.LabelledMetric;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class BackwardHeaderDriverTest {

  private static final int BATCH_SIZE = 4;
  private static final LabelledMetric<Counter> NO_OP_RECOVERY_COUNTER =
      new NoOpMetricsSystem()
          .createLabelledCounter(
              BesuMetricCategory.SYNCHRONIZER,
              "test_recovery_counter",
              "test recovery counter",
              "result");

  private static void noOpDepthSetter(final long ignored) {
    // No-op depth setter for tests that do not exercise the metrics gauge update path.
  }

  @Mock private MutableBlockchain blockchain;
  @Captor private ArgumentCaptor<List<BlockHeader>> headersCaptor;

  private static List<Block> blocks;
  private static BlockHeader anchorHeader;
  private static BlockHeader pivotHeader;

  @BeforeAll
  public static void setUp() {
    final BlockDataGenerator blockDataGenerator = new BlockDataGenerator();

    // Generate a chain of 101 blocks (blocks 0-100)
    // We'll use block 0 as anchor and block 100 as pivot
    blocks = blockDataGenerator.blockSequence(101);

    // Anchor is block 0
    anchorHeader = blocks.getFirst().getHeader();

    // Pivot is block 100
    pivotHeader = blocks.get(100).getHeader();
  }

  @Test
  public void shouldStorePivotHeaderDuringConstruction() {
    new BackwardHeaderDriver(
        BATCH_SIZE,
        anchorHeader,
        pivotHeader,
        blockchain,
        NO_OP_RECOVERY_COUNTER,
        BackwardHeaderDriverTest::noOpDepthSetter);

    verify(blockchain).storeBlockHeaders(headersCaptor.capture());
    final List<BlockHeader> storedHeaders = headersCaptor.getValue();
    assertThat(storedHeaders).hasSize(1);
    assertThat(storedHeaders.getFirst()).isEqualTo(pivotHeader);
  }

  @Test
  public void shouldImportMultipleBatches() {
    final BackwardHeaderDriver driver =
        new BackwardHeaderDriver(
            BATCH_SIZE,
            anchorHeader,
            pivotHeader,
            blockchain,
            NO_OP_RECOVERY_COUNTER,
            BackwardHeaderDriverTest::noOpDepthSetter);

    // Import in batches going backward, verifying state updates between batches
    final List<BlockHeader> batch1 = getHeaders(99, 98, 97, 96);
    driver.accept(batch1);

    final List<BlockHeader> batch2 = getHeaders(95, 94, 93, 92);
    driver.accept(batch2);

    final List<BlockHeader> batch3 = getHeaders(91, 90, 89, 88);
    driver.accept(batch3);

    // Verify all three batches were stored (plus one for pivot in constructor)
    verify(blockchain, times(4)).storeBlockHeaders(any());
    verify(blockchain).storeBlockHeaders(batch1);
    verify(blockchain).storeBlockHeaders(batch2);
    verify(blockchain).storeBlockHeaders(batch3);
  }

  @Test
  public void shouldTrackLowestImportedHeader() {
    final BackwardHeaderDriver driver =
        new BackwardHeaderDriver(
            BATCH_SIZE,
            anchorHeader,
            pivotHeader,
            blockchain,
            NO_OP_RECOVERY_COUNTER,
            BackwardHeaderDriverTest::noOpDepthSetter);

    // Initially the lowest imported header is the pivot
    assertThat(driver.getLowestImportedHeader()).isEqualTo(pivotHeader);

    // After importing a batch, lowest should be the last header in the batch
    driver.accept(getHeaders(99, 98, 97, 96));
    assertThat(driver.getLowestImportedHeader()).isEqualTo(blocks.get(96).getHeader());

    // After another batch, lowest should update again
    driver.accept(getHeaders(95, 94, 93, 92));
    assertThat(driver.getLowestImportedHeader()).isEqualTo(blocks.get(92).getHeader());
  }

  @Test
  public void shouldCompleteSuccessfullyWhenImportingToLowestHeader() {
    final BackwardHeaderDriver driver =
        new BackwardHeaderDriver(
            BATCH_SIZE,
            anchorHeader,
            pivotHeader,
            blockchain,
            NO_OP_RECOVERY_COUNTER,
            BackwardHeaderDriverTest::noOpDepthSetter);

    // Import all headers down to block 1 (lowestHeaderToImport)
    driver.accept(getHeadersRange(99, 1));

    // Verify all headers were stored (once for pivot in constructor, once for the full range)
    verify(blockchain, times(2)).storeBlockHeaders(any());
  }

  @Test
  public void shouldThrowWhenHeaderDoesNotMatchExpectedParentHash() {
    final BackwardHeaderDriver driver =
        new BackwardHeaderDriver(
            BATCH_SIZE,
            anchorHeader,
            pivotHeader,
            blockchain,
            NO_OP_RECOVERY_COUNTER,
            BackwardHeaderDriverTest::noOpDepthSetter);

    // Create headers that don't link properly to pivot (skipping blocks)
    final List<BlockHeader> invalidHeaders = getHeaders(50, 51, 52);

    assertThatThrownBy(() -> driver.accept(invalidHeaders))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Received invalid header list: expected hash");
  }

  @Test
  public void shouldDetectBoundaryWhenBatchEndsAtLowestHeaderToImport() {
    // The download step clamps the last Phase 1 batch to end exactly at lowestHeaderToImport
    // (trustAnchorBlockNumber = anchor number in the pipeline factory). So the boundary batch
    // is a single block [51] when batchSize=4 and anchor=50.
    //
    // Scenario: pivot 100, anchor at block 50 (canonical). Mid-walk batches walk down, then
    // the boundary batch is exactly [51] — block 51's parent is block 50 = anchor → happy match.
    final BlockHeader anchorAt50 = blocks.get(50).getHeader();

    final BackwardHeaderDriver driver =
        new BackwardHeaderDriver(
            BATCH_SIZE,
            anchorAt50,
            pivotHeader,
            blockchain,
            NO_OP_RECOVERY_COUNTER,
            BackwardHeaderDriverTest::noOpDepthSetter);

    // Mid-walk batches from 99 down to 55 (each entirely above the boundary).
    for (int top = 99; top >= 55; top -= BATCH_SIZE) {
      driver.accept(getHeadersRange(top, top - BATCH_SIZE + 1));
    }
    // Boundary batch: exactly one block at lowestHeaderToImport (clamped by download step).
    driver.accept(List.of(blocks.get(51).getHeader()));

    // Happy match: no recovery, iterator finished.
    assertThat(driver.hasNext()).isFalse();
    assertThat(driver.getMatchedAncestor()).isEmpty();
  }

  @Test
  public void shouldThrowWrongChainExceptionWhenRecoveryWalksToGenesisWithMismatchedParent() {
    // Pivot at wrong-chain block 100, anchor at default-chain block 50 (the anchor hash does
    // not match wrong-chain's block 50). Recovery walks down to block 1 via two batches. At
    // that point lookup at height 0 returns OUR local genesis (default-chain block 0). Its
    // hash does NOT equal wrong-chain block 1's parent hash (which is wrong-chain's block 0
    // hash), and parentNumber == 0, so the driver throws WrongChainException.
    final BlockDataGenerator wrongGen = new BlockDataGenerator(99);
    final List<Block> wrongChain = wrongGen.blockSequence(101);
    final BlockHeader wrongPivot = wrongChain.get(100).getHeader();
    final BlockHeader reorgedAnchor = blocks.get(50).getHeader();
    assertThat(reorgedAnchor.getHash()).isNotEqualTo(wrongChain.get(50).getHeader().getHash());

    // No canonical ancestor stored at the intermediate recovery heights; only our local genesis
    // at height 0. That genesis is from the default chain, NOT the wrong chain.
    final BlockHeader localGenesis = blocks.get(0).getHeader();
    lenient().when(blockchain.getBlockHeader(anyLong())).thenReturn(Optional.empty());
    lenient().when(blockchain.getBlockHeader(0L)).thenReturn(Optional.of(localGenesis));

    final BackwardHeaderDriver driver =
        new BackwardHeaderDriver(
            BATCH_SIZE,
            reorgedAnchor,
            wrongPivot,
            blockchain,
            NO_OP_RECOVERY_COUNTER,
            BackwardHeaderDriverTest::noOpDepthSetter);

    // First batch [51..99]: triggers boundary detection, starts recovery.
    final List<BlockHeader> batch1 = new ArrayList<>();
    for (int i = 99; i >= 51; i--) {
      batch1.add(wrongChain.get(i).getHeader());
    }
    driver.accept(batch1);

    // Second batch [1..50]: recovery branch runs; lowestImportedHeader is wrong-chain block 1,
    // parentNumber is 0, lookup returns our local genesis with a different hash → throw.
    final List<BlockHeader> batch2 = new ArrayList<>();
    for (int i = 50; i >= 1; i--) {
      batch2.add(wrongChain.get(i).getHeader());
    }
    assertThatThrownBy(() -> driver.accept(batch2))
        .isInstanceOf(WrongChainException.class)
        .hasMessageContaining("genesis");
  }

  @Test
  public void shouldMatchStoredAncestorAfterRecoveryStartsWhenParentCanonicallyStored() {
    // Scenario: the previously-stored anchor at height 50 has been reorged off the canonical
    // chain. The canonical block at height 46 is already in storage from a prior cycle.
    //  - First batch (99..51) reaches the original boundary; parent (block 50) is NOT equal to
    //    the reorged anchor's hash, so recovery starts and one EXTEND decision is queued.
    //  - Second batch (50..47) runs through the recovery branch; the recovery-match check
    //    looks up the canonical header at lowestImportedHeader.number - 1 = 46. It finds
    //    canonical block 46, whose hash matches block 47's parent hash. Match.
    final BlockDataGenerator otherGenerator = new BlockDataGenerator(99);
    final BlockHeader reorgedAnchor = otherGenerator.blockSequence(51).get(50).getHeader();
    assertThat(reorgedAnchor.getHash()).isNotEqualTo(blocks.get(50).getHeader().getHash());

    final BlockHeader canonicalBlock46 = blocks.get(46).getHeader();
    lenient().when(blockchain.getBlockHeader(anyLong())).thenReturn(Optional.empty());
    lenient().when(blockchain.getBlockHeader(46L)).thenReturn(Optional.of(canonicalBlock46));

    final BackwardHeaderDriver driver =
        new BackwardHeaderDriver(
            BATCH_SIZE,
            reorgedAnchor,
            pivotHeader,
            blockchain,
            NO_OP_RECOVERY_COUNTER,
            BackwardHeaderDriverTest::noOpDepthSetter);

    // First batch: boundary fires, anchor mismatch, recovery starts.
    driver.accept(getHeadersRange(99, 51));
    assertThat(driver.getMatchedAncestor()).isEmpty();

    // Second batch (recovery branch): lookup at height 46 returns canonical block 46. Match.
    driver.accept(getHeadersRange(50, 47));

    assertThat(driver.getMatchedAncestor()).contains(canonicalBlock46);
    assertThat(driver.hasNext()).isFalse();
  }

  @Test
  public void shouldExtendWalkWhenAnchorBoundaryHasNoStoredCanonicalAncestor() {
    // Scenario: the previously-stored anchor has been reorged off the canonical chain and the
    // first boundary batch's parent does NOT match the anchor. The driver should enter recovery
    // mode, queue an EXTEND decision, and the iterator should report more emissions available.
    final BlockDataGenerator otherGenerator = new BlockDataGenerator(99);
    final BlockHeader reorgedAnchor = otherGenerator.blockSequence(1).getFirst().getHeader();
    assertThat(reorgedAnchor.getHash()).isNotEqualTo(anchorHeader.getHash());

    final BackwardHeaderDriver driver =
        new BackwardHeaderDriver(
            BATCH_SIZE,
            reorgedAnchor,
            pivotHeader,
            blockchain,
            NO_OP_RECOVERY_COUNTER,
            BackwardHeaderDriverTest::noOpDepthSetter);

    // Feed a single batch that reaches block 1 — the original anchor boundary.
    driver.accept(getHeadersRange(99, 1));

    // No match: the driver should be in recovery mode and no matched ancestor populated.
    assertThat(driver.getMatchedAncestor()).isEmpty();
    // The iterator should once again have something to emit (stopBlock was lowered by batchSize).
    assertThat(driver.hasNext()).isTrue();
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldIncrementRecoveryCountersAndUpdateDepthGaugeOnRecoverySuccess() {
    // Scenario forces an extension on the first boundary then a match on the second batch:
    //  - Reorged anchor lives at height 50 with a hash different from blocks.get(50).
    //  - First batch (99..51) reaches the original boundary; parent (block 50) is NOT stored
    //    AND not equal to the reorged anchor's hash, so recovery extends stopBlock and fires
    //    the "started" counter.
    //  - Second batch (50..47) is in recovery mode; block 47's parent is block 46, which IS
    //    stored, so the recovery match fires the "succeeded" counter and records depth = 1.
    final BlockDataGenerator otherGenerator = new BlockDataGenerator(99);
    final BlockHeader reorgedAnchor = otherGenerator.blockSequence(51).get(50).getHeader();
    assertThat(reorgedAnchor.getHash()).isNotEqualTo(blocks.get(50).getHeader().getHash());

    lenient().when(blockchain.getBlockHeader(anyLong())).thenReturn(Optional.empty());
    lenient()
        .when(blockchain.getBlockHeader(46L))
        .thenReturn(Optional.of(blocks.get(46).getHeader()));

    final Counter startedCounter = org.mockito.Mockito.mock(Counter.class);
    final Counter succeededCounter = org.mockito.Mockito.mock(Counter.class);
    final LabelledMetric<Counter> recoveryCounter = org.mockito.Mockito.mock(LabelledMetric.class);
    lenient().when(recoveryCounter.labels("started")).thenReturn(startedCounter);
    lenient().when(recoveryCounter.labels("succeeded")).thenReturn(succeededCounter);
    final java.util.concurrent.atomic.AtomicLong observedDepth =
        new java.util.concurrent.atomic.AtomicLong(-1L);

    final BackwardHeaderDriver driver =
        new BackwardHeaderDriver(
            BATCH_SIZE,
            reorgedAnchor,
            pivotHeader,
            blockchain,
            recoveryCounter,
            observedDepth::set);

    driver.accept(getHeadersRange(99, 51));
    driver.accept(getHeadersRange(50, 47));

    verify(startedCounter).inc();
    verify(succeededCounter).inc();
    assertThat(observedDepth.get()).isEqualTo(1L);
  }

  @Test
  public void hasNextAndNextEmitDescendingBlockNumbersInBatchSizeStrides() {
    // Pivot at block 100, anchor at block 0, batch size 4.
    // Iterator should emit pivot-1 = 99, then 99 - 4 = 95, 91, ..., down to >= 1 (anchor+1).
    final BackwardHeaderDriver driver =
        new BackwardHeaderDriver(
            BATCH_SIZE,
            anchorHeader,
            pivotHeader,
            blockchain,
            NO_OP_RECOVERY_COUNTER,
            BackwardHeaderDriverTest::noOpDepthSetter);

    // Build the expected sequence: starting at 99, decrement by 4, while >= 1.
    final List<Long> expected = new ArrayList<>();
    for (long v = 99L; v >= 1L; v -= BATCH_SIZE) {
      expected.add(v);
    }

    // Call next() the expected number of times. hasNext() now blocks at the boundary until the
    // import side signals done or extends the walk, so we drive the iterator directly here.
    final List<Long> emitted = new ArrayList<>();
    for (int i = 0; i < expected.size(); i++) {
      assertThat(driver.hasNext()).isTrue();
      emitted.add(driver.next());
    }

    assertThat(emitted).containsExactlyElementsOf(expected);
  }

  @Test
  public void nextThrowsNoSuchElementWhenExhausted() {
    final BackwardHeaderDriver driver =
        new BackwardHeaderDriver(
            BATCH_SIZE,
            anchorHeader,
            pivotHeader,
            blockchain,
            NO_OP_RECOVERY_COUNTER,
            BackwardHeaderDriverTest::noOpDepthSetter);

    // Drive the iterator through its natural emissions (99, 95, ..., 3) by calling next() the
    // expected number of times. After this, the next next() call falls below stopBlock and
    // throws NoSuchElementException.
    for (long v = 99L; v >= 1L; v -= BATCH_SIZE) {
      driver.next();
    }
    assertThatThrownBy(driver::next).isInstanceOf(NoSuchElementException.class);
  }

  @Test
  public void getMatchedAncestorIsEmptyOnHappyPathAnchorMatch() {
    // Pivot at 100, anchor at 0 — chain links cleanly. The happy-path boundary match must NOT
    // populate matchedAncestor; presence of a value is reserved for recovery-driven matches.
    final BackwardHeaderDriver driver =
        new BackwardHeaderDriver(
            BATCH_SIZE,
            anchorHeader,
            pivotHeader,
            blockchain,
            NO_OP_RECOVERY_COUNTER,
            BackwardHeaderDriverTest::noOpDepthSetter);

    // Feed the whole range as one logical sequence from 99 down to 1.
    driver.accept(getHeadersRange(99, 1));

    // hasNext() should report done.
    assertThat(driver.hasNext()).isFalse();
    // No recovery fired, so matchedAncestor is empty.
    assertThat(driver.getMatchedAncestor()).isEmpty();
  }

  @Test
  public void getMatchedAncestorReturnsEmptyBeforeBoundaryIsReached() {
    final BackwardHeaderDriver driver =
        new BackwardHeaderDriver(
            BATCH_SIZE,
            anchorHeader,
            pivotHeader,
            blockchain,
            NO_OP_RECOVERY_COUNTER,
            BackwardHeaderDriverTest::noOpDepthSetter);

    assertThat(driver.getMatchedAncestor()).isEqualTo(Optional.empty());
  }

  /**
   * Gets headers for specified block numbers.
   *
   * @param blockNumbers the block numbers to get headers for
   * @return list of headers
   */
  private List<BlockHeader> getHeaders(final int... blockNumbers) {
    final List<BlockHeader> headers = new ArrayList<>();
    for (int blockNumber : blockNumbers) {
      headers.add(blocks.get(blockNumber).getHeader());
    }
    return headers;
  }

  /**
   * Gets a range of headers in descending order.
   *
   * @param start starting block number (inclusive)
   * @param end ending block number (inclusive)
   * @return list of headers in descending block number order
   */
  private List<BlockHeader> getHeadersRange(final int start, final int end) {
    final List<BlockHeader> headers = new ArrayList<>();
    for (int i = start; i >= end; i--) {
      headers.add(blocks.get(i).getHeader());
    }
    return headers;
  }
}
