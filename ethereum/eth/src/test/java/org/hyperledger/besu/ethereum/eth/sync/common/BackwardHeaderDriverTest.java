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
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.core.BlockHeader;

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
    new BackwardHeaderDriver(BATCH_SIZE, anchorHeader, pivotHeader, blockchain, false);

    verify(blockchain).storeBlockHeaders(headersCaptor.capture());
    final List<BlockHeader> storedHeaders = headersCaptor.getValue();
    assertThat(storedHeaders).hasSize(1);
    assertThat(storedHeaders.getFirst()).isEqualTo(pivotHeader);
  }

  @Test
  public void shouldImportMultipleBatches() {
    final BackwardHeaderDriver driver =
        new BackwardHeaderDriver(BATCH_SIZE, anchorHeader, pivotHeader, blockchain, false);

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
        new BackwardHeaderDriver(BATCH_SIZE, anchorHeader, pivotHeader, blockchain, false);

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
        new BackwardHeaderDriver(BATCH_SIZE, anchorHeader, pivotHeader, blockchain, false);

    // Import all headers down to block 1 (lowestHeaderToImport)
    driver.accept(getHeadersRange(99, 1));

    // Verify all headers were stored (once for pivot in constructor, once for the full range)
    verify(blockchain, times(2)).storeBlockHeaders(any());
  }

  @Test
  public void shouldThrowWhenHeaderDoesNotMatchExpectedParentHash() {
    final BackwardHeaderDriver driver =
        new BackwardHeaderDriver(BATCH_SIZE, anchorHeader, pivotHeader, blockchain, false);

    // Create headers that don't link properly to pivot (skipping blocks)
    final List<BlockHeader> invalidHeaders = getHeaders(50, 51, 52);

    assertThatThrownBy(() -> driver.accept(invalidHeaders))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Received invalid header list: expected hash");
  }

  @Test
  public void shouldMatchStoredAncestorOnFirstBoundaryWhenParentAlreadyStored() {
    // Scenario: the previously-stored anchor (passed to the constructor) has been reorged off the
    // canonical chain. The canonical block at the anchor's height (block 0 in our test chain) is
    // already present in storage from a prior sync cycle. When the backward walk reaches the
    // boundary, it should consult the blockchain and stop at the stored canonical ancestor.
    final BlockHeader canonicalBlock0 = anchorHeader; // canonical chain's block 0
    lenient()
        .when(blockchain.getBlockHeader(canonicalBlock0.getHash()))
        .thenReturn(Optional.of(canonicalBlock0));

    // Use an unrelated block 0 as the "previously-stored anchor" — same height, different hash.
    final BlockDataGenerator otherGenerator = new BlockDataGenerator(99);
    final BlockHeader reorgedAnchor = otherGenerator.blockSequence(1).getFirst().getHeader();
    assertThat(reorgedAnchor.getHash()).isNotEqualTo(canonicalBlock0.getHash());

    final BackwardHeaderDriver driver =
        new BackwardHeaderDriver(BATCH_SIZE, reorgedAnchor, pivotHeader, blockchain, false);

    // Walk all the way down from 99 to 1; block 1's parent is the canonical block 0.
    driver.accept(getHeadersRange(99, 1));

    assertThat(driver.getMatchedAncestor()).contains(canonicalBlock0);
    // hasNext() should return false because done has been signaled.
    assertThat(driver.hasNext()).isFalse();
  }

  @Test
  public void shouldExtendWalkWhenAnchorBoundaryHasNoStoredCanonicalAncestor() {
    // Scenario: the previously-stored anchor has been reorged off the canonical chain and there is
    // NO stored canonical ancestor available. The driver should enter recovery mode, store the
    // batch, lower stopBlock, and allow the iterator to continue descending below the original
    // anchor's height.
    lenient().when(blockchain.getBlockHeader(any(Hash.class))).thenReturn(Optional.empty());

    final BlockDataGenerator otherGenerator = new BlockDataGenerator(99);
    final BlockHeader reorgedAnchor = otherGenerator.blockSequence(1).getFirst().getHeader();
    assertThat(reorgedAnchor.getHash()).isNotEqualTo(anchorHeader.getHash());

    final BackwardHeaderDriver driver =
        new BackwardHeaderDriver(BATCH_SIZE, reorgedAnchor, pivotHeader, blockchain, false);

    // Feed a single batch that reaches block 1 — the original anchor boundary.
    driver.accept(getHeadersRange(99, 1));

    // No match: the driver should be in recovery mode and no matched ancestor populated.
    assertThat(driver.getMatchedAncestor()).isEmpty();
    // The iterator should once again have something to emit (stopBlock was lowered by batchSize).
    assertThat(driver.hasNext()).isTrue();
  }

  @Test
  public void hasNextAndNextEmitDescendingBlockNumbersInBatchSizeStrides() {
    // Pivot at block 100, anchor at block 0, batch size 4.
    // Iterator should emit pivot-1 = 99, then 99 - 4 = 95, 91, ..., down to >= 1 (anchor+1).
    final BackwardHeaderDriver driver =
        new BackwardHeaderDriver(BATCH_SIZE, anchorHeader, pivotHeader, blockchain, false);

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
        new BackwardHeaderDriver(BATCH_SIZE, anchorHeader, pivotHeader, blockchain, false);

    // Drive the iterator through its natural emissions (99, 95, ..., 3) by calling next() the
    // expected number of times. After this, the next next() call falls below stopBlock and
    // throws NoSuchElementException.
    for (long v = 99L; v >= 1L; v -= BATCH_SIZE) {
      driver.next();
    }
    assertThatThrownBy(driver::next).isInstanceOf(NoSuchElementException.class);
  }

  @Test
  public void getMatchedAncestorReturnsEmptyBeforeBoundaryIsReached() {
    final BackwardHeaderDriver driver =
        new BackwardHeaderDriver(BATCH_SIZE, anchorHeader, pivotHeader, blockchain, false);

    assertThat(driver.getMatchedAncestor()).isEqualTo(Optional.empty());
  }

  @Test
  public void previousPivotWasSafeAccessorReturnsConstructorValue() {
    final BackwardHeaderDriver safeDriver =
        new BackwardHeaderDriver(BATCH_SIZE, anchorHeader, pivotHeader, blockchain, true);
    assertThat(safeDriver.previousPivotWasSafe()).isTrue();

    final BackwardHeaderDriver unsafeDriver =
        new BackwardHeaderDriver(BATCH_SIZE, anchorHeader, pivotHeader, blockchain, false);
    assertThat(unsafeDriver.previousPivotWasSafe()).isFalse();
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
