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
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

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
  public void shouldThrowAtAnchorBoundaryWhenParentDoesNotMatchAnchorHash() {
    // Use a different chain's block-0 as the anchor so that the chain we use to descend
    // to block 1 will not have a parent hash matching the anchor's hash.
    final BlockDataGenerator generator = new BlockDataGenerator();
    final List<Block> otherChain = generator.blockSequence(1);
    final BlockHeader unrelatedAnchor = otherChain.getFirst().getHeader();

    // Sanity check: the chains should have different block-0 hashes.
    assertThat(unrelatedAnchor.getHash()).isNotEqualTo(anchorHeader.getHash());

    final BackwardHeaderDriver driver =
        new BackwardHeaderDriver(BATCH_SIZE, unrelatedAnchor, pivotHeader, blockchain, false);

    // Walk all the way down from 99 to 1; block 1's parent hash is the real chain's anchor,
    // which differs from the unrelated anchor we used in the constructor.
    final List<BlockHeader> headersToAnchor = getHeadersRange(99, 1);

    assertThatThrownBy(() -> driver.accept(headersToAnchor))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("lower header parent hash does not match");
  }

  @Test
  public void hasNextAndNextEmitDescendingBlockNumbersInBatchSizeStrides() {
    // Pivot at block 100, anchor at block 0, batch size 4.
    // Iterator should emit pivot-1 = 99, then 99 - 4 = 95, 91, ..., down to >= 1 (anchor+1).
    final BackwardHeaderDriver driver =
        new BackwardHeaderDriver(BATCH_SIZE, anchorHeader, pivotHeader, blockchain, false);

    final List<Long> emitted = new ArrayList<>();
    while (driver.hasNext()) {
      emitted.add(driver.next());
    }

    // Build the expected sequence: starting at 99, decrement by 4, while >= 1.
    final List<Long> expected = new ArrayList<>();
    for (long v = 99L; v >= 1L; v -= BATCH_SIZE) {
      expected.add(v);
    }

    assertThat(emitted).containsExactlyElementsOf(expected);
  }

  @Test
  public void nextThrowsNoSuchElementWhenExhausted() {
    final BackwardHeaderDriver driver =
        new BackwardHeaderDriver(BATCH_SIZE, anchorHeader, pivotHeader, blockchain, false);

    // Drain the iterator.
    while (driver.hasNext()) {
      driver.next();
    }

    assertThat(driver.hasNext()).isFalse();
    assertThatThrownBy(driver::next).isInstanceOf(NoSuchElementException.class);
  }

  @Test
  public void getMatchedAncestorReturnsEmptyInB1() {
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
