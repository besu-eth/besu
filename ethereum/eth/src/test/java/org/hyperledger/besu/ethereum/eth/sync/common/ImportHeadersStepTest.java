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

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class ImportHeadersStepTest {

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
    new ImportHeadersStep(blockchain, anchorHeader, pivotHeader);

    verify(blockchain).storeBlockHeaders(headersCaptor.capture());
    final List<BlockHeader> storedHeaders = headersCaptor.getValue();
    assertThat(storedHeaders).hasSize(1);
    assertThat(storedHeaders.getFirst()).isEqualTo(pivotHeader);
  }

  @Test
  public void shouldImportMultipleBatches() {
    final ImportHeadersStep step = new ImportHeadersStep(blockchain, anchorHeader, pivotHeader);

    // Import in batches going backward, verifying state updates between batches
    final List<BlockHeader> batch1 = getHeaders(99, 98, 97, 96);
    step.accept(batch1);

    final List<BlockHeader> batch2 = getHeaders(95, 94, 93, 92);
    step.accept(batch2);

    final List<BlockHeader> batch3 = getHeaders(91, 90, 89, 88);
    step.accept(batch3);

    // Verify all three batches were stored (plus one for pivot in constructor)
    verify(blockchain, times(4)).storeBlockHeaders(any());
    verify(blockchain).storeBlockHeaders(batch1);
    verify(blockchain).storeBlockHeaders(batch2);
    verify(blockchain).storeBlockHeaders(batch3);
  }

  @Test
  public void shouldCompleteSuccessfullyWhenImportingToLowestHeader() {
    final ImportHeadersStep step = new ImportHeadersStep(blockchain, anchorHeader, pivotHeader);

    // Import all headers down to block 1 (lowestHeaderToImport)
    step.accept(getHeadersRange(99, 1));

    // Verify all headers were stored (once for pivot in constructor, once for the full range)
    verify(blockchain, times(2)).storeBlockHeaders(any());
  }

  @Test
  public void shouldThrowWhenHeaderDoesNotMatchExpectedParentHash() {
    final ImportHeadersStep step = new ImportHeadersStep(blockchain, anchorHeader, pivotHeader);

    // Create headers that don't link properly to pivot (skipping blocks)
    final List<BlockHeader> invalidHeaders = getHeaders(50, 51, 52);

    assertThatThrownBy(() -> step.accept(invalidHeaders))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Received invalid header list: expected hash");
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
