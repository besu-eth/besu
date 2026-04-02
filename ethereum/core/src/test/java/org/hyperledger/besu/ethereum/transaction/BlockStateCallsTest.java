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
package org.hyperledger.besu.ethereum.transaction;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.transaction.exceptions.BlockStateCallError;
import org.hyperledger.besu.ethereum.transaction.exceptions.BlockStateCallException;
import org.hyperledger.besu.plugin.data.BlockOverrides;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class BlockStateCallsTest {
  private static final long MAX_BLOCK_CALL_SIZE = 256;
  private BlockHeader mockBlockHeader;
  private final long headerTimestamp = 1000L;

  @BeforeEach
  void setUp() {
    mockBlockHeader = mock(BlockHeader.class);
    when(mockBlockHeader.getTimestamp()).thenReturn(headerTimestamp);
    when(mockBlockHeader.getNumber()).thenReturn(1L);
  }

  /** Tests that gaps between block numbers are filled correctly when adding a BlockStateCall. */
  @Test
  void shouldFillGapsBetweenBlockNumbers() {
    // BlockHeader is at block number 1
    // BlockStateCall is at block number 4
    // Should fill gaps between 1 and 4 with block numbers 2 and 3

    BlockStateCall blockStateCall = createBlockStateCall(4L, 1036L);

    List<BlockStateCall> blockStateCalls =
        BlockStateCalls.fillBlockStateCalls(List.of(blockStateCall), mockBlockHeader);
    assertEquals(3, blockStateCalls.size());
    assertEquals(2L, blockStateCalls.get(0).getBlockOverrides().getBlockNumber().orElseThrow());
    assertEquals(1012L, blockStateCalls.get(0).getBlockOverrides().getTimestamp().orElseThrow());
    assertEquals(3L, blockStateCalls.get(1).getBlockOverrides().getBlockNumber().orElseThrow());
    assertEquals(1024L, blockStateCalls.get(1).getBlockOverrides().getTimestamp().orElseThrow());
    assertEquals(4L, blockStateCalls.get(2).getBlockOverrides().getBlockNumber().orElseThrow());
    assertEquals(1036L, blockStateCalls.get(2).getBlockOverrides().getTimestamp().orElseThrow());
  }

  /**
   * Tests that the block number is updated correctly if it is not present in the BlockStateCall.
   */
  @Test
  void shouldUpdateBlockNumberIfNotPresent() {
    // BlockHeader is at block number 1
    // BlockStateCall does not have a block number set
    // Should set block number to 2

    long expectedBlockNumber = 2L;
    BlockStateCall blockStateCall = createBlockStateCall(null, null);
    List<BlockStateCall> blockStateCalls =
        BlockStateCalls.fillBlockStateCalls(List.of(blockStateCall), mockBlockHeader);

    assertEquals(1, blockStateCalls.size());
    assertEquals(
        expectedBlockNumber,
        blockStateCalls.getFirst().getBlockOverrides().getBlockNumber().orElseThrow());
  }

  /** Tests that the timestamp is updated correctly if it is not present in the BlockStateCall. */
  @Test
  void shouldUpdateTimestampIfNotPresent() {
    // BlockHeader is at block number 1 and timestamp 1000
    // BlockStateCall does not have a timestamp set
    // Should set timestamp to 1024

    long blockNumber = 3L;
    long expectedTimestamp = headerTimestamp + (blockNumber - 1L) * 12;
    BlockStateCall blockStateCall = createBlockStateCall(blockNumber, null);
    List<BlockStateCall> blockStateCalls =
        BlockStateCalls.fillBlockStateCalls(List.of(blockStateCall), mockBlockHeader);
    assertEquals(
        expectedTimestamp,
        blockStateCalls.getLast().getBlockOverrides().getTimestamp().orElseThrow());
  }

  /**
   * Tests that a list of BlockStateCalls is normalized correctly by filling gaps and setting block
   * numbers and timestamps.
   */
  @Test
  void shouldFillBlockStateCalls() {
    // BlockHeader is at block number 1 and timestamp 1000
    // BlockStateCalls are at block numbers 3 and 5
    // Should fill gaps between 1 and 3 and 3 and 5 with block numbers 2 and 4

    List<BlockStateCall> blockStateCalls = new ArrayList<>();
    blockStateCalls.add(createBlockStateCall(3L, 1024L));
    blockStateCalls.add(createBlockStateCall(5L, 1048L));

    var normalizedCalls = BlockStateCalls.fillBlockStateCalls(blockStateCalls, mockBlockHeader);

    assertEquals(4, normalizedCalls.size());
    assertEquals(2L, normalizedCalls.get(0).getBlockOverrides().getBlockNumber().orElseThrow());
    assertEquals(1012L, normalizedCalls.get(0).getBlockOverrides().getTimestamp().orElseThrow());
    assertEquals(3L, normalizedCalls.get(1).getBlockOverrides().getBlockNumber().orElseThrow());
    assertEquals(1024L, normalizedCalls.get(1).getBlockOverrides().getTimestamp().orElseThrow());
    assertEquals(4L, normalizedCalls.get(2).getBlockOverrides().getBlockNumber().orElseThrow());
    assertEquals(1036L, normalizedCalls.get(2).getBlockOverrides().getTimestamp().orElseThrow());
    assertEquals(5L, normalizedCalls.get(3).getBlockOverrides().getBlockNumber().orElseThrow());
    assertEquals(1048L, normalizedCalls.get(3).getBlockOverrides().getTimestamp().orElseThrow());
  }

  /**
   * Tests that an exception is thrown when a BlockStateCall with a block number less than or equal
   * to the last block number is added.
   */
  @Test
  void shouldThrowExceptionForInvalidBlockNumber() {
    // BlockHeader is at block number 1
    // BlockStateCall block number is 1
    // Should throw an exception because the block number is not greater than 1
    BlockStateCallException exception =
        assertThrows(
            BlockStateCallException.class,
            () ->
                BlockStateCalls.fillBlockStateCalls(
                    List.of(createBlockStateCall(1L, 1012L)), mockBlockHeader));
    assertThat(exception.getError()).isEqualTo(BlockStateCallError.BLOCK_NUMBERS_NOT_ASCENDING);
    String expectedMessage =
        String.format(
            "Block number is invalid. Trying to add a call at block number %s, while current block number is %s.",
            1L, 1L);
    assertEquals(expectedMessage, exception.getMessage());
  }

  /**
   * Tests that an exception is thrown when a BlockStateCall with a timestamp less than or equal to
   * the last timestamp is added.
   */
  @Test
  void shouldThrowExceptionForInvalidTimestamp() {
    // BlockHeader is at block number 1 and timestamp 1000
    // BlockStateCall is at block number 2 and timestamp 1000
    // Should throw an exception because the timestamp is not greater than the 1000
    BlockStateCallException exception =
        assertThrows(
            BlockStateCallException.class,
            () ->
                BlockStateCalls.fillBlockStateCalls(
                    List.of(createBlockStateCall(2L, headerTimestamp)), mockBlockHeader));
    assertThat(exception.getError()).isEqualTo(BlockStateCallError.TIMESTAMPS_NOT_ASCENDING);
    String expectedMessage =
        String.format(
            "Timestamp is invalid. Trying to add a call at timestamp %s, while current timestamp is %s.",
            headerTimestamp, headerTimestamp); // next timestamp
    assertEquals(expectedMessage, exception.getMessage());
  }

  /**
   * Tests that the chain is normalized by adding intermediate blocks and then fails when adding the
   * last call due to an invalid timestamp.
   */
  @Test
  void shouldNormalizeChainAndFailOnInvalidTimestamp() {
    // BlockHeader is at block number 1 and timestamp 1000
    // BlockStateCall is at block number 3 and timestamp 1012
    // Should throw an exception because the timestamp is not greater than 1012
    BlockStateCallException exception =
        assertThrows(
            BlockStateCallException.class,
            () ->
                BlockStateCalls.fillBlockStateCalls(
                    List.of(createBlockStateCall(3L, 1012L)), mockBlockHeader));
    assertThat(exception.getError()).isEqualTo(BlockStateCallError.TIMESTAMPS_NOT_ASCENDING);
    assertEquals(
        "Timestamp is invalid. Trying to add a call at timestamp 1012, while current timestamp is 1012.",
        exception.getMessage());
  }

  /** Tests that an exception is thrown when the maximum number of BlockStateCalls is exceeded. */
  @Test
  void shouldThrowExceptionWhenExceedingMaxBlocks() {
    long maxAllowedBlockNumber = MAX_BLOCK_CALL_SIZE + 1;
    long invalidBlockNumber = maxAllowedBlockNumber + 1;
    BlockStateCall blockStateCall = createBlockStateCall(invalidBlockNumber, null);
    BlockStateCallException exception =
        assertThrows(
            BlockStateCallException.class,
            () -> BlockStateCalls.fillBlockStateCalls(List.of(blockStateCall), mockBlockHeader));
    assertThat(exception.getError()).isEqualTo(BlockStateCallError.TOO_MANY_BLOCK_CALLS);
    String expectedMessage =
        String.format(
            "Block number %d exceeds the limit of %d (header: %d + MAX_BLOCK_CALL_SIZE: %d)",
            invalidBlockNumber, maxAllowedBlockNumber, 1L, MAX_BLOCK_CALL_SIZE);
    assertEquals(expectedMessage, exception.getMessage());
  }

  @Test
  void shouldThrowExceptionWhenFillBlockStateCallsExceedsMaxBlockCallSize() {
    List<BlockStateCall> blockStateCalls = new ArrayList<>();
    blockStateCalls.add(createBlockStateCall(101L, 1609459212L));
    blockStateCalls.add(createBlockStateCall(257L, 1609459248L));
    blockStateCalls.add(createBlockStateCall(null, 1609459224L));
    BlockStateCallException exception =
        assertThrows(
            BlockStateCallException.class,
            () -> BlockStateCalls.fillBlockStateCalls(blockStateCalls, mockBlockHeader));
    assertThat(exception.getError()).isEqualTo(BlockStateCallError.TOO_MANY_BLOCK_CALLS);
    assertEquals(
        "Block number 258 exceeds the limit of 257 (header: 1 + MAX_BLOCK_CALL_SIZE: 256)",
        exception.getMessage());
  }

  private BlockStateCall createBlockStateCall(final Long blockNumber, final Long timestamp) {
    BlockOverrides blockOverrides =
        BlockOverrides.builder().blockNumber(blockNumber).timestamp(timestamp).build();
    return new BlockStateCall(Collections.emptyList(), blockOverrides, null);
  }
}
