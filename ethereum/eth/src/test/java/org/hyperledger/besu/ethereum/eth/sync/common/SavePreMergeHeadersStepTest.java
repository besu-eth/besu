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
package org.hyperledger.besu.ethereum.eth.sync.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.ConsensusContext;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.Difficulty;
import org.hyperledger.besu.ethereum.eth.sync.SavePreMergeHeadersStep;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SavePreMergeHeadersStepTest {
  private MutableBlockchain blockchain;
  private ConsensusContext consensusContext;
  private SavePreMergeHeadersStep savePreMergeHeadersStep;
  private static final long FIRST_POS_BLOCK_NUMBER = 1000;

  @BeforeEach
  void setUp() {
    blockchain = mock(MutableBlockchain.class);
    consensusContext = mock(ConsensusContext.class);
    savePreMergeHeadersStep =
        new SavePreMergeHeadersStep(
            blockchain, true, FIRST_POS_BLOCK_NUMBER, Optional.of(consensusContext));
  }

  @Test
  void shouldSaveFullBlockForFirstPoSBlock() {
    BlockHeader blockHeader = createMockBlockHeader(FIRST_POS_BLOCK_NUMBER);
    Difficulty difficulty = mock(Difficulty.class);
    when(blockchain.calculateTotalDifficulty(blockHeader)).thenReturn(difficulty);

    Stream<BlockHeader> nextProcessingStateInput = savePreMergeHeadersStep.apply(blockHeader);

    assertEquals(1, nextProcessingStateInput.count());
    verify(blockchain, never()).unsafeStoreHeader(blockHeader, difficulty);
  }

  @Test
  void shouldSaveFullBlockWhenPostMergeBlock() {
    BlockHeader blockHeader = createMockBlockHeader(FIRST_POS_BLOCK_NUMBER + 1);

    Stream<BlockHeader> nextProcessingStateInput = savePreMergeHeadersStep.apply(blockHeader);

    assertEquals(1, nextProcessingStateInput.count());
    verify(blockchain, never()).unsafeStoreHeader(any(), any());
  }

  @Test
  void shouldSaveFullBlocksIfCheckpointIsGenesisBlock() {
    savePreMergeHeadersStep =
        new SavePreMergeHeadersStep(
            blockchain, true, BlockHeader.GENESIS_BLOCK_NUMBER, Optional.of(consensusContext));

    BlockHeader block0 = createMockBlockHeader(0);
    BlockHeader block1 = createMockBlockHeader(1);

    List<BlockHeader> nextProcessingStateInput = savePreMergeHeadersStep.apply(block0).toList();
    assertEquals(1, nextProcessingStateInput.size());
    assertEquals(nextProcessingStateInput.getFirst(), block0);

    nextProcessingStateInput = savePreMergeHeadersStep.apply(block1).toList();
    assertEquals(1, nextProcessingStateInput.size());
    assertEquals(nextProcessingStateInput.getFirst(), block1);

    verify(blockchain, never()).unsafeStoreHeader(any(), any());
  }

  @Test
  void shouldStoreOnlyBlockHeaderWhenPreMergeBlock() {
    shouldStoreOnlyHeaderWhenPreMergeBlockNumber(1);
    shouldStoreOnlyHeaderWhenPreMergeBlockNumber(FIRST_POS_BLOCK_NUMBER - 1);
  }

  private void shouldStoreOnlyHeaderWhenPreMergeBlockNumber(final long blockNumber) {
    BlockHeader blockHeader = createMockBlockHeader(blockNumber);
    Difficulty difficulty = mock(Difficulty.class);
    when(blockchain.calculateTotalDifficulty(blockHeader)).thenReturn(difficulty);

    Stream<BlockHeader> nextProcessingStateInput = savePreMergeHeadersStep.apply(blockHeader);

    assertEquals(0, nextProcessingStateInput.count());
    verify(blockchain).unsafeStoreHeader(blockHeader, difficulty);
  }

  @Test
  void shouldSetIsPostMergeWhenLastPoWBlock() {
    BlockHeader lastPoWBlockHeader = createMockBlockHeader(FIRST_POS_BLOCK_NUMBER - 1);
    when(blockchain.getTotalDifficultyByHash(lastPoWBlockHeader.getHash()))
        .thenReturn(Optional.of(Difficulty.ONE));
    savePreMergeHeadersStep.apply(lastPoWBlockHeader);
    verify(consensusContext).setIsPostMerge(Difficulty.ONE);
  }

  @Test
  void shouldNotSetIsPostMergeWhenPoAChain() {
    BlockHeader lastPoWBlockHeader = createMockBlockHeader(FIRST_POS_BLOCK_NUMBER - 1);
    final SavePreMergeHeadersStep forPoA =
        new SavePreMergeHeadersStep(
            blockchain, false, FIRST_POS_BLOCK_NUMBER, Optional.of(consensusContext));
    forPoA.apply(lastPoWBlockHeader);
    verifyNoInteractions(consensusContext);
  }

  private BlockHeader createMockBlockHeader(final long blockNumber) {
    BlockHeader blockHeader = mock(BlockHeader.class);
    when(blockHeader.getNumber()).thenReturn(blockNumber);
    when(blockHeader.getHash()).thenReturn(Hash.EMPTY);
    return blockHeader;
  }
}
