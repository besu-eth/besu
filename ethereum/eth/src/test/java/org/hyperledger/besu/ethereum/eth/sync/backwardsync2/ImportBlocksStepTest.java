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
package org.hyperledger.besu.ethereum.eth.sync.backwardsync2;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider.createInMemoryBlockchain;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.ethereum.BlockProcessingOutputs;
import org.hyperledger.besu.ethereum.BlockProcessingResult;
import org.hyperledger.besu.ethereum.BlockValidator;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.chain.BadBlockCause;
import org.hyperledger.besu.ethereum.chain.BadBlockManager;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.TransactionReceipt;
import org.hyperledger.besu.ethereum.mainnet.HeaderValidationMode;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.mainnet.inclusionlist.InclusionListTransactions;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.stubbing.OngoingStubbing;

@ExtendWith(MockitoExtension.class)
class ImportBlocksStepTest {

  private final BlockDataGenerator generator = new BlockDataGenerator();
  private final BadBlockManager badBlockManager = new BadBlockManager();
  private final List<BlockHeader> reportedBadBlocks = new ArrayList<>();

  @Mock private ProtocolContext protocolContext;
  @Mock private ProtocolSchedule protocolSchedule;
  @Mock private ProtocolSpec protocolSpec;
  @Mock private BlockValidator blockValidator;

  private MutableBlockchain blockchain;
  private Block block1;
  private Block block2;
  private final BlockAccessList bal = new BlockAccessList(List.of());

  @BeforeEach
  void setUp() {
    final Block genesis = generator.genesisBlock();
    blockchain = createInMemoryBlockchain(genesis);
    block1 = generator.nextBlock(genesis);
    block2 = generator.nextBlock(block1);
    when(protocolContext.getBadBlockManager()).thenReturn(badBlockManager);
    lenient().when(protocolContext.getBlockchain()).thenReturn(blockchain);
    lenient().when(protocolSchedule.getByBlockHeader(any())).thenReturn(protocolSpec);
    lenient().when(protocolSpec.getBlockValidator()).thenReturn(blockValidator);
  }

  @Test
  void executesEveryBlockOfABatchInOrder() {
    executionSucceeds(block1, generator.receipts(block1));
    whenExecuted(block2, Optional.empty())
        .thenReturn(
            new BlockProcessingResult(
                Optional.of(new BlockProcessingOutputs(null, generator.receipts(block2)))));

    step().accept(new BlockBatch(List.of(block1, block2), Map.of(block1.getHash(), bal)));

    assertThat(blockchain.getChainHeadHash()).isEqualTo(block2.getHash());
  }

  @Test
  void failsOnAKnownBadBlockWithoutExecutingIt() {
    badBlockManager.addBadBlock(block1, BadBlockCause.fromValidationFailure("invalid"));

    assertThatThrownBy(() -> step().importBlock(block1, Optional.of(bal)))
        .isInstanceOf(IllegalStateException.class);

    assertThat(reportedBadBlocks).containsExactly(block1.getHeader());
    verifyNoInteractions(blockValidator);
    assertThat(blockchain.contains(block1.getHash())).isFalse();
  }

  @Test
  void reportsABlockItsExecutionFindsInvalid() {
    whenExecuted(block1)
        .thenAnswer(
            invocation -> {
              badBlockManager.addBadBlock(block1, BadBlockCause.fromValidationFailure("invalid"));
              return new BlockProcessingResult("invalid");
            });

    assertThatThrownBy(() -> step().importBlock(block1, Optional.of(bal)))
        .isInstanceOf(IllegalStateException.class);

    assertThat(reportedBadBlocks).containsExactly(block1.getHeader());
    assertThat(blockchain.contains(block1.getHash())).isFalse();
  }

  @Test
  void doesNotReportABlockThatFailsForALocalReason() {
    whenExecuted(block1).thenReturn(new BlockProcessingResult("storage failure"));

    assertThatThrownBy(() -> step().importBlock(block1, Optional.of(bal)))
        .isInstanceOf(IllegalStateException.class);

    assertThat(reportedBadBlocks).isEmpty();
  }

  private ImportBlocksStep step() {
    return new ImportBlocksStep(protocolContext, protocolSchedule, 2, reportedBadBlocks::add);
  }

  private void executionSucceeds(final Block block, final List<TransactionReceipt> receipts) {
    whenExecuted(block)
        .thenReturn(
            new BlockProcessingResult(Optional.of(new BlockProcessingOutputs(null, receipts))));
  }

  private OngoingStubbing<BlockProcessingResult> whenExecuted(final Block block) {
    return whenExecuted(block, Optional.of(bal));
  }

  private OngoingStubbing<BlockProcessingResult> whenExecuted(
      final Block block, final Optional<BlockAccessList> blockAccessList) {
    return when(
        blockValidator.validateAndProcessBlock(
            protocolContext,
            block,
            HeaderValidationMode.FULL,
            HeaderValidationMode.NONE,
            blockAccessList,
            InclusionListTransactions.EMPTY,
            true));
  }
}
