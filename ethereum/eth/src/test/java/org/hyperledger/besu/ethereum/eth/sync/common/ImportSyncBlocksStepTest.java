/*
 * Copyright ConsenSys AG.
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

import static java.util.stream.Collectors.toList;
import static org.hyperledger.besu.ethereum.core.encoding.receipt.TransactionReceiptEncodingConfiguration.ETH69_RECEIPT_CONFIGURATION;
import static org.hyperledger.besu.ethereum.eth.core.Utils.blocksToSyncBlocks;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.core.SyncBlock;
import org.hyperledger.besu.ethereum.core.SyncBlockWithReceipts;
import org.hyperledger.besu.ethereum.eth.core.Utils;
import org.hyperledger.besu.ethereum.eth.sync.state.SyncState;

import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class ImportSyncBlocksStepTest {

  @Mock private ProtocolContext protocolContext;
  @Mock private MutableBlockchain blockchain;
  @Mock private SyncState syncState;
  private final BlockDataGenerator gen = new BlockDataGenerator();

  private ImportSyncBlocksStep importSyncBlocksStep;

  @BeforeEach
  public void setUp() {
    when(protocolContext.getBlockchain()).thenReturn(blockchain);

    importSyncBlocksStep =
        new ImportSyncBlocksStep(protocolContext, null, syncState, 0L, 10L, false);
  }

  @Test
  public void shouldImportBlocks() {
    final List<Block> realBlocks = gen.blockSequence(5);
    final List<SyncBlock> blocks = blocksToSyncBlocks(realBlocks);
    final List<SyncBlockWithReceipts> blocksWithReceipts =
        blocks.stream()
            .map(
                block ->
                    new SyncBlockWithReceipts(
                        block,
                        Utils.receiptsToSyncReceipts(
                            gen.receipts(realBlocks.get(blocks.indexOf(block))),
                            ETH69_RECEIPT_CONFIGURATION)))
            .collect(toList());

    importSyncBlocksStep.accept(blocksWithReceipts);

    verify(blockchain).unsafeImportSyncBodiesAndReceipts(blocksWithReceipts, false);
    verify(syncState).setSyncProgress(0L, blocksWithReceipts.getLast().getNumber(), 10L);
  }
}
