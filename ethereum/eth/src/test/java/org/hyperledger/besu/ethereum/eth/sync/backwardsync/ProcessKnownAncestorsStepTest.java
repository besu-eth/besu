/*
 * Copyright contributors to Hyperledger Besu.
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
package org.hyperledger.besu.ethereum.eth.sync.backwardsync;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider.createInMemoryBlockchain;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator.BlockWithAccessList;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.mainnet.MainnetBlockHeaderFunctions;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.services.kvstore.InMemoryKeyValueStorage;

import java.nio.charset.StandardCharsets;
import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class ProcessKnownAncestorsStepTest {

  private static final BlockDataGenerator blockDataGenerator = new BlockDataGenerator();

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private BackwardSyncContext context;

  @Mock private MutableBlockchain blockchain;

  private MutableBlockchain localBlockchain;
  private GenericKeyValueStorageFacade<Hash, BlockHeader> headersStorage;
  private GenericKeyValueStorageFacade<Hash, Block> blocksStorage;
  private GenericKeyValueStorageFacade<Hash, Hash> chainStorage;
  private GenericKeyValueStorageFacade<String, BlockHeader> sessionDataStorage;

  @BeforeEach
  void setUp() {
    headersStorage =
        new GenericKeyValueStorageFacade<>(
            hash -> hash.getBytes().toArrayUnsafe(),
            new BlocksHeadersConvertor(new MainnetBlockHeaderFunctions()),
            new InMemoryKeyValueStorage());
    blocksStorage =
        new GenericKeyValueStorageFacade<>(
            hash -> hash.getBytes().toArrayUnsafe(),
            new BlocksConvertor(new MainnetBlockHeaderFunctions()),
            new InMemoryKeyValueStorage());
    chainStorage =
        new GenericKeyValueStorageFacade<>(
            hash -> hash.getBytes().toArrayUnsafe(),
            new HashConvertor(),
            new InMemoryKeyValueStorage());
    sessionDataStorage =
        new GenericKeyValueStorageFacade<>(
            key -> key.getBytes(StandardCharsets.UTF_8),
            new BlocksHeadersConvertor(new MainnetBlockHeaderFunctions()),
            new InMemoryKeyValueStorage());

    final Block genesis = blockDataGenerator.genesisBlock();
    localBlockchain = createInMemoryBlockchain(genesis);
    when(context.getProtocolContext().getBlockchain()).thenReturn(blockchain);
  }

  @Test
  void processKnownAncestors_passesStoredBalToSaveBlock() {
    final BlockWithAccessList withBal =
        blockDataGenerator.blockWithAccessList(
            new BlockDataGenerator.BlockOptions()
                .setBlockNumber(1)
                .setParentHash(localBlockchain.getChainHeadHash())
                .withGeneratedBlockAccessList(2));
    final Block block = withBal.getBlock();
    final BlockAccessList bal = withBal.getBlockAccessList().orElseThrow();

    stubParentPresentButBlockNotImported(block);
    when(blockchain.getBlockAccessList(block.getHash())).thenReturn(Optional.of(bal));

    final BackwardChain backwardChain =
        new BackwardChain(headersStorage, blocksStorage, chainStorage, sessionDataStorage);
    backwardChain.appendTrustedBlock(block);

    new ProcessKnownAncestorsStep(context, backwardChain).processKnownAncestors();

    @SuppressWarnings("unchecked")
    final ArgumentCaptor<Optional<BlockAccessList>> balCaptor =
        ArgumentCaptor.forClass(Optional.class);
    verify(context).saveBlock(any(Block.class), balCaptor.capture());
    assertThat(balCaptor.getValue()).contains(bal);
  }

  @Test
  void processKnownAncestors_passesEmptyOptionalWhenBalMissing() {
    final Block block =
        blockDataGenerator.block(
            new BlockDataGenerator.BlockOptions()
                .setBlockNumber(1)
                .setParentHash(localBlockchain.getChainHeadHash()));

    stubParentPresentButBlockNotImported(block);
    when(blockchain.getBlockAccessList(block.getHash())).thenReturn(Optional.empty());

    final BackwardChain backwardChain =
        new BackwardChain(headersStorage, blocksStorage, chainStorage, sessionDataStorage);
    backwardChain.appendTrustedBlock(block);

    new ProcessKnownAncestorsStep(context, backwardChain).processKnownAncestors();

    verify(context).saveBlock(eq(block), eq(Optional.empty()));
  }

  private void stubParentPresentButBlockNotImported(final Block block) {
    when(blockchain.getChainHeadBlockNumber()).thenReturn(0L);
    when(blockchain.contains(block.getHash())).thenReturn(false);
    when(blockchain.contains(block.getHeader().getParentHash())).thenReturn(true);
  }
}
