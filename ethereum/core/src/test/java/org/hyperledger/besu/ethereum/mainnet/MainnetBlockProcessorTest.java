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
package org.hyperledger.besu.ethereum.mainnet;

import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.MutableWorldState;
import org.hyperledger.besu.ethereum.mainnet.blockhash.FrontierPreExecutionProcessor;
import org.hyperledger.besu.ethereum.mainnet.staterootcommitter.DefaultStateRootCommitterFactory;
import org.hyperledger.besu.ethereum.referencetests.ReferenceTestBlockchain;
import org.hyperledger.besu.ethereum.referencetests.ReferenceTestWorldState;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class MainnetBlockProcessorTest extends AbstractBlockProcessorTest {

  private final MainnetTransactionProcessor transactionProcessor =
      mock(MainnetTransactionProcessor.class);
  private final AbstractBlockProcessor.TransactionReceiptFactory transactionReceiptFactory =
      mock(AbstractBlockProcessor.TransactionReceiptFactory.class);
  private final ProtocolSchedule protocolSchedule = mock(ProtocolSchedule.class);
  private final ProtocolSpec protocolSpec = mock(ProtocolSpec.class);
  private final ProtocolContext protocolContext = mock(ProtocolContext.class);

  @BeforeEach
  public void setup() {
    when(protocolSchedule.getByBlockHeader(any())).thenReturn(protocolSpec);
    when(protocolSpec.getPreExecutionProcessor()).thenReturn(new FrontierPreExecutionProcessor());
    when(protocolSpec.getStateRootCommitterFactory())
        .thenReturn(new DefaultStateRootCommitterFactory());
  }

  @Test
  public void noAccountCreatedWhenBlockRewardIsZeroAndSkipped() {
    final Blockchain blockchain = new ReferenceTestBlockchain();
    final MainnetBlockProcessor blockProcessor =
        new MainnetBlockProcessor(
            transactionProcessor,
            transactionReceiptFactory,
            Wei.ZERO,
            BlockHeader::getCoinbase,
            true,
            protocolSchedule,
            BalConfiguration.DEFAULT);

    final MutableWorldState worldState = ReferenceTestWorldState.create(emptyMap());
    final Hash initialHash = worldState.rootHash();

    final Block emptyBlock =
        new Block(
            new BlockHeaderTestFixture()
                .transactionsRoot(Hash.EMPTY_LIST_HASH)
                .ommersHash(Hash.EMPTY_LIST_HASH)
                .buildHeader(),
            BlockBody.empty());
    blockProcessor.processBlock(protocolContext, blockchain, worldState, emptyBlock);

    // An empty block with 0 reward should not change the world state
    assertThat(worldState.rootHash()).isEqualTo(initialHash);
  }

  @Test
  public void accountCreatedWhenBlockRewardIsZeroAndNotSkipped() {
    final Blockchain blockchain = new ReferenceTestBlockchain();
    final MainnetBlockProcessor blockProcessor =
        new MainnetBlockProcessor(
            transactionProcessor,
            transactionReceiptFactory,
            Wei.ZERO,
            BlockHeader::getCoinbase,
            false,
            protocolSchedule,
            BalConfiguration.DEFAULT);

    final MutableWorldState worldState = ReferenceTestWorldState.create(emptyMap());
    final Hash initialHash = worldState.rootHash();

    final Block emptyBlock =
        new Block(
            new BlockHeaderTestFixture()
                .transactionsRoot(Hash.EMPTY_LIST_HASH)
                .stateRoot(
                    Hash.fromHexString(
                        "0xa6b5d50f7b3c39b969c2fe8fed091939c674fef49b4826309cb6994361e39b71"))
                .ommersHash(Hash.EMPTY_LIST_HASH)
                .buildHeader(),
            BlockBody.empty());
    blockProcessor.processBlock(protocolContext, blockchain, worldState, emptyBlock);

    // An empty block with 0 reward should change the world state prior to EIP158
    assertThat(worldState.rootHash()).isNotEqualTo(initialHash);
  }
}
