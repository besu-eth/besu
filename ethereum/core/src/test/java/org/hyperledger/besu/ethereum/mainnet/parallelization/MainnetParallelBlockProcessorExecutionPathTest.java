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
package org.hyperledger.besu.ethereum.mainnet.parallelization;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.mainnet.AbstractBlockProcessor.TransactionReceiptFactory;
import org.hyperledger.besu.ethereum.mainnet.BlockExecutionPathListener;
import org.hyperledger.besu.ethereum.mainnet.ImmutableBalConfiguration;
import org.hyperledger.besu.ethereum.mainnet.MainnetTransactionProcessor;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.provider.PathBasedWorldStateProvider;
import org.hyperledger.besu.ethereum.worldstate.WorldStateArchive;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;

import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.Test;

class MainnetParallelBlockProcessorExecutionPathTest {

  private final BlockHeader header = new BlockHeaderTestFixture().number(1).buildHeader();
  private final BlockExecutionPathListener listener = mock(BlockExecutionPathListener.class);
  private final ProtocolContext protocolContext = mock(ProtocolContext.class);
  private final MainnetParallelBlockProcessor processor =
      new MainnetParallelBlockProcessor(
          mock(MainnetTransactionProcessor.class),
          mock(TransactionReceiptFactory.class),
          BlockHeader::getCoinbase,
          mock(ProtocolSchedule.class),
          ImmutableBalConfiguration.builder().executionPathListener(listener).build(),
          new NoOpMetricsSystem(),
          Runnable::run);

  @Test
  void reportsBalSchedulerWhenAccessListIsDelivered() {
    when(protocolContext.getWorldStateArchive())
        .thenReturn(mock(PathBasedWorldStateProvider.class));

    assertThat(run(Optional.of(new BlockAccessList(List.of())))).isPresent();

    verify(listener).onParallel(header, "bal");
    verifyNoMoreInteractions(listener);
  }

  @Test
  void reportsOptimisticSchedulerWithoutAccessList() {
    when(protocolContext.getWorldStateArchive())
        .thenReturn(mock(PathBasedWorldStateProvider.class));

    assertThat(run(Optional.empty())).isPresent();

    verify(listener).onParallel(header, "optimistic");
    verifyNoMoreInteractions(listener);
  }

  @Test
  void reportsSequentialWhenWorldStateIsNotPathBased() {
    when(protocolContext.getWorldStateArchive()).thenReturn(mock(WorldStateArchive.class));

    assertThat(run(Optional.of(new BlockAccessList(List.of())))).isEmpty();

    verify(listener).onSequential(header, "not-path-based");
    verifyNoMoreInteractions(listener);
  }

  private Optional<ParallelBlockTransactionProcessor> run(
      final Optional<BlockAccessList> blockAccessList) {
    return processor.startParallelExecution(
        protocolContext,
        header,
        List.of(),
        Address.ZERO,
        (frame, number) -> null,
        Wei.ZERO,
        Optional.empty(),
        blockAccessList,
        Optional.empty());
  }
}
