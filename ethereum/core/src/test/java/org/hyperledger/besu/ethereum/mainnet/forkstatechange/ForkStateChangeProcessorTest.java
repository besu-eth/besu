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
package org.hyperledger.besu.ethereum.mainnet.forkstatechange;

import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.mainnet.systemcall.BlockProcessingContext;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.InOrder;

class ForkStateChangeProcessorTest {

  private static final long FORK_BLOCK = 5;

  private final ForkStateChange change = mock(ForkStateChange.class);
  private final MutableWorldState worldState = mock(MutableWorldState.class);
  private final WorldUpdater updater = mock(WorldUpdater.class);

  @Test
  void appliesTheChangeOnTheForkBlockAndCommitsIt() {
    when(worldState.updater()).thenReturn(updater);

    ForkStateChangeProcessor.atBlock(FORK_BLOCK, change).process(context(FORK_BLOCK));

    final InOrder inOrder = inOrder(change, updater);
    inOrder.verify(change).apply(updater);
    inOrder.verify(updater).commit();
  }

  @ParameterizedTest
  @ValueSource(longs = {FORK_BLOCK - 1, FORK_BLOCK + 1})
  void doesNothingOnAnyOtherBlock(final long blockNumber) {
    ForkStateChangeProcessor.atBlock(FORK_BLOCK, change).process(context(blockNumber));

    verifyNoInteractions(change, worldState);
  }

  @Test
  void noneDoesNothing() {
    ForkStateChangeProcessor.NONE.process(context(FORK_BLOCK));

    verifyNoInteractions(worldState);
  }

  private BlockProcessingContext context(final long blockNumber) {
    final BlockProcessingContext context = mock(BlockProcessingContext.class);
    when(context.getBlockHeader())
        .thenReturn(new BlockHeaderTestFixture().number(blockNumber).buildHeader());
    when(context.getWorldState()).thenReturn(worldState);
    return context;
  }
}
