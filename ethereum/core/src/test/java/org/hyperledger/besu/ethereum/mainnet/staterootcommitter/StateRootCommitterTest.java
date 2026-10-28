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
package org.hyperledger.besu.ethereum.mainnet.staterootcommitter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessListAccountLookup;
import org.hyperledger.besu.ethereum.mainnet.staterootcommitter.patricia.DefaultPatriciaStateRootCommitter;
import org.hyperledger.besu.ethereum.mainnet.staterootcommitter.patricia.PatriciaBalEngine;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.data.BlockHeader;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;
import org.hyperledger.besu.plugin.services.worldstate.StateRootCommitter;
import org.hyperledger.besu.plugin.services.worldstate.StateRootComputation;
import org.hyperledger.besu.plugin.services.worldstate.TrieBranchType;

import java.util.List;

import org.junit.jupiter.api.Test;

class StateRootCommitterTest {

  @Test
  void committersUseThePatriciaBranchByDefault() {
    assertThat(ForestStateRootCommitter.INSTANCE.getTrieBranchType())
        .isEqualTo(TrieBranchType.PATRICIA);
  }

  @Test
  void patriciaCommittersUseThePatriciaBranch() {
    assertThat(new DefaultPatriciaStateRootCommitter().getTrieBranchType())
        .isEqualTo(TrieBranchType.PATRICIA);
    assertThat(PatriciaBalEngine.INSTANCE.getTrieBranchType()).isEqualTo(TrieBranchType.PATRICIA);
  }

  @Test
  void balCommitterUsesTheBranchOfItsEngine() {
    final BalStateRootCommitter.Engine binaryEngine =
        new BalStateRootCommitter.Engine() {
          @Override
          public TrieBranchType getTrieBranchType() {
            return TrieBranchType.BINARY;
          }

          @Override
          public BalStateRootCommitter.Result compute(
              final BonsaiWorldState parent,
              final BlockAccessListAccountLookup accountLookup,
              final boolean storageFrozen) {
            throw new UnsupportedOperationException();
          }
        };

    final BalStateRootCommitter committer =
        new BalStateRootCommitter(
            mock(ProtocolContext.class),
            mock(BlockHeader.class),
            BlockAccessListAccountLookup.of(new BlockAccessList(List.of())),
            false,
            binaryEngine);

    assertThat(committer.getTrieBranchType()).isEqualTo(TrieBranchType.BINARY);
  }

  @Test
  void timedCommitterKeepsTheTrieBranchType() {
    final StateRootCommitter binaryCommitter =
        new StateRootCommitter() {
          @Override
          public TrieBranchType getTrieBranchType() {
            return TrieBranchType.BINARY;
          }

          @Override
          public StateRootComputation compute(
              final MutableWorldState worldState,
              final BlockHeader blockHeader,
              final WorldUpdater worldUpdater) {
            throw new UnsupportedOperationException();
          }
        };

    assertThat(binaryCommitter.timed(NoOpMetricsSystem.NO_OP_OPERATION_TIMER).getTrieBranchType())
        .isEqualTo(TrieBranchType.BINARY);
  }
}
