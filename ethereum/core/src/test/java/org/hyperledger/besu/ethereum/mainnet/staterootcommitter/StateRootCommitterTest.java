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

import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.data.BlockHeader;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;
import org.hyperledger.besu.plugin.services.worldstate.StateRootCommitter;
import org.hyperledger.besu.plugin.services.worldstate.StateRootComputation;
import org.hyperledger.besu.plugin.services.worldstate.TrieBranchType;

import org.junit.jupiter.api.Test;

class StateRootCommitterTest {

  @Test
  void committersUseThePatriciaBranchByDefault() {
    assertThat(new DefaultStateRootCommitter().getTrieBranchType())
        .isEqualTo(TrieBranchType.PATRICIA);
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
