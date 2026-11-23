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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.code.BonsaiCodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;

import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class BalanceObservationForwardingTest {

  private static final Address READ = Address.fromHexString("0x1");
  private static final Address SPENT_FROM = Address.fromHexString("0x2");

  @Mock private BonsaiWorldState worldState;

  @ParameterizedTest
  @EnumSource(EvmConfiguration.WorldUpdaterMode.class)
  void nestedFramesReportToTheAccumulatorDirectly(final EvmConfiguration.WorldUpdaterMode mode) {
    final BonsaiWorldStateUpdateAccumulator accumulator =
        new BonsaiWorldStateUpdateAccumulator(
            worldState,
            (__, ___) -> {},
            (__, ___) -> {},
            new EvmConfiguration(
                EvmConfiguration.DEFAULT.jumpDestCacheWeightKB(), mode, true, false),
            new BonsaiCodeCache());
    accumulator.recordBalanceObservations();
    // the transaction's updater, then one per nested call frame
    WorldUpdater frame = accumulator.updater();
    for (int depth = 0; depth < 8; depth++) {
      frame = frame.updater();
    }

    frame.observeBalance(READ);
    frame.observeSufficientBalance(SPENT_FROM, Wei.of(7));

    assertThat(frame.balanceObserver())
        .as("a frame reports without passing through the frames below it")
        .isSameAs(accumulator);
    assertThat(accumulator.getObservedBalances()).containsExactly(READ);
    assertThat(accumulator.getBalanceMargin(SPENT_FROM)).contains(Wei.of(7));
  }
}
