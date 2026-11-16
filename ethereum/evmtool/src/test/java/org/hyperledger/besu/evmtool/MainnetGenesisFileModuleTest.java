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
package org.hyperledger.besu.evmtool;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.config.StubGenesisConfigOptions;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.evm.internal.EvmConfiguration;

import java.math.BigInteger;
import java.util.Optional;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class MainnetGenesisFileModuleTest {

  static Stream<String> forks() {
    return MainnetGenesisFileModule.createSchedules(BigInteger.ONE).keySet().stream().sorted();
  }

  @ParameterizedTest
  @MethodSource("forks")
  void forkRunsOnTheConfiguredEvm(final String fork) {
    for (final boolean enableEvmV2 : new boolean[] {false, true}) {
      final ProtocolSchedule schedule =
          new MainnetGenesisFileModule("{}")
              .provideProtocolSchedule(
                  new StubGenesisConfigOptions(),
                  Optional.of(fork),
                  false,
                  new EvmConfiguration(
                      32_000L, EvmConfiguration.WorldUpdaterMode.STACKED, true, enableEvmV2));

      assertThat(schedule.anyMatch(spec -> true)).isTrue();
      assertThat(
              schedule.anyMatch(
                  spec -> spec.spec().getEvm().getEvmConfiguration().enableEvmV2() != enableEvmV2))
          .as("a %s spec runs on the other EVM", fork)
          .isFalse();
    }
  }
}
