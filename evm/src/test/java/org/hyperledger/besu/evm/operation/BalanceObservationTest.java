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
package org.hyperledger.besu.evm.operation;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.EvmSpecVersion;
import org.hyperledger.besu.evm.fluent.EVMExecutor;
import org.hyperledger.besu.evm.fluent.EvmSpec;
import org.hyperledger.besu.evm.fluent.SimpleWorld;
import org.hyperledger.besu.evm.internal.EvmConfiguration;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Which balances the operations report a transaction to depend on, on both interpreter loops. */
class BalanceObservationTest {

  private static final Address CONTRACT =
      Address.fromHexString("0x00000000000000000000000000000000000c0de1");
  private static final Address OTHER =
      Address.fromHexString("0x00000000000000000000000000000000000c0de2");
  private static final String PUSH_OTHER = "73" + OTHER.getBytes().toUnprefixedHexString();

  private final Map<Address, Wei> margins = new HashMap<>();

  private Set<Address> observedBalances(final boolean evmV2, final String code, final Wei value) {
    final Set<Address> observed = new HashSet<>();
    final SimpleWorld world =
        new SimpleWorld() {
          @Override
          public void observeBalance(final Address address) {
            observed.add(address);
          }

          @Override
          public void observeSufficientBalance(final Address address, final Wei margin) {
            margins.merge(address, margin, (a, b) -> a.compareTo(b) <= 0 ? a : b);
          }
        };
    world.createAccount(CONTRACT, 1, Wei.of(1_000));
    world.createAccount(OTHER, 1, Wei.of(1_000));
    final EvmConfiguration configuration =
        new EvmConfiguration(
            EvmConfiguration.DEFAULT.jumpDestCacheWeightKB(),
            EvmConfiguration.WorldUpdaterMode.STACKED,
            true,
            evmV2);
    new EVMExecutor(EvmSpec.evmSpec(EvmSpecVersion.CANCUN, BigInteger.ONE, configuration))
        .worldUpdater(world)
        .sender(OTHER)
        .receiver(CONTRACT)
        .contract(CONTRACT)
        .ethValue(value)
        .gas(1_000_000)
        .code(Bytes.fromHexString(code))
        .execute();
    return observed;
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void balanceObservesTheTarget(final boolean evmV2) {
    assertThat(observedBalances(evmV2, PUSH_OTHER + "3100", Wei.ZERO)).containsExactly(OTHER);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void selfBalanceObservesTheContract(final boolean evmV2) {
    assertThat(observedBalances(evmV2, "4700", Wei.ZERO)).containsExactly(CONTRACT);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void callWithValueOnlyNeedsASufficientBalance(final boolean evmV2) {
    assertThat(observedBalances(evmV2, "5f5f5f5f6001" + PUSH_OTHER + "5af100", Wei.ZERO)).isEmpty();
    assertThat(margins).containsExactly(Map.entry(CONTRACT, Wei.of(999)));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void callWithMoreValueThanTheBalanceObservesTheCaller(final boolean evmV2) {
    assertThat(observedBalances(evmV2, "5f5f5f5f6107d0" + PUSH_OTHER + "5af100", Wei.ZERO))
        .containsExactly(CONTRACT);
    assertThat(margins).isEmpty();
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void theSmallestMarginOfSeveralSpendingsCounts(final boolean evmV2) {
    observedBalances(
        evmV2,
        "5f5f5f5f6001" + PUSH_OTHER + "5af1" + "5f5f5f5f6064" + PUSH_OTHER + "5af100",
        Wei.ZERO);
    // the second call spends 100 of the 999 left after the first
    assertThat(margins).containsExactly(Map.entry(CONTRACT, Wei.of(899)));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void callWithoutValueObservesNothing(final boolean evmV2) {
    assertThat(observedBalances(evmV2, "5f5f5f5f5f" + PUSH_OTHER + "5af100", Wei.ZERO)).isEmpty();
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void createWithValueOnlyNeedsASufficientBalance(final boolean evmV2) {
    assertThat(observedBalances(evmV2, "5f5f6001f000", Wei.ZERO)).isEmpty();
    assertThat(margins).containsExactly(Map.entry(CONTRACT, Wei.of(999)));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void selfDestructObservesTheContract(final boolean evmV2) {
    assertThat(observedBalances(evmV2, PUSH_OTHER + "ff", Wei.ZERO)).containsExactly(CONTRACT);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void receivingValueObservesNothing(final boolean evmV2) {
    assertThat(observedBalances(evmV2, "00", Wei.of(5))).isEmpty();
  }
}
