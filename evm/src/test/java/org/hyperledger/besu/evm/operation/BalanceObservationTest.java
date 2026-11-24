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
import static org.assertj.core.api.Assertions.entry;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.EvmSpecVersion;
import org.hyperledger.besu.evm.fluent.EVMExecutor;
import org.hyperledger.besu.evm.fluent.EvmSpec;
import org.hyperledger.besu.evm.fluent.SimpleWorld;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Which balances the operations report a transaction to depend on, on both interpreter loops. */
class BalanceObservationTest {

  private static final Address CONTRACT =
      Address.fromHexString("0x00000000000000000000000000000000000c0de1");
  private static final Address OTHER =
      Address.fromHexString("0x00000000000000000000000000000000000c0de2");
  private static final String PUSH_OTHER = "73" + OTHER.getBytes().toUnprefixedHexString();

  private static final String STOP = "00";
  private static final String BALANCE = "31";
  private static final String SELFBALANCE = "47";
  private static final String GAS = "5a";
  private static final String CREATE = "f0";
  private static final String CALL = "f1";
  private static final String SELFDESTRUCT = "ff";

  /** PUSH0, or the shortest PUSHn of the value. */
  private static String push(final long value) {
    if (value == 0) {
      return "5f";
    }
    final Bytes bytes = Bytes.minimalBytes(value);
    return String.format("%02x", 0x5f + bytes.size()) + bytes.toUnprefixedHexString();
  }

  /** CALL to OTHER sending the value with all gas, without input or output. */
  private static String callOther(final long value) {
    return push(0) + push(0) + push(0) + push(0) + push(value) + PUSH_OTHER + GAS + CALL;
  }

  /** CREATE sending the value, with empty init code. */
  private static String createWithValue(final long value) {
    return push(0) + push(0) + push(value) + CREATE;
  }

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

  @Test
  void updaterWithoutOverridesPassesObservationsToItsParent() {
    final Set<Address> observed = new HashSet<>();
    final SimpleWorld root =
        new SimpleWorld() {
          @Override
          public void observeBalance(final Address address) {
            observed.add(address);
          }

          @Override
          public void observeSufficientBalance(final Address address, final Wei margin) {
            margins.put(address, margin);
          }
        };
    final WorldUpdater nested = root.updater().updater();

    nested.observeBalance(OTHER);
    nested.observeSufficientBalance(CONTRACT, Wei.of(3));

    assertThat(nested.balanceObserver()).isSameAs(root);
    assertThat(observed).containsExactly(OTHER);
    assertThat(margins).containsExactly(entry(CONTRACT, Wei.of(3)));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void balanceObservesTheTarget(final boolean evmV2) {
    assertThat(observedBalances(evmV2, PUSH_OTHER + BALANCE + STOP, Wei.ZERO))
        .containsExactly(OTHER);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void selfBalanceObservesTheContract(final boolean evmV2) {
    assertThat(observedBalances(evmV2, SELFBALANCE + STOP, Wei.ZERO)).containsExactly(CONTRACT);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void callWithValueOnlyNeedsASufficientBalance(final boolean evmV2) {
    assertThat(observedBalances(evmV2, callOther(1) + STOP, Wei.ZERO)).isEmpty();
    assertThat(margins).containsExactly(Map.entry(CONTRACT, Wei.of(999)));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void callWithMoreValueThanTheBalanceObservesTheCaller(final boolean evmV2) {
    // the contract holds 1000
    assertThat(observedBalances(evmV2, callOther(2000) + STOP, Wei.ZERO)).containsExactly(CONTRACT);
    assertThat(margins).isEmpty();
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void theSmallestMarginOfSeveralSpendingsCounts(final boolean evmV2) {
    observedBalances(evmV2, callOther(1) + callOther(100) + STOP, Wei.ZERO);
    // the second call spends 100 of the 999 left after the first
    assertThat(margins).containsExactly(Map.entry(CONTRACT, Wei.of(899)));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void callWithoutValueObservesNothing(final boolean evmV2) {
    assertThat(observedBalances(evmV2, callOther(0) + STOP, Wei.ZERO)).isEmpty();
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void createWithValueOnlyNeedsASufficientBalance(final boolean evmV2) {
    assertThat(observedBalances(evmV2, createWithValue(1) + STOP, Wei.ZERO)).isEmpty();
    assertThat(margins).containsExactly(Map.entry(CONTRACT, Wei.of(999)));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void selfDestructObservesTheContract(final boolean evmV2) {
    assertThat(observedBalances(evmV2, PUSH_OTHER + SELFDESTRUCT, Wei.ZERO))
        .containsExactly(CONTRACT);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void receivingValueObservesNothing(final boolean evmV2) {
    assertThat(observedBalances(evmV2, STOP, Wei.of(5))).isEmpty();
  }
}
