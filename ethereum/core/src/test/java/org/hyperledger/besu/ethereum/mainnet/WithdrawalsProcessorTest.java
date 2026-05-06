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
package org.hyperledger.besu.ethereum.mainnet;

import static java.util.Map.entry;
import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.GWei;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.core.Withdrawal;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.tuweni.units.bigints.UInt64;
import org.junit.jupiter.api.Test;

class WithdrawalsProcessorTest {

  @Test
  void shouldProcessEmptyWithdrawalsWithoutChangingWorldState() {
    final MutableWorldState worldState =
        createWorldStateWithAccounts(List.of(entry("0x1", 1), entry("0x2", 2), entry("0x3", 3)));
    final MutableWorldState originalWorldState =
        createWorldStateWithAccounts(List.of(entry("0x1", 1), entry("0x2", 2), entry("0x3", 3)));
    final WorldUpdater updater = worldState.updater();

    final WithdrawalsProcessor withdrawalsProcessor = new WithdrawalsProcessor();
    withdrawalsProcessor.processWithdrawals(
        Collections.emptyList(), updater, Optional.empty(), Optional.empty());

    assertThat(worldState.get(Address.fromHexString("0x1")).getBalance()).isEqualTo(Wei.of(1));
    assertThat(worldState.get(Address.fromHexString("0x2")).getBalance()).isEqualTo(Wei.of(2));
    assertThat(worldState.get(Address.fromHexString("0x3")).getBalance()).isEqualTo(Wei.of(3));
    assertThat(originalWorldState).isEqualTo(worldState);
  }

  @Test
  void shouldProcessWithdrawalsUpdatingExistingAccountsBalance() {
    final MutableWorldState worldState =
        createWorldStateWithAccounts(List.of(entry("0x1", 1), entry("0x2", 2), entry("0x3", 3)));
    final WorldUpdater updater = worldState.updater();

    final List<Withdrawal> withdrawals =
        List.of(
            new Withdrawal(
                UInt64.valueOf(100),
                UInt64.valueOf(1000),
                Address.fromHexString("0x1"),
                GWei.of(100)),
            new Withdrawal(
                UInt64.valueOf(200),
                UInt64.valueOf(2000),
                Address.fromHexString("0x2"),
                GWei.of(200)));
    final WithdrawalsProcessor withdrawalsProcessor = new WithdrawalsProcessor();
    withdrawalsProcessor.processWithdrawals(
        withdrawals, updater, Optional.empty(), Optional.empty());

    assertThat(worldState.get(Address.fromHexString("0x1")).getBalance())
        .isEqualTo(GWei.of(100).getAsWei().add(1));
    assertThat(worldState.get(Address.fromHexString("0x2")).getBalance())
        .isEqualTo(GWei.of(200).getAsWei().add(2));
    assertThat(worldState.get(Address.fromHexString("0x3")).getBalance()).isEqualTo(Wei.of(3));
  }

  @Test
  void shouldProcessWithdrawalsUpdatingEmptyAccountsBalance() {
    final MutableWorldState worldState = createWorldStateWithAccounts(Collections.emptyList());
    final WorldUpdater updater = worldState.updater();

    final List<Withdrawal> withdrawals =
        List.of(
            new Withdrawal(
                UInt64.valueOf(100),
                UInt64.valueOf(1000),
                Address.fromHexString("0x1"),
                GWei.of(100)),
            new Withdrawal(
                UInt64.valueOf(200),
                UInt64.valueOf(2000),
                Address.fromHexString("0x2"),
                GWei.of(200)));
    final WithdrawalsProcessor withdrawalsProcessor = new WithdrawalsProcessor();
    withdrawalsProcessor.processWithdrawals(
        withdrawals, updater, Optional.empty(), Optional.empty());

    assertThat(worldState.get(Address.fromHexString("0x1")).getBalance())
        .isEqualTo(GWei.of(100).getAsWei());
    assertThat(worldState.get(Address.fromHexString("0x2")).getBalance())
        .isEqualTo(GWei.of(200).getAsWei());
  }

  @Test
  void shouldDeleteEmptyAccounts() {
    final MutableWorldState worldState = createWorldStateWithAccounts(List.of(entry("0x1", 0)));
    final WorldUpdater updater = worldState.updater();

    final List<Withdrawal> withdrawals =
        List.of(
            new Withdrawal(
                UInt64.valueOf(100), UInt64.valueOf(1000), Address.fromHexString("0x1"), GWei.ZERO),
            new Withdrawal(
                UInt64.valueOf(200),
                UInt64.valueOf(2000),
                Address.fromHexString("0x2"),
                GWei.ZERO));
    final WithdrawalsProcessor withdrawalsProcessor = new WithdrawalsProcessor();
    withdrawalsProcessor.processWithdrawals(
        withdrawals, updater, Optional.empty(), Optional.empty());

    assertThat(worldState.get(Address.fromHexString("0x1"))).isNull();
    assertThat(worldState.get(Address.fromHexString("0x2"))).isNull();
  }

  private MutableWorldState createWorldStateWithAccounts(
      final List<Map.Entry<String, Integer>> accountBalances) {
    final MutableWorldState worldState = InMemoryKeyValueStorageProvider.createInMemoryWorldState();
    final WorldUpdater updater = worldState.updater();
    accountBalances.forEach(
        entry ->
            updater.createAccount(
                Address.fromHexString(entry.getKey()), 0, Wei.of(entry.getValue())));
    updater.commit();
    return worldState;
  }
}
