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
package org.hyperledger.besu.ethereum.eth.sync.backwardsync2;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.AccountChanges;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.BalanceChange;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.CodeChange;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.NonceChange;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.SlotChanges;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.SlotRead;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.StorageChange;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;

import java.util.List;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class BalImporterTest {

  private static final Address CHANGED = Address.fromHexString("0x01");
  private static final Address EMPTIED = Address.fromHexString("0x02");
  private static final Address CREATED = Address.fromHexString("0x03");
  private static final Address READ_ONLY = Address.fromHexString("0x04");

  private final MutableWorldState worldState =
      InMemoryKeyValueStorageProvider.createInMemoryWorldState();

  @BeforeEach
  void setUp() {
    final WorldUpdater updater = worldState.updater();
    final MutableAccount changed = updater.createAccount(CHANGED, 1, Wei.of(10));
    changed.setStorageValue(UInt256.ONE, UInt256.valueOf(5));
    changed.setStorageValue(UInt256.valueOf(2), UInt256.valueOf(7));
    updater.createAccount(EMPTIED, 0, Wei.of(3));
    updater.commit();
  }

  @Test
  void writesTheLastValueOfEachChangedFieldAndSlot() {
    apply(
        new AccountChanges(
            CHANGED,
            List.of(
                slot(
                    1,
                    new StorageChange(0, UInt256.valueOf(9)),
                    new StorageChange(1, UInt256.ZERO)),
                slot(3, new StorageChange(1, UInt256.valueOf(4)))),
            List.of(),
            List.of(new BalanceChange(0, Wei.of(20)), new BalanceChange(1, Wei.of(25))),
            List.of(new NonceChange(1, 2)),
            List.of()));

    final Account account = worldState.get(CHANGED);
    assertThat(account.getBalance()).isEqualTo(Wei.of(25));
    assertThat(account.getNonce()).isEqualTo(2);
    assertThat(account.getStorageValue(UInt256.ONE)).isEqualTo(UInt256.ZERO);
    assertThat(account.getStorageValue(UInt256.valueOf(2))).isEqualTo(UInt256.valueOf(7));
    assertThat(account.getStorageValue(UInt256.valueOf(3))).isEqualTo(UInt256.valueOf(4));
  }

  @Test
  void deletesAnAccountTheBlockLeavesEmpty() {
    apply(
        new AccountChanges(
            EMPTIED,
            List.of(),
            List.of(),
            List.of(new BalanceChange(0, Wei.ZERO)),
            List.of(),
            List.of()));

    assertThat(worldState.get(EMPTIED)).isNull();
  }

  @Test
  void createsAnAccountWithItsCodeAndLeavesAReadAccountAbsent() {
    final Bytes code = Bytes.fromHexString("0x6001600055");
    apply(
        new AccountChanges(
            CREATED,
            List.of(),
            List.of(),
            List.of(),
            List.of(new NonceChange(0, 1)),
            List.of(new CodeChange(0, code))),
        new AccountChanges(
            READ_ONLY,
            List.of(),
            List.of(new SlotRead(new StorageSlotKey(UInt256.ONE))),
            List.of(),
            List.of(),
            List.of()));

    assertThat(worldState.get(CREATED).getCode()).isEqualTo(code);
    assertThat(worldState.get(READ_ONLY)).isNull();
  }

  private void apply(final AccountChanges... changes) {
    final WorldUpdater updater = worldState.updater();
    BalImporter.applyChanges(updater, new BlockAccessList(List.of(changes)));
    updater.commit();
  }

  private static SlotChanges slot(final long key, final StorageChange... changes) {
    return new SlotChanges(new StorageSlotKey(UInt256.valueOf(key)), List.of(changes));
  }
}
