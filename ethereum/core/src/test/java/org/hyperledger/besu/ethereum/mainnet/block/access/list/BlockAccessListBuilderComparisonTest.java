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
package org.hyperledger.besu.ethereum.mainnet.block.access.list;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.AccountChanges;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.BlockAccessListBuilder;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.BlockAccessListBuilder.AccountBuilder;

import java.util.ArrayList;
import java.util.List;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;

class BlockAccessListBuilderComparisonTest {

  private static final StorageSlotKey SLOT_LOW = new StorageSlotKey(UInt256.valueOf(1));
  private static final StorageSlotKey SLOT_HIGH =
      new StorageSlotKey(UInt256.fromHexString("0xff" + "00".repeat(31)));

  @Test
  void aBuilderHasTheChangesOfTheListItBuilds() {
    final BlockAccessListBuilder builder = builder(3, 1);

    assertThat(builder.hasSameChangesAs(builder.build())).isTrue();
  }

  @Test
  void aBuilderOfManyAccountsHasTheChangesOfTheListItBuilds() {
    final BlockAccessListBuilder builder = builder(600, 1);

    assertThat(builder.hasSameChangesAs(builder.build())).isTrue();
  }

  @Test
  void aChangeTheBuilderDoesNotHaveDiffers() {
    final BlockAccessList list = builder(600, 1).build();

    for (final BlockAccessListBuilder other :
        List.of(builder(600, 2), builder(599, 1), builder(601, 1))) {
      assertThat(other.hasSameChangesAs(list)).isFalse();
      assertThat(other.build().hasSameChangesAs(list)).isFalse();
    }
  }

  @Test
  void aListOutOfCanonicalOrderNeverMatches() {
    final BlockAccessListBuilder builder = builder(3, 1);
    final BlockAccessList list = builder.build();
    final List<AccountChanges> accounts = new ArrayList<>(list.accountChanges());
    accounts.add(accounts.removeFirst());
    final AccountChanges account = accounts.getFirst();
    final AccountChanges slotsReversed =
        new AccountChanges(
            account.address(),
            account.storageChanges().reversed(),
            account.storageReads(),
            account.balanceChanges(),
            account.nonceChanges(),
            account.codeChanges());
    final List<AccountChanges> sortedWithSlotsReversed = new ArrayList<>(list.accountChanges());
    sortedWithSlotsReversed.set(sortedWithSlotsReversed.indexOf(account), slotsReversed);

    for (final BlockAccessList unsorted :
        List.of(new BlockAccessList(accounts), new BlockAccessList(sortedWithSlotsReversed))) {
      assertThat(builder.build().hasSameChangesAs(unsorted)).isFalse();
      assertThat(builder.hasSameChangesAs(unsorted)).isFalse();
    }
  }

  @Test
  void changesAddedOutOfTransactionOrderAreOnlyMatchedOnceBuilt() {
    final BlockAccessListBuilder builder = BlockAccessList.builder();
    final AccountBuilder account = builder.getOrCreateAccountBuilder(address(1));
    account.addBalanceChange(2, Wei.of(20));
    account.addBalanceChange(1, Wei.of(10));
    final BlockAccessList list = builder.build();

    assertThat(list.hasSameChangesAs(builder.build())).isTrue();
    assertThat(builder.hasSameChangesAs(list)).isFalse();
  }

  /** {@code count} accounts, added from the highest address down, each changed by every kind. */
  private static BlockAccessListBuilder builder(final int count, final long value) {
    final BlockAccessListBuilder builder = BlockAccessList.builder();
    for (int i = count; i > 0; i--) {
      final AccountBuilder account = builder.getOrCreateAccountBuilder(address(i));
      account.addStorageWrite(SLOT_HIGH, 1, UInt256.valueOf(value));
      account.addStorageWrite(SLOT_LOW, 1, UInt256.valueOf(value));
      account.addStorageWrite(SLOT_LOW, 2, UInt256.valueOf(value + 1));
      account.addStorageRead(new StorageSlotKey(UInt256.valueOf(7)));
      account.addStorageRead(new StorageSlotKey(UInt256.valueOf(3)));
      account.addBalanceChange(1, Wei.of(value));
      account.addBalanceChange(2, Wei.of(value + i));
      account.addNonceChange(1, value);
      account.addCodeChange(2, Bytes.of((int) value));
    }
    return builder;
  }

  private static Address address(final int i) {
    return Address.fromHexString(String.format("0x%040x", i));
  }
}
