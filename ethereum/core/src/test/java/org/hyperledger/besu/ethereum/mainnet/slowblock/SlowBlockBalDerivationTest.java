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
package org.hyperledger.besu.ethereum.mainnet.slowblock;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.AccountChanges;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.BalanceChange;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.CodeChange;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.NonceChange;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.SlotChanges;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.SlotRead;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.StorageChange;

import java.util.List;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;

class SlowBlockBalDerivationTest {

  private static final Address ACCOUNT_A =
      Address.fromHexString("0x00000000000000000000000000000000000000aa");
  private static final Address ACCOUNT_B =
      Address.fromHexString("0x00000000000000000000000000000000000000bb");

  private static final StorageSlotKey SLOT_0 = new StorageSlotKey(UInt256.ZERO);
  private static final StorageSlotKey SLOT_1 = new StorageSlotKey(UInt256.ONE);
  private static final StorageSlotKey SLOT_2 = new StorageSlotKey(UInt256.valueOf(2));

  @Test
  void writesAreCountedPerChangeEntryNotPerDistinctSlot() {
    final BlockAccessList bal =
        new BlockAccessList(
            List.of(
                new AccountChanges(
                    ACCOUNT_A,
                    List.of(
                        // one slot written by three transactions counts as three write events
                        new SlotChanges(
                            SLOT_0,
                            List.of(
                                new StorageChange(1, UInt256.valueOf(1)),
                                new StorageChange(2, UInt256.valueOf(2)),
                                new StorageChange(3, UInt256.valueOf(3)))),
                        new SlotChanges(SLOT_1, List.of(new StorageChange(1, UInt256.valueOf(9))))),
                    List.of(new SlotRead(SLOT_2)),
                    List.of(new BalanceChange(1, Wei.of(10)), new BalanceChange(2, Wei.of(20))),
                    List.of(new NonceChange(1, 4L)),
                    List.of(new CodeChange(1, Bytes.fromHexString("0x60006000")))))); // 4 bytes

    final SlowBlockMetrics metrics = new SlowBlockMetrics(0);
    SlowBlockBalDerivation.apply(bal, metrics);

    assertThat(metrics.writeStorageSlots()).isEqualTo(4);
    assertThat(metrics.writeAccounts()).isEqualTo(4); // 2 balance + 1 nonce + 1 code
    assertThat(metrics.writeCode()).isEqualTo(1);
    assertThat(metrics.writeCodeBytes()).isEqualTo(4);
  }

  @Test
  void uniqueCountsCoverReadAndWrittenSlots() {
    final BlockAccessList bal =
        new BlockAccessList(
            List.of(
                new AccountChanges(
                    ACCOUNT_A,
                    List.of(new SlotChanges(SLOT_0, List.of(new StorageChange(1, UInt256.ONE)))),
                    List.of(new SlotRead(SLOT_1), new SlotRead(SLOT_2)),
                    List.of(),
                    List.of(),
                    List.of()),
                new AccountChanges(
                    ACCOUNT_B,
                    List.of(),
                    List.of(new SlotRead(SLOT_0)),
                    List.of(),
                    List.of(),
                    List.of())));

    final SlowBlockMetrics metrics = new SlowBlockMetrics(0);
    SlowBlockBalDerivation.apply(bal, metrics);

    assertThat(metrics.uniqueAccounts()).isEqualTo(2);
    assertThat(metrics.uniqueStorageSlots()).isEqualTo(4);
  }

  @Test
  void slotDeletionIsTheNetOutcomeNotEveryZeroWrite() {
    final BlockAccessList bal =
        new BlockAccessList(
            List.of(
                new AccountChanges(
                    ACCOUNT_A,
                    List.of(
                        // zeroed then rewritten: not a deletion
                        new SlotChanges(
                            SLOT_0,
                            List.of(
                                new StorageChange(1, UInt256.ZERO),
                                new StorageChange(2, UInt256.valueOf(7)))),
                        // written then zeroed: a deletion
                        new SlotChanges(
                            SLOT_1,
                            List.of(
                                new StorageChange(1, UInt256.valueOf(7)),
                                new StorageChange(2, UInt256.ZERO)))),
                    List.of(),
                    List.of(),
                    List.of(),
                    List.of())));

    final SlowBlockMetrics metrics = new SlowBlockMetrics(0);
    SlowBlockBalDerivation.apply(bal, metrics);

    assertThat(metrics.writeStorageSlots()).isEqualTo(4);
    assertThat(metrics.storageSlotsDeleted()).isEqualTo(1);
  }

  @Test
  void anAccountLeftEmptyCountsAsDeleted() {
    final BlockAccessList bal =
        new BlockAccessList(
            List.of(
                new AccountChanges(
                    ACCOUNT_A,
                    List.of(),
                    List.of(),
                    List.of(new BalanceChange(1, Wei.of(50)), new BalanceChange(2, Wei.ZERO)),
                    List.of(new NonceChange(2, 0L)),
                    List.of())));

    final SlowBlockMetrics metrics = new SlowBlockMetrics(0);
    SlowBlockBalDerivation.apply(bal, metrics);

    assertThat(metrics.accountsDeleted()).isEqualTo(1);
  }

  @Test
  void anAccountLeftWithBalanceOrNonceOrCodeIsNotDeleted() {
    final BlockAccessList bal =
        new BlockAccessList(
            List.of(
                // ends with a balance
                new AccountChanges(
                    ACCOUNT_A,
                    List.of(),
                    List.of(),
                    List.of(new BalanceChange(1, Wei.of(50))),
                    List.of(),
                    List.of()),
                // zero balance but a non-zero nonce
                new AccountChanges(
                    ACCOUNT_B,
                    List.of(),
                    List.of(),
                    List.of(new BalanceChange(1, Wei.ZERO)),
                    List.of(new NonceChange(1, 3L)),
                    List.of())));

    final SlowBlockMetrics metrics = new SlowBlockMetrics(0);
    SlowBlockBalDerivation.apply(bal, metrics);

    assertThat(metrics.accountsDeleted()).isZero();
  }

  @Test
  void anEmptyBlockAccessListDerivesZeroes() {
    final SlowBlockMetrics metrics = new SlowBlockMetrics(0);
    SlowBlockBalDerivation.apply(new BlockAccessList(List.of()), metrics);

    assertThat(metrics.uniqueAccounts()).isZero();
    assertThat(metrics.uniqueStorageSlots()).isZero();
    assertThat(metrics.writeAccounts()).isZero();
    assertThat(metrics.writeStorageSlots()).isZero();
    assertThat(metrics.accountsDeleted()).isZero();
    assertThat(metrics.storageSlotsDeleted()).isZero();
  }
}
