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

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.datatypes.Wei;

import java.util.List;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

class BlockAccessListSortingTest {

  // First-byte values chosen so that signed-byte sort (-128, -1, 1) and unsigned-byte sort
  // (1, 128, 255) produce different orderings; this guards against an accidental signed compare.
  private static final Address ADDR_LOW =
      Address.fromHexString("0x0100000000000000000000000000000000000001");
  private static final Address ADDR_MID =
      Address.fromHexString("0x8000000000000000000000000000000000000080");
  private static final Address ADDR_HIGH =
      Address.fromHexString("0xff000000000000000000000000000000000000ff");

  // Same idea for storage slots: first byte spans 0x00 / 0x80 / 0xff.
  private static final StorageSlotKey SLOT_LOW = new StorageSlotKey(UInt256.valueOf(1L));
  private static final StorageSlotKey SLOT_MID =
      new StorageSlotKey(
          UInt256.fromHexString(
              "0x8000000000000000000000000000000000000000000000000000000000000000"));
  private static final StorageSlotKey SLOT_HIGH =
      new StorageSlotKey(
          UInt256.fromHexString(
              "0xff00000000000000000000000000000000000000000000000000000000000000"));

  @Test
  void blockAccessListAccountChangesSortedByAddressBytesAscending() {
    final BlockAccessList.BlockAccessListBuilder builder = BlockAccessList.builder();
    // Insert in a non-sorted order.
    builder.getOrCreateAccountBuilder(ADDR_HIGH);
    builder.getOrCreateAccountBuilder(ADDR_LOW);
    builder.getOrCreateAccountBuilder(ADDR_MID);

    final List<Address> built =
        builder.build().accountChanges().stream()
            .map(BlockAccessList.AccountChanges::address)
            .toList();

    Assertions.assertThat(built).containsExactly(ADDR_LOW, ADDR_MID, ADDR_HIGH);
  }

  @Test
  void slotsAndTxChangesSortedWithinAccount() {
    final BlockAccessList.BlockAccessListBuilder builder = BlockAccessList.builder();
    final BlockAccessList.BlockAccessListBuilder.AccountBuilder ab =
        builder.getOrCreateAccountBuilder(ADDR_LOW);

    // Slot writes in non-sorted order, with non-monotonic txIndex inside each slot.
    ab.addStorageWrite(SLOT_HIGH, 5L, UInt256.valueOf(3L));
    ab.addStorageWrite(SLOT_HIGH, 1L, UInt256.valueOf(1L));
    ab.addStorageWrite(SLOT_LOW, 4L, UInt256.valueOf(7L));
    ab.addStorageWrite(SLOT_LOW, 2L, UInt256.valueOf(6L));
    ab.addStorageWrite(SLOT_MID, 3L, UInt256.valueOf(9L));

    // Reads on slots that have no writes (writes shadow reads at builder level).
    final StorageSlotKey readLow = new StorageSlotKey(UInt256.valueOf(2L));
    final StorageSlotKey readMid =
        new StorageSlotKey(
            UInt256.fromHexString(
                "0x8100000000000000000000000000000000000000000000000000000000000000"));
    final StorageSlotKey readHigh =
        new StorageSlotKey(
            UInt256.fromHexString(
                "0xfe00000000000000000000000000000000000000000000000000000000000000"));
    ab.addStorageRead(readHigh);
    ab.addStorageRead(readLow);
    ab.addStorageRead(readMid);

    // Per-tx scalar changes inserted with non-monotonic txIndex.
    ab.addBalanceChange(7L, Wei.of(70L));
    ab.addBalanceChange(2L, Wei.of(20L));
    ab.addBalanceChange(5L, Wei.of(50L));

    ab.addNonceChange(9L, 99L);
    ab.addNonceChange(1L, 11L);
    ab.addNonceChange(4L, 44L);

    ab.addCodeChange(8L, Bytes.fromHexString("0x08"));
    ab.addCodeChange(3L, Bytes.fromHexString("0x03"));
    ab.addCodeChange(6L, Bytes.fromHexString("0x06"));

    final BlockAccessList.AccountChanges out = builder.build().accountChanges().getFirst();

    // Slot writes: outer list sorted by slot bytes ascending; inner change list by txIndex.
    Assertions.assertThat(out.storageChanges().stream().map(BlockAccessList.SlotChanges::slot))
        .containsExactly(SLOT_LOW, SLOT_MID, SLOT_HIGH);
    Assertions.assertThat(
            out.storageChanges().get(0).changes().stream()
                .map(BlockAccessList.StorageChange::txIndex))
        .containsExactly(2L, 4L);
    Assertions.assertThat(
            out.storageChanges().get(2).changes().stream()
                .map(BlockAccessList.StorageChange::txIndex))
        .containsExactly(1L, 5L);

    // Slot reads: sorted by slot bytes ascending.
    Assertions.assertThat(out.storageReads().stream().map(BlockAccessList.SlotRead::slot))
        .containsExactly(readLow, readMid, readHigh);

    // Per-tx scalar changes sorted by txIndex ascending.
    Assertions.assertThat(out.balanceChanges().stream().map(BlockAccessList.BalanceChange::txIndex))
        .containsExactly(2L, 5L, 7L);
    Assertions.assertThat(out.nonceChanges().stream().map(BlockAccessList.NonceChange::txIndex))
        .containsExactly(1L, 4L, 9L);
    Assertions.assertThat(out.codeChanges().stream().map(BlockAccessList.CodeChange::txIndex))
        .containsExactly(3L, 6L, 8L);
  }

  @Test
  void partialBlockAccessViewAccountChangesSortedByAddressBytesAscending() {
    final PartialBlockAccessView.PartialBlockAccessViewBuilder builder =
        new PartialBlockAccessView.PartialBlockAccessViewBuilder().withTxIndex(0L);
    builder.getOrCreateAccountBuilder(ADDR_HIGH);
    builder.getOrCreateAccountBuilder(ADDR_LOW);
    builder.getOrCreateAccountBuilder(ADDR_MID);

    final List<Address> built =
        builder.build().accountChanges().stream()
            .map(PartialBlockAccessView.AccountChanges::getAddress)
            .toList();

    Assertions.assertThat(built).containsExactly(ADDR_LOW, ADDR_MID, ADDR_HIGH);
  }
}
