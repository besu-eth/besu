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
package org.hyperledger.besu.ethereum.mainnet.block.access.list;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.datatypes.Wei;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;

import org.apache.tuweni.units.bigints.UInt256;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class BlockAccessListBuilderEip7928Test {

  private static final Address ADDR_1 =
      Address.fromHexString("0x1000000000000000000000000000000000000001");
  private static final Address ADDR_2 =
      Address.fromHexString("0x2000000000000000000000000000000000000002");
  private static final StorageSlotKey SLOT_1 = new StorageSlotKey(UInt256.ONE);

  @Test
  void builderEip7928ItemCountMatchesBuiltList() {
    final BlockAccessList.BlockAccessListBuilder builder = BlockAccessList.builder();
    final BlockAccessList.BlockAccessListBuilder.AccountBuilder ab1 =
        builder.getOrCreateAccountBuilder(ADDR_1);
    ab1.addStorageRead(SLOT_1);
    ab1.addBalanceChange(0, Wei.ONE);
    builder.getOrCreateAccountBuilder(ADDR_2);
    final BlockAccessList built = builder.build();
    Assertions.assertThat(builder.eip7928ItemCount()).isEqualTo(built.eip7928ItemCount());
  }

  /**
   * Replaying a built list into a fresh builder must count the same items as applying the views
   * that produced it.
   */
  @Test
  void mergeFromReplayMatchesIncrementalApply() {
    final PartialBlockAccessView p0 = partialWithOneAccountAndStorageWrite(0);
    final PartialBlockAccessView p1 = partialWithOneAccountAndStorageWrite(1);
    final BlockAccessList.BlockAccessListBuilder main = BlockAccessList.builder();
    main.apply(p0);
    final BlockAccessList snap = main.build();

    final BlockAccessList.BlockAccessListBuilder viaMerge = BlockAccessList.builder();
    viaMerge.mergeFrom(snap);
    viaMerge.apply(p1);

    final BlockAccessList.BlockAccessListBuilder direct = BlockAccessList.builder();
    direct.apply(p0);
    direct.apply(p1);

    Assertions.assertThat(viaMerge.eip7928ItemCount())
        .isEqualTo(direct.eip7928ItemCount())
        .isEqualTo(4L);
  }

  /** A slot written back to where a shared index found it ends up as a read. */
  @Test
  void laterViewAtSameIndexReplacesEarlierOne() {
    final PartialBlockAccessView.PartialBlockAccessViewBuilder first =
        new PartialBlockAccessView.PartialBlockAccessViewBuilder()
            .withTxIndex(1)
            .withSharedIndex(true);
    first
        .getOrCreateAccountBuilder(ADDR_1)
        .withPostBalance(Wei.of(5))
        .withNonceChange(2L)
        .addStorageChange(SLOT_1, UInt256.ZERO, UInt256.ONE);

    final PartialBlockAccessView.PartialBlockAccessViewBuilder second =
        new PartialBlockAccessView.PartialBlockAccessViewBuilder()
            .withTxIndex(1)
            .withSharedIndex(true);
    second.getOrCreateAccountBuilder(ADDR_1).withNonceChange(3L).addStorageRead(SLOT_1);

    final BlockAccessList.BlockAccessListBuilder builder = BlockAccessList.builder();
    builder.apply(first.build());
    builder.apply(second.build());
    final BlockAccessList.AccountChanges account = builder.build().accountChanges().getFirst();

    Assertions.assertThat(account.storageChanges()).isEmpty();
    Assertions.assertThat(account.storageReads())
        .containsExactly(new BlockAccessList.SlotRead(SLOT_1));
    Assertions.assertThat(account.balanceChanges()).isEmpty();
    Assertions.assertThat(account.nonceChanges())
        .containsExactly(new BlockAccessList.NonceChange(1, 3L));
  }

  @Test
  void itemCountIfAppliedCountsOnlyItemsNewToTheList() {
    final StorageSlotKey slot2 = new StorageSlotKey(UInt256.valueOf(2));
    final StorageSlotKey slot3 = new StorageSlotKey(UInt256.valueOf(3));
    final PartialBlockAccessView.PartialBlockAccessViewBuilder committed =
        new PartialBlockAccessView.PartialBlockAccessViewBuilder().withTxIndex(1);
    committed
        .getOrCreateAccountBuilder(ADDR_1)
        .addStorageChange(SLOT_1, UInt256.ZERO, UInt256.ONE)
        .addStorageRead(slot2);
    final BlockAccessList.BlockAccessListBuilder builder = BlockAccessList.builder();
    builder.apply(committed.build());

    final PartialBlockAccessView.PartialBlockAccessViewBuilder candidate =
        new PartialBlockAccessView.PartialBlockAccessViewBuilder().withTxIndex(2);
    candidate
        .getOrCreateAccountBuilder(ADDR_1)
        .addStorageRead(SLOT_1)
        .addStorageChange(slot2, UInt256.ZERO, UInt256.ONE)
        .addStorageChange(slot3, UInt256.ZERO, UInt256.ONE)
        .addStorageRead(slot3);
    candidate.getOrCreateAccountBuilder(ADDR_2).addStorageRead(SLOT_1);

    // Only slot3 and ADDR_2 with its read are new.
    Assertions.assertThat(builder.eip7928ItemCountIfApplied(candidate.build())).isEqualTo(6L);
    Assertions.assertThat(builder.eip7928ItemCount()).isEqualTo(3L);
  }

  @Test
  void itemCountIfAppliedDropsWritesReplacedAtASharedIndex() {
    final PartialBlockAccessView.PartialBlockAccessViewBuilder first =
        new PartialBlockAccessView.PartialBlockAccessViewBuilder()
            .withTxIndex(0)
            .withSharedIndex(true);
    first.getOrCreateAccountBuilder(ADDR_1).addStorageChange(SLOT_1, UInt256.ZERO, UInt256.ONE);
    final BlockAccessList.BlockAccessListBuilder builder = BlockAccessList.builder();
    builder.apply(first.build());

    final PartialBlockAccessView.PartialBlockAccessViewBuilder second =
        new PartialBlockAccessView.PartialBlockAccessViewBuilder()
            .withTxIndex(0)
            .withSharedIndex(true);
    second.getOrCreateAccountBuilder(ADDR_1).withNonceChange(1L);

    Assertions.assertThat(builder.eip7928ItemCountIfApplied(second.build())).isEqualTo(1L);
    Assertions.assertThat(builder.eip7928ItemCount()).isEqualTo(2L);
  }

  /**
   * Random views over a few accounts and slots often overlap the list and each other, repeat
   * accounts and slots within a view, and reuse the shared indexes of earlier views.
   */
  @ParameterizedTest
  @ValueSource(longs = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10})
  void itemCountIfAppliedMatchesApplyingToACopy(final long seed) {
    final Random random = new Random(seed);
    final BlockAccessList.BlockAccessListBuilder builder = BlockAccessList.builder();
    long nextTxIndex = 0;
    for (int round = 0; round < 500; round++) {
      final PartialBlockAccessView view = randomView(random, nextTxIndex);
      final BlockAccessList before = builder.build();
      final BlockAccessList.BlockAccessListBuilder copy = BlockAccessList.builder();
      copy.mergeFrom(before);
      copy.apply(view);
      final long expected = copy.build().eip7928ItemCount();

      Assertions.assertThat(builder.eip7928ItemCountIfApplied(view))
          .as("round %d, view %s", round, view)
          .isEqualTo(expected);
      Assertions.assertThat(builder.build()).isEqualTo(before);

      if (random.nextBoolean()) {
        builder.apply(view);
        nextTxIndex++;
        Assertions.assertThat(builder.eip7928ItemCount())
            .isEqualTo(builder.build().eip7928ItemCount())
            .isEqualTo(expected);
      }
    }
  }

  @Test
  void mergeFromRejectsStorageSlotWithEmptyChanges() {
    final BlockAccessList invalid =
        new BlockAccessList(
            List.of(
                new BlockAccessList.AccountChanges(
                    ADDR_1,
                    List.of(new BlockAccessList.SlotChanges(SLOT_1, List.of())),
                    List.of(),
                    List.of(),
                    List.of(),
                    List.of())));

    Assertions.assertThatThrownBy(() -> BlockAccessList.builder().mergeFrom(invalid))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("at least one storage change");
  }

  private static PartialBlockAccessView randomView(final Random random, final long nextTxIndex) {
    final List<PartialBlockAccessView.AccountChanges> accounts = new ArrayList<>();
    for (int i = random.nextInt(5); i > 0; i--) {
      final List<PartialBlockAccessView.SlotChange> writes = new ArrayList<>();
      for (int w = random.nextInt(4); w > 0; w--) {
        writes.add(
            new PartialBlockAccessView.SlotChange(
                randomSlot(random), UInt256.ZERO, UInt256.valueOf(random.nextInt(3))));
      }
      final List<StorageSlotKey> reads = new ArrayList<>();
      for (int r = random.nextInt(4); r > 0; r--) {
        reads.add(randomSlot(random));
      }
      accounts.add(
          new PartialBlockAccessView.AccountChanges(
              Address.fromHexString(String.format("0x%040x", 0x100 + random.nextInt(6))),
              random.nextBoolean() ? Optional.of(Wei.of(random.nextInt(100))) : Optional.empty(),
              random.nextBoolean() ? Optional.of(random.nextLong(10)) : Optional.empty(),
              Optional.empty(),
              reads,
              writes));
    }
    final long txIndex = random.nextInt(4) == 0 ? random.nextLong(nextTxIndex + 1) : nextTxIndex;
    return new PartialBlockAccessView(accounts, txIndex, random.nextInt(3) == 0);
  }

  private static StorageSlotKey randomSlot(final Random random) {
    return new StorageSlotKey(UInt256.valueOf(random.nextInt(5)));
  }

  private static PartialBlockAccessView partialWithOneAccountAndStorageWrite(final int txIndex) {
    final Address addr = Address.fromHexString(String.format("0x%040x", txIndex + 100L));
    final PartialBlockAccessView.PartialBlockAccessViewBuilder b =
        new PartialBlockAccessView.PartialBlockAccessViewBuilder().withTxIndex(txIndex);
    b.getOrCreateAccountBuilder(addr)
        .addStorageChange(new StorageSlotKey(UInt256.ONE), null, UInt256.ZERO);
    return b.build();
  }
}
