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
import static org.mockito.Mockito.mock;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.account.BonsaiAccount;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.provider.BonsaiWorldStateProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;

import java.util.List;
import java.util.function.Consumer;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;

class BlockAccessListStateApplierTest {

  private static final Address ALICE =
      Address.fromHexString("0x1000000000000000000000000000000000000001");
  private static final Address BOB =
      Address.fromHexString("0x1000000000000000000000000000000000000002");
  private static final Address CAROL =
      Address.fromHexString("0x1000000000000000000000000000000000000003");
  private static final UInt256 SLOT_1 = UInt256.ONE;
  private static final UInt256 SLOT_2 = UInt256.valueOf(2);

  private final BonsaiWorldStateProvider archive =
      InMemoryKeyValueStorageProvider.createBonsaiInMemoryWorldStateArchive(mock(Blockchain.class));
  private final BonsaiWorldState worldState = (BonsaiWorldState) archive.getWorldState();

  @Test
  void writesTheFinalValueOfEveryChangedField() {
    populate(
        updater ->
            updater
                .createAccount(ALICE, 1, Wei.of(10))
                .setStorageValue(SLOT_1, UInt256.valueOf(5)));

    apply(
        new BlockAccessList(
            List.of(
                accountChanges(
                    ALICE,
                    List.of(slotChanges(SLOT_1, 5L, 9L)),
                    List.of(new BlockAccessList.BalanceChange(1, Wei.of(3))),
                    List.of(new BlockAccessList.NonceChange(1, 2L)),
                    List.of(new BlockAccessList.CodeChange(1, Bytes.fromHexString("0x6001")))))));

    final Account alice = worldState.get(ALICE);
    assertThat(alice.getBalance()).isEqualTo(Wei.of(3));
    assertThat(alice.getNonce()).isEqualTo(2L);
    assertThat(alice.getCode()).isEqualTo(Bytes.fromHexString("0x6001"));
    assertThat(alice.getStorageValue(SLOT_1)).isEqualTo(UInt256.valueOf(9));
  }

  @Test
  void theLastChangeOfSeveralTransactionsWins() {
    populate(updater -> updater.createAccount(ALICE, 1, Wei.of(10)));

    apply(
        new BlockAccessList(
            List.of(
                accountChanges(
                    ALICE,
                    List.of(slotChanges(SLOT_1, 4L, 7L, 6L)),
                    List.of(
                        new BlockAccessList.BalanceChange(1, Wei.of(50)),
                        new BlockAccessList.BalanceChange(2, Wei.of(20)),
                        new BlockAccessList.BalanceChange(3, Wei.of(30))),
                    List.of(
                        new BlockAccessList.NonceChange(1, 2L),
                        new BlockAccessList.NonceChange(2, 3L)),
                    List.of(
                        new BlockAccessList.CodeChange(1, Bytes.fromHexString("0x60")),
                        new BlockAccessList.CodeChange(2, Bytes.fromHexString("0x6002")))))));

    final Account alice = worldState.get(ALICE);
    assertThat(alice.getBalance()).isEqualTo(Wei.of(30));
    assertThat(alice.getNonce()).isEqualTo(3L);
    assertThat(alice.getCode()).isEqualTo(Bytes.fromHexString("0x6002"));
    assertThat(alice.getStorageValue(SLOT_1)).isEqualTo(UInt256.valueOf(6));
  }

  @Test
  void createsAnAccountTheStateDoesNotHold() {
    apply(
        new BlockAccessList(
            List.of(
                accountChanges(
                    BOB,
                    List.of(slotChanges(SLOT_2, 8L)),
                    List.of(new BlockAccessList.BalanceChange(1, Wei.of(1))),
                    List.of(),
                    List.of()))));

    final Account bob = worldState.get(BOB);
    assertThat(bob).isNotNull();
    assertThat(bob.getBalance()).isEqualTo(Wei.of(1));
    assertThat(bob.getStorageValue(SLOT_2)).isEqualTo(UInt256.valueOf(8));
  }

  @Test
  void deletesAnAccountThatEndsEmpty() {
    populate(updater -> updater.createAccount(ALICE, 0, Wei.of(10)));

    apply(
        new BlockAccessList(
            List.of(
                accountChanges(
                    ALICE,
                    List.of(),
                    List.of(new BlockAccessList.BalanceChange(1, Wei.ZERO)),
                    List.of(),
                    List.of()))));

    assertThat(worldState.get(ALICE)).isNull();
  }

  @Test
  void doesNotCreateAnAccountThatEndsEmpty() {
    apply(
        new BlockAccessList(
            List.of(
                accountChanges(
                    BOB,
                    List.of(),
                    List.of(new BlockAccessList.BalanceChange(1, Wei.ZERO)),
                    List.of(),
                    List.of()))));

    assertThat(worldState.get(BOB)).isNull();
  }

  @Test
  void removesASlotWhoseFinalValueIsZero() {
    populate(
        updater -> {
          final var carol = updater.createAccount(CAROL, 1, Wei.of(1));
          carol.setStorageValue(SLOT_1, UInt256.valueOf(5));
          carol.setStorageValue(SLOT_2, UInt256.valueOf(6));
        });

    apply(
        new BlockAccessList(
            List.of(
                accountChanges(
                    CAROL,
                    List.of(slotChanges(SLOT_1, 7L, 0L)),
                    List.of(),
                    List.of(),
                    List.of()))));

    final Account carol = worldState.get(CAROL);
    assertThat(carol.getStorageValue(SLOT_1)).isEqualTo(UInt256.ZERO);
    assertThat(carol.getStorageValue(SLOT_2)).isEqualTo(UInt256.valueOf(6));
    final Hash storageRoot = ((BonsaiAccount) carol).getStorageRoot();
    assertThat(worldState.getAllAccountStorage(CAROL, storageRoot)).hasSize(1);
  }

  @Test
  void leavesAccountsTheListOnlyReadsUntouched() {
    populate(updater -> updater.createAccount(ALICE, 1, Wei.of(10)));
    final Hash rootBefore = worldState.rootHash();

    apply(
        new BlockAccessList(
            List.of(
                new BlockAccessList.AccountChanges(
                    ALICE,
                    List.of(),
                    List.of(new BlockAccessList.SlotRead(new StorageSlotKey(SLOT_1))),
                    List.of(),
                    List.of(),
                    List.of()),
                new BlockAccessList.AccountChanges(
                    BOB, List.of(), List.of(), List.of(), List.of(), List.of()))));

    assertThat(worldState.rootHash()).isEqualTo(rootBefore);
    assertThat(worldState.get(ALICE).getBalance()).isEqualTo(Wei.of(10));
    assertThat(worldState.get(BOB)).isNull();
  }

  private void populate(final Consumer<WorldUpdater> setup) {
    final WorldUpdater updater = worldState.updater();
    setup.accept(updater);
    updater.commit();
    worldState.persist(null);
  }

  private void apply(final BlockAccessList blockAccessList) {
    final WorldUpdater updater = worldState.updater();
    BlockAccessListStateApplier.apply(updater, blockAccessList);
    updater.commit();
    worldState.persist(null);
  }

  private static BlockAccessList.AccountChanges accountChanges(
      final Address address,
      final List<BlockAccessList.SlotChanges> storageChanges,
      final List<BlockAccessList.BalanceChange> balanceChanges,
      final List<BlockAccessList.NonceChange> nonceChanges,
      final List<BlockAccessList.CodeChange> codeChanges) {
    return new BlockAccessList.AccountChanges(
        address, storageChanges, List.of(), balanceChanges, nonceChanges, codeChanges);
  }

  /** The values the slot takes in the transactions 1, 2, ... of the block. */
  private static BlockAccessList.SlotChanges slotChanges(final UInt256 slot, final long... values) {
    final BlockAccessList.StorageChange[] changes =
        new BlockAccessList.StorageChange[values.length];
    for (int i = 0; i < values.length; i++) {
      changes[i] = new BlockAccessList.StorageChange(i + 1, UInt256.valueOf(values[i]));
    }
    return new BlockAccessList.SlotChanges(new StorageSlotKey(slot), List.of(changes));
  }
}
