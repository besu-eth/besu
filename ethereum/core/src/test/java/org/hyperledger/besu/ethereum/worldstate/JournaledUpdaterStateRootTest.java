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
package org.hyperledger.besu.ethereum.worldstate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.storage.keyvalue.WorldStatePreimageKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.forest.storage.ForestWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.forest.worldview.ForestMutableWorldState;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.evm.internal.EvmConfiguration.WorldUpdaterMode;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;
import org.hyperledger.besu.services.kvstore.InMemoryKeyValueStorage;

import java.util.function.Consumer;

import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/** Commits the same transaction in both world updater modes and compares the state roots. */
class JournaledUpdaterStateRootTest {

  private static final Address ADDRESS = Address.fromHexString("0x1234");
  private static final Address NEW_ADDRESS = Address.fromHexString("0x5678");
  private static final UInt256 SLOT_1 = UInt256.ONE;
  private static final UInt256 SLOT_2 = UInt256.valueOf(2);
  private static final UInt256 STORED = UInt256.valueOf(5);

  enum Storage {
    FOREST {
      @Override
      MutableWorldState create(final EvmConfiguration evmConfiguration) {
        return new ForestMutableWorldState(
            new ForestWorldStateKeyValueStorage(new InMemoryKeyValueStorage()),
            new WorldStatePreimageKeyValueStorage(new InMemoryKeyValueStorage()),
            evmConfiguration);
      }
    },
    BONSAI {
      @Override
      MutableWorldState create(final EvmConfiguration evmConfiguration) {
        return InMemoryKeyValueStorageProvider.createBonsaiInMemoryWorldStateArchive(
                mock(Blockchain.class), evmConfiguration, null)
            .getWorldState();
      }
    };

    abstract MutableWorldState create(EvmConfiguration evmConfiguration);
  }

  @ParameterizedTest
  @EnumSource(Storage.class)
  void keepsAStorageClearCommittedFromAFrame(final Storage storage) {
    final MutableWorldState worldState =
        assertSameStateRootInBothModes(
            storage,
            transaction -> {
              final WorldUpdater frame = transaction.updater();
              final MutableAccount account = frame.getAccount(ADDRESS);
              account.clearStorage();
              account.setStorageValue(SLOT_1, UInt256.ONE);
              frame.commit();
            });

    assertThat(worldState.get(ADDRESS).getStorageValue(SLOT_1)).isEqualTo(UInt256.ONE);
    assertThat(worldState.get(ADDRESS).getStorageValue(SLOT_2)).isEqualTo(UInt256.ZERO);
  }

  @ParameterizedTest
  @EnumSource(Storage.class)
  void dropsAStorageClearFromARevertedFrame(final Storage storage) {
    final MutableWorldState worldState =
        assertSameStateRootInBothModes(
            storage,
            transaction -> {
              // loaded before the frame, so the revert keeps the account in the journal
              final MutableAccount account = transaction.getAccount(ADDRESS);
              final WorldUpdater frame = transaction.updater();
              frame.getAccount(ADDRESS).clearStorage();
              frame.revert();
              account.setStorageValue(SLOT_1, account.getStorageValue(SLOT_2).add(1));
            });

    assertThat(worldState.get(ADDRESS).getStorageValue(SLOT_1)).isEqualTo(STORED.add(1));
    assertThat(worldState.get(ADDRESS).getStorageValue(SLOT_2)).isEqualTo(STORED);
  }

  @ParameterizedTest
  @EnumSource(Storage.class)
  void keepsAnAccountCreatedAfterItWasDeleted(final Storage storage) {
    final MutableWorldState worldState =
        assertSameStateRootInBothModes(
            storage,
            transaction -> {
              transaction.deleteAccount(ADDRESS);
              transaction.createAccount(ADDRESS).setBalance(Wei.of(2));
            });

    final Account account = worldState.get(ADDRESS);
    assertThat(account).isNotNull();
    assertThat(account.getBalance()).isEqualTo(Wei.of(2));
  }

  @ParameterizedTest
  @EnumSource(Storage.class)
  void keepsAnAccountCreatedInACommittedFrame(final Storage storage) {
    final MutableWorldState worldState =
        assertSameStateRootInBothModes(
            storage,
            transaction -> {
              final WorldUpdater frame = transaction.updater();
              final MutableAccount account = frame.createAccount(NEW_ADDRESS, 1, Wei.of(2));
              account.setStorageValue(SLOT_1, STORED);
              frame.commit();
            });

    final Account account = worldState.get(NEW_ADDRESS);
    assertThat(account.getNonce()).isEqualTo(1);
    assertThat(account.getBalance()).isEqualTo(Wei.of(2));
    assertThat(account.getStorageValue(SLOT_1)).isEqualTo(STORED);
  }

  @ParameterizedTest
  @EnumSource(Storage.class)
  void dropsAnAccountCreatedInARevertedFrame(final Storage storage) {
    final MutableWorldState worldState =
        assertSameStateRootInBothModes(
            storage,
            transaction -> {
              final WorldUpdater frame = transaction.updater();
              frame.createAccount(NEW_ADDRESS);
              frame.revert();
            });

    assertThat(worldState.get(NEW_ADDRESS)).isNull();
  }

  @ParameterizedTest
  @EnumSource(Storage.class)
  void dropsTheChangesOfARevertedFrameThatCreatedTheAccountAgain(final Storage storage) {
    final MutableWorldState worldState =
        assertSameStateRootInBothModes(
            storage,
            transaction -> {
              transaction.getAccount(ADDRESS).setBalance(Wei.of(7));
              final WorldUpdater frame = transaction.updater();
              frame.getAccount(ADDRESS).setBalance(Wei.of(99));
              frame.createAccount(ADDRESS);
              frame.revert();
            });

    assertThat(worldState.get(ADDRESS).getBalance()).isEqualTo(Wei.of(7));
    assertThat(worldState.get(ADDRESS).getStorageValue(SLOT_1)).isEqualTo(STORED);
  }

  private static MutableWorldState assertSameStateRootInBothModes(
      final Storage storage, final Consumer<WorldUpdater> transaction) {
    final MutableWorldState stacked = commit(storage, WorldUpdaterMode.STACKED, transaction);
    final MutableWorldState journaled = commit(storage, WorldUpdaterMode.JOURNALED, transaction);
    assertThat(journaled.rootHash()).isEqualTo(stacked.rootHash());
    return journaled;
  }

  private static MutableWorldState commit(
      final Storage storage,
      final WorldUpdaterMode mode,
      final Consumer<WorldUpdater> transaction) {
    final MutableWorldState worldState = storage.create(new EvmConfiguration(32_000L, mode, true));

    // storage without a nonce or code, which a contract creation may still target
    final WorldUpdater genesis = worldState.updater();
    final MutableAccount account = genesis.createAccount(ADDRESS, 0, Wei.ONE);
    account.setStorageValue(SLOT_1, STORED);
    account.setStorageValue(SLOT_2, STORED);
    genesis.commit();
    worldState.persist(null);

    final WorldUpdater block = worldState.updater();
    final WorldUpdater transactionUpdater = block.updater();
    transaction.accept(transactionUpdater);
    transactionUpdater.commit();
    block.commit();
    worldState.persist(null);
    return worldState;
  }
}
