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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.WorldStateConfig.createStatefulConfigWithTrie;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.AccountValue;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessListAccountLookup;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.PartialBlockAccessView;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.trie.common.PatriciaTrieAccountValue;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.account.BonsaiAccount;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.code.BonsaiCodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.trielog.BonsaiTrieLogFactory;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.trielog.NoOpTrieLogManager;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.trielog.TrieLogLayer;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.preload.BonsaiCachedMerkleTrieLoader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.preload.NoOpBonsaiCachedMerkleTrieLoader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.cache.NoOpBonsaiWorldStateCacheManager;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;

/** Tests for {@link BonsaiWorldStateUpdateAccumulator}. */
class BonsaiWorldStateUpdateAccumulatorTest {

  private static final Address ACCOUNT =
      Address.fromHexString("0x1000000000000000000000000000000000000001");
  private static final Address ABSENT =
      Address.fromHexString("0x1000000000000000000000000000000000000002");
  private static final Address CREATED =
      Address.fromHexString("0x1000000000000000000000000000000000000003");
  private static final StorageSlotKey SLOT = new StorageSlotKey(UInt256.valueOf(7));
  private static final UInt256 V0 = UInt256.valueOf(100);
  private static final UInt256 V1 = UInt256.valueOf(200);
  private static final UInt256 V2 = UInt256.valueOf(300);

  @Test
  void importPartialView_sameSlotTwoTransactions_keepsBlockStartPriorAndFinalUpdated() {
    final BonsaiCachedMerkleTrieLoader loader = spy(new NoOpBonsaiCachedMerkleTrieLoader());
    try (BonsaiWorldState worldState = newEmptyWorldState(loader)) {
      final BonsaiWorldStateUpdateAccumulator accumulator = persistAccountWithSlot(worldState);
      final Hash storageRoot = storageRoot(worldState);
      clearInvocations(loader);

      final PartialBlockAccessView tx0 = partialView(0, V0, V1);
      final PartialBlockAccessView tx1 = partialView(1, V1, V2);

      accumulator.importStateChangesFromPartialView(tx0);
      accumulator.importStateChangesFromPartialView(tx1);

      verify(loader).preLoadStorageSlot(any(), any(), any(), any());
      verify(loader).preLoadStorageSlot(any(), eq(ACCOUNT), eq(storageRoot), eq(SLOT));
      verify(loader).preLoadAccount(any(), any(), eq(ACCOUNT));

      final BonsaiValue<UInt256> merged = accumulator.getStorageToUpdate().get(ACCOUNT).get(SLOT);
      assertThat(merged.getPrior()).isEqualTo(V0);
      assertThat(merged.getUpdated()).isEqualTo(V2);

      final TrieLogLayer trieLog =
          new BonsaiTrieLogFactory()
              .create(
                  accumulator,
                  new BlockHeaderTestFixture().number(1).stateRoot(Hash.EMPTY).buildHeader());
      final BonsaiValue<UInt256> trieLogSlot = trieLog.getStorageChanges(ACCOUNT).get(SLOT);
      assertThat(trieLogSlot.getPrior()).isEqualTo(V0);
      assertThat(trieLogSlot.getUpdated()).isEqualTo(V2);
    }
  }

  @Test
  void importAndCommitAccountChanges_leavesTheStateOfImportingEachTransactionInTurn() {
    final List<PartialBlockAccessView> transactions =
        List.of(
            partialView(
                1,
                view -> {
                  view.getOrCreateAccountBuilder(ACCOUNT)
                      .withPostBalance(Wei.of(5))
                      .withNonceChange(1L)
                      .addStorageChange(SLOT, V0, V1);
                  view.getOrCreateAccountBuilder(CREATED).withPostBalance(Wei.of(10));
                }),
            partialView(
                2,
                view -> {
                  view.getOrCreateAccountBuilder(ACCOUNT)
                      .withPostBalance(Wei.of(7))
                      .addStorageChange(SLOT, V1, V2);
                  view.getOrCreateAccountBuilder(CREATED).withNonceChange(3L);
                }),
            partialView(
                3, view -> view.getOrCreateAccountBuilder(ABSENT).withPostBalance(Wei.of(4))));

    assertThat(trieLogOfImportingTogether(transactions).dump())
        .isEqualTo(trieLogOfImportingOneByOne(transactions).dump());
  }

  @Test
  void importAndCommitAccountChanges_keepsTheStorageOfAnAccountDeletedAndCreatedAgainCleared() {
    // the account has neither nonce nor code, so a zero balance empties it and deletes its slot
    final List<PartialBlockAccessView> transactions =
        List.of(
            partialView(
                1, view -> view.getOrCreateAccountBuilder(ACCOUNT).withPostBalance(Wei.ZERO)),
            partialView(
                2, view -> view.getOrCreateAccountBuilder(ACCOUNT).withPostBalance(Wei.of(3))));

    final TrieLogLayer trieLog = trieLogOfImportingTogether(transactions);

    assertThat(trieLog.dump()).isEqualTo(trieLogOfImportingOneByOne(transactions).dump());
    assertThat(trieLog.getStorageChanges(ACCOUNT).get(SLOT).getUpdated()).isNull();
    assertThat(trieLog.getAccountChanges().get(ACCOUNT).getUpdated().getBalance())
        .isEqualTo(Wei.of(3));
  }

  @Test
  void importAndCommitAccountChanges_mergesTheChangesBeforeAndAfterOneThatEmptiesTheAccount() {
    // the slot is written before and after the account is emptied, and the account that ends
    // non-empty is emptied by its last change
    final List<PartialBlockAccessView> transactions =
        List.of(
            partialView(
                1,
                view ->
                    view.getOrCreateAccountBuilder(ACCOUNT)
                        .withPostBalance(Wei.of(5))
                        .addStorageChange(SLOT, V0, V1)),
            partialView(
                2, view -> view.getOrCreateAccountBuilder(ACCOUNT).withPostBalance(Wei.of(6))),
            partialView(
                3, view -> view.getOrCreateAccountBuilder(ACCOUNT).withPostBalance(Wei.ZERO)),
            partialView(
                4,
                view ->
                    view.getOrCreateAccountBuilder(ACCOUNT)
                        .withPostBalance(Wei.of(2))
                        .addStorageChange(SLOT, UInt256.ZERO, V2)),
            partialView(
                5,
                view -> {
                  view.getOrCreateAccountBuilder(ACCOUNT).withPostBalance(Wei.of(1));
                  view.getOrCreateAccountBuilder(CREATED).withPostBalance(Wei.of(10));
                }),
            partialView(
                6, view -> view.getOrCreateAccountBuilder(CREATED).withPostBalance(Wei.ZERO)));

    assertThat(trieLogOfImportingTogether(transactions).dump())
        .isEqualTo(trieLogOfImportingOneByOne(transactions).dump());
  }

  @Test
  void importAndCommitAccountChanges_readsTheAccountsThroughTheLookup() {
    // the lookup holds an account that the world state does not, as if read for the block before
    final BlockAccessListAccountLookup lookup =
        BlockAccessListAccountLookup.of(
            new BlockAccessList(
                List.of(
                    new BlockAccessList.AccountChanges(
                        ABSENT, List.of(), List.of(), List.of(), List.of(), List.of()))));
    lookup.putParentAccount(
        ABSENT,
        Optional.of(
            RLP.encode(
                new PatriciaTrieAccountValue(3L, Wei.of(42), Hash.EMPTY_TRIE_HASH, Hash.EMPTY)
                    ::writeTo)));

    try (BonsaiWorldState worldState = newEmptyWorldState()) {
      final BonsaiWorldStateUpdateAccumulator accumulator = persistAccountWithSlot(worldState);
      accumulator.importAndCommitAccountChanges(
          byAccount(
              partialView(
                      1, view -> view.getOrCreateAccountBuilder(ABSENT).withPostBalance(Wei.of(5)))
                  .accountChanges()),
          true,
          lookup);

      final BonsaiValue<?> account = accumulator.getAccountsToUpdate().get(ABSENT);
      assertThat(((BonsaiAccount) account.getPrior()).getBalance()).isEqualTo(Wei.of(42));
      assertThat(((BonsaiAccount) account.getUpdated()).getBalance()).isEqualTo(Wei.of(5));
      assertThat(((BonsaiAccount) account.getUpdated()).getNonce()).isEqualTo(3L);
    }
  }

  /**
   * Imports and commits the transactions one at a time, as the block's thread does. Trie logs are
   * compared by their dump: their accounts compare by identity.
   */
  private static TrieLogLayer trieLogOfImportingOneByOne(
      final List<PartialBlockAccessView> transactions) {
    try (BonsaiWorldState worldState = newEmptyWorldState()) {
      final BonsaiWorldStateUpdateAccumulator accumulator = persistAccountWithSlot(worldState);
      for (final PartialBlockAccessView transaction : transactions) {
        accumulator.importStateChangesFromPartialView(transaction, true);
        accumulator.commit();
        accumulator.markTransactionBoundary();
      }
      return trieLog(accumulator);
    }
  }

  private static TrieLogLayer trieLogOfImportingTogether(
      final List<PartialBlockAccessView> transactions) {
    try (BonsaiWorldState worldState = newEmptyWorldState()) {
      final BonsaiWorldStateUpdateAccumulator accumulator = persistAccountWithSlot(worldState);
      accumulator.importAndCommitAccountChanges(
          byAccount(transactions.stream().flatMap(t -> t.accountChanges().stream()).toList()),
          true,
          BlockAccessListAccountLookup.of(new BlockAccessList(List.of())));
      return trieLog(accumulator);
    }
  }

  /** The changes grouped by account, each account's in the order given. */
  private static Map<Address, List<PartialBlockAccessView.AccountChanges>> byAccount(
      final List<PartialBlockAccessView.AccountChanges> changes) {
    return changes.stream()
        .collect(
            Collectors.groupingBy(
                PartialBlockAccessView.AccountChanges::getAddress,
                LinkedHashMap::new,
                Collectors.toList()));
  }

  private static TrieLogLayer trieLog(final BonsaiWorldStateUpdateAccumulator accumulator) {
    return new BonsaiTrieLogFactory()
        .create(
            accumulator,
            new BlockHeaderTestFixture().number(1).stateRoot(Hash.EMPTY).buildHeader());
  }

  private static PartialBlockAccessView partialView(
      final long txIndex,
      final Consumer<PartialBlockAccessView.PartialBlockAccessViewBuilder> changes) {
    final PartialBlockAccessView.PartialBlockAccessViewBuilder builder =
        new PartialBlockAccessView.PartialBlockAccessViewBuilder().withTxIndex(txIndex);
    changes.accept(builder);
    return builder.build();
  }

  @Test
  void rollForward_keepsTheStorageRootOfThePatriciaAccountValue() {
    final Hash storageRoot = Hash.hash(Bytes.of(1, 2, 3));
    final AccountValue accountValue =
        new PatriciaTrieAccountValue(1L, Wei.ONE, storageRoot, Hash.EMPTY);

    assertThat(rollForwardStorageRoot(accountValue)).isEqualTo(storageRoot);
  }

  @Test
  void rollForward_keepsTheStorageRootOfAPluginAccountValue() {
    // a trie log from a plugin factory may hold another account value implementation
    final Hash storageRoot = Hash.hash(Bytes.of(4, 5, 6));
    final AccountValue accountValue = mock(AccountValue.class);
    when(accountValue.getNonce()).thenReturn(1L);
    when(accountValue.getBalance()).thenReturn(Wei.ONE);
    when(accountValue.getStorageRoot()).thenReturn(storageRoot);
    when(accountValue.getCodeHash()).thenReturn(Hash.EMPTY);

    assertThat(rollForwardStorageRoot(accountValue)).isEqualTo(storageRoot);
  }

  private static Hash rollForwardStorageRoot(final AccountValue accountValue) {
    try (BonsaiWorldState worldState = newEmptyWorldState()) {
      final BonsaiWorldStateUpdateAccumulator accumulator =
          (BonsaiWorldStateUpdateAccumulator) worldState.updater();
      final TrieLogLayer trieLog = new TrieLogLayer();
      trieLog.addAccountChange(ACCOUNT, null, accountValue);

      accumulator.rollForward(trieLog);

      final BonsaiAccount account = accumulator.getAccountsToUpdate().get(ACCOUNT).getUpdated();
      return account.getStorageRoot();
    }
  }

  @Test
  void reads_doNotPreloadTriePaths() {
    final BonsaiCachedMerkleTrieLoader loader = spy(new NoOpBonsaiCachedMerkleTrieLoader());
    try (BonsaiWorldState worldState = newEmptyWorldState(loader)) {
      final BonsaiWorldStateUpdateAccumulator accumulator = persistAccountWithSlot(worldState);
      clearInvocations(loader);

      assertThat(accumulator.get(ACCOUNT).getBalance()).isEqualTo(Wei.ONE);
      assertThat(accumulator.getStorageValue(ACCOUNT, SLOT.getSlotKey().orElseThrow()))
          .isEqualTo(V0);
      assertThat(accumulator.get(ABSENT)).isNull();
      accumulator.commit();

      verify(loader, never()).preLoadAccount(any(), any(), any());
      verify(loader, never()).preLoadStorageSlot(any(), any(), any(), any());
    }
  }

  @Test
  void slotWrittenBackInTheSameTransaction_preloadsNothing() {
    final BonsaiCachedMerkleTrieLoader loader = spy(new NoOpBonsaiCachedMerkleTrieLoader());
    try (BonsaiWorldState worldState = newEmptyWorldState(loader)) {
      final BonsaiWorldStateUpdateAccumulator accumulator = persistAccountWithSlot(worldState);
      clearInvocations(loader);

      final MutableAccount account = accumulator.getAccount(ACCOUNT);
      account.setStorageValue(SLOT.getSlotKey().orElseThrow(), V1);
      account.setStorageValue(SLOT.getSlotKey().orElseThrow(), V0);
      accumulator.commit();

      verify(loader, never()).preLoadAccount(any(), any(), any());
      verify(loader, never()).preLoadStorageSlot(any(), any(), any(), any());
    }
  }

  @Test
  void slotWriteAfterClearingStorage_preloadsFromTheEmptyRoot() {
    final BonsaiCachedMerkleTrieLoader loader = spy(new NoOpBonsaiCachedMerkleTrieLoader());
    try (BonsaiWorldState worldState = newEmptyWorldState(loader)) {
      final BonsaiWorldStateUpdateAccumulator accumulator = persistAccountWithSlot(worldState);
      clearInvocations(loader);

      final MutableAccount account = accumulator.getAccount(ACCOUNT);
      account.clearStorage();
      account.setStorageValue(SLOT.getSlotKey().orElseThrow(), V1);
      accumulator.commit();

      verify(loader).preLoadStorageSlot(any(), eq(ACCOUNT), eq(Hash.EMPTY_TRIE_HASH), eq(SLOT));
    }
  }

  @Test
  void slotWrites_preloadTheSlotAndItsAccountOnce() {
    final BonsaiCachedMerkleTrieLoader loader = spy(new NoOpBonsaiCachedMerkleTrieLoader());
    try (BonsaiWorldState worldState = newEmptyWorldState(loader)) {
      final BonsaiWorldStateUpdateAccumulator accumulator = persistAccountWithSlot(worldState);
      clearInvocations(loader);

      accumulator.getAccount(ACCOUNT).setStorageValue(SLOT.getSlotKey().orElseThrow(), V1);
      accumulator.commit();
      accumulator.getAccount(ACCOUNT).setStorageValue(SLOT.getSlotKey().orElseThrow(), V2);
      accumulator.commit();

      verify(loader).preLoadStorageSlot(any(), eq(ACCOUNT), eq(storageRoot(worldState)), eq(SLOT));
      verify(loader).preLoadAccount(any(), any(), eq(ACCOUNT));
    }
  }

  @Test
  void balanceChange_preloadsTheAccountOnly() {
    final BonsaiCachedMerkleTrieLoader loader = spy(new NoOpBonsaiCachedMerkleTrieLoader());
    try (BonsaiWorldState worldState = newEmptyWorldState(loader)) {
      final BonsaiWorldStateUpdateAccumulator accumulator = persistAccountWithSlot(worldState);
      clearInvocations(loader);

      accumulator.getAccount(ACCOUNT).setBalance(Wei.of(2));
      accumulator.commit();

      verify(loader).preLoadAccount(any(), any(), eq(ACCOUNT));
      verify(loader, never()).preLoadStorageSlot(any(), any(), any(), any());
    }
  }

  @Test
  void slotReadThenWritten_preloadsTheSlotAndItsAccountOnce() {
    final BonsaiCachedMerkleTrieLoader loader = spy(new NoOpBonsaiCachedMerkleTrieLoader());
    try (BonsaiWorldState worldState = newEmptyWorldState(loader)) {
      final BonsaiWorldStateUpdateAccumulator accumulator = persistAccountWithSlot(worldState);
      final Hash storageRoot = storageRoot(worldState);
      clearInvocations(loader);

      final MutableAccount account = accumulator.getAccount(ACCOUNT);
      assertThat(account.getStorageValue(SLOT.getSlotKey().orElseThrow())).isEqualTo(V0);
      account.setStorageValue(SLOT.getSlotKey().orElseThrow(), V1);
      accumulator.commit();

      verify(loader).preLoadStorageSlot(any(), any(), any(), any());
      verify(loader).preLoadStorageSlot(any(), eq(ACCOUNT), eq(storageRoot), eq(SLOT));
      verify(loader).preLoadAccount(any(), any(), eq(ACCOUNT));
    }
  }

  @Test
  void slotReadThenWrittenWithItsValue_preloadsNothing() {
    final BonsaiCachedMerkleTrieLoader loader = spy(new NoOpBonsaiCachedMerkleTrieLoader());
    try (BonsaiWorldState worldState = newEmptyWorldState(loader)) {
      final BonsaiWorldStateUpdateAccumulator accumulator = persistAccountWithSlot(worldState);
      clearInvocations(loader);

      final MutableAccount account = accumulator.getAccount(ACCOUNT);
      assertThat(account.getStorageValue(SLOT.getSlotKey().orElseThrow())).isEqualTo(V0);
      account.setStorageValue(SLOT.getSlotKey().orElseThrow(), V0);
      accumulator.commit();

      verify(loader, never()).preLoadAccount(any(), any(), any());
      verify(loader, never()).preLoadStorageSlot(any(), any(), any(), any());
    }
  }

  @Test
  void slotOfANewAccount_preloadsFromTheEmptyRoot() {
    final BonsaiCachedMerkleTrieLoader loader = spy(new NoOpBonsaiCachedMerkleTrieLoader());
    try (BonsaiWorldState worldState = newEmptyWorldState(loader)) {
      final BonsaiWorldStateUpdateAccumulator accumulator = persistAccountWithSlot(worldState);
      clearInvocations(loader);

      accumulator
          .createAccount(CREATED, 0L, Wei.ONE)
          .setStorageValue(SLOT.getSlotKey().orElseThrow(), V1);
      accumulator.commit();

      verify(loader).preLoadStorageSlot(any(), any(), any(), any());
      verify(loader).preLoadStorageSlot(any(), eq(CREATED), eq(Hash.EMPTY_TRIE_HASH), eq(SLOT));
      verify(loader).preLoadAccount(any(), any(), eq(CREATED));
    }
  }

  @Test
  void slotReadThenAccountDeleted_preloadsOnlyFromTheEmptyRoot() {
    final BonsaiCachedMerkleTrieLoader loader = spy(new NoOpBonsaiCachedMerkleTrieLoader());
    try (BonsaiWorldState worldState = newEmptyWorldState(loader)) {
      final BonsaiWorldStateUpdateAccumulator accumulator = persistAccountWithSlot(worldState);
      clearInvocations(loader);

      assertThat(accumulator.getAccount(ACCOUNT).getStorageValue(SLOT.getSlotKey().orElseThrow()))
          .isEqualTo(V0);
      accumulator.deleteAccount(ACCOUNT);
      accumulator.commit();

      verify(loader).preLoadStorageSlot(any(), any(), any(), any());
      verify(loader).preLoadStorageSlot(any(), eq(ACCOUNT), eq(Hash.EMPTY_TRIE_HASH), eq(SLOT));
      verify(loader).preLoadAccount(any(), any(), eq(ACCOUNT));
    }
  }

  @Test
  void rollForward_slotChangedTwice_preloadsTheSlotAndItsAccountOnce() {
    final BonsaiCachedMerkleTrieLoader loader = spy(new NoOpBonsaiCachedMerkleTrieLoader());
    try (BonsaiWorldState worldState = newEmptyWorldState(loader)) {
      final BonsaiWorldStateUpdateAccumulator accumulator = persistAccountWithSlot(worldState);
      clearInvocations(loader);

      accumulator.rollForward(new TrieLogLayer().addStorageChange(ACCOUNT, SLOT, V0, V1));
      accumulator.rollForward(new TrieLogLayer().addStorageChange(ACCOUNT, SLOT, V1, V2));

      verify(loader).preLoadStorageSlot(any(), eq(ACCOUNT), any(), eq(SLOT));
      verify(loader).preLoadAccount(any(), any(), eq(ACCOUNT));
    }
  }

  @Test
  void importStateChangesFromSource_slotChangedTwice_preloadsTheSlotAndItsAccountOnce() {
    final BonsaiCachedMerkleTrieLoader loader = spy(new NoOpBonsaiCachedMerkleTrieLoader());
    try (BonsaiWorldState worldState = newEmptyWorldState(loader)) {
      final BonsaiWorldStateUpdateAccumulator accumulator = persistAccountWithSlot(worldState);
      final Hash storageRoot = storageRoot(worldState);
      clearInvocations(loader);

      accumulator.importStateChangesFromSource(transactionWritingSlot(worldState, V1));
      accumulator.importStateChangesFromSource(transactionWritingSlot(worldState, V2));

      verify(loader).preLoadStorageSlot(any(), any(), any(), any());
      verify(loader).preLoadStorageSlot(any(), eq(ACCOUNT), eq(storageRoot), eq(SLOT));
      verify(loader).preLoadAccount(any(), any(), eq(ACCOUNT));
    }
  }

  @Test
  void importStateChangesFromSource_clearedStorage_preloadsOnlyFromTheEmptyRoot() {
    final BonsaiCachedMerkleTrieLoader loader = spy(new NoOpBonsaiCachedMerkleTrieLoader());
    try (BonsaiWorldState worldState = newEmptyWorldState(loader)) {
      final BonsaiWorldStateUpdateAccumulator accumulator = persistAccountWithSlot(worldState);
      clearInvocations(loader);

      try (BonsaiWorldState transactionState = newWorldStateOnTheSameStorage(worldState)) {
        final BonsaiWorldStateUpdateAccumulator transaction =
            (BonsaiWorldStateUpdateAccumulator) transactionState.updater();
        transaction.deleteAccount(ACCOUNT);
        transaction.commit();

        accumulator.importStateChangesFromSource(transaction);
      }

      verify(loader).preLoadStorageSlot(any(), any(), any(), any());
      verify(loader).preLoadStorageSlot(any(), eq(ACCOUNT), eq(Hash.EMPTY_TRIE_HASH), eq(SLOT));
      verify(loader).preLoadAccount(any(), any(), eq(ACCOUNT));
    }
  }

  /**
   * Runs a transaction that writes the slot on a world state of its own, as parallel transactions
   * do, and returns its accumulator.
   */
  private static BonsaiWorldStateUpdateAccumulator transactionWritingSlot(
      final BonsaiWorldState worldState, final UInt256 value) {
    try (BonsaiWorldState transactionState = newWorldStateOnTheSameStorage(worldState)) {
      final BonsaiWorldStateUpdateAccumulator transaction =
          (BonsaiWorldStateUpdateAccumulator) transactionState.updater();
      transaction.getAccount(ACCOUNT).setStorageValue(SLOT.getSlotKey().orElseThrow(), value);
      transaction.commit();
      return transaction;
    }
  }

  private static Hash storageRoot(final BonsaiWorldState worldState) {
    return ((BonsaiAccount) worldState.get(ACCOUNT)).getStorageRoot();
  }

  private static BonsaiWorldStateUpdateAccumulator persistAccountWithSlot(
      final BonsaiWorldState worldState) {
    final BonsaiWorldStateUpdateAccumulator accumulator =
        (BonsaiWorldStateUpdateAccumulator) worldState.updater();
    accumulator.createAccount(ACCOUNT, 0L, Wei.ONE);
    accumulator.getAccount(ACCOUNT).setStorageValue(SLOT.getSlotKey().orElseThrow(), V0);
    accumulator.commit();
    worldState.persist(null);
    return accumulator;
  }

  private static PartialBlockAccessView partialView(
      final long txIndex, final UInt256 prior, final UInt256 updated) {
    final PartialBlockAccessView.PartialBlockAccessViewBuilder builder =
        new PartialBlockAccessView.PartialBlockAccessViewBuilder().withTxIndex(txIndex);
    builder.getOrCreateAccountBuilder(ACCOUNT).addStorageChange(SLOT, prior, updated);
    return builder.build();
  }

  private static BonsaiWorldState newEmptyWorldState() {
    return newEmptyWorldState(new NoOpBonsaiCachedMerkleTrieLoader());
  }

  private static BonsaiWorldState newEmptyWorldState(final BonsaiCachedMerkleTrieLoader loader) {
    return newWorldState(
        new BonsaiWorldStateKeyValueStorage(
            new InMemoryKeyValueStorageProvider(),
            new NoOpMetricsSystem(),
            DataStorageConfiguration.DEFAULT_BONSAI_CONFIG),
        loader);
  }

  /** A world state without preloads on the storage of another, as a parallel transaction runs. */
  private static BonsaiWorldState newWorldStateOnTheSameStorage(final BonsaiWorldState worldState) {
    return newWorldState(worldState.getWorldStateStorage(), new NoOpBonsaiCachedMerkleTrieLoader());
  }

  private static BonsaiWorldState newWorldState(
      final BonsaiWorldStateKeyValueStorage storage, final BonsaiCachedMerkleTrieLoader loader) {
    return new BonsaiWorldState(
        storage,
        loader,
        new NoOpBonsaiWorldStateCacheManager(
            storage, EvmConfiguration.DEFAULT, new BonsaiCodeCache()),
        new NoOpTrieLogManager(),
        EvmConfiguration.DEFAULT,
        createStatefulConfigWithTrie(),
        new BonsaiCodeCache());
  }
}
