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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.trie.MerkleTrie;
import org.hyperledger.besu.ethereum.trie.MerkleTrieException;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.BonsaiWorldStateProvider;
import org.hyperledger.besu.ethereum.trie.patricia.StoredMerklePatriciaTrie;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;

import java.util.Optional;
import java.util.function.Function;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link FrontierRootHashTracker} verifying correctness of incremental frontier root hash
 * computation through the public BonsaiWorldState API.
 */
class FrontierRootHashTrackerTest {

  private static final Address ACCOUNT_A =
      Address.fromHexString("0x1000000000000000000000000000000000000001");
  private static final Address ACCOUNT_B =
      Address.fromHexString("0x1000000000000000000000000000000000000002");

  @Test
  void incrementalResultMatchesFullRecalculationForSingleTransaction() {
    final BonsaiWorldState worldState = createWorldStateWithAccounts();

    final WorldUpdater updater = worldState.updater();
    updater.getAccount(ACCOUNT_A).setBalance(Wei.of(100));
    updater.commit();
    updater.markTransactionBoundary();

    assertThat(worldState.frontierRootHash()).isEqualTo(fullRecalculatedRoot(worldState));
  }

  @Test
  void incrementalResultMatchesFullRecalculationAcrossMultipleTransactions() {
    final BonsaiWorldState worldState = createWorldStateWithAccounts();

    for (int i = 0; i < 10; i++) {
      final WorldUpdater updater = worldState.updater();
      updater.getAccount(ACCOUNT_A).setBalance(Wei.of(100 + i));
      updater.getAccount(ACCOUNT_B).setStorageValue(UInt256.valueOf(i), UInt256.valueOf(i));
      updater.commit();
      updater.markTransactionBoundary();

      assertThat(worldState.frontierRootHash())
          .as("Mismatch at tx %d", i)
          .isEqualTo(fullRecalculatedRoot(worldState));
    }
  }

  @Test
  void resetClearsTrieCacheAndNextCallRebuildsFromScratch() {
    final BonsaiWorldState worldState = createWorldStateWithAccounts();

    final WorldUpdater updater = worldState.updater();
    updater.getAccount(ACCOUNT_A).setBalance(Wei.of(42));
    updater.commit();
    updater.markTransactionBoundary();

    final Hash beforeReset = worldState.frontierRootHash();

    // Persist resets the tracker; new block starts fresh
    worldState.persist(null);

    final WorldUpdater updater2 = worldState.updater();
    updater2.getAccount(ACCOUNT_B).setBalance(Wei.of(99));
    updater2.commit();
    updater2.markTransactionBoundary();

    final Hash afterReset = worldState.frontierRootHash();
    assertThat(afterReset).isEqualTo(fullRecalculatedRoot(worldState));
    assertThat(afterReset).isNotEqualTo(beforeReset);
  }

  @Test
  void returnsBaseRootHashWhenNothingIsDirty() {
    final BonsaiWorldState worldState = createWorldStateWithAccounts();
    assertThat(worldState.frontierRootHash()).isEqualTo(worldState.rootHash());
  }

  private static BonsaiWorldState createWorldStateWithAccounts() {
    final Blockchain blockchain = mock(Blockchain.class);
    final BonsaiWorldStateProvider archive =
        InMemoryKeyValueStorageProvider.createBonsaiInMemoryWorldStateArchive(blockchain);
    final BonsaiWorldState worldState = (BonsaiWorldState) archive.getWorldState();

    final WorldUpdater setup = worldState.updater();
    final MutableAccount a = setup.createAccount(ACCOUNT_A);
    a.setBalance(Wei.of(1));
    a.setStorageValue(UInt256.ONE, UInt256.ONE);
    final MutableAccount b = setup.createAccount(ACCOUNT_B);
    b.setBalance(Wei.of(2));
    b.setStorageValue(UInt256.ONE, UInt256.valueOf(2));
    setup.commit();
    setup.markTransactionBoundary();
    worldState.persist(null);

    return worldState;
  }

  @Test
  @SuppressWarnings("unchecked")
  void recoversAfterMerkleTrieExceptionByRebuildingFromScratch() {
    final BonsaiWorldState worldState = createWorldStateWithAccounts();
    final BonsaiWorldStateUpdateAccumulator acc =
        (BonsaiWorldStateUpdateAccumulator) worldState.getAccumulator();

    final WorldUpdater updater = worldState.updater();
    updater.getAccount(ACCOUNT_A).setBalance(Wei.of(100));
    updater.commit();
    updater.markTransactionBoundary();

    final Hash expectedRoot = fullRecalculatedRoot(worldState);

    final MerkleTrie<Bytes, Bytes> brokenTrie = mock(MerkleTrie.class);
    doThrow(new MerkleTrieException("simulated node missing"))
        .when(brokenTrie)
        .put(any(Bytes.class), any(Bytes.class));

    final FrontierRootHashTracker.AccountTrieFactory factory =
        mock(FrontierRootHashTracker.AccountTrieFactory.class);
    when(factory.create(any()))
        .thenReturn(brokenTrie)
        .thenAnswer(
            inv ->
                new StoredMerklePatriciaTrie<>(
                    (location, hash) ->
                        worldState.getWorldStateStorage().getAccountStateTrieNode(location, hash),
                    inv.<Bytes32>getArgument(0),
                    Function.identity(),
                    Function.identity()));

    final FrontierRootHashTracker tracker =
        new FrontierRootHashTracker(acc, factory, (address, storageUpdates) -> {});

    assertThatThrownBy(() -> tracker.frontierRootHash(worldState.rootHash()))
        .isInstanceOf(MerkleTrieException.class);

    final Hash result = tracker.frontierRootHash(worldState.rootHash());
    assertThat(result).isEqualTo(expectedRoot);
  }

  private static Hash fullRecalculatedRoot(final BonsaiWorldState worldState) {
    final BonsaiWorldStateUpdateAccumulator accumulatorCopy =
        (BonsaiWorldStateUpdateAccumulator) worldState.getAccumulator().copy();
    return worldState.calculateRootHash(Optional.empty(), accumulatorCopy);
  }
}
