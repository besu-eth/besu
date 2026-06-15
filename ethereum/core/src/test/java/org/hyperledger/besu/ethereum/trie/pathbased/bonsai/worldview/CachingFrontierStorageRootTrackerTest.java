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
 * Tests for {@link CachingFrontierStorageRootTracker} verifying correctness of incremental storage
 * trie updates through the public BonsaiWorldState API. The tracker is invoked indirectly via
 * {@code worldState.frontierRootHash()}; a mismatched storage root surfaces as a mismatched
 * world-state root against the full-recalculation ground truth.
 */
class CachingFrontierStorageRootTrackerTest {

  private static final Address ACCOUNT_A =
      Address.fromHexString("0x1000000000000000000000000000000000000001");
  private static final Address ACCOUNT_B =
      Address.fromHexString("0x1000000000000000000000000000000000000002");

  @Test
  void revertOfPriorTxWriteProducesCorrectStorageRoot() {
    // Regression: an earlier implementation skipped any slot whose accumulator entry reported
    // isUnchanged, which is true when a later tx restores the original value. The cached trie
    // would keep the earlier tx's write and produce a wrong root. The fix tracks lastAppliedValue
    // and re-applies the original value, restoring the trie to its base shape.
    final BonsaiWorldState worldState = createWorldStateWithAccounts();
    final UInt256 slot = UInt256.ONE; // ACCOUNT_A.slot[1] starts at 1 from the setup.

    final WorldUpdater tx1 = worldState.updater();
    tx1.getAccount(ACCOUNT_A).setStorageValue(slot, UInt256.valueOf(99));
    tx1.commit();
    tx1.markTransactionBoundary();
    worldState.frontierRootHash(); // Forces tx1's write into the cached trie.

    final WorldUpdater tx2 = worldState.updater();
    tx2.getAccount(ACCOUNT_A).setStorageValue(slot, UInt256.ONE);
    tx2.commit();
    tx2.markTransactionBoundary();

    assertThat(worldState.frontierRootHash()).isEqualTo(fullRecalculatedRoot(worldState));
  }

  @Test
  void accountDeletionMidBlockRebuildsCacheFromEmptyRoot() {
    // After an account is deleted, the next storage update on that address must build a fresh
    // trie from EMPTY_TRIE_HASH rather than reuse the cached trie keyed at the old base root.
    final BonsaiWorldState worldState = createWorldStateWithAccounts();

    final WorldUpdater tx1 = worldState.updater();
    tx1.getAccount(ACCOUNT_A).setStorageValue(UInt256.valueOf(5), UInt256.valueOf(50));
    tx1.commit();
    tx1.markTransactionBoundary();
    worldState.frontierRootHash();

    final WorldUpdater tx2 = worldState.updater();
    tx2.deleteAccount(ACCOUNT_A);
    tx2.commit();
    tx2.markTransactionBoundary();
    worldState.frontierRootHash();

    final WorldUpdater tx3 = worldState.updater();
    final MutableAccount recreated = tx3.createAccount(ACCOUNT_A);
    recreated.setBalance(Wei.of(7));
    recreated.setStorageValue(UInt256.valueOf(5), UInt256.valueOf(99));
    tx3.commit();
    tx3.markTransactionBoundary();

    assertThat(worldState.frontierRootHash()).isEqualTo(fullRecalculatedRoot(worldState));
  }

  @Test
  @SuppressWarnings("unchecked")
  void recoversAfterMerkleTrieExceptionByRebuildingFresh() {
    final BonsaiWorldState worldState = createWorldStateWithAccounts();
    final BonsaiWorldStateUpdateAccumulator acc =
        (BonsaiWorldStateUpdateAccumulator) worldState.getAccumulator();

    final WorldUpdater updater = worldState.updater();
    updater.getAccount(ACCOUNT_A).setStorageValue(UInt256.valueOf(11), UInt256.valueOf(111));
    updater.commit();
    updater.markTransactionBoundary();

    final MerkleTrie<Bytes, Bytes> brokenTrie = mock(MerkleTrie.class);
    doThrow(new MerkleTrieException("simulated node missing"))
        .when(brokenTrie)
        .put(any(Bytes.class), any(Bytes.class));

    final CachingFrontierStorageRootTracker.StorageTrieFactory factory =
        mock(CachingFrontierStorageRootTracker.StorageTrieFactory.class);
    when(factory.create(any(), any()))
        .thenReturn(brokenTrie)
        .thenAnswer(
            inv ->
                new StoredMerklePatriciaTrie<>(
                    (location, hash) ->
                        worldState
                            .getWorldStateStorage()
                            .getAccountStorageTrieNode(inv.<Hash>getArgument(0), location, hash),
                    Bytes32.wrap(inv.<Hash>getArgument(1).getBytes()),
                    Function.identity(),
                    Function.identity()));

    final CachingFrontierStorageRootTracker tracker =
        new CachingFrontierStorageRootTracker(acc, factory);

    assertThatThrownBy(() -> tracker.update(ACCOUNT_A, acc.getStorageToUpdate().get(ACCOUNT_A)))
        .isInstanceOf(MerkleTrieException.class);

    // Drop the cache entry that holds the broken trie so the next call rebuilds.
    tracker.reset();
    tracker.update(ACCOUNT_A, acc.getStorageToUpdate().get(ACCOUNT_A));

    assertThat(worldState.frontierRootHash()).isEqualTo(fullRecalculatedRoot(worldState));
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

  private static Hash fullRecalculatedRoot(final BonsaiWorldState worldState) {
    final BonsaiWorldStateUpdateAccumulator accumulatorCopy =
        (BonsaiWorldStateUpdateAccumulator) worldState.getAccumulator().copy();
    return worldState.calculateRootHash(Optional.empty(), accumulatorCopy);
  }
}
