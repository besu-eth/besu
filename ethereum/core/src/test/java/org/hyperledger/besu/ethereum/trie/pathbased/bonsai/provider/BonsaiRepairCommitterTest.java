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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.provider;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.ethereum.mainnet.staterootcommitter.DefaultStateRootCommitter;
import org.hyperledger.besu.ethereum.trie.MerkleTrie;
import org.hyperledger.besu.ethereum.trie.MerkleTrieException;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.account.BonsaiAccount;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.BonsaiValue;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.BonsaiWorldStateUpdateAccumulator;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.preload.StorageConsumingMap;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;

class BonsaiRepairCommitterTest {
  @Test
  @SuppressWarnings("unchecked")
  void privateTrialPropagatesOriginalStorageTrieFault() {
    final BonsaiWorldState state = mock(BonsaiWorldState.class);
    final BonsaiWorldStateUpdateAccumulator updates = mock(BonsaiWorldStateUpdateAccumulator.class);
    final BonsaiAccount account = mock(BonsaiAccount.class);
    final MerkleTrie<Bytes, Bytes> accountTrie = mock(MerkleTrie.class);
    final StorageConsumingMap<StorageSlotKey, BonsaiValue<UInt256>> storage =
        new StorageConsumingMap<>(Address.ZERO, new ConcurrentHashMap<>(), (address, slot) -> {});
    final MerkleTrieException missing =
        new MerkleTrieException(
            "missing storage node", Optional.of(Address.ZERO), Bytes32.ZERO, Bytes.of(1));
    when(updates.getStorageToClear()).thenReturn(Set.of());
    when(updates.getCodeToUpdate()).thenReturn(Map.of());
    when(updates.getStorageToUpdate()).thenReturn(Map.of(Address.ZERO, storage));
    when(updates.getAccountsToUpdate())
        .thenReturn(Map.of(Address.ZERO, new BonsaiValue<>(account, null)));
    when(account.getStorageRoot()).thenReturn(Hash.EMPTY_TRIE_HASH);
    when(state.createAccountStateTrie()).thenReturn(accountTrie);
    doThrow(missing).when(state).createStorageTrie(any(), any());

    assertThatThrownBy(() -> BonsaiWorldStateRepair.computeTrialStateRoot(state, null, updates))
        .isSameAs(missing);
  }

  @Test
  @SuppressWarnings("unchecked")
  void directStorageWorkHasDrainedWhenAccountComputationFails() {
    final BonsaiWorldState state = mock(BonsaiWorldState.class);
    final BonsaiWorldStateUpdateAccumulator updates = mock(BonsaiWorldStateUpdateAccumulator.class);
    final BonsaiAccount account = mock(BonsaiAccount.class);
    final MerkleTrie<Bytes, Bytes> accountTrie = mock(MerkleTrie.class);
    final MerkleTrie<Bytes, Bytes> storageTrie = mock(MerkleTrie.class);
    final StorageConsumingMap<StorageSlotKey, BonsaiValue<UInt256>> storage =
        new StorageConsumingMap<>(Address.ZERO, new ConcurrentHashMap<>(), (address, slot) -> {});
    when(updates.getStorageToClear()).thenReturn(Set.of());
    when(updates.getCodeToUpdate()).thenReturn(Map.of());
    when(updates.getStorageToUpdate()).thenReturn(Map.of(Address.ZERO, storage));
    when(updates.getAccountsToUpdate())
        .thenReturn(Map.of(Address.ZERO, new BonsaiValue<>(account, account)));
    when(account.getStorageRoot()).thenReturn(Hash.EMPTY_TRIE_HASH);
    when(state.createAccountStateTrie()).thenReturn(accountTrie);
    when(state.createStorageTrie(any(), any())).thenReturn(storageTrie);
    when(storageTrie.getRootHash()).thenReturn(Bytes32.ZERO);
    doThrow(new MerkleTrieException("missing account node", Bytes32.ZERO, Bytes.EMPTY))
        .when(accountTrie)
        .putDeferred(any(), any());
    final Thread caller = Thread.currentThread();
    final AtomicInteger active = new AtomicInteger();
    final AtomicInteger completed = new AtomicInteger();
    final DefaultStateRootCommitter committer =
        new DefaultStateRootCommitter(
            (worldState, address) -> address.addressHash(),
            command -> {
              assertThat(Thread.currentThread()).isSameAs(caller);
              active.incrementAndGet();
              try {
                command.run();
              } finally {
                active.decrementAndGet();
                completed.incrementAndGet();
              }
            });

    assertThatThrownBy(() -> committer.compute(state, null, updates))
        .isInstanceOf(MerkleTrieException.class);
    assertThat(completed).hasValue(1);
    assertThat(active).hasValue(0);
  }
}
