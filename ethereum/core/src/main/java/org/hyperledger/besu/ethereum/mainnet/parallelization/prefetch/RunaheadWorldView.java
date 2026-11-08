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
package org.hyperledger.besu.ethereum.mainnet.parallelization.prefetch;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.code.BonsaiCodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldView;
import org.hyperledger.besu.evm.Code;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;

import java.util.Map;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;

/**
 * The parent world state as a transaction run ahead of its execution sees it. A storage slot that
 * is not in memory reads as empty, and a background read loads it, so the run does not wait for the
 * disk and the execution after it finds the slot in memory. Accounts and code are read as usual, so
 * that calls run the code they would run.
 */
final class RunaheadWorldView implements BonsaiWorldView {

  private final BonsaiWorldState worldState;
  private final RunaheadPrefetcher.Loads loads;

  RunaheadWorldView(final BonsaiWorldState worldState, final RunaheadPrefetcher.Loads loads) {
    this.worldState = worldState;
    this.loads = loads;
  }

  @Override
  public Account get(final Address address) {
    return worldState.get(address);
  }

  @Override
  public Optional<Code> getCode(final Address address, final Hash codeHash) {
    return worldState.getCode(address, codeHash);
  }

  @Override
  public UInt256 getStorageValue(final Address address, final UInt256 key) {
    return getStorageValueByStorageSlotKey(address, new StorageSlotKey(key)).orElse(UInt256.ZERO);
  }

  @Override
  public Optional<UInt256> getStorageValueByStorageSlotKey(
      final Address address, final StorageSlotKey storageSlotKey) {
    final Optional<Optional<Bytes>> inMemory =
        worldState
            .getWorldStateStorage()
            .peekStorageValueByStorageSlotKey(address.addressHash(), storageSlotKey);
    if (inMemory.isPresent()) {
      return inMemory.get().map(UInt256::fromBytes);
    }
    loads.load(address, storageSlotKey);
    return Optional.empty();
  }

  @Override
  public UInt256 getPriorStorageValue(final Address address, final UInt256 key) {
    return getStorageValue(address, key);
  }

  @Override
  public Map<Bytes32, Bytes> getAllAccountStorage(final Address address, final Hash rootHash) {
    // only the execution needs a whole storage, the run ahead only fetches what it reads
    return Map.of();
  }

  @Override
  public boolean isModifyingHeadWorldState() {
    return false;
  }

  @Override
  public BonsaiWorldStateKeyValueStorage getWorldStateStorage() {
    return worldState.getWorldStateStorage();
  }

  @Override
  public WorldUpdater updater() {
    throw new UnsupportedOperationException("a run ahead does not update its parent world state");
  }

  @Override
  public BonsaiCodeCache codeCache() {
    return worldState.codeCache();
  }
}
