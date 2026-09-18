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
package org.hyperledger.besu.ethereum.eth.manager.snap;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.WorldStateKeyValueStorage;

import java.util.NavigableMap;
import java.util.Optional;
import java.util.function.Predicate;

import kotlin.Pair;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

class BonsaiSnapWorldStateStorage implements SnapWorldStateStorage {

  private final BonsaiWorldStateKeyValueStorage storage;

  BonsaiSnapWorldStateStorage(final BonsaiWorldStateKeyValueStorage storage) {
    this.storage = storage;
  }

  @Override
  public NavigableMap<Bytes32, Bytes> streamFlatAccounts(
      final Bytes startKeyHash, final Predicate<Pair<Bytes32, Bytes>> takeWhile) {
    return storage.streamFlatAccounts(startKeyHash, takeWhile);
  }

  @Override
  public NavigableMap<Bytes32, Bytes> streamFlatAccounts(
      final Bytes startKeyHash, final Bytes32 endKeyHash, final long max) {
    return storage.streamFlatAccounts(startKeyHash, endKeyHash, max);
  }

  @Override
  public NavigableMap<Bytes32, Bytes> streamFlatStorages(
      final Hash accountHash,
      final Bytes startKeyHash,
      final Predicate<Pair<Bytes32, Bytes>> takeWhile) {
    return storage.streamFlatStorages(accountHash, startKeyHash, takeWhile);
  }

  @Override
  public NavigableMap<Bytes32, Bytes> streamFlatStorages(
      final Hash accountHash, final Bytes startKeyHash, final Bytes32 endKeyHash, final long max) {
    return storage.streamFlatStorages(accountHash, startKeyHash, endKeyHash, max);
  }

  @Override
  public Optional<Bytes> getTrieNodeUnsafe(final Bytes location) {
    return storage.getTrieNodeUnsafe(location);
  }

  @Override
  public Optional<Bytes> getAccount(final Hash accountHash) {
    return storage.getAccount(accountHash);
  }

  @Override
  public Hash getAccountStorageRoot(final Hash accountHash) {
    return storage
        .getTrieNodeUnsafe(Bytes.concatenate(accountHash.getBytes(), Bytes.EMPTY))
        .map(Hash::hash)
        .orElse(Hash.EMPTY_TRIE_HASH);
  }

  @Override
  public WorldStateKeyValueStorage asWorldStateKeyValueStorage() {
    return storage;
  }
}
