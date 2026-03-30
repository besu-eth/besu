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
package org.hyperledger.besu.ethereum.core;

import static org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator.applyForStrategy;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.trie.MerkleTrie;
import org.hyperledger.besu.ethereum.trie.common.PmtStateTrieAccountValue;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.patricia.StoredMerklePatriciaTrie;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator;
import org.hyperledger.besu.plugin.services.storage.WorldStateKeyValueStorage;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;

public class TrieGenerator {

  public static MerkleTrie<Bytes, Bytes> generateTrie(
      final WorldStateStorageCoordinator worldStateStorageCoordinator, final int nbAccounts) {
    return generateTrie(
        worldStateStorageCoordinator,
        IntStream.range(0, nbAccounts)
            .mapToObj(operand -> Hash.wrap(Bytes32.leftPad(Bytes.of(operand + 1))))
            .collect(Collectors.toList()));
  }

  public static MerkleTrie<Bytes, Bytes> generateTrie(
      final WorldStateStorageCoordinator worldStateStorageCoordinator, final List<Hash> accounts) {
    final MerkleTrie<Bytes, Bytes> accountStateTrie =
        emptyAccountStateTrie(worldStateStorageCoordinator);
    // Add some storage values
    for (int i = 0; i < accounts.size(); i++) {
      final WorldStateKeyValueStorage.Updater updater = worldStateStorageCoordinator.updater();
      final MerkleTrie<Bytes, Bytes> storageTrie =
          emptyStorageTrie(worldStateStorageCoordinator, accounts.get(i));
      writeStorageValue(updater, storageTrie, accounts.get(i), UInt256.ONE, UInt256.valueOf(2L));
      writeStorageValue(
          updater, storageTrie, accounts.get(i), UInt256.valueOf(2L), UInt256.valueOf(4L));
      writeStorageValue(
          updater, storageTrie, accounts.get(i), UInt256.valueOf(3L), UInt256.valueOf(6L));
      int accountIndex = i;
      storageTrie.commit(
          (location, hash, value) -> {
            applyForStrategy(
                updater,
                onBonsai -> {
                  onBonsai.putAccountStorageTrieNode(
                      accounts.get(accountIndex), location, hash, value);
                },
                onForest -> {
                  onForest.putAccountStorageTrieNode(hash, value);
                });
          });
      final Bytes code = Bytes32.leftPad(Bytes.of(i + 10));
      final Hash codeHash = Hash.hash(code);
      final PmtStateTrieAccountValue accountValue =
          new PmtStateTrieAccountValue(
              1L, Wei.of(2L), Hash.wrap(storageTrie.getRootHash()), codeHash);
      accountStateTrie.put(accounts.get(i).getBytes(), RLP.encode(accountValue::writeTo));
      applyForStrategy(
          updater,
          onBonsai -> {
            onBonsai.putAccountInfoState(
                accounts.get(accountIndex), RLP.encode(accountValue::writeTo));
            accountStateTrie.commit(onBonsai::putAccountStateTrieNode);
            onBonsai.putCode(accounts.get(accountIndex), codeHash, code);
          },
          onForest -> {
            accountStateTrie.commit(
                (location, hash, value) -> onForest.putAccountStateTrieNode(hash, value));
            onForest.putCode(code);
          });

      // Persist updates
      updater.commit();
    }
    return accountStateTrie;
  }

  private static void writeStorageValue(
      final WorldStateKeyValueStorage.Updater updater,
      final MerkleTrie<Bytes, Bytes> storageTrie,
      final Hash hash,
      final UInt256 key,
      final UInt256 value) {
    final Hash keyHash = storageKeyHash(key);
    final Bytes encodedValue = encodeStorageValue(value);
    storageTrie.put(keyHash.getBytes(), encodeStorageValue(value));
    if (updater instanceof BonsaiWorldStateKeyValueStorage.Updater) {
      ((BonsaiWorldStateKeyValueStorage.Updater) updater)
          .putStorageValueBySlotHash(hash, keyHash, encodedValue);
    }
  }

  private static Hash storageKeyHash(final UInt256 storageKey) {
    return Hash.hash(storageKey);
  }

  private static Bytes encodeStorageValue(final UInt256 storageValue) {
    return RLP.encode(out -> out.writeBytes(storageValue.toMinimalBytes()));
  }

  public static MerkleTrie<Bytes, Bytes> emptyStorageTrie(
      final WorldStateStorageCoordinator worldStateStorageCoordinator, final Hash accountHash) {
    return new StoredMerklePatriciaTrie<>(
        (location, hash) ->
            worldStateStorageCoordinator.getAccountStorageTrieNode(accountHash, location, hash),
        b -> b,
        b -> b);
  }

  public static MerkleTrie<Bytes, Bytes> emptyAccountStateTrie(
      final WorldStateStorageCoordinator worldStateStorageCoordinator) {
    return new StoredMerklePatriciaTrie<>(
        worldStateStorageCoordinator::getAccountStateTrieNode, b -> b, b -> b);
  }
}
