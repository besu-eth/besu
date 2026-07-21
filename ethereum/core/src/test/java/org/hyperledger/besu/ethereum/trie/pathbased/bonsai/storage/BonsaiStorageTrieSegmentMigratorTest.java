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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.STORAGE_TRIE_BRANCH_STORAGE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE;
import static org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiTrieStorageKeys.STORAGE_TRIE_CF_MIGRATED_KEY;
import static org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedWorldStateKeyValueStorage.BONSAI_WORLD_STATE_SEGMENTS;
import static org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedWorldStateKeyValueStorage.WORLD_ROOT_HASH_KEY;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;

class BonsaiStorageTrieSegmentMigratorTest {

  @Test
  void migratesLegacyStorageTrieNodesIntoDedicatedColumnFamily() {
    final InMemoryKeyValueStorageProvider provider = new InMemoryKeyValueStorageProvider();
    final SegmentedKeyValueStorage storage =
        provider.getStorageBySegmentIdentifiers(BONSAI_WORLD_STATE_SEGMENTS);
    final Hash accountHash = Hash.wrap(Bytes32.repeat((byte) 0xab));
    final byte[] storageKey =
        BonsaiTrieStorageKeys.storageTrieKey(accountHash, Bytes.fromHexString("0x01"));
    final byte[] accountKey = Bytes.fromHexString("0x02").toArrayUnsafe();
    final byte[] nodeValue = Bytes.fromHexString("0x1234").toArrayUnsafe();

    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    tx.put(TRIE_BRANCH_STORAGE, storageKey, nodeValue);
    tx.put(TRIE_BRANCH_STORAGE, accountKey, nodeValue);
    tx.put(TRIE_BRANCH_STORAGE, WORLD_ROOT_HASH_KEY, Bytes32.ZERO.toArrayUnsafe());
    tx.commit();

    BonsaiStorageTrieSegmentMigrator.migrateIfNeeded(storage);

    assertThat(storage.get(TRIE_BRANCH_STORAGE, storageKey)).isEmpty();
    assertThat(storage.get(STORAGE_TRIE_BRANCH_STORAGE, storageKey)).contains(nodeValue);
    assertThat(storage.get(TRIE_BRANCH_STORAGE, accountKey)).contains(nodeValue);
    assertThat(storage.get(TRIE_BRANCH_STORAGE, STORAGE_TRIE_CF_MIGRATED_KEY)).isPresent();
  }

  @Test
  void storageWritesUseDedicatedColumnFamilyAfterMigration() {
    final BonsaiWorldStateKeyValueStorage worldStateStorage =
        new BonsaiWorldStateKeyValueStorage(
            new InMemoryKeyValueStorageProvider(),
            new NoOpMetricsSystem(),
            DataStorageConfiguration.DEFAULT_BONSAI_CONFIG);
    final Hash accountHash = Hash.wrap(Bytes32.repeat((byte) 0xcd));
    final Bytes location = Bytes.fromHexString("0x03");
    final Bytes node = Bytes.fromHexString("0x5678");
    final Bytes32 nodeHash = Bytes32.wrap(Hash.hash(node).getBytes());

    worldStateStorage
        .updater()
        .putAccountStorageTrieNode(accountHash, location, nodeHash, node)
        .commit();

    final byte[] storageKey = BonsaiTrieStorageKeys.storageTrieKey(accountHash, location);
    final SegmentedKeyValueStorage composedStorage = worldStateStorage.getComposedWorldStateStorage();
    assertThat(composedStorage.get(STORAGE_TRIE_BRANCH_STORAGE, storageKey)).contains(node.toArrayUnsafe());
    assertThat(composedStorage.get(TRIE_BRANCH_STORAGE, storageKey)).isEmpty();
    assertThat(worldStateStorage.getAccountStorageTrieNode(accountHash, location, nodeHash))
        .contains(node);
  }
}
