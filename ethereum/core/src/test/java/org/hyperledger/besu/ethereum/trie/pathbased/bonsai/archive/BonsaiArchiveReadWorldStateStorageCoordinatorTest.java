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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE_ARCHIVE;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.trie.MerkleTrie;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode.ArchiveHistoryReader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode.ArchiveNodeHistoryStore;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode.ArchiveNodeKey;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode.ArchiveTrieNodeCodec;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.services.kvstore.SegmentedInMemoryKeyValueStorage;

import java.util.List;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class BonsaiArchiveReadWorldStateStorageCoordinatorTest {

  private SegmentedKeyValueStorage archiveStorage;
  private ArchiveHistoryReader historyReader;
  private BonsaiWorldStateKeyValueStorage keyValueStorage;

  @BeforeEach
  void setUp() {
    archiveStorage =
        new SegmentedInMemoryKeyValueStorage(
            List.of(TRIE_BRANCH_STORAGE, TRIE_BRANCH_STORAGE_ARCHIVE));
    final ArchiveNodeHistoryStore historyStore = new ArchiveNodeHistoryStore(archiveStorage);
    historyReader = new ArchiveHistoryReader(historyStore);
    keyValueStorage =
        new BonsaiWorldStateKeyValueStorage(
            new InMemoryKeyValueStorageProvider(),
            new NoOpMetricsSystem(),
            DataStorageConfiguration.DEFAULT_BONSAI_CONFIG);
  }

  private static Bytes32 hash(final Bytes value) {
    return Bytes32.wrap(Hash.hash(value).getBytes());
  }

  private void writeAccountNode(final Bytes location, final Bytes node, final long block) {
    final ArchiveNodeHistoryStore store = new ArchiveNodeHistoryStore(archiveStorage);
    final SegmentedKeyValueStorageTransaction tx = archiveStorage.startTransaction();
    store.putEncoded(
        tx,
        ArchiveNodeKey.historyKey(ArchiveNodeKey.account(location), block),
        ArchiveNodeHistoryStore.encodeStoredValue(0, ArchiveTrieNodeCodec.encodeFull(node)));
    tx.commit();
  }

  private void writeStorageNode(
      final Hash accountHash, final Bytes location, final Bytes node, final long block) {
    final ArchiveNodeHistoryStore store = new ArchiveNodeHistoryStore(archiveStorage);
    final SegmentedKeyValueStorageTransaction tx = archiveStorage.startTransaction();
    store.putEncoded(
        tx,
        ArchiveNodeKey.historyKey(ArchiveNodeKey.storage(accountHash.getBytes(), location), block),
        ArchiveNodeHistoryStore.encodeStoredValue(0, ArchiveTrieNodeCodec.encodeFull(node)));
    tx.commit();
  }

  @Test
  void isWorldStateAvailableAlwaysReturnsTrue() {
    final BonsaiArchiveReadWorldStateStorageCoordinator coordinator =
        new BonsaiArchiveReadWorldStateStorageCoordinator(keyValueStorage, historyReader, 0L);
    assertThat(coordinator.isWorldStateAvailable(Bytes32.ZERO, Hash.ZERO)).isTrue();
    assertThat(coordinator.isWorldStateAvailable(hash(Bytes.of(1)), Hash.ZERO)).isTrue();
  }

  @Test
  void getAccountStateTrieNodeReturnsArchivedNode() {
    final Bytes location = Bytes.of(0x0a);
    final Bytes node = Bytes.fromHexString("0xdeadbeef");
    writeAccountNode(location, node, 5L);

    final BonsaiArchiveReadWorldStateStorageCoordinator coordinator =
        new BonsaiArchiveReadWorldStateStorageCoordinator(keyValueStorage, historyReader, 5L);
    assertThat(coordinator.getAccountStateTrieNode(location, hash(node))).contains(node);
  }

  @Test
  void getAccountStateTrieNodeReturnsEmptyWhenBlockNotYetArchived() {
    final Bytes location = Bytes.of(0x0a);
    final Bytes node = Bytes.fromHexString("0xdeadbeef");
    writeAccountNode(location, node, 5L);

    // Querying block 4: node was archived at 5, so no entry exists at or before 4
    final BonsaiArchiveReadWorldStateStorageCoordinator coordinator =
        new BonsaiArchiveReadWorldStateStorageCoordinator(keyValueStorage, historyReader, 4L);
    assertThat(coordinator.getAccountStateTrieNode(location, hash(node))).isEmpty();
  }

  @Test
  void getAccountStateTrieNodeReturnsEmptyOnHashMismatch() {
    final Bytes location = Bytes.of(0x0b);
    final Bytes node = Bytes.fromHexString("0xaabb");
    writeAccountNode(location, node, 3L);

    final BonsaiArchiveReadWorldStateStorageCoordinator coordinator =
        new BonsaiArchiveReadWorldStateStorageCoordinator(keyValueStorage, historyReader, 3L);
    assertThat(coordinator.getAccountStateTrieNode(location, hash(Bytes.fromHexString("0xccdd"))))
        .isEmpty();
  }

  @Test
  void getAccountStateTrieNodeReturnsEmptyTrieNodeForEmptyHash() {
    final BonsaiArchiveReadWorldStateStorageCoordinator coordinator =
        new BonsaiArchiveReadWorldStateStorageCoordinator(keyValueStorage, historyReader, 0L);
    assertThat(coordinator.getAccountStateTrieNode(Bytes.EMPTY, MerkleTrie.EMPTY_TRIE_NODE_HASH))
        .contains(MerkleTrie.EMPTY_TRIE_NODE);
  }

  @Test
  void getAccountStorageTrieNodeReturnsArchivedNode() {
    final Hash accountHash =
        Hash.wrap(
            Bytes32.fromHexString(
                "0xaabbccddaabbccddaabbccddaabbccddaabbccddaabbccddaabbccddaabbccdd"));
    final Bytes location = Bytes.of(0x01);
    final Bytes node = Bytes.fromHexString("0xffee");
    writeStorageNode(accountHash, location, node, 7L);

    final BonsaiArchiveReadWorldStateStorageCoordinator coordinator =
        new BonsaiArchiveReadWorldStateStorageCoordinator(keyValueStorage, historyReader, 7L);
    assertThat(coordinator.getAccountStorageTrieNode(accountHash, location, hash(node)))
        .contains(node);
  }

  @Test
  void accountAndStorageTrieNamespacesAreIsolated() {
    final Hash accountHash =
        Hash.wrap(
            Bytes32.fromHexString(
                "0x1234567812345678123456781234567812345678123456781234567812345678"));
    final Bytes location = Bytes.of(0x02);
    final Bytes node = Bytes.fromHexString("0xabcd");
    writeStorageNode(accountHash, location, node, 2L);

    // Account-trie coordinator must not return anything for the same location
    final BonsaiArchiveReadWorldStateStorageCoordinator coordinator =
        new BonsaiArchiveReadWorldStateStorageCoordinator(keyValueStorage, historyReader, 2L);
    assertThat(coordinator.getAccountStateTrieNode(location, hash(node))).isEmpty();
  }
}
