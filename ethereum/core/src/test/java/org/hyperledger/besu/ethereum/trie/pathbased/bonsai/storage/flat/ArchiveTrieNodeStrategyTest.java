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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE_ARCHIVE;
import static org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedWorldStateKeyValueStorage.WORLD_BLOCK_NUMBER_KEY;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode.ArchiveCoverageTracker;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode.ArchiveNodeHistoryStore;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode.ArchiveNodeKey;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode.ArchiveTrieNodeStrategy;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.services.kvstore.SegmentedInMemoryKeyValueStorage;

import java.util.List;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ArchiveTrieNodeStrategyTest {

  private SegmentedKeyValueStorage storage;
  private ArchiveNodeHistoryStore historyStore;
  private ArchiveCoverageTracker coverageTracker;

  @BeforeEach
  void setUp() {
    storage =
        new SegmentedInMemoryKeyValueStorage(
            List.of(TRIE_BRANCH_STORAGE, TRIE_BRANCH_STORAGE_ARCHIVE));
    historyStore = new ArchiveNodeHistoryStore(storage);
    coverageTracker = new ArchiveCoverageTracker(storage);
  }

  private ArchiveTrieNodeStrategy strategyWithGate(final boolean gateOpen) {
    return new ArchiveTrieNodeStrategy(
        new BonsaiTrieNodeStrategy(), historyStore, coverageTracker, () -> gateOpen);
  }

  private static Bytes32 hash(final Bytes value) {
    return Bytes32.wrap(Hash.hash(value).getBytes());
  }

  private void setStoredBlockNumber(final long block) {
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    tx.put(
        TRIE_BRANCH_STORAGE, WORLD_BLOCK_NUMBER_KEY, Bytes.ofUnsignedLong(block).toArrayUnsafe());
    tx.commit();
  }

  @Test
  void archivesFullNodeWhenGateOpen() {
    // Gate wide open (initial sync): block 0 (no prior stored block) must be archived.
    final ArchiveTrieNodeStrategy strategy = strategyWithGate(true);
    final Bytes location = Bytes.of(0x0e);
    final Bytes node = Bytes.fromHexString("0xdeadbeef");

    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    strategy.putFlatAccountTrieNode(storage, tx, location, hash(node), node);
    tx.commit();

    assertThat(historyStore.getLatestBefore(ArchiveNodeKey.account(location), 0L)).contains(node);
    assertThat(coverageTracker.hasArchiveBlock(0L)).isTrue();
  }

  @Test
  void doesNotArchiveWhenGateClosedAndNotGenesis() {
    // Gate closed (at-head sync): node writes go live but must NOT be archived.
    // Store block 5 as the last committed block, making the current block 6.
    setStoredBlockNumber(5L);

    final ArchiveTrieNodeStrategy strategy = strategyWithGate(false);
    final Bytes location = Bytes.of(0x0e);
    final Bytes node = Bytes.fromHexString("0xcafe");

    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    strategy.putFlatAccountTrieNode(storage, tx, location, hash(node), node);
    tx.commit();

    assertThat(storage.get(TRIE_BRANCH_STORAGE, location.toArrayUnsafe())).isPresent();
    assertThat(historyStore.getLatestBefore(ArchiveNodeKey.account(location), 6L)).isEmpty();
    assertThat(coverageTracker.hasArchiveBlock(6L)).isFalse();
  }
}
