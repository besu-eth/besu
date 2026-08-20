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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE_ARCHIVE;

import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.services.kvstore.SegmentedInMemoryKeyValueStorage;

import java.util.List;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ArchiveNodeHistoryStoreTest {

  private SegmentedKeyValueStorage storage;
  private ArchiveNodeHistoryStore store;

  @BeforeEach
  void setUp() {
    storage = new SegmentedInMemoryKeyValueStorage(List.of(TRIE_BRANCH_STORAGE_ARCHIVE));
    store = new ArchiveNodeHistoryStore(storage);
  }

  private void putEncoded(
      final Bytes naturalKey, final long block, final int counter, final Bytes codecEntry) {
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    store.putEncoded(
        tx,
        ArchiveNodeKey.historyKey(naturalKey, block),
        ArchiveNodeHistoryStore.encodeStoredValue(counter, codecEntry));
    tx.commit();
  }

  @Test
  void returnsDecodedHistoryEntry() {
    final Bytes nk = ArchiveNodeKey.account(Bytes.of(0x0e));
    final Bytes fullNode = Bytes.fromHexString("0xdeadbeef");
    final Bytes codecEntry = ArchiveTrieNodeCodec.encodeFull(fullNode);
    putEncoded(nk, 5L, 0, codecEntry);

    final var entry = store.getLatestBefore(nk, 7L);
    assertThat(entry).isPresent();
    assertThat(entry.get().counter()).isEqualTo(0);
    assertThat(entry.get().block()).isEqualTo(5L);
    assertThat(entry.get().codecEntry().isFull()).isTrue();
    assertThat(entry.get().codecEntry().fullNode()).isEqualTo(fullNode);
  }

  @Test
  void returnsEmptyWhenNothingBeforeTarget() {
    final Bytes nk = ArchiveNodeKey.account(Bytes.of(0x0e));
    putEncoded(nk, 10L, 0, ArchiveTrieNodeCodec.encodeFull(Bytes.of(0xAA)));
    assertThat(store.getLatestBefore(nk, 5L)).isEmpty();
  }

  @Test
  void prefixNaturalKeyDoesNotMatchLongerKey() {
    final Bytes shallow = ArchiveNodeKey.account(Bytes.of(0x0e));
    final Bytes deep = ArchiveNodeKey.account(Bytes.of(0x0e, 0x00));
    putEncoded(shallow, 5L, 0, ArchiveTrieNodeCodec.encodeFull(Bytes.of(0xAA)));
    assertThat(store.getLatestBefore(deep, 9L)).isEmpty();
  }

  @Test
  void foreignKeyShorterThanBlockSuffixDoesNotMatch() {
    final Bytes nk = ArchiveNodeKey.account(Bytes.EMPTY); // 1 byte: [0x00]
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    tx.put(TRIE_BRANCH_STORAGE_ARCHIVE, Bytes.of(0x00).toArrayUnsafe(), new byte[] {1});
    tx.commit();

    assertThat(store.getLatestBefore(nk, 100L)).isEmpty();
  }

  @Test
  void encodeStoredValueRejectsCounterOutOfByteRange() {
    final Bytes codecEntry = ArchiveTrieNodeCodec.encodeFull(Bytes.of(0xAA));
    assertThatThrownBy(() -> ArchiveNodeHistoryStore.encodeStoredValue(256, codecEntry))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> ArchiveNodeHistoryStore.encodeStoredValue(-1, codecEntry))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
