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

import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.services.kvstore.SegmentedInMemoryKeyValueStorage;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TrieNodeHistoryStoreTest {

  private SegmentedKeyValueStorage storage;
  private TrieNodeHistoryStore historyStore;

  @BeforeEach
  void setUp() {
    storage = new SegmentedInMemoryKeyValueStorage();
    historyStore = new TrieNodeHistoryStore(storage);
  }

  private void put(final Bytes naturalKey, final long block, final int counter, final Bytes entry) {
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    historyStore.putEncoded(
        tx,
        ArchiveNodeKey.historyKey(naturalKey, block),
        TrieNodeHistoryStore.encodeStoredValue(counter, entry));
    tx.commit();
  }

  @Test
  void putThenGetRoundTripsFullEntryWithCounter() {
    final Bytes naturalKey = Bytes.fromHexString("0x01");
    final Bytes fullEntry = ArchiveTrieNodeCodec.encodeFull(Bytes.fromHexString("0xdeadbeef"));
    put(naturalKey, 100L, 0, fullEntry);

    final TrieNodeHistoryStore.HistoryEntry entry =
        historyStore.getLatestBefore(naturalKey, 100L).orElseThrow();
    assertThat(entry.counter()).isEqualTo(0);
    assertThat(entry.block()).isEqualTo(100L);
    assertThat(entry.codecEntry().isFull()).isTrue();
    assertThat(entry.codecEntry().fullNode()).isEqualTo(Bytes.fromHexString("0xdeadbeef"));
  }

  @Test
  void putThenGetRoundTripsDiffEntryAtMaxCounterValue() {
    final Bytes naturalKey = Bytes.fromHexString("0x02");
    // encodeDiff RLP-decodes its inputs as trie nodes, so the node must be valid RLP (a bare
    // 0xaa byte is not a valid short/branch node list and would throw CorruptedRLPInputException).
    final Bytes node =
        RLP.encode(
            out -> {
              out.startList();
              out.writeBytes(Bytes.fromHexString("0x0102"));
              out.writeBytes(Bytes.fromHexString("0xaa"));
              out.endList();
            });
    final Bytes diffEntry = ArchiveTrieNodeCodec.encodeDiff(node, node);
    put(naturalKey, 5L, 15, diffEntry); // CHECKPOINT_INTERVAL - 1 = 15

    final TrieNodeHistoryStore.HistoryEntry entry =
        historyStore.getLatestBefore(naturalKey, 5L).orElseThrow();
    assertThat(entry.counter()).isEqualTo(15);
  }

  @Test
  void putThenGetRoundTripsTombstoneEntry() {
    final Bytes naturalKey = Bytes.fromHexString("0x03");
    put(naturalKey, 7L, 0, ArchiveTrieNodeCodec.encodeDiff(Bytes.fromHexString("0xaa"), null));

    final TrieNodeHistoryStore.HistoryEntry entry =
        historyStore.getLatestBefore(naturalKey, 7L).orElseThrow();
    assertThat(entry.codecEntry().isDeletion()).isTrue();
  }

  @Test
  void getReturnsEmptyForNeverWrittenKey() {
    assertThat(historyStore.getLatestBefore(Bytes.fromHexString("0x04"), 1L)).isEmpty();
  }

  @Test
  void getLatestBeforeReturnsExactBlockWhenPresent() {
    final Bytes naturalKey = Bytes.fromHexString("0x05");
    put(naturalKey, 100L, 0, ArchiveTrieNodeCodec.encodeFull(Bytes.fromHexString("0xaa")));
    assertThat(historyStore.getLatestBefore(naturalKey, 100L).orElseThrow().block())
        .isEqualTo(100L);
  }

  @Test
  void getLatestBeforeSkipsPastBlocksWithNoEntryToFindEarlierOne() {
    final Bytes naturalKey = Bytes.fromHexString("0x06");
    put(naturalKey, 100L, 0, ArchiveTrieNodeCodec.encodeFull(Bytes.fromHexString("0xaa")));
    // No entry at block 150; getLatestBefore(200) must find block 100, not fail.
    assertThat(historyStore.getLatestBefore(naturalKey, 200L).orElseThrow().block())
        .isEqualTo(100L);
  }

  @Test
  void getLatestBeforeReturnsEmptyWhenNoEntryAtOrBeforeRequestedBlock() {
    final Bytes naturalKey = Bytes.fromHexString("0x07");
    put(naturalKey, 100L, 0, ArchiveTrieNodeCodec.encodeFull(Bytes.fromHexString("0xaa")));
    assertThat(historyStore.getLatestBefore(naturalKey, 50L)).isEmpty();
  }

  @Test
  void getLatestBeforeDoesNotCrossIntoADifferentNaturalKeysEntries() {
    // Two natural keys whose byte prefixes could tempt an incorrect seekForPrev impl to
    // cross-match if the key encoding didn't fully disambiguate them.
    final Bytes keyA = Bytes.fromHexString("0x0100");
    final Bytes keyB = Bytes.fromHexString("0x0101");
    put(keyA, 500L, 0, ArchiveTrieNodeCodec.encodeFull(Bytes.fromHexString("0xaa")));
    // keyB has no entry at all before block 1000 — must not see keyA's block 500 entry.
    assertThat(historyStore.getLatestBefore(keyB, 1000L)).isEmpty();
  }
}
