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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.services.kvstore.SegmentedInMemoryKeyValueStorage;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TrieNodeHistoryReaderTest {

  private static Bytes shortNode(final Bytes path, final Bytes value) {
    return RLP.encode(
        out -> {
          out.startList();
          out.writeBytes(path);
          out.writeBytes(value);
          out.endList();
        });
  }

  private SegmentedKeyValueStorage storage;
  private TrieNodeHistoryStore historyStore;
  private TrieNodeHistoryReader reader;
  private final Bytes naturalKey = Bytes.fromHexString("0x01");

  @BeforeEach
  void setUp() {
    storage = new SegmentedInMemoryKeyValueStorage();
    historyStore = new TrieNodeHistoryStore(storage);
    reader = new TrieNodeHistoryReader(historyStore);
  }

  private void write(final long block, final int counter, final Bytes codecEntry) {
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    historyStore.putEncoded(
        tx,
        ArchiveNodeKey.historyKey(naturalKey, block),
        TrieNodeHistoryStore.encodeStoredValue(counter, codecEntry));
    tx.commit();
  }

  @Test
  void nodeAtReturnsFullNodeDirectlyWithNoWalk() {
    final Bytes node = shortNode(Bytes.fromHexString("0x01"), Bytes.fromHexString("0xaa"));
    write(100L, 0, ArchiveTrieNodeCodec.encodeFull(node));
    assertThat(reader.nodeAt(naturalKey, 100L)).contains(node);
  }

  @Test
  void nodeAtReconstructsThroughSingleDiff() {
    final Bytes v1 = shortNode(Bytes.fromHexString("0x01"), Bytes.fromHexString("0xaa"));
    final Bytes v2 = shortNode(Bytes.fromHexString("0x01"), Bytes.fromHexString("0xbb"));
    write(100L, 0, ArchiveTrieNodeCodec.encodeFull(v1));
    write(101L, 1, ArchiveTrieNodeCodec.encodeDiff(v1, v2));
    assertThat(reader.nodeAt(naturalKey, 101L)).contains(v2);
  }

  @Test
  void nodeAtReconstructsThroughMaximumLengthDiffChain() {
    Bytes current = shortNode(Bytes.fromHexString("0x01"), Bytes.fromHexString("0x00"));
    write(100L, 0, ArchiveTrieNodeCodec.encodeFull(current));
    for (int i = 1; i <= TrieNodeHistoryReader.CHECKPOINT_INTERVAL - 1; i++) {
      final Bytes prev = current;
      current = shortNode(Bytes.fromHexString("0x01"), Bytes.ofUnsignedInt(i));
      write(100L + i, i, ArchiveTrieNodeCodec.encodeDiff(prev, current));
    }
    assertThat(reader.nodeAt(naturalKey, 100L + TrieNodeHistoryReader.CHECKPOINT_INTERVAL - 1))
        .contains(current);
  }

  @Test
  void nodeAtReturnsEmptyForTombstoneAtTargetBlock() {
    final Bytes node = shortNode(Bytes.fromHexString("0x01"), Bytes.fromHexString("0xaa"));
    write(100L, 0, ArchiveTrieNodeCodec.encodeFull(node));
    write(101L, 0, ArchiveTrieNodeCodec.encodeDiff(node, null));
    assertThat(reader.nodeAt(naturalKey, 101L)).isEmpty();
  }

  @Test
  void nodeAtReturnsEmptyForNeverWrittenKey() {
    assertThat(reader.nodeAt(Bytes.fromHexString("0x99"), 100L)).isEmpty();
  }

  @Test
  void nodeAtReturnsCorrectHistoricalVersionNotLatest() {
    final Bytes v1 = shortNode(Bytes.fromHexString("0x01"), Bytes.fromHexString("0xaa"));
    final Bytes v2 = shortNode(Bytes.fromHexString("0x01"), Bytes.fromHexString("0xbb"));
    write(100L, 0, ArchiveTrieNodeCodec.encodeFull(v1));
    write(105L, 1, ArchiveTrieNodeCodec.encodeDiff(v1, v2));
    // Query at block 102: v1 is still the correct version (v2 wasn't written until 105).
    assertThat(reader.nodeAt(naturalKey, 102L)).contains(v1);
  }

  @Test
  void nodeAtReconstructsBranchNodeThroughDiffChain() {
    final Bytes[] children = new Bytes[16];
    java.util.Arrays.fill(children, Bytes.EMPTY);
    final Bytes v1 =
        RLP.encode(
            out -> {
              out.startList();
              for (int i = 0; i < children.length; i++) {
                out.writeNull();
              }
              out.writeBytes(Bytes.EMPTY);
              out.endList();
            });
    final Bytes[] mutated = children.clone();
    mutated[2] = Bytes.fromHexString("0xa0" + "11".repeat(32));
    final Bytes v2 =
        RLP.encode(
            out -> {
              out.startList();
              for (final Bytes c : mutated) {
                if (c.isEmpty()) out.writeNull();
                else out.writeRaw(c);
              }
              out.writeBytes(Bytes.EMPTY);
              out.endList();
            });
    write(100L, 0, ArchiveTrieNodeCodec.encodeFull(v1));
    write(101L, 1, ArchiveTrieNodeCodec.encodeDiff(v1, v2));
    assertThat(reader.nodeAt(naturalKey, 101L)).contains(v2);
  }
}
