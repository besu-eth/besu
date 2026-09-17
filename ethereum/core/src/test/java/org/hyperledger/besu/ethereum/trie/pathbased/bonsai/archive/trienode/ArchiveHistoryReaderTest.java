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

import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.services.kvstore.SegmentedInMemoryKeyValueStorage;

import java.util.List;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;

class ArchiveHistoryReaderTest {
  @Test
  void returnsLatestFullNodeAtOrBeforeTarget() {
    final SegmentedKeyValueStorage storage =
        new SegmentedInMemoryKeyValueStorage(
            List.of(KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE_ARCHIVE));
    final ArchiveNodeHistoryStore store = new ArchiveNodeHistoryStore(storage);
    final Bytes nk = ArchiveNodeKey.account(Bytes.of(0x0e));
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    store.put(tx, nk, 5, Bytes.of(0xAA));
    tx.commit();
    final ArchiveHistoryReader reader = new ArchiveHistoryReader(store);
    assertThat(reader.nodeAt(nk, 9)).contains(Bytes.of(0xAA));
    assertThat(reader.nodeAt(nk, 4)).isEmpty();
  }

  @Test
  void rejectsNegativeBlock() {
    final ArchiveHistoryReader reader =
        new ArchiveHistoryReader(
            new ArchiveNodeHistoryStore(
                new SegmentedInMemoryKeyValueStorage(
                    List.of(KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE_ARCHIVE))));
    assertThatThrownBy(() -> reader.nodeAt(Bytes.of(0x01, 0x0e), -1))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
