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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE;

import org.hyperledger.besu.crypto.Hash;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.services.kvstore.SegmentedInMemoryKeyValueStorage;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ArchiveProofNodeLoaderTest {

  private SegmentedKeyValueStorage storage;
  private TrieNodeHistoryStore historyStore;
  private TrieNodeHistoryReader reader;

  @BeforeEach
  void setUp() {
    storage = new SegmentedInMemoryKeyValueStorage();
    historyStore = new TrieNodeHistoryStore(storage);
    reader = new TrieNodeHistoryReader(historyStore);
  }

  @Test
  void hashFirstFastPathReturnsLiveNodeWhenHashMatchesWithoutTouchingHistory() {
    final Bytes location = Bytes.fromHexString("0x01");
    final Bytes liveNode = Bytes.fromHexString("0xaa");
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    tx.put(TRIE_BRANCH_STORAGE, location.toArrayUnsafe(), liveNode.toArrayUnsafe());
    tx.commit();

    final ArchiveProofNodeLoader loader = new ArchiveProofNodeLoader(reader, storage, 100L);
    final Bytes32 expectedHash = Hash.keccak256(liveNode);
    assertThat(loader.accountNodeLoader().getNode(location, expectedHash)).contains(liveNode);
  }

  @Test
  void hashMismatchFallsThroughToHistoryReconstruction() {
    final Bytes location = Bytes.fromHexString("0x02");
    final Bytes liveNode =
        Bytes.fromHexString("0xbb"); // current live node (changed since block 50)
    final Bytes historicalNode = Bytes.fromHexString("0xaa"); // node as of block 50
    final SegmentedKeyValueStorageTransaction liveTx = storage.startTransaction();
    liveTx.put(TRIE_BRANCH_STORAGE, location.toArrayUnsafe(), liveNode.toArrayUnsafe());
    liveTx.commit();
    final SegmentedKeyValueStorageTransaction histTx = storage.startTransaction();
    historyStore.putEncoded(
        histTx,
        ArchiveNodeKey.historyKey(ArchiveNodeKey.account(location), 50L),
        TrieNodeHistoryStore.encodeStoredValue(0, ArchiveTrieNodeCodec.encodeFull(historicalNode)));
    histTx.commit();

    final ArchiveProofNodeLoader loader = new ArchiveProofNodeLoader(reader, storage, 50L);
    assertThat(loader.accountNodeLoader().getNode(location, Hash.keccak256(historicalNode)))
        .contains(historicalNode);
  }

  @Test
  void hashMismatchAfterReconstructionThrowsIllegalStateException() {
    final Bytes location = Bytes.fromHexString("0x03");
    final Bytes historicalNode = Bytes.fromHexString("0xaa");
    final SegmentedKeyValueStorageTransaction histTx = storage.startTransaction();
    historyStore.putEncoded(
        histTx,
        ArchiveNodeKey.historyKey(ArchiveNodeKey.account(location), 50L),
        TrieNodeHistoryStore.encodeStoredValue(0, ArchiveTrieNodeCodec.encodeFull(historicalNode)));
    histTx.commit();

    final ArchiveProofNodeLoader loader = new ArchiveProofNodeLoader(reader, storage, 50L);
    final Bytes32 wrongHash = Bytes32.random();
    assertThatThrownBy(() -> loader.accountNodeLoader().getNode(location, wrongHash))
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  void absentNodeAtTargetBlockReturnsEmptyNotException() {
    final ArchiveProofNodeLoader loader = new ArchiveProofNodeLoader(reader, storage, 50L);
    assertThat(loader.accountNodeLoader().getNode(Bytes.fromHexString("0x04"), Bytes32.random()))
        .isEmpty();
  }

  @Test
  void storageNodeLoaderKeysByAccountHashConcatLocation() {
    final Bytes32 accountHash = Bytes32.random();
    final Bytes location = Bytes.fromHexString("0x05");
    final Bytes historicalNode = Bytes.fromHexString("0xcc");
    final SegmentedKeyValueStorageTransaction histTx = storage.startTransaction();
    historyStore.putEncoded(
        histTx,
        ArchiveNodeKey.historyKey(ArchiveNodeKey.storage(accountHash, location), 50L),
        TrieNodeHistoryStore.encodeStoredValue(0, ArchiveTrieNodeCodec.encodeFull(historicalNode)));
    histTx.commit();

    final ArchiveProofNodeLoader loader = new ArchiveProofNodeLoader(reader, storage, 50L);
    assertThat(
            loader.storageNodeLoader(accountHash).getNode(location, Hash.keccak256(historicalNode)))
        .contains(historicalNode);
  }
}
