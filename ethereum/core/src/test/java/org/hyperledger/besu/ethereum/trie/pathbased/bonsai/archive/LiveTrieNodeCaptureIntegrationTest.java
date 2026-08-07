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
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE;
import static org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedWorldStateKeyValueStorage.WORLD_BLOCK_NUMBER_KEY;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.trie.MerkleTrie;
import org.hyperledger.besu.ethereum.trie.common.PmtStateTrieAccountValue;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.BonsaiArchiveTrieNodeStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.BonsaiTrieNodeStrategy;
import org.hyperledger.besu.ethereum.trie.patricia.StoredMerklePatriciaTrie;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.services.kvstore.SegmentedInMemoryKeyValueStorage;

import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class LiveTrieNodeCaptureIntegrationTest {

  private SegmentedKeyValueStorage storage;
  private TrieNodeHistoryStore historyStore;
  private TrieNodeHistoryReader reader;
  private TrieNodeHistoryProgress progress;
  private BonsaiArchiveTrieNodeStrategy strategy;

  // Capture everything (network head effectively infinite) for the account-trie scenario;
  // the trailing-window behaviour is asserted separately in trailsHeadByMaxLayers().
  private long highestSafeBlock = Long.MAX_VALUE;

  @BeforeEach
  void setUp() {
    storage = new SegmentedInMemoryKeyValueStorage();
    historyStore = new TrieNodeHistoryStore(storage);
    reader = new TrieNodeHistoryReader(historyStore);
    progress = new TrieNodeHistoryProgress();
    strategy =
        new BonsaiArchiveTrieNodeStrategy(
            new BonsaiTrieNodeStrategy(), historyStore, progress, () -> highestSafeBlock);
  }

  private void setWorldBlockNumber(final long n) {
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    tx.put(TRIE_BRANCH_STORAGE, WORLD_BLOCK_NUMBER_KEY, Bytes.ofUnsignedLong(n).toArrayUnsafe());
    tx.commit();
  }

  /**
   * Applies {@code address -> value} to a fresh trie built on the current live nodes. All node puts
   * land in a single transaction; {@code flushCaptures} is called before commit — mirroring the
   * real {@code BonsaiWorldStateKeyValueStorage.Updater} lifecycle.
   */
  private Bytes32 importAccountBlock(final Address address, final PmtStateTrieAccountValue value) {
    final MerkleTrie<Bytes, Bytes> trie =
        new StoredMerklePatriciaTrie<>(
            (location, hash) ->
                new BonsaiTrieNodeStrategy().getFlatAccountTrieNode(location, hash, storage),
            b -> b,
            b -> b);
    trie.put(address.addressHash().getBytes(), RLP.encode(value::writeTo));
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    trie.commit(
        (location, nodeHash, nodeValue) ->
            strategy.putFlatAccountTrieNode(storage, tx, location, nodeHash, nodeValue));
    strategy.flushCaptures(storage, tx);
    tx.commit();
    return trie.getRootHash();
  }

  @Test
  void capturesGenesisAndSubsequentBlocksReconstructableViaReader() {
    final Address a = Address.fromHexString("0x1111111111111111111111111111111111111111");
    // Block 0 (genesis): no WORLD_BLOCK_NUMBER_KEY.
    importAccountBlock(
        a, new PmtStateTrieAccountValue(0L, Wei.of(1L), Hash.EMPTY_TRIE_HASH, Hash.EMPTY));
    // Block 1.
    setWorldBlockNumber(0L);
    importAccountBlock(
        a, new PmtStateTrieAccountValue(1L, Wei.of(2L), Hash.EMPTY_TRIE_HASH, Hash.EMPTY));

    // The account-trie root node (location EMPTY) has a history entry at both blocks 0 and 1.
    assertThat(historyStore.getLatestBefore(ArchiveNodeKey.account(Bytes.EMPTY), 0L)).isPresent();
    assertThat(historyStore.getLatestBefore(ArchiveNodeKey.account(Bytes.EMPTY), 1L)).isPresent();
    // Reconstruction of the root at block 0 differs from block 1 (state changed).
    final Optional<Bytes> root0 = reader.nodeAt(ArchiveNodeKey.account(Bytes.EMPTY), 0L);
    final Optional<Bytes> root1 = reader.nodeAt(ArchiveNodeKey.account(Bytes.EMPTY), 1L);
    assertThat(root0).isPresent();
    assertThat(root1).isPresent();
    assertThat(root0).isNotEqualTo(root1);
    // Progress covers [0, 1].
    assertThat(progress.covers(0L)).isTrue();
    assertThat(progress.covers(1L)).isTrue();
  }

  @Test
  void trailsHeadByMaxLayersAndAlwaysCapturesGenesis() {
    // Simulate a 1000-block network head with maxLayers = 512: safe block = 1000 - 512 = 488.
    highestSafeBlock = 488L;
    final Address a = Address.fromHexString("0x2222222222222222222222222222222222222222");

    // Genesis (block 0) is captured even though 0 <= 488 would also pass — assert it directly.
    importAccountBlock(
        a, new PmtStateTrieAccountValue(0L, Wei.of(1L), Hash.EMPTY_TRIE_HASH, Hash.EMPTY));
    assertThat(historyStore.getLatestBefore(ArchiveNodeKey.account(Bytes.EMPTY), 0L)).isPresent();

    // A block at 488 (currentBlock = 488 => WORLD_BLOCK_NUMBER_KEY = 487) is captured.
    setWorldBlockNumber(487L);
    importAccountBlock(
        a, new PmtStateTrieAccountValue(2L, Wei.of(3L), Hash.EMPTY_TRIE_HASH, Hash.EMPTY));
    assertThat(historyStore.getLatestBefore(ArchiveNodeKey.account(Bytes.EMPTY), 488L)).isPresent();

    // A block at 489 (in the reorg window) is NOT captured, but the live node IS written.
    setWorldBlockNumber(488L);
    final Bytes32 root489 =
        importAccountBlock(
            a, new PmtStateTrieAccountValue(3L, Wei.of(4L), Hash.EMPTY_TRIE_HASH, Hash.EMPTY));
    // Block 489 is in the reorg window — not captured; the latest visible entry is still from 488.
    assertThat(historyStore.getLatestBefore(ArchiveNodeKey.account(Bytes.EMPTY), 489L))
        .hasValueSatisfying(entry -> assertThat(entry.block()).isEqualTo(488L));
    assertThat(
            new BonsaiTrieNodeStrategy()
                .getFlatAccountTrieNode(Bytes.EMPTY, Bytes32.wrap(root489), storage))
        .isPresent();
    // Coverage stops at 488.
    assertThat(progress.lastIndexedBlock()).isEqualTo(488L);
  }
}
