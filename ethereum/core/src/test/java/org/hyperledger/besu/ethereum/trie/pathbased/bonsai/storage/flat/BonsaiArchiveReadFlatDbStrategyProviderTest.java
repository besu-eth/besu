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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_STORAGE_STORAGE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.CODE_STORAGE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE_ARCHIVE;
import static org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedWorldStateKeyValueStorage.WORLD_BLOCK_NUMBER_KEY;
import static org.hyperledger.besu.ethereum.trie.pathbased.common.storage.flat.FlatDbStrategyProvider.FLAT_DB_MODE;

import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.FlatDbMode;
import org.hyperledger.besu.ethereum.worldstate.ImmutableDataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.ImmutablePathBasedExtraStorageConfiguration;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;

import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;

public class BonsaiArchiveReadFlatDbStrategyProviderTest {

  private static final DataStorageConfiguration CONFIG =
      ImmutableDataStorageConfiguration.builder()
          .dataStorageFormat(DataStorageFormat.X_BONSAI_ARCHIVE)
          .pathBasedExtraStorageConfiguration(
              ImmutablePathBasedExtraStorageConfiguration.builder().build())
          .build();

  @Test
  void alwaysReturnsBonsaiArchiveFlatDbStrategy() {
    final SegmentedKeyValueStorage storage =
        new InMemoryKeyValueStorageProvider()
            .getStorageBySegmentIdentifiers(
                List.of(
                    TRIE_BRANCH_STORAGE,
                    ACCOUNT_INFO_STATE,
                    CODE_STORAGE,
                    ACCOUNT_STORAGE_STORAGE));

    final BonsaiArchiveReadFlatDbStrategyProvider provider =
        new BonsaiArchiveReadFlatDbStrategyProvider(new NoOpMetricsSystem(), CONFIG);
    provider.loadFlatDbStrategy(storage);

    assertThat(provider.getFlatDbStrategy(storage)).isInstanceOf(BonsaiArchiveFlatDbStrategy.class);
  }

  @Test
  void returnsArchiveStrategyEvenForFullMode() {
    // Simulate a DB that has FlatDbMode.FULL stored — provider should still return archive
    final SegmentedKeyValueStorage storage =
        new InMemoryKeyValueStorageProvider()
            .getStorageBySegmentIdentifiers(
                List.of(
                    TRIE_BRANCH_STORAGE,
                    ACCOUNT_INFO_STATE,
                    CODE_STORAGE,
                    ACCOUNT_STORAGE_STORAGE));

    // Write FULL mode to the DB
    final var tx = storage.startTransaction();
    tx.put(TRIE_BRANCH_STORAGE, FLAT_DB_MODE, FlatDbMode.FULL.getVersion().toArrayUnsafe());
    tx.commit();

    final BonsaiArchiveReadFlatDbStrategyProvider provider =
        new BonsaiArchiveReadFlatDbStrategyProvider(new NoOpMetricsSystem(), CONFIG);
    provider.loadFlatDbStrategy(storage);

    assertThat(provider.getFlatDbStrategy(storage)).isInstanceOf(BonsaiArchiveFlatDbStrategy.class);
  }

  // --- Trie-node strategy toggle tests ---
  // Trie-node reads were moved out of FlatDbStrategy into TrieNodeStrategy.
  // BonsaiArchiveTrieNodeStrategy handles reads from TRIE_BRANCH_STORAGE_ARCHIVE (proofs enabled).
  // BonsaiTrieNodeStrategy returns empty (proofs disabled / default).

  @Test
  void proofsEnabled_getFlatAccountTrieNode_returnsArchivedNode() {
    // BonsaiArchiveTrieNodeStrategy reads from TRIE_BRANCH_STORAGE_ARCHIVE.
    final long interval = 100L;
    final SegmentedKeyValueStorage storage =
        new InMemoryKeyValueStorageProvider()
            .getStorageBySegmentIdentifiers(
                List.of(
                    TRIE_BRANCH_STORAGE,
                    ACCOUNT_INFO_STATE,
                    CODE_STORAGE,
                    ACCOUNT_STORAGE_STORAGE,
                    TRIE_BRANCH_STORAGE_ARCHIVE));

    final Bytes location = Bytes.of(1, 2, 3);
    final Bytes nodeData = Bytes.of(0xAA, 0xBB, 0xCC);
    final Bytes32 nodeHash = Bytes32.ZERO;

    // Set WORLD_BLOCK_NUMBER_KEY=0 so getStateArchiveContextForRead returns block 0.
    // Write node to TRIE_BRANCH_STORAGE_ARCHIVE with key = location + suffix(0).
    // windowStart for block 0, interval 100 = ((0+1)/100)*100 = 0.
    final byte[] archiveKey = Bytes.concatenate(location, Bytes.ofUnsignedLong(0L)).toArrayUnsafe();
    final SegmentedKeyValueStorageTransaction setup = storage.startTransaction();
    setup.put(
        TRIE_BRANCH_STORAGE, WORLD_BLOCK_NUMBER_KEY, Bytes.ofUnsignedLong(0L).toArrayUnsafe());
    setup.put(TRIE_BRANCH_STORAGE_ARCHIVE, archiveKey, nodeData.toArrayUnsafe());
    setup.commit();

    final BonsaiArchiveTrieNodeStrategy trieNodeStrategy =
        new BonsaiArchiveTrieNodeStrategy(interval);
    final Optional<Bytes> result =
        trieNodeStrategy.getFlatAccountTrieNode(location, nodeHash, storage);

    assertThat(result).isPresent();
    assertThat(result.get()).isEqualTo(nodeData);
  }

  @Test
  void proofsDisabled_getFlatAccountTrieNode_returnsEmpty() {
    // BonsaiTrieNodeStrategy (default) always returns empty for trie node reads.
    final SegmentedKeyValueStorage storage =
        new InMemoryKeyValueStorageProvider()
            .getStorageBySegmentIdentifiers(
                List.of(
                    TRIE_BRANCH_STORAGE,
                    ACCOUNT_INFO_STATE,
                    CODE_STORAGE,
                    ACCOUNT_STORAGE_STORAGE));

    final Bytes location = Bytes.of(1, 2, 3);
    final Bytes32 nodeHash = Bytes32.ZERO;

    final BonsaiTrieNodeStrategy trieNodeStrategy = new BonsaiTrieNodeStrategy();
    final Optional<Bytes> result =
        trieNodeStrategy.getFlatAccountTrieNode(location, nodeHash, storage);

    assertThat(result).isEmpty();
  }
}
