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
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE_ARCHIVE;
import static org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedWorldStateKeyValueStorage.ARCHIVE_PROOF_BLOCK_NUMBER_KEY;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.BonsaiArchiveKeyUtil;
import org.hyperledger.besu.ethereum.trie.pathbased.common.BonsaiContext;
import org.hyperledger.besu.ethereum.worldstate.ImmutableDataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.ImmutablePathBasedExtraStorageConfiguration;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;
import org.hyperledger.besu.plugin.services.storage.SegmentIdentifier;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.services.kvstore.SegmentedInMemoryKeyValueStorage;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;

/**
 * {@link BonsaiArchiveWorldStateLayerStorage#getAccountStateTrieNode} and {@code
 * getAccountStorageTrieNode} must resolve a historical trie node from the archive column family
 * ({@code TRIE_BRANCH_STORAGE_ARCHIVE}) without also reading the live head column family ({@code
 * TRIE_BRANCH_STORAGE}) when the archive CF already holds the authoritative node. The live-CF probe
 * is only useful as a last resort when the archive CF has no entry for the location (e.g. a node
 * never touched by migration); duplicating it on every lookup wastes a disk read per trie-node
 * fetch during a cold proof roll.
 */
class BonsaiArchiveWorldStateLayerStorageTest {

  private static final long INTERVAL = 4L;
  private static final long WINDOW_START = 0L;

  /** In-memory storage that counts get() calls for a specific (segment, key) pair. */
  private static class CountingStorage extends SegmentedInMemoryKeyValueStorage {
    private final SegmentIdentifier watchedSegment;
    private final byte[] watchedKey;
    final AtomicInteger watchedKeyGetCount = new AtomicInteger();

    CountingStorage(final SegmentIdentifier watchedSegment, final byte[] watchedKey) {
      this.watchedSegment = watchedSegment;
      this.watchedKey = watchedKey;
    }

    @Override
    public Optional<byte[]> get(final SegmentIdentifier segmentIdentifier, final byte[] key) {
      if (segmentIdentifier.equals(watchedSegment) && java.util.Arrays.equals(key, watchedKey)) {
        watchedKeyGetCount.incrementAndGet();
      }
      return super.get(segmentIdentifier, key);
    }
  }

  @Test
  void archiveHitDoesNotAlsoReadTheLiveHeadColumnFamily() throws Exception {
    final Bytes location = Bytes.fromHexString("0x0102030405060708090a");
    final Bytes nodeValue = Bytes.fromHexString("0xdeadbeef");
    final Bytes32 nodeHash = Bytes32.wrap(Hash.hash(nodeValue).getBytes());

    final CountingStorage countingStorage =
        new CountingStorage(TRIE_BRANCH_STORAGE, location.toArrayUnsafe());

    final InMemoryKeyValueStorageProvider countingProvider =
        new InMemoryKeyValueStorageProvider() {
          @Override
          public SegmentedKeyValueStorage getStorageBySegmentIdentifiers(
              final List<SegmentIdentifier> segments) {
            return countingStorage;
          }
        };

    final ImmutableDataStorageConfiguration archiveConfig =
        ImmutableDataStorageConfiguration.builder()
            .dataStorageFormat(DataStorageFormat.X_BONSAI_ARCHIVE)
            .pathBasedExtraStorageConfiguration(
                ImmutablePathBasedExtraStorageConfiguration.builder()
                    .maxLayersToLoad(INTERVAL)
                    .unstable(
                        ImmutablePathBasedExtraStorageConfiguration.PathBasedUnstable.builder()
                            .stateProofsEnabled(true)
                            .archiveTrieNodeCheckpointInterval(INTERVAL)
                            .build())
                    .build())
            .build();

    final BonsaiWorldStateKeyValueStorage headStorage =
        new BonsaiWorldStateKeyValueStorage(
            countingProvider, new NoOpMetricsSystem(), archiveConfig);

    // Seed the archive CF directly with the node the proof will look up, at the pinned window's
    // suffix (0), bypassing the full migrator since only the read path is under test here.
    final byte[] archiveKey =
        BonsaiArchiveKeyUtil.calculateArchiveKeyWithMinSuffix(
            new BonsaiContext(WINDOW_START), location.toArrayUnsafe());
    final SegmentedKeyValueStorageTransaction seedTx =
        headStorage.getComposedWorldStateStorage().startTransaction();
    seedTx.put(TRIE_BRANCH_STORAGE_ARCHIVE, archiveKey, nodeValue.toArrayUnsafe());
    seedTx.commit();

    final BonsaiArchiveWorldStateLayerStorage layer =
        new BonsaiArchiveWorldStateLayerStorage(headStorage);
    // Pin the trie-node read context to the checkpoint window, as
    // BonsaiArchiveWorldStateProvider.rollArchiveProofWorldStateToBlockHash does for a real proof.
    final SegmentedKeyValueStorageTransaction pinTx =
        layer.getComposedWorldStateStorage().startTransaction();
    pinTx.put(
        TRIE_BRANCH_STORAGE,
        ARCHIVE_PROOF_BLOCK_NUMBER_KEY,
        Bytes.ofUnsignedLong(WINDOW_START).toArrayUnsafe());
    pinTx.commit();

    // Isolate the read under test from any setup-time gets.
    countingStorage.watchedKeyGetCount.set(0);

    final Optional<Bytes> result = layer.getAccountStateTrieNode(location, nodeHash);

    assertThat(result).contains(nodeValue);
    assertThat(countingStorage.watchedKeyGetCount.get())
        .withFailMessage(
            "expected the archive-CF hit to resolve the trie node without also reading the live"
                + " head column family for the same location, but it was read %s time(s)",
            countingStorage.watchedKeyGetCount.get())
        .isZero();

    headStorage.close();
  }
}
