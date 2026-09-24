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
package org.hyperledger.besu.ethereum.eth.sync.snapsync.request.v2;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.ethereum.trie.MerkleTrie;
import org.hyperledger.besu.ethereum.trie.NodeUpdater;
import org.hyperledger.besu.ethereum.trie.forest.storage.ForestWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.patricia.StoredMerklePatriciaTrie;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator;
import org.hyperledger.besu.plugin.services.storage.WorldStateKeyValueStorage;
import org.hyperledger.besu.services.kvstore.InMemoryKeyValueStorage;

import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.function.Function;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ForestTrieStitcherTest {

  private WorldStateStorageCoordinator coordinator;
  private ForestWorldStateKeyValueStorage forest;
  private ForestTrieStitcher stitcher;

  @BeforeEach
  void setUp() {
    forest = new ForestWorldStateKeyValueStorage(new InMemoryKeyValueStorage());
    coordinator = new WorldStateStorageCoordinator(forest);
    stitcher = new ForestTrieStitcher(coordinator);
  }

  @Test
  void emptyDownloadReturnsSameRoot() {
    final Bytes32 originalRoot = Bytes32.fromHexString("0x" + "ab".repeat(32));
    final WorldStateKeyValueStorage.Updater updater = coordinator.updater();

    final Bytes32 result = stitcher.stitchAccounts(originalRoot, new TreeMap<>(), updater);

    assertThat(result).isEqualTo(originalRoot);
    updater.commit();
  }

  @Test
  void singleAccountStitch() {
    final Bytes32 key = Bytes32.fromHexString("0x" + "11".repeat(32));
    final Bytes value = Bytes.fromHexString("0xdeadbeef");

    final NavigableMap<Bytes32, Bytes> accounts = new TreeMap<>();
    accounts.put(key, value);

    final WorldStateKeyValueStorage.Updater updater = coordinator.updater();
    final Bytes32 newRoot =
        stitcher.stitchAccounts(MerkleTrie.EMPTY_TRIE_NODE_HASH, accounts, updater);
    updater.commit();

    // Verify the new root matches a reference trie built independently
    final Bytes32 expectedRoot = buildReferenceRoot(accounts);
    assertThat(newRoot).isEqualTo(expectedRoot);
  }

  @Test
  void twoAccountsStitch() {
    final Bytes32 key1 = Bytes32.fromHexString("0x" + "11".repeat(32));
    final Bytes32 key2 = Bytes32.fromHexString("0x" + "22".repeat(32));
    final Bytes value1 = Bytes.fromHexString("0xaabb");
    final Bytes value2 = Bytes.fromHexString("0xccdd");

    final NavigableMap<Bytes32, Bytes> accounts = new TreeMap<>();
    accounts.put(key1, value1);
    accounts.put(key2, value2);

    final WorldStateKeyValueStorage.Updater updater = coordinator.updater();
    final Bytes32 newRoot =
        stitcher.stitchAccounts(MerkleTrie.EMPTY_TRIE_NODE_HASH, accounts, updater);
    updater.commit();

    final Bytes32 expectedRoot = buildReferenceRoot(accounts);
    assertThat(newRoot).isEqualTo(expectedRoot);
  }

  @Test
  void incrementalStitch() {
    final Bytes32 keyA = Bytes32.fromHexString("0x" + "aa".repeat(32));
    final Bytes32 keyB = Bytes32.fromHexString("0x" + "bb".repeat(32));
    final Bytes32 keyC = Bytes32.fromHexString("0x" + "cc".repeat(32));
    final Bytes valA = Bytes.fromHexString("0x01");
    final Bytes valB = Bytes.fromHexString("0x02");
    final Bytes valC = Bytes.fromHexString("0x03");

    // Stitch [A, B] first
    final NavigableMap<Bytes32, Bytes> firstBatch = new TreeMap<>();
    firstBatch.put(keyA, valA);
    firstBatch.put(keyB, valB);

    final WorldStateKeyValueStorage.Updater updater1 = coordinator.updater();
    final Bytes32 rootAfterFirstBatch =
        stitcher.stitchAccounts(MerkleTrie.EMPTY_TRIE_NODE_HASH, firstBatch, updater1);
    updater1.commit();

    // Stitch [C] on top of the previous result
    final NavigableMap<Bytes32, Bytes> secondBatch = new TreeMap<>();
    secondBatch.put(keyC, valC);

    final WorldStateKeyValueStorage.Updater updater2 = coordinator.updater();
    final Bytes32 finalRoot = stitcher.stitchAccounts(rootAfterFirstBatch, secondBatch, updater2);
    updater2.commit();

    // Expected: a fresh trie built with all three entries at once
    final NavigableMap<Bytes32, Bytes> allAccounts = new TreeMap<>();
    allAccounts.put(keyA, valA);
    allAccounts.put(keyB, valB);
    allAccounts.put(keyC, valC);
    final Bytes32 expectedRoot = buildReferenceRoot(allAccounts);

    assertThat(finalRoot).isEqualTo(expectedRoot);
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  /**
   * Builds a reference root by inserting all entries into a fresh in-memory trie that writes nodes
   * to the coordinator's storage, and returns the resulting root hash.
   */
  private Bytes32 buildReferenceRoot(final NavigableMap<Bytes32, Bytes> accounts) {
    // Use a separate coordinator backed by a fresh in-memory store so there is no cross-test
    // contamination.  We only need the root hash here, not the stored nodes.
    final MerkleTrie<Bytes, Bytes> trie =
        new StoredMerklePatriciaTrie<>(
            (location, hash) -> java.util.Optional.empty(),
            MerkleTrie.EMPTY_TRIE_NODE_HASH,
            Function.identity(),
            Function.identity());

    for (final var entry : accounts.entrySet()) {
      trie.put(entry.getKey(), entry.getValue());
    }

    // Commit to a no-op store — we only care about the root hash
    trie.commit((NodeUpdater) (location, hash, value) -> {});

    return Bytes32.wrap(trie.getRootHash());
  }
}
