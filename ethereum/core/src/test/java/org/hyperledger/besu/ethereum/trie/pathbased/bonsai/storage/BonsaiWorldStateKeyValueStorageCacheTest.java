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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_STORAGE_STORAGE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.cache.FlatDbCacheManager;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.cache.VersionedFlatDbCacheManager;
import org.hyperledger.besu.ethereum.worldstate.ImmutableDataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.ImmutableExtraStorageConfiguration;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;

import java.io.Closeable;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Cross-block (versioned) cache behaviour on {@link BonsaiWorldStateKeyValueStorage}: monotonic
 * versions, commit/rollback interaction with {@link VersionedFlatDbCacheManager}, and snapshot
 * views pinned to a cache epoch so they do not observe newer cache-only state after the head moves.
 */
public class BonsaiWorldStateKeyValueStorageCacheTest {

  private static final Bytes TRIE_NODE_LOCATION = Bytes.of(1);
  private static final Bytes ACCOUNT_TRIE_NODE = Bytes.fromHexString("0xc58320aaaa01");
  private static final Bytes STORAGE_TRIE_NODE = Bytes.fromHexString("0xc58320bbbb02");
  private static final Bytes32 ACCOUNT_TRIE_NODE_HASH =
      Bytes32.wrap(Hash.hash(ACCOUNT_TRIE_NODE).getBytes());
  private static final Bytes32 STORAGE_TRIE_NODE_HASH =
      Bytes32.wrap(Hash.hash(STORAGE_TRIE_NODE).getBytes());

  private BonsaiWorldStateKeyValueStorage head;

  @AfterEach
  void tearDownCacheExecutor() throws Exception {
    disposeHead();
  }

  private void disposeHead() throws Exception {
    if (head != null) {
      if (head.getCacheManager() instanceof final Closeable closeable) {
        closeable.close();
      }
      head.close();
      head = null;
    }
  }

  private static ImmutableDataStorageConfiguration.Builder dataConfigBuilder(
      final boolean crossBlockEnabled) {
    return ImmutableDataStorageConfiguration.builder()
        .dataStorageFormat(DataStorageFormat.BONSAI)
        .extraStorageConfiguration(
            ImmutableExtraStorageConfiguration.builder()
                .unstable(
                    ImmutableExtraStorageConfiguration.Unstable.builder()
                        .bonsaiCrossBlockCacheEnabled(crossBlockEnabled)
                        .build())
                .build());
  }

  private void newHead(final boolean crossBlockEnabled) throws Exception {
    disposeHead();
    head =
        new BonsaiWorldStateKeyValueStorage(
            new InMemoryKeyValueStorageProvider(),
            new NoOpMetricsSystem(),
            dataConfigBuilder(crossBlockEnabled).build());
  }

  @Test
  void crossBlockDisabled_usesEmptyCacheManager() throws Exception {
    newHead(false);
    assertThat(head.getCacheManager()).isSameAs(FlatDbCacheManager.NO_OP_CACHE);

    final Hash account = Hash.hash(Bytes.of(1));
    commitAccount(account, Bytes.of(1, 2, 3));

    assertThat(head.isCached(ACCOUNT_INFO_STATE, account.getBytes())).isFalse();
    assertThat(head.getCacheSize(ACCOUNT_INFO_STATE)).isZero();
  }

  @Test
  void crossBlockEnabled_usesVersionedFlatDbCacheManager() throws Exception {
    newHead(true);
    assertThat(head.getCacheManager()).isInstanceOf(VersionedFlatDbCacheManager.class);
    assertThat(head.getCacheSize(ACCOUNT_INFO_STATE)).isZero();
    assertThat(head.getCacheSize(ACCOUNT_STORAGE_STORAGE)).isZero();
  }

  @Test
  void eachSuccessfulCommitAdvancesHeadAndGlobalVersion() throws Exception {
    newHead(true);
    assertThat(head.getCurrentVersion()).isZero();
    assertThat(head.getCacheManager().getCurrentVersion()).isZero();
    assertThat(head.getCacheSize(ACCOUNT_INFO_STATE)).isZero();

    final Hash acc1 = Hash.hash(Bytes.of(1));
    commitAccount(acc1, Bytes.of(1));
    assertThat(head.getCurrentVersion()).isEqualTo(1);
    assertThat(head.getCacheManager().getCurrentVersion()).isEqualTo(1);
    assertThat(head.getCacheSize(ACCOUNT_INFO_STATE)).isEqualTo(1);
    assertThat(head.isCached(ACCOUNT_INFO_STATE, acc1.getBytes())).isTrue();
    assertThat(head.getCachedValue(ACCOUNT_INFO_STATE, acc1.getBytes()))
        .hasValueSatisfying(
            cv -> {
              assertThat(cv.getVersion()).isEqualTo(1);
              assertThat(cv.getValue()).isEqualTo(Bytes.of(1));
              assertThat(cv.isRemoval()).isFalse();
            });

    final Hash acc2 = Hash.hash(Bytes.of(2));
    commitAccount(acc2, Bytes.of(2));
    assertThat(head.getCurrentVersion()).isEqualTo(2);
    assertThat(head.getCacheManager().getCurrentVersion()).isEqualTo(2);
    assertThat(head.getCacheSize(ACCOUNT_INFO_STATE)).isEqualTo(2);
    assertThat(head.getCachedValue(ACCOUNT_INFO_STATE, acc1.getBytes()))
        .hasValueSatisfying(cv -> assertThat(cv.getVersion()).isEqualTo(1));
    assertThat(head.getCachedValue(ACCOUNT_INFO_STATE, acc2.getBytes()))
        .hasValueSatisfying(
            cv -> {
              assertThat(cv.getVersion()).isEqualTo(2);
              assertThat(cv.getValue()).isEqualTo(Bytes.of(2));
            });
  }

  @Test
  void rollbackDoesNotAdvanceVersionOrPopulateCache() throws Exception {
    newHead(true);
    final Hash account = Hash.hash(Bytes.of(1));
    final long v0 = head.getCurrentVersion();

    final var updater = (BonsaiWorldStateKeyValueStorage.CachedUpdater) head.updater();
    updater.putAccountInfoState(account, Bytes.of(9, 9, 9));
    updater.rollback();

    assertThat(head.getCurrentVersion()).isEqualTo(v0);
    assertThat(head.isCached(ACCOUNT_INFO_STATE, account.getBytes())).isFalse();
    assertThat(head.getCacheSize(ACCOUNT_INFO_STATE)).isZero();
  }

  @Test
  void commitWritesVersionedAccountAndStorageSlotEntries() throws Exception {
    newHead(true);
    final Hash account = Hash.hash(Bytes.of(1));
    final StorageSlotKey slot = new StorageSlotKey(UInt256.valueOf(1));
    final Bytes slotKey = Bytes.concatenate(account.getBytes(), slot.getSlotHash().getBytes());
    final Bytes accBytes = Bytes.of(1, 2, 3);
    final Bytes slotBytes = Bytes.of(7);

    final var u = (BonsaiWorldStateKeyValueStorage.CachedUpdater) head.updater();
    u.putAccountInfoState(account, accBytes);
    u.putStorageValueBySlotHash(account, slot.getSlotHash(), slotBytes);
    u.commit();

    final long v = head.getCurrentVersion();
    assertThat(head.getCacheSize(ACCOUNT_INFO_STATE)).isEqualTo(1);
    assertThat(head.getCacheSize(ACCOUNT_STORAGE_STORAGE)).isEqualTo(1);
    assertThat(head.isCached(ACCOUNT_INFO_STATE, account.getBytes())).isTrue();
    assertThat(head.isCached(ACCOUNT_STORAGE_STORAGE, slotKey)).isTrue();
    assertThat(head.getCachedValue(ACCOUNT_INFO_STATE, account.getBytes()))
        .hasValueSatisfying(
            cv -> {
              assertThat(cv.getVersion()).isEqualTo(v);
              assertThat(cv.getValue()).isEqualTo(accBytes);
            });
    assertThat(head.getCachedValue(ACCOUNT_STORAGE_STORAGE, slotKey))
        .hasValueSatisfying(
            cv -> {
              assertThat(cv.getVersion()).isEqualTo(v);
              assertThat(cv.getValue()).isEqualTo(slotBytes);
            });
  }

  @Test
  void removalCommitsTombstoneInCacheAtNewVersion() throws Exception {
    newHead(true);
    final Hash account = Hash.hash(Bytes.of(1));
    commitAccount(account, Bytes.of(1, 2, 3));

    assertThat(head.getCachedValue(ACCOUNT_INFO_STATE, account.getBytes()))
        .hasValueSatisfying(
            cv -> {
              assertThat(cv.isRemoval()).isFalse();
              assertThat(cv.getValue()).isEqualTo(Bytes.of(1, 2, 3));
              assertThat(cv.getVersion()).isEqualTo(1);
            });

    final var u = (BonsaiWorldStateKeyValueStorage.CachedUpdater) head.updater();
    u.removeAccountInfoState(account);
    u.commit();

    assertThat(head.getCachedValue(ACCOUNT_INFO_STATE, account.getBytes()))
        .hasValueSatisfying(
            cv -> {
              assertThat(cv.isRemoval()).isTrue();
              assertThat(cv.getVersion()).isEqualTo(head.getCurrentVersion());
            });
  }

  @Test
  void snapshotPinsCacheEpochWhileHeadKeepsAdvancing() throws Exception {
    newHead(true);
    final Hash account = Hash.hash(Bytes.of(1));
    commitAccount(account, Bytes.of(1));

    assertThat(head.getCachedValue(ACCOUNT_INFO_STATE, account.getBytes()))
        .hasValueSatisfying(
            cv -> {
              assertThat(cv.getVersion()).isEqualTo(1);
              assertThat(cv.getValue()).isEqualTo(Bytes.of(1));
            });

    final long epochAtSnapshot;
    try (BonsaiSnapshotWorldStateKeyValueStorage snapshot =
        new BonsaiSnapshotWorldStateKeyValueStorage(head)) {
      epochAtSnapshot = snapshot.getCurrentVersion();
      assertThat(epochAtSnapshot).isEqualTo(head.getCurrentVersion());

      commitAccount(account, Bytes.of(2));

      assertThat(head.getCurrentVersion()).isGreaterThan(epochAtSnapshot);
      assertThat(snapshot.getCurrentVersion()).isEqualTo(epochAtSnapshot);
      assertThat(head.getCachedValue(ACCOUNT_INFO_STATE, account.getBytes()))
          .hasValueSatisfying(
              cv -> {
                assertThat(cv.getVersion()).isEqualTo(head.getCurrentVersion());
                assertThat(cv.getValue()).isEqualTo(Bytes.of(2));
              });
    }
  }

  @Test
  void snapshotReadsPersistedAccountNotNewerHeadCacheEntry() throws Exception {
    newHead(true);
    final Hash account = Hash.hash(Bytes.of(1));
    final Bytes onSnapshot = Bytes.of(10, 11, 12);
    final Bytes onHeadAfter = Bytes.of(20, 21, 22);

    commitAccount(account, onSnapshot);

    assertThat(head.getCachedValue(ACCOUNT_INFO_STATE, account.getBytes()))
        .hasValueSatisfying(
            cv -> {
              assertThat(cv.getVersion()).isEqualTo(1);
              assertThat(cv.getValue()).isEqualTo(onSnapshot);
            });

    try (BonsaiSnapshotWorldStateKeyValueStorage snapshot =
        new BonsaiSnapshotWorldStateKeyValueStorage(head)) {
      commitAccount(account, onHeadAfter);

      assertThat(head.getAccount(account)).contains(onHeadAfter);
      assertThat(head.getCachedValue(ACCOUNT_INFO_STATE, account.getBytes()))
          .hasValueSatisfying(
              cv -> {
                assertThat(cv.getVersion()).isEqualTo(head.getCurrentVersion());
                assertThat(cv.getValue()).isEqualTo(onHeadAfter);
                assertThat(cv.isRemoval()).isFalse();
              });

      assertThat(snapshot.getAccount(account)).contains(onSnapshot);
    }
  }

  @Test
  void snapshotReadsPersistedStorageSlotNotNewerHeadCacheEntry() throws Exception {
    newHead(true);
    final Hash account = Hash.hash(Bytes.of(1));
    final StorageSlotKey slot = new StorageSlotKey(UInt256.valueOf(3));
    final Bytes slotKey = Bytes.concatenate(account.getBytes(), slot.getSlotHash().getBytes());
    final Bytes vSnapshot = Bytes.of(1);
    final Bytes vHead = Bytes.of(2);

    final var u0 = (BonsaiWorldStateKeyValueStorage.CachedUpdater) head.updater();
    u0.putAccountInfoState(account, Bytes.of(1, 2, 3));
    u0.putStorageValueBySlotHash(account, slot.getSlotHash(), vSnapshot);
    u0.commit();

    assertThat(head.getCachedValue(ACCOUNT_STORAGE_STORAGE, slotKey))
        .hasValueSatisfying(
            cv -> {
              assertThat(cv.getVersion()).isEqualTo(1);
              assertThat(cv.getValue()).isEqualTo(vSnapshot);
              assertThat(cv.isRemoval()).isFalse();
            });

    try (BonsaiSnapshotWorldStateKeyValueStorage snapshot =
        new BonsaiSnapshotWorldStateKeyValueStorage(head)) {
      final var u1 = (BonsaiWorldStateKeyValueStorage.CachedUpdater) head.updater();
      u1.putStorageValueBySlotHash(account, slot.getSlotHash(), vHead);
      u1.commit();

      assertThat(head.getStorageValueByStorageSlotKey(account, slot)).contains(vHead);
      assertThat(head.getCachedValue(ACCOUNT_STORAGE_STORAGE, slotKey))
          .hasValueSatisfying(
              cv -> {
                assertThat(cv.getVersion()).isEqualTo(head.getCurrentVersion());
                assertThat(cv.getValue()).isEqualTo(vHead);
              });
      assertThat(snapshot.getStorageValueByStorageSlotKey(account, slot)).contains(vSnapshot);
    }
  }

  @Test
  void snapshotStillSeesAccountAfterHeadRemovesIt() throws Exception {
    newHead(true);
    final Hash account = Hash.hash(Bytes.of(1));
    commitAccount(account, Bytes.of(1, 2, 3));

    assertThat(head.getCachedValue(ACCOUNT_INFO_STATE, account.getBytes()))
        .hasValueSatisfying(
            cv -> {
              assertThat(cv.isRemoval()).isFalse();
              assertThat(cv.getValue()).isEqualTo(Bytes.of(1, 2, 3));
              assertThat(cv.getVersion()).isEqualTo(1);
            });

    try (BonsaiSnapshotWorldStateKeyValueStorage snapshot =
        new BonsaiSnapshotWorldStateKeyValueStorage(head)) {
      final var u = (BonsaiWorldStateKeyValueStorage.CachedUpdater) head.updater();
      u.removeAccountInfoState(account);
      u.commit();

      assertThat(head.getAccount(account)).isEmpty();
      assertThat(head.getCachedValue(ACCOUNT_INFO_STATE, account.getBytes()))
          .hasValueSatisfying(
              cv -> {
                assertThat(cv.isRemoval()).isTrue();
                assertThat(cv.getVersion()).isEqualTo(head.getCurrentVersion());
              });
      assertThat(snapshot.getAccount(account)).isPresent();
    }
  }

  @Test
  void sequentialCommitsAlongOneBranchKeepSingleLatestCachedValuePerKey() throws Exception {
    newHead(true);
    final Hash account = Hash.hash(Bytes.of(1));
    commitAccount(account, Bytes.of(1));
    assertThat(head.getCachedValue(ACCOUNT_INFO_STATE, account.getBytes()))
        .hasValueSatisfying(cv -> assertThat(cv.getVersion()).isEqualTo(1));

    commitAccount(account, Bytes.of(2));
    assertThat(head.getCachedValue(ACCOUNT_INFO_STATE, account.getBytes()))
        .hasValueSatisfying(
            cv -> {
              assertThat(cv.getVersion()).isEqualTo(2);
              assertThat(cv.getValue()).isEqualTo(Bytes.of(2));
            });

    commitAccount(account, Bytes.of(3));

    assertThat(head.getCacheSize(ACCOUNT_INFO_STATE)).isEqualTo(1);
    assertThat(head.getAccount(account)).contains(Bytes.of(3));
    assertThat(head.getCachedValue(ACCOUNT_INFO_STATE, account.getBytes()))
        .hasValueSatisfying(
            cv -> {
              assertThat(cv.getVersion()).isEqualTo(3);
              assertThat(cv.getValue()).isEqualTo(Bytes.of(3));
              assertThat(cv.isRemoval()).isFalse();
            });
  }

  @Test
  void missDuringCommitCacheBypassDoesNotCacheAbsent() throws Exception {
    newHead(true);
    final Hash account = Hash.hash(Bytes.of(42));

    head.getCacheManager().beginCommitCacheBypass();
    assertThat(head.getAccount(account)).isEmpty();
    assertThat(head.isCached(ACCOUNT_INFO_STATE, account.getBytes())).isFalse();
    head.getCacheManager().endCommitCacheBypass();

    assertThat(head.getAccount(account)).isEmpty();
    assertThat(head.isCached(ACCOUNT_INFO_STATE, account.getBytes())).isTrue();
    assertThat(head.getCachedValue(ACCOUNT_INFO_STATE, account.getBytes()))
        .hasValueSatisfying(
            cv -> {
              assertThat(cv.isRemoval()).isTrue();
              assertThat(cv.getVersion()).isZero();
            });
  }

  @Test
  void commitCacheBypassIgnoresExistingCacheHits() throws Exception {
    newHead(true);
    final Hash account = Hash.hash(Bytes.of(43));
    final Bytes value = Bytes.of(1, 2, 3);
    commitAccount(account, value);

    // Install a wrong cache entry at the current version (clear first so put is not a no-op tie).
    head.clearCrossBlockCache();
    head.getCacheManager()
        .putInCache(
            ACCOUNT_INFO_STATE, account.getBytes(), Bytes.of(9, 9, 9), head.getCurrentVersion());

    head.getCacheManager().beginCommitCacheBypass();
    assertThat(head.getAccount(account)).contains(value);
    head.getCacheManager().endCommitCacheBypass();
  }

  @Test
  void clearCrossBlockCacheDropsEntriesButKeepsStorage() throws Exception {
    newHead(true);
    final Hash account = Hash.hash(Bytes.of(46));
    final Bytes value = Bytes.of(7, 7, 7);
    commitAccount(account, value);
    assertThat(head.getCacheSize(ACCOUNT_INFO_STATE)).isEqualTo(1);

    head.clearCrossBlockCache();

    assertThat(head.getCacheSize(ACCOUNT_INFO_STATE)).isZero();
    assertThat(head.isCached(ACCOUNT_INFO_STATE, account.getBytes())).isFalse();
    assertThat(head.getAccount(account)).contains(value);
    assertThat(head.isCached(ACCOUNT_INFO_STATE, account.getBytes())).isTrue();
  }

  @Test
  void committedTrieNodesAreCachedByHash() throws Exception {
    newHead(true);
    final Hash account = Hash.hash(Bytes.of(1));
    final var updater = head.updater();
    updater.putAccountStateTrieNode(TRIE_NODE_LOCATION, ACCOUNT_TRIE_NODE_HASH, ACCOUNT_TRIE_NODE);
    updater.putAccountStorageTrieNode(
        account, TRIE_NODE_LOCATION, STORAGE_TRIE_NODE_HASH, STORAGE_TRIE_NODE);
    updater.commit();
    awaitCachedTrieNodes(2);

    final var removal = head.updater();
    removal.removeAccountStateTrieNode(TRIE_NODE_LOCATION);
    removal.commit();

    // the cache answers what the node is; only the plain read says whether storage holds it
    assertThat(
            head.getAccountStateTrieNodeFromCacheOrStorage(
                TRIE_NODE_LOCATION, ACCOUNT_TRIE_NODE_HASH))
        .contains(ACCOUNT_TRIE_NODE);
    assertThat(head.getAccountStateTrieNode(TRIE_NODE_LOCATION, ACCOUNT_TRIE_NODE_HASH)).isEmpty();

    final Hash accountWithSameNode = Hash.hash(Bytes.of(2));
    assertThat(
            head.getAccountStorageTrieNodeFromCacheOrStorage(
                accountWithSameNode, TRIE_NODE_LOCATION, STORAGE_TRIE_NODE_HASH))
        .contains(STORAGE_TRIE_NODE);
    assertThat(
            head.getAccountStorageTrieNode(
                accountWithSameNode, TRIE_NODE_LOCATION, STORAGE_TRIE_NODE_HASH))
        .isEmpty();
  }

  @Test
  void layerReadsCommittedTrieNodesFromHeadCache() throws Exception {
    newHead(true);
    commitAccountTrieNode(ACCOUNT_TRIE_NODE_HASH, ACCOUNT_TRIE_NODE);
    awaitCachedTrieNodes(1);
    final var removal = head.updater();
    removal.removeAccountStateTrieNode(TRIE_NODE_LOCATION);
    removal.commit();

    try (BonsaiWorldStateLayerStorage layer = new BonsaiWorldStateLayerStorage(head)) {
      assertThat(
              layer.getAccountStateTrieNodeFromCacheOrStorage(
                  TRIE_NODE_LOCATION, ACCOUNT_TRIE_NODE_HASH))
          .contains(ACCOUNT_TRIE_NODE);
      assertThat(layer.getAccountStateTrieNode(TRIE_NODE_LOCATION, ACCOUNT_TRIE_NODE_HASH))
          .isEmpty();
    }
  }

  @Test
  void rollbackDoesNotCacheTrieNodes() throws Exception {
    newHead(true);
    final var rolledBack = head.updater();
    rolledBack.putAccountStateTrieNode(
        TRIE_NODE_LOCATION, ACCOUNT_TRIE_NODE_HASH, ACCOUNT_TRIE_NODE);
    rolledBack.rollback();

    // nodes reach the cache in commit order, so the rolled back one would be there by now
    commitAccountTrieNode(STORAGE_TRIE_NODE_HASH, STORAGE_TRIE_NODE);
    awaitCachedTrieNodes(1);

    assertThat(head.getCacheManager().getAccountTrieNode(ACCOUNT_TRIE_NODE_HASH)).isEmpty();
  }

  @Test
  void clearCrossBlockCacheDropsTrieNodes() throws Exception {
    newHead(true);
    commitAccountTrieNode(ACCOUNT_TRIE_NODE_HASH, ACCOUNT_TRIE_NODE);
    awaitCachedTrieNodes(1);

    head.clearCrossBlockCache();

    assertThat(head.getCacheSize(TRIE_BRANCH_STORAGE)).isZero();
    assertThat(
            head.getAccountStateTrieNodeFromCacheOrStorage(
                TRIE_NODE_LOCATION, ACCOUNT_TRIE_NODE_HASH))
        .contains(ACCOUNT_TRIE_NODE);
  }

  @Test
  void crossBlockDisabled_trieNodeReadsComeFromStorage() throws Exception {
    newHead(false);
    commitAccountTrieNode(ACCOUNT_TRIE_NODE_HASH, ACCOUNT_TRIE_NODE);

    assertThat(head.getCacheSize(TRIE_BRANCH_STORAGE)).isZero();
    assertThat(
            head.getAccountStateTrieNodeFromCacheOrStorage(
                TRIE_NODE_LOCATION, ACCOUNT_TRIE_NODE_HASH))
        .contains(ACCOUNT_TRIE_NODE);
  }

  private void commitAccountTrieNode(final Bytes32 nodeHash, final Bytes node) {
    final var updater = head.updater();
    updater.putAccountStateTrieNode(TRIE_NODE_LOCATION, nodeHash, node);
    updater.commit();
  }

  private void awaitCachedTrieNodes(final long count) {
    await()
        .untilAsserted(() -> assertThat(head.getCacheSize(TRIE_BRANCH_STORAGE)).isEqualTo(count));
  }

  private void commitAccount(final Hash accountHash, final Bytes value) {
    final var u = (BonsaiWorldStateKeyValueStorage.CachedUpdater) head.updater();
    u.putAccountInfoState(accountHash, value);
    u.commit();
  }
}
