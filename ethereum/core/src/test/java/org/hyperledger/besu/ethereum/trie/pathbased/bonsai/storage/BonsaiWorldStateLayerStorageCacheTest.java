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
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_STORAGE_STORAGE;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.cache.CacheManager;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.cache.VersionedCacheManager;
import org.hyperledger.besu.ethereum.worldstate.ImmutableDataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.ImmutablePathBasedExtraStorageConfiguration;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;

import java.io.Closeable;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * {@link BonsaiWorldStateLayerStorage} shares the head's {@link VersionedCacheManager} but pins the
 * parent's {@link BonsaiWorldStateKeyValueStorage#getCurrentVersion()} at layer construction. Reads
 * use {@code getFromCacheOrStorage} with that pin so cache hits apply only when the cached writer
 * version is not ahead of the layer's epoch; local layer writes override via {@link
 * org.hyperledger.besu.services.kvstore.LayeredKeyValueStorage}.
 */
public class BonsaiWorldStateLayerStorageCacheTest {

  private BonsaiWorldStateKeyValueStorage head;

  @AfterEach
  void tearDown() throws Exception {
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

  private void newHeadWithCrossBlockCache() throws Exception {
    disposeHead();
    head =
        new BonsaiWorldStateKeyValueStorage(
            new InMemoryKeyValueStorageProvider(),
            new NoOpMetricsSystem(),
            ImmutableDataStorageConfiguration.builder()
                .dataStorageFormat(DataStorageFormat.BONSAI)
                .pathBasedExtraStorageConfiguration(
                    ImmutablePathBasedExtraStorageConfiguration.builder()
                        .unstable(
                            ImmutablePathBasedExtraStorageConfiguration.PathBasedUnstable.builder()
                                .bonsaiCrossBlockCacheEnabled(true)
                                .build())
                        .build())
                .build());
  }

  @Test
  void layerPinsParentCacheEpoch_headCommitsAdvanceIndependently() throws Exception {
    newHeadWithCrossBlockCache();
    final Hash acc1 = Hash.hash(Bytes.of(1));
    commitOnHead(acc1, Bytes.of(1));
    assertThat(head.getCachedValue(ACCOUNT_INFO_STATE, acc1.getBytes()))
        .hasValueSatisfying(
            cv -> {
              assertThat(cv.getVersion()).isEqualTo(1);
              assertThat(cv.getValue()).isEqualTo(Bytes.of(1));
            });

    try (BonsaiWorldStateLayerStorage layer = new BonsaiWorldStateLayerStorage(head)) {
      final long pinned = layer.getCurrentVersion();
      assertThat(pinned).isEqualTo(head.getCurrentVersion());

      final Hash acc2 = Hash.hash(Bytes.of(2));
      commitOnHead(acc2, Bytes.of(2));

      assertThat(head.getCurrentVersion()).isGreaterThan(pinned);
      assertThat(layer.getCurrentVersion()).isEqualTo(pinned);
      assertThat(head.getCacheSize(ACCOUNT_INFO_STATE)).isEqualTo(2);
      assertThat(head.getCachedValue(ACCOUNT_INFO_STATE, acc1.getBytes()))
          .hasValueSatisfying(cv -> assertThat(cv.getVersion()).isEqualTo(1));
      assertThat(head.getCachedValue(ACCOUNT_INFO_STATE, acc2.getBytes()))
          .hasValueSatisfying(
              cv -> {
                assertThat(cv.getVersion()).isEqualTo(head.getCurrentVersion());
                assertThat(cv.getValue()).isEqualTo(Bytes.of(2));
              });
    }
  }

  @Test
  void layerReadsWarmCrossBlockCacheEntryWhenReaderEpochMatchesWriter() throws Exception {
    newHeadWithCrossBlockCache();
    final Hash account = Hash.hash(Bytes.of(1));
    final Bytes value = Bytes.of(1, 2, 3);
    commitOnHead(account, value);

    assertThat(head.isCached(ACCOUNT_INFO_STATE, account.getBytes())).isTrue();
    assertThat(head.getCachedValue(ACCOUNT_INFO_STATE, account.getBytes()))
        .hasValueSatisfying(
            cv -> {
              assertThat(cv.getVersion()).isEqualTo(head.getCurrentVersion());
              assertThat(cv.getValue()).isEqualTo(value);
            });

    try (BonsaiWorldStateLayerStorage layer = new BonsaiWorldStateLayerStorage(head)) {
      assertThat(layer.getCurrentVersion()).isEqualTo(head.getCurrentVersion());
      assertThat(layer.getAccount(account)).contains(value);
      assertThat(head.getCachedValue(ACCOUNT_INFO_STATE, account.getBytes()))
          .hasValueSatisfying(cv -> assertThat(cv.getValue()).isEqualTo(value));
    }
  }

  @Test
  void layerLocalPutShadowsParentForLayerReadsHeadUnchanged() throws Exception {
    newHeadWithCrossBlockCache();
    final Hash account = Hash.hash(Bytes.of(1));
    final Bytes onHead = Bytes.of(1, 1, 1);
    commitOnHead(account, onHead);

    try (BonsaiWorldStateLayerStorage layer = new BonsaiWorldStateLayerStorage(head)) {
      final Bytes onLayer = Bytes.of(2, 2, 2);
      final var layerUpdater = layer.updater();
      layerUpdater.putAccountInfoState(account, onLayer);
      layerUpdater.commit();

      assertThat(layer.getAccount(account)).contains(onLayer);
      assertThat(head.getAccount(account)).contains(onHead);
      assertThat(head.getCachedValue(ACCOUNT_INFO_STATE, account.getBytes()))
          .hasValueSatisfying(
              cv -> {
                assertThat(cv.getValue()).isEqualTo(onHead);
                assertThat(cv.getVersion()).isEqualTo(1);
              });
    }
  }

  @Test
  void layerRemoveShadowsCachedParentAccountUntilHeadReads() throws Exception {
    newHeadWithCrossBlockCache();
    final Hash account = Hash.hash(Bytes.of(1));
    commitOnHead(account, Bytes.of(9, 9, 9));

    try (BonsaiWorldStateLayerStorage layer = new BonsaiWorldStateLayerStorage(head)) {
      final var u = layer.updater();
      u.removeAccountInfoState(account);
      u.commit();

      assertThat(layer.getAccount(account)).isEmpty();
      assertThat(head.getAccount(account)).contains(Bytes.of(9, 9, 9));
      assertThat(head.getCachedValue(ACCOUNT_INFO_STATE, account.getBytes()))
          .hasValueSatisfying(
              cv -> {
                assertThat(cv.isRemoval()).isFalse();
                assertThat(cv.getValue()).isEqualTo(Bytes.of(9, 9, 9));
              });
    }
  }

  @Test
  void stackedLayersEachSeeOwnOverlayWhileSharingPinnedEpochRules() throws Exception {
    newHeadWithCrossBlockCache();
    final Hash account = Hash.hash(Bytes.of(1));
    commitOnHead(account, Bytes.of(1));

    try (BonsaiWorldStateLayerStorage layer1 = new BonsaiWorldStateLayerStorage(head)) {
      var u = layer1.updater();
      u.putAccountInfoState(account, Bytes.of(2));
      u.commit();

      try (BonsaiWorldStateLayerStorage layer2 = new BonsaiWorldStateLayerStorage(layer1)) {
        u = layer2.updater();
        u.putAccountInfoState(account, Bytes.of(3));
        u.commit();

        assertThat(head.getAccount(account)).contains(Bytes.of(1));
        assertThat(layer1.getAccount(account)).contains(Bytes.of(2));
        assertThat(layer2.getAccount(account)).contains(Bytes.of(3));
        assertThat(head.getCachedValue(ACCOUNT_INFO_STATE, account.getBytes()))
            .hasValueSatisfying(
                cv -> {
                  assertThat(cv.getValue()).isEqualTo(Bytes.of(1));
                  assertThat(cv.getVersion()).isEqualTo(1);
                });
      }
    }
  }

  @Test
  void storageSlot_layerOverlayVsParentAndCrossBlockCache() throws Exception {
    newHeadWithCrossBlockCache();
    final Hash account = Hash.hash(Bytes.of(1));
    final StorageSlotKey slot = new StorageSlotKey(UInt256.ONE);
    commitOnHead(account, Bytes.of(1, 2, 3));

    final Bytes slotCacheKey = Bytes.concatenate(account.getBytes(), slot.getSlotHash().getBytes());

    final var u0 = (BonsaiWorldStateKeyValueStorage.CachedUpdater) head.updater();
    u0.putStorageValueBySlotHash(account, slot.getSlotHash(), Bytes.of(10));
    u0.commit();

    assertThat(head.getCachedValue(ACCOUNT_STORAGE_STORAGE, slotCacheKey))
        .hasValueSatisfying(
            cv -> {
              assertThat(cv.getValue()).isEqualTo(Bytes.of(10));
              assertThat(cv.getVersion()).isEqualTo(head.getCurrentVersion());
            });
    assertThat(head.getCacheSize(ACCOUNT_STORAGE_STORAGE)).isEqualTo(1);

    try (BonsaiWorldStateLayerStorage layer = new BonsaiWorldStateLayerStorage(head)) {
      final var lu = layer.updater();
      lu.putStorageValueBySlotHash(account, slot.getSlotHash(), Bytes.of(20));
      lu.commit();

      assertThat(layer.getStorageValueByStorageSlotKey(account, slot)).contains(Bytes.of(20));
      assertThat(head.getStorageValueByStorageSlotKey(account, slot)).contains(Bytes.of(10));
      assertThat(head.getCachedValue(ACCOUNT_STORAGE_STORAGE, slotCacheKey))
          .hasValueSatisfying(
              cv -> {
                assertThat(cv.getValue()).isEqualTo(Bytes.of(10));
                assertThat(cv.isRemoval()).isFalse();
              });
    }
  }

  @Test
  void emptyLayerDelegatesReadsThroughVersionedCachePath() throws Exception {
    newHeadWithCrossBlockCache();
    final Hash account = Hash.hash(Bytes.of(1));
    commitOnHead(account, Bytes.of(5, 6, 7));

    assertThat(head.getCachedValue(ACCOUNT_INFO_STATE, account.getBytes()))
        .hasValueSatisfying(cv -> assertThat(cv.getValue()).isEqualTo(Bytes.of(5, 6, 7)));

    try (BonsaiWorldStateLayerStorage layer = new BonsaiWorldStateLayerStorage(head)) {
      assertThat(layer.getAccount(account)).contains(Bytes.of(5, 6, 7));
      assertThat(head.getCachedValue(ACCOUNT_INFO_STATE, account.getBytes()))
          .hasValueSatisfying(cv -> assertThat(cv.getValue()).isEqualTo(Bytes.of(5, 6, 7)));
    }
  }

  /**
   * After {@link CacheManager#clear}, a read miss on {@link BonsaiWorldStateLayerStorage} may
   * repopulate the shared versioned cache only when the layer's pinned reader version still equals
   * {@link VersionedCacheManager#getCurrentVersion()} (global epoch). If the head advanced, the
   * layer pin is behind global and read-path inserts are skipped (see {@code
   * VersionedCacheManager#getFromCacheOrStorage}).
   */
  @Test
  void layerReadAfterCacheClearRepopulatesOnlyWhenPinMatchesGlobalVersion() throws Exception {
    newHeadWithCrossBlockCache();
    final Hash account = Hash.hash(Bytes.of(1));
    final Bytes atV1 = Bytes.of(1);
    final Bytes atV2 = Bytes.of(2);

    commitOnHead(account, atV1);
    final long epochV1 = head.getCurrentVersion();
    assertThat(head.getCacheManager().getCurrentVersion()).isEqualTo(epochV1);

    try (BonsaiWorldStateLayerStorage layer = new BonsaiWorldStateLayerStorage(head)) {
      assertThat(layer.getCurrentVersion()).isEqualTo(epochV1);

      head.getCacheManager().clear(ACCOUNT_INFO_STATE);
      assertThat(head.isCached(ACCOUNT_INFO_STATE, account.getBytes())).isFalse();

      assertThat(layer.getAccount(account)).contains(atV1);
      assertThat(head.getCacheManager().getCurrentVersion()).isEqualTo(epochV1);
      assertThat(head.isCached(ACCOUNT_INFO_STATE, account.getBytes())).isTrue();
      assertThat(head.getCachedValue(ACCOUNT_INFO_STATE, account.getBytes()))
          .hasValueSatisfying(
              cv -> {
                assertThat(cv.getVersion()).isEqualTo(epochV1);
                assertThat(cv.getValue()).isEqualTo(atV1);
              });

      commitOnHead(account, atV2);
      final long epochV2 = head.getCurrentVersion();
      assertThat(epochV2).isGreaterThan(epochV1);
      assertThat(layer.getCurrentVersion()).isEqualTo(epochV1);
      assertThat(head.getCacheManager().getCurrentVersion()).isEqualTo(epochV2);

      head.getCacheManager().clear(ACCOUNT_INFO_STATE);
      assertThat(head.isCached(ACCOUNT_INFO_STATE, account.getBytes())).isFalse();

      assertThat(layer.getAccount(account)).contains(atV2);
      assertThat(head.isCached(ACCOUNT_INFO_STATE, account.getBytes()))
          .as("read miss with layer pin < globalVersion must not warm the shared cache")
          .isFalse();

      assertThat(head.getAccount(account)).contains(atV2);
      assertThat(head.isCached(ACCOUNT_INFO_STATE, account.getBytes())).isTrue();
      assertThat(head.getCachedValue(ACCOUNT_INFO_STATE, account.getBytes()))
          .hasValueSatisfying(
              cv -> {
                assertThat(cv.getVersion()).isEqualTo(epochV2);
                assertThat(cv.getValue()).isEqualTo(atV2);
              });
    }
  }

  private void commitOnHead(final Hash account, final Bytes accountRlp) {
    final var u = (BonsaiWorldStateKeyValueStorage.CachedUpdater) head.updater();
    u.putAccountInfoState(account, accountRlp);
    u.commit();
  }
}
