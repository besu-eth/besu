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
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.cache.CacheManager.VersionedValue;
import org.hyperledger.besu.ethereum.worldstate.ImmutableDataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.PathBasedExtraStorageConfiguration;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;

import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for cache behavior in BonsaiWorldStateKeyValueStorage. */
public class BonsaiWorldStateKeyValueStorageCacheTest {

  private BonsaiWorldStateKeyValueStorage baseStorage;

  @BeforeEach
  public void setup() {
    baseStorage =
        new BonsaiWorldStateKeyValueStorage(
            new InMemoryKeyValueStorageProvider(),
            new NoOpMetricsSystem(),
            ImmutableDataStorageConfiguration.builder()
                .dataStorageFormat(DataStorageFormat.BONSAI)
                .bonsaiCacheEnabled(true)
                .pathBasedExtraStorageConfiguration(PathBasedExtraStorageConfiguration.DEFAULT)
                .build());
  }

  @Test
  public void testCache_basicWriteAndRead() {
    Hash accountHash = Hash.hash(Bytes.of(1));
    Bytes accountData = Bytes.of(1, 2, 3);

    // Write to storage and cache
    BonsaiWorldStateKeyValueStorage.CachedUpdater updater =
        (BonsaiWorldStateKeyValueStorage.CachedUpdater) baseStorage.updater();
    updater.putAccountInfoState(accountHash, accountData);
    updater.commit();

    // Verify in cache
    assertThat(baseStorage.isCached(ACCOUNT_INFO_STATE, accountHash.getBytes().toArrayUnsafe()))
        .isTrue();

    Optional<VersionedValue> cachedValue =
        baseStorage.getCachedValue(ACCOUNT_INFO_STATE, accountHash.getBytes().toArrayUnsafe());
    assertThat(cachedValue).isPresent();
    assertThat(Bytes.wrap(cachedValue.get().getValue())).isEqualTo(accountData);
    assertThat(cachedValue.get().isRemoval()).isFalse();
  }

  @Test
  public void testCache_versionProgression() {
    Hash accountHash = Hash.hash(Bytes.of(1));

    long v0 = baseStorage.getCurrentVersion();
    assertThat(v0).isEqualTo(0);

    // First commit
    BonsaiWorldStateKeyValueStorage.CachedUpdater updater1 =
        (BonsaiWorldStateKeyValueStorage.CachedUpdater) baseStorage.updater();
    updater1.putAccountInfoState(accountHash, Bytes.of(1));
    updater1.commit();

    long v1 = baseStorage.getCurrentVersion();
    assertThat(v1).isEqualTo(1);

    // Second commit
    BonsaiWorldStateKeyValueStorage.CachedUpdater updater2 =
        (BonsaiWorldStateKeyValueStorage.CachedUpdater) baseStorage.updater();
    updater2.putAccountInfoState(accountHash, Bytes.of(2));
    updater2.commit();

    long v2 = baseStorage.getCurrentVersion();
    assertThat(v2).isEqualTo(2);

    // Cache should have latest version
    Optional<VersionedValue> cachedValue =
        baseStorage.getCachedValue(ACCOUNT_INFO_STATE, accountHash.getBytes().toArrayUnsafe());
    assertThat(cachedValue).isPresent();
    assertThat(cachedValue.get().getVersion()).isEqualTo(v2);
  }

  @Test
  public void testCache_removal() {
    Hash accountHash = Hash.hash(Bytes.of(1));
    Bytes accountData = Bytes.of(1, 2, 3);

    // Add account
    BonsaiWorldStateKeyValueStorage.CachedUpdater updater1 =
        (BonsaiWorldStateKeyValueStorage.CachedUpdater) baseStorage.updater();
    updater1.putAccountInfoState(accountHash, accountData);
    updater1.commit();

    long v1 = baseStorage.getCurrentVersion();

    // Remove account
    BonsaiWorldStateKeyValueStorage.CachedUpdater updater2 =
        (BonsaiWorldStateKeyValueStorage.CachedUpdater) baseStorage.updater();
    updater2.removeAccountInfoState(accountHash);
    updater2.commit();

    long v2 = baseStorage.getCurrentVersion();
    assertThat(v2).isGreaterThan(v1);

    // Cache should show removal
    Optional<VersionedValue> cachedValue =
        baseStorage.getCachedValue(ACCOUNT_INFO_STATE, accountHash.getBytes().toArrayUnsafe());
    assertThat(cachedValue).isPresent();
    assertThat(cachedValue.get().isRemoval()).isTrue();
    assertThat(cachedValue.get().getVersion()).isEqualTo(v2);
  }

  @Test
  public void testCache_multipleAccountsTracking() {
    Hash acc1 = Hash.hash(Bytes.of(1));
    Hash acc2 = Hash.hash(Bytes.of(2));
    Hash acc3 = Hash.hash(Bytes.of(3));

    assertThat(baseStorage.getCacheSize(ACCOUNT_INFO_STATE)).isEqualTo(0);

    // Add first two accounts
    BonsaiWorldStateKeyValueStorage.CachedUpdater updater1 =
        (BonsaiWorldStateKeyValueStorage.CachedUpdater) baseStorage.updater();
    updater1.putAccountInfoState(acc1, Bytes.of(1));
    updater1.putAccountInfoState(acc2, Bytes.of(2));
    updater1.commit();

    assertThat(baseStorage.getCacheSize(ACCOUNT_INFO_STATE)).isEqualTo(2);
    assertThat(baseStorage.isCached(ACCOUNT_INFO_STATE, acc1.getBytes().toArrayUnsafe())).isTrue();
    assertThat(baseStorage.isCached(ACCOUNT_INFO_STATE, acc2.getBytes().toArrayUnsafe())).isTrue();

    // Add third account
    BonsaiWorldStateKeyValueStorage.CachedUpdater updater2 =
        (BonsaiWorldStateKeyValueStorage.CachedUpdater) baseStorage.updater();
    updater2.putAccountInfoState(acc3, Bytes.of(3));
    updater2.commit();

    assertThat(baseStorage.getCacheSize(ACCOUNT_INFO_STATE)).isEqualTo(3);
    assertThat(baseStorage.isCached(ACCOUNT_INFO_STATE, acc3.getBytes().toArrayUnsafe())).isTrue();
  }

  @Test
  public void testCache_storageSlots() {
    Hash accountHash = Hash.hash(Bytes.of(1));
    StorageSlotKey slot1 = new StorageSlotKey(UInt256.valueOf(1));
    StorageSlotKey slot2 = new StorageSlotKey(UInt256.valueOf(2));
    Bytes value1 = Bytes.of(10);
    Bytes value2 = Bytes.of(20);

    // Add storage slots
    BonsaiWorldStateKeyValueStorage.CachedUpdater updater =
        (BonsaiWorldStateKeyValueStorage.CachedUpdater) baseStorage.updater();
    updater.putStorageValueBySlotHash(accountHash, slot1.getSlotHash(), value1);
    updater.putStorageValueBySlotHash(accountHash, slot2.getSlotHash(), value2);
    updater.commit();

    // Verify both slots are cached
    Bytes key1 = Bytes.concatenate(accountHash.getBytes(), slot1.getSlotHash().getBytes());
    Bytes key2 = Bytes.concatenate(accountHash.getBytes(), slot2.getSlotHash().getBytes());

    assertThat(baseStorage.isCached(ACCOUNT_STORAGE_STORAGE, key1.toArrayUnsafe())).isTrue();
    assertThat(baseStorage.isCached(ACCOUNT_STORAGE_STORAGE, key2.toArrayUnsafe())).isTrue();

    Optional<VersionedValue> cached1 =
        baseStorage.getCachedValue(ACCOUNT_STORAGE_STORAGE, key1.toArrayUnsafe());
    assertThat(cached1).isPresent();
    assertThat(Bytes.wrap(cached1.get().getValue())).isEqualTo(value1);

    Optional<VersionedValue> cached2 =
        baseStorage.getCachedValue(ACCOUNT_STORAGE_STORAGE, key2.toArrayUnsafe());
    assertThat(cached2).isPresent();
    assertThat(Bytes.wrap(cached2.get().getValue())).isEqualTo(value2);
  }

  @Test
  public void testCache_rollbackDoesNotUpdateCache() {
    Hash accountHash = Hash.hash(Bytes.of(1));
    Bytes accountData = Bytes.of(1, 2, 3);

    long v1 = baseStorage.getCurrentVersion();

    // Stage changes but rollback
    BonsaiWorldStateKeyValueStorage.CachedUpdater updater =
        (BonsaiWorldStateKeyValueStorage.CachedUpdater) baseStorage.updater();
    updater.putAccountInfoState(accountHash, accountData);
    updater.rollback();

    // Version should not change
    long v2 = baseStorage.getCurrentVersion();
    assertThat(v2).isEqualTo(v1);

    // Cache should not contain the rolled-back data
    assertThat(baseStorage.isCached(ACCOUNT_INFO_STATE, accountHash.getBytes().toArrayUnsafe()))
        .isFalse();
  }

  @Test
  public void testCache_emptyValuesNotStored() {
    Hash accountHash = Hash.hash(Bytes.of(1));

    // Try to put empty value
    BonsaiWorldStateKeyValueStorage.CachedUpdater updater =
        (BonsaiWorldStateKeyValueStorage.CachedUpdater) baseStorage.updater();
    updater.putAccountInfoState(accountHash, Bytes.EMPTY);
    updater.commit();

    // Empty values should not be cached
    assertThat(baseStorage.getCacheSize(ACCOUNT_INFO_STATE)).isEqualTo(0);
  }

  @Test
  public void testCache_updateOverwritesPreviousVersion() {
    Hash accountHash = Hash.hash(Bytes.of(1));
    Bytes data1 = Bytes.of(1);
    Bytes data2 = Bytes.of(2);
    Bytes data3 = Bytes.of(3);

    // Version 1
    BonsaiWorldStateKeyValueStorage.CachedUpdater updater1 =
        (BonsaiWorldStateKeyValueStorage.CachedUpdater) baseStorage.updater();
    updater1.putAccountInfoState(accountHash, data1);
    updater1.commit();

    // Version 2
    BonsaiWorldStateKeyValueStorage.CachedUpdater updater2 =
        (BonsaiWorldStateKeyValueStorage.CachedUpdater) baseStorage.updater();
    updater2.putAccountInfoState(accountHash, data2);
    updater2.commit();
    // Version 3
    BonsaiWorldStateKeyValueStorage.CachedUpdater updater3 =
        (BonsaiWorldStateKeyValueStorage.CachedUpdater) baseStorage.updater();
    updater3.putAccountInfoState(accountHash, data3);
    updater3.commit();

    long v3 = baseStorage.getCurrentVersion();

    // Cache should only have latest version
    assertThat(baseStorage.getCacheSize(ACCOUNT_INFO_STATE)).isEqualTo(1);

    Optional<VersionedValue> cachedValue =
        baseStorage.getCachedValue(ACCOUNT_INFO_STATE, accountHash.getBytes().toArrayUnsafe());
    assertThat(cachedValue).isPresent();
    assertThat(Bytes.wrap(cachedValue.get().getValue())).isEqualTo(data3);
    assertThat(cachedValue.get().getVersion()).isEqualTo(v3);
  }

  @Test
  public void testCache_storageRemoval() {
    Hash accountHash = Hash.hash(Bytes.of(1));
    StorageSlotKey slotKey = new StorageSlotKey(UInt256.valueOf(1));
    Bytes storageValue = Bytes.of(10);
    Bytes concatenatedKey =
        Bytes.concatenate(accountHash.getBytes(), slotKey.getSlotHash().getBytes());

    // Add storage
    BonsaiWorldStateKeyValueStorage.CachedUpdater updater1 =
        (BonsaiWorldStateKeyValueStorage.CachedUpdater) baseStorage.updater();
    updater1.putStorageValueBySlotHash(accountHash, slotKey.getSlotHash(), storageValue);
    updater1.commit();

    // Remove storage
    BonsaiWorldStateKeyValueStorage.CachedUpdater updater2 =
        (BonsaiWorldStateKeyValueStorage.CachedUpdater) baseStorage.updater();
    updater2.removeStorageValueBySlotHash(accountHash, slotKey.getSlotHash());
    updater2.commit();

    // Cache should show removal
    Optional<VersionedValue> cachedValue =
        baseStorage.getCachedValue(ACCOUNT_STORAGE_STORAGE, concatenatedKey.toArrayUnsafe());
    assertThat(cachedValue).isPresent();
    assertThat(cachedValue.get().isRemoval()).isTrue();
  }

  @Test
  public void testCache_multipleBatchUpdates() {
    // Create multiple accounts in one batch
    BonsaiWorldStateKeyValueStorage.CachedUpdater updater =
        (BonsaiWorldStateKeyValueStorage.CachedUpdater) baseStorage.updater();

    for (int i = 0; i < 10; i++) {
      Hash accountHash = Hash.hash(Bytes.of(i));
      updater.putAccountInfoState(accountHash, Bytes.of(i));
    }

    updater.commit();

    // All should be in cache with same version
    long version = baseStorage.getCurrentVersion();
    assertThat(baseStorage.getCacheSize(ACCOUNT_INFO_STATE)).isEqualTo(10);

    for (int i = 0; i < 10; i++) {
      Hash accountHash = Hash.hash(Bytes.of(i));
      Optional<VersionedValue> cached =
          baseStorage.getCachedValue(ACCOUNT_INFO_STATE, accountHash.getBytes().toArrayUnsafe());
      assertThat(cached).isPresent();
      assertThat(cached.get().getVersion()).isEqualTo(version);
    }
  }
}
