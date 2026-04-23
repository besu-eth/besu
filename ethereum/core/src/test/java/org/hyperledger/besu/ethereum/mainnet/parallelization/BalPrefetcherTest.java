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
package org.hyperledger.besu.ethereum.mainnet.parallelization;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_STORAGE_STORAGE;
import static org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.WorldStateConfig.createStatefulConfigWithTrie;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.mainnet.parallelization.prefetch.BalPrefetcher;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.NoOpBonsaiCachedWorldStorageManager;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache.CodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache.NoopBonsaiCachedMerkleTrieLoader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiSnapshotWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateLayerStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.cache.VersionedCacheManager;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.common.trielog.NoOpTrieLogManager;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class BalPrefetcherTest {

  private BonsaiWorldStateKeyValueStorage baseStorage;
  private VersionedCacheManager cacheManager;

  private static final Executor SYNC_EXECUTOR = Runnable::run;

  enum StorageMode {
    BASE,
    SNAPSHOT,
    LAYER
  }

  @BeforeEach
  public void setup() {
    cacheManager =
        new VersionedCacheManager(
            1000, // accountCacheSize
            5000, // storageCacheSize
            new NoOpMetricsSystem());

    baseStorage =
        new BonsaiWorldStateKeyValueStorage(
            new InMemoryKeyValueStorageProvider(),
            new NoOpMetricsSystem(),
            DataStorageConfiguration.DEFAULT_BONSAI_CONFIG,
            cacheManager);
  }

  static Stream<Arguments> storageModeProvider() {
    return Stream.of(
        Arguments.of(StorageMode.BASE, false, 0),
        Arguments.of(StorageMode.BASE, true, 0),
        Arguments.of(StorageMode.BASE, false, 5),
        Arguments.of(StorageMode.BASE, true, 5),
        Arguments.of(StorageMode.BASE, false, 10),
        Arguments.of(StorageMode.BASE, true, 10),
        Arguments.of(StorageMode.BASE, false, 50),
        Arguments.of(StorageMode.BASE, true, 50),
        Arguments.of(StorageMode.SNAPSHOT, false, 0),
        Arguments.of(StorageMode.SNAPSHOT, true, 10),
        Arguments.of(StorageMode.LAYER, false, 0),
        Arguments.of(StorageMode.LAYER, true, 10));
  }

  private BonsaiWorldStateKeyValueStorage createStorage(final StorageMode mode) {
    switch (mode) {
      case BASE:
        return baseStorage;
      case SNAPSHOT:
        return new BonsaiSnapshotWorldStateKeyValueStorage(baseStorage);
      case LAYER:
        return new BonsaiWorldStateLayerStorage(baseStorage);
      default:
        throw new IllegalArgumentException("Unknown mode: " + mode);
    }
  }

  private BonsaiWorldState createWorldState(final BonsaiWorldStateKeyValueStorage storage) {
    return new BonsaiWorldState(
        storage,
        new NoopBonsaiCachedMerkleTrieLoader(),
        new NoOpBonsaiCachedWorldStorageManager(storage, EvmConfiguration.DEFAULT, new CodeCache()),
        new NoOpTrieLogManager(),
        EvmConfiguration.DEFAULT,
        createStatefulConfigWithTrie(),
        new CodeCache());
  }

  private Bytes bytesFromInt(final int value) {
    return Bytes.wrap(ByteBuffer.allocate(4).putInt(value).array());
  }

  private void clearCache() {
    cacheManager.clear(ACCOUNT_INFO_STATE);
    cacheManager.clear(ACCOUNT_STORAGE_STORAGE);
  }

  @ParameterizedTest
  @MethodSource("storageModeProvider")
  public void testPrefetch_cacheClearedAfterCommit_ensuresPrefetchPopulatesCache(
      final StorageMode mode, final boolean sortingEnabled, final int batchSize) {
    Address address1 = Address.fromHexString("0x1111111111111111111111111111111111111111");
    Address address2 = Address.fromHexString("0x2222222222222222222222222222222222222222");

    Bytes accountData1 = Bytes.of(1, 2, 3);
    Bytes accountData2 = Bytes.of(4, 5, 6);

    // Commit data to storage
    BonsaiWorldStateKeyValueStorage.Updater updater = baseStorage.updater();
    updater.putAccountInfoState(address1.addressHash(), accountData1);
    updater.putAccountInfoState(address2.addressHash(), accountData2);
    updater.commit();

    long versionAfterCommit = baseStorage.getCurrentVersion();

    // Clear cache to ensure prefetch populates it
    clearCache();

    // Verify cache is empty
    assertThat(cacheManager.getCacheSize(ACCOUNT_INFO_STATE)).isEqualTo(0);
    assertThat(
            cacheManager.isCached(
                ACCOUNT_INFO_STATE, address1.addressHash().getBytes().toArrayUnsafe()))
        .isFalse();

    BonsaiWorldStateKeyValueStorage storage = createStorage(mode);
    BonsaiWorldState worldState = createWorldState(storage);

    List<BlockAccessList.AccountChanges> accountChangesList = new ArrayList<>();
    accountChangesList.add(
        new BlockAccessList.AccountChanges(
            address1, List.of(), List.of(), List.of(), List.of(), List.of()));
    accountChangesList.add(
        new BlockAccessList.AccountChanges(
            address2, List.of(), List.of(), List.of(), List.of(), List.of()));

    BlockAccessList blockAccessList = new BlockAccessList(accountChangesList);
    BalPrefetcher prefetchMechanism = new BalPrefetcher(sortingEnabled, batchSize);

    // Execute prefetch
    prefetchMechanism.prefetch(worldState, blockAccessList, SYNC_EXECUTOR, SYNC_EXECUTOR).join();

    // Verify cache was populated by prefetch
    assertThat(cacheManager.getCacheSize(ACCOUNT_INFO_STATE)).isGreaterThanOrEqualTo(2);
    assertThat(
            cacheManager.isCached(
                ACCOUNT_INFO_STATE, address1.addressHash().getBytes().toArrayUnsafe()))
        .isTrue();
    assertThat(
            cacheManager.isCached(
                ACCOUNT_INFO_STATE, address2.addressHash().getBytes().toArrayUnsafe()))
        .isTrue();

    // Verify cached values
    assertThat(
            cacheManager.getCachedValue(
                ACCOUNT_INFO_STATE, address1.addressHash().getBytes().toArrayUnsafe()))
        .isPresent()
        .get()
        .satisfies(
            versionedValue -> {
              assertThat(versionedValue.isRemoval()).isFalse();
              assertThat(Bytes.wrap(versionedValue.getValue())).isEqualTo(accountData1);
              assertThat(versionedValue.getVersion()).isGreaterThanOrEqualTo(versionAfterCommit);
            });

    worldState.close();
  }

  @ParameterizedTest
  @MethodSource("storageModeProvider")
  public void testPrefetch_withNonExistentAccounts_cachesNegativeResults(
      final StorageMode mode, final boolean sortingEnabled, final int batchSize) {
    Address existingAddress = Address.fromHexString("0x1111111111111111111111111111111111111111");
    Address nonExistentAddress1 =
        Address.fromHexString("0x9999999999999999999999999999999999999999");
    Address nonExistentAddress2 =
        Address.fromHexString("0x8888888888888888888888888888888888888888");

    Bytes accountData = Bytes.of(1, 2, 3);

    // Only commit one account
    BonsaiWorldStateKeyValueStorage.Updater updater = baseStorage.updater();
    updater.putAccountInfoState(existingAddress.addressHash(), accountData);
    updater.commit();

    clearCache();
    assertThat(cacheManager.getCacheSize(ACCOUNT_INFO_STATE)).isEqualTo(0);

    BonsaiWorldStateKeyValueStorage storage = createStorage(mode);
    BonsaiWorldState worldState = createWorldState(storage);

    // Prefetch list includes existing and non-existent accounts
    List<BlockAccessList.AccountChanges> accountChangesList = new ArrayList<>();
    accountChangesList.add(
        new BlockAccessList.AccountChanges(
            existingAddress, List.of(), List.of(), List.of(), List.of(), List.of()));
    accountChangesList.add(
        new BlockAccessList.AccountChanges(
            nonExistentAddress1, List.of(), List.of(), List.of(), List.of(), List.of()));
    accountChangesList.add(
        new BlockAccessList.AccountChanges(
            nonExistentAddress2, List.of(), List.of(), List.of(), List.of(), List.of()));

    BlockAccessList blockAccessList = new BlockAccessList(accountChangesList);
    BalPrefetcher prefetchMechanism = new BalPrefetcher(sortingEnabled, batchSize);

    prefetchMechanism.prefetch(worldState, blockAccessList, SYNC_EXECUTOR, SYNC_EXECUTOR).join();

    // Cache should contain all 3 entries (1 existing + 2 negative)
    assertThat(cacheManager.getCacheSize(ACCOUNT_INFO_STATE)).isGreaterThanOrEqualTo(3);

    // Verify existing account
    assertThat(
            cacheManager.getCachedValue(
                ACCOUNT_INFO_STATE, existingAddress.addressHash().getBytes().toArrayUnsafe()))
        .isPresent()
        .get()
        .satisfies(
            versionedValue -> {
              assertThat(versionedValue.isRemoval()).isFalse();
              assertThat(Bytes.wrap(versionedValue.getValue())).isEqualTo(accountData);
            });

    // Verify non-existent accounts are cached as removals
    assertThat(
            cacheManager.getCachedValue(
                ACCOUNT_INFO_STATE, nonExistentAddress1.addressHash().getBytes().toArrayUnsafe()))
        .isPresent()
        .get()
        .satisfies(versionedValue -> assertThat(versionedValue.isRemoval()).isTrue());

    assertThat(
            cacheManager.getCachedValue(
                ACCOUNT_INFO_STATE, nonExistentAddress2.addressHash().getBytes().toArrayUnsafe()))
        .isPresent()
        .get()
        .satisfies(versionedValue -> assertThat(versionedValue.isRemoval()).isTrue());

    worldState.close();
  }

  @Test
  public void testPrefetch_multipleBatchSizes_allDataCached() {
    final int numAccounts = 100;
    final int[] batchSizes = {0, 5, 10, 25, 50, 100, 150};

    for (int batchSize : batchSizes) {
      setup(); // Reset for each batch size test

      Map<Address, Bytes> expectedData = new HashMap<>();
      List<BlockAccessList.AccountChanges> accountChangesList = new ArrayList<>();

      // Create and commit accounts
      for (int i = 0; i < numAccounts; i++) {
        Address address = Address.fromHexString(String.format("0x%040d", i + 1));
        Bytes accountData = bytesFromInt(i);
        expectedData.put(address, accountData);

        BonsaiWorldStateKeyValueStorage.Updater updater = baseStorage.updater();
        updater.putAccountInfoState(address.addressHash(), accountData);
        updater.commit();

        accountChangesList.add(
            new BlockAccessList.AccountChanges(
                address, List.of(), List.of(), List.of(), List.of(), List.of()));
      }

      clearCache();
      assertThat(cacheManager.getCacheSize(ACCOUNT_INFO_STATE)).isEqualTo(0);

      BonsaiWorldState worldState = createWorldState(baseStorage);
      BlockAccessList blockAccessList = new BlockAccessList(accountChangesList);
      BalPrefetcher prefetchMechanism = new BalPrefetcher(true, batchSize);

      long startTime = System.currentTimeMillis();
      prefetchMechanism.prefetch(worldState, blockAccessList, SYNC_EXECUTOR, SYNC_EXECUTOR).join();
      long duration = System.currentTimeMillis() - startTime;

      // Verify all accounts are cached
      assertThat(cacheManager.getCacheSize(ACCOUNT_INFO_STATE)).isGreaterThanOrEqualTo(numAccounts);

      for (Map.Entry<Address, Bytes> entry : expectedData.entrySet()) {
        Address address = entry.getKey();
        Bytes expectedAccountData = entry.getValue();

        assertThat(
                cacheManager.getCachedValue(
                    ACCOUNT_INFO_STATE, address.addressHash().getBytes().toArrayUnsafe()))
            .as("Account %s should be cached with batch size %d", address, batchSize)
            .isPresent()
            .get()
            .satisfies(
                versionedValue -> {
                  assertThat(versionedValue.isRemoval()).isFalse();
                  assertThat(Bytes.wrap(versionedValue.getValue())).isEqualTo(expectedAccountData);
                });
      }

      System.out.printf(
          "Batch size %d: Prefetched %d accounts in %d ms%n", batchSize, numAccounts, duration);

      worldState.close();
    }
  }

  @Test
  public void testPrefetch_withSorting_maintainsCorrectData() {
    List<Address> unsortedAddresses = new ArrayList<>();
    unsortedAddresses.add(Address.fromHexString("0x5555555555555555555555555555555555555555"));
    unsortedAddresses.add(Address.fromHexString("0x1111111111111111111111111111111111111111"));
    unsortedAddresses.add(Address.fromHexString("0x9999999999999999999999999999999999999999"));
    unsortedAddresses.add(Address.fromHexString("0x3333333333333333333333333333333333333333"));
    unsortedAddresses.add(Address.fromHexString("0x7777777777777777777777777777777777777777"));

    // Commit all accounts
    for (int i = 0; i < unsortedAddresses.size(); i++) {
      Address address = unsortedAddresses.get(i);
      Bytes accountData = bytesFromInt(i);

      BonsaiWorldStateKeyValueStorage.Updater updater = baseStorage.updater();
      updater.putAccountInfoState(address.addressHash(), accountData);
      updater.commit();
    }

    clearCache();

    BonsaiWorldState worldState = createWorldState(baseStorage);

    List<BlockAccessList.AccountChanges> accountChangesList = new ArrayList<>();
    for (Address address : unsortedAddresses) {
      accountChangesList.add(
          new BlockAccessList.AccountChanges(
              address, List.of(), List.of(), List.of(), List.of(), List.of()));
    }

    BlockAccessList blockAccessList = new BlockAccessList(accountChangesList);

    // Test with sorting enabled
    BalPrefetcher prefetchWithSorting = new BalPrefetcher(true, 0);
    prefetchWithSorting.prefetch(worldState, blockAccessList, SYNC_EXECUTOR, SYNC_EXECUTOR).join();

    // Verify all addresses are cached with correct data
    for (int i = 0; i < unsortedAddresses.size(); i++) {
      Address address = unsortedAddresses.get(i);
      Bytes expectedData = bytesFromInt(i);

      assertThat(
              cacheManager.getCachedValue(
                  ACCOUNT_INFO_STATE, address.addressHash().getBytes().toArrayUnsafe()))
          .as("Address %s should be cached with correct data", address)
          .isPresent()
          .get()
          .satisfies(
              versionedValue -> {
                assertThat(versionedValue.isRemoval()).isFalse();
                assertThat(Bytes.wrap(versionedValue.getValue())).isEqualTo(expectedData);
              });
    }

    worldState.close();

    // Test without sorting
    clearCache();
    worldState = createWorldState(baseStorage);

    BalPrefetcher prefetchWithoutSorting = new BalPrefetcher(false, 0);
    prefetchWithoutSorting
        .prefetch(worldState, blockAccessList, SYNC_EXECUTOR, SYNC_EXECUTOR)
        .join();

    // Verify all addresses still cached correctly
    for (int i = 0; i < unsortedAddresses.size(); i++) {
      Address address = unsortedAddresses.get(i);
      Bytes expectedData = bytesFromInt(i);

      assertThat(
              cacheManager.getCachedValue(
                  ACCOUNT_INFO_STATE, address.addressHash().getBytes().toArrayUnsafe()))
          .as("Address %s should be cached with correct data (no sorting)", address)
          .isPresent()
          .get()
          .satisfies(
              versionedValue -> {
                assertThat(versionedValue.isRemoval()).isFalse();
                assertThat(Bytes.wrap(versionedValue.getValue())).isEqualTo(expectedData);
              });
    }

    worldState.close();
  }

  @Test
  public void testPrefetch_largeDataset_withMixedExistingAndNonExisting() {
    final int numExisting = 50;
    final int numNonExisting = 50;
    final int batchSize = 10;

    List<Address> existingAddresses = new ArrayList<>();
    List<Address> nonExistingAddresses = new ArrayList<>();
    Map<Address, Bytes> expectedData = new HashMap<>();

    // Create existing accounts
    for (int i = 0; i < numExisting; i++) {
      Address address = Address.fromHexString(String.format("0x%040d", i + 1));
      Bytes accountData = bytesFromInt(i);
      existingAddresses.add(address);
      expectedData.put(address, accountData);

      BonsaiWorldStateKeyValueStorage.Updater updater = baseStorage.updater();
      updater.putAccountInfoState(address.addressHash(), accountData);
      updater.commit();
    }

    // Create non-existing addresses (not committed)
    for (int i = 0; i < numNonExisting; i++) {
      Address address = Address.fromHexString(String.format("0x%040d", numExisting + i + 1000));
      nonExistingAddresses.add(address);
    }

    clearCache();

    BonsaiWorldState worldState = createWorldState(baseStorage);

    // Mix existing and non-existing in BAL
    List<BlockAccessList.AccountChanges> accountChangesList = new ArrayList<>();
    for (int i = 0; i < numExisting; i++) {
      accountChangesList.add(
          new BlockAccessList.AccountChanges(
              existingAddresses.get(i), List.of(), List.of(), List.of(), List.of(), List.of()));
      accountChangesList.add(
          new BlockAccessList.AccountChanges(
              nonExistingAddresses.get(i), List.of(), List.of(), List.of(), List.of(), List.of()));
    }

    BlockAccessList blockAccessList = new BlockAccessList(accountChangesList);
    BalPrefetcher prefetchMechanism = new BalPrefetcher(true, batchSize);

    prefetchMechanism.prefetch(worldState, blockAccessList, SYNC_EXECUTOR, SYNC_EXECUTOR).join();

    // Verify all are cached
    assertThat(cacheManager.getCacheSize(ACCOUNT_INFO_STATE))
        .isGreaterThanOrEqualTo(numExisting + numNonExisting);

    // Verify existing accounts have correct data
    for (Map.Entry<Address, Bytes> entry : expectedData.entrySet()) {
      assertThat(
              cacheManager.getCachedValue(
                  ACCOUNT_INFO_STATE, entry.getKey().addressHash().getBytes().toArrayUnsafe()))
          .isPresent()
          .get()
          .satisfies(
              versionedValue -> {
                assertThat(versionedValue.isRemoval()).isFalse();
                assertThat(Bytes.wrap(versionedValue.getValue())).isEqualTo(entry.getValue());
              });
    }

    // Verify non-existing accounts are cached as removals
    for (Address address : nonExistingAddresses) {
      assertThat(
              cacheManager.getCachedValue(
                  ACCOUNT_INFO_STATE, address.addressHash().getBytes().toArrayUnsafe()))
          .isPresent()
          .get()
          .satisfies(versionedValue -> assertThat(versionedValue.isRemoval()).isTrue());
    }

    worldState.close();
  }

  @ParameterizedTest
  @MethodSource("storageModeProvider")
  public void testPrefetch_loadsStorageSlotsIntoCache(
      final StorageMode mode, final boolean sortingEnabled, final int batchSize) {
    Address address = Address.fromHexString("0x1111111111111111111111111111111111111111");
    StorageSlotKey slot1 = new StorageSlotKey(UInt256.valueOf(1));
    StorageSlotKey slot2 = new StorageSlotKey(UInt256.valueOf(2));

    Bytes accountData = Bytes.of(1, 2, 3);
    Bytes storageValue1 = Bytes.of(10);
    Bytes storageValue2 = Bytes.of(20);

    BonsaiWorldStateKeyValueStorage.Updater updater = baseStorage.updater();
    updater.putAccountInfoState(address.addressHash(), accountData);
    updater.putStorageValueBySlotHash(address.addressHash(), slot1.getSlotHash(), storageValue1);
    updater.putStorageValueBySlotHash(address.addressHash(), slot2.getSlotHash(), storageValue2);
    updater.commit();

    clearCache();

    BonsaiWorldStateKeyValueStorage storage = createStorage(mode);
    BonsaiWorldState worldState = createWorldState(storage);

    List<BlockAccessList.SlotChanges> storageChangesList = new ArrayList<>();
    storageChangesList.add(new BlockAccessList.SlotChanges(slot1, List.of()));
    storageChangesList.add(new BlockAccessList.SlotChanges(slot2, List.of()));

    List<BlockAccessList.AccountChanges> accountChangesList = new ArrayList<>();
    accountChangesList.add(
        new BlockAccessList.AccountChanges(
            address, storageChangesList, List.of(), List.of(), List.of(), List.of()));

    BlockAccessList blockAccessList = new BlockAccessList(accountChangesList);
    BalPrefetcher prefetchMechanism = new BalPrefetcher(sortingEnabled, batchSize);

    prefetchMechanism.prefetch(worldState, blockAccessList, SYNC_EXECUTOR, SYNC_EXECUTOR).join();

    // Verify account and storage are cached
    assertThat(
            cacheManager.isCached(
                ACCOUNT_INFO_STATE, address.addressHash().getBytes().toArrayUnsafe()))
        .isTrue();
    assertThat(cacheManager.getCacheSize(ACCOUNT_STORAGE_STORAGE)).isGreaterThanOrEqualTo(2);

    // Verify storage values
    byte[] storageKey1 =
        Bytes.concatenate(address.addressHash().getBytes(), slot1.getSlotHash().getBytes())
            .toArrayUnsafe();
    byte[] storageKey2 =
        Bytes.concatenate(address.addressHash().getBytes(), slot2.getSlotHash().getBytes())
            .toArrayUnsafe();

    assertThat(cacheManager.getCachedValue(ACCOUNT_STORAGE_STORAGE, storageKey1))
        .isPresent()
        .get()
        .satisfies(
            versionedValue -> {
              assertThat(versionedValue.isRemoval()).isFalse();
              assertThat(Bytes.wrap(versionedValue.getValue())).isEqualTo(storageValue1);
            });

    assertThat(cacheManager.getCachedValue(ACCOUNT_STORAGE_STORAGE, storageKey2))
        .isPresent()
        .get()
        .satisfies(
            versionedValue -> {
              assertThat(versionedValue.isRemoval()).isFalse();
              assertThat(Bytes.wrap(versionedValue.getValue())).isEqualTo(storageValue2);
            });

    worldState.close();
  }
}
