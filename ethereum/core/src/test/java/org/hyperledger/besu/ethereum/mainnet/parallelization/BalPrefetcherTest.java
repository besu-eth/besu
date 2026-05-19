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
import org.hyperledger.besu.plugin.services.storage.SegmentIdentifier;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

public class BalPrefetcherTest {

  private static final Executor SYNC_EXECUTOR = Runnable::run;

  private BonsaiWorldStateKeyValueStorage baseStorage;
  private VersionedCacheManager cacheManager;

  enum StorageMode {
    BASE,
    SNAPSHOT,
    LAYER
  }

  @BeforeEach
  public void setUp() {
    cacheManager = new VersionedCacheManager(1_000, 5_000, new NoOpMetricsSystem());
    baseStorage =
        new BonsaiWorldStateKeyValueStorage(
            new InMemoryKeyValueStorageProvider(),
            new NoOpMetricsSystem(),
            DataStorageConfiguration.DEFAULT_BONSAI_CONFIG,
            cacheManager);
  }

  static Stream<Arguments> storageModes() {
    return Stream.of(
        Arguments.of(StorageMode.BASE, false, 0, 1),
        Arguments.of(StorageMode.BASE, true, 1, 2),
        Arguments.of(StorageMode.SNAPSHOT, true, 2, 2),
        Arguments.of(StorageMode.LAYER, false, 1, 2));
  }

  @ParameterizedTest
  @MethodSource("storageModes")
  public void prefetchAccountsIntoCache(
      final StorageMode mode,
      final boolean sortingEnabled,
      final int batchSize,
      final int fetchConcurrency) {
    final Address address1 = Address.fromHexString("0x1111111111111111111111111111111111111111");
    final Address address2 = Address.fromHexString("0x2222222222222222222222222222222222222222");
    final Bytes accountData1 = Bytes.of(1, 2, 3);
    final Bytes accountData2 = Bytes.of(4, 5, 6);

    final BonsaiWorldStateKeyValueStorage.Updater updater = baseStorage.updater();
    updater.putAccountInfoState(address1.addressHash(), accountData1);
    updater.putAccountInfoState(address2.addressHash(), accountData2);
    updater.commit();
    clearCache();

    final BonsaiWorldState worldState = createWorldState(createStorage(mode));
    final BlockAccessList blockAccessList =
        new BlockAccessList(
            List.of(
                accountChanges(address2, List.of(), List.of()),
                accountChanges(address1, List.of(), List.of())));

    new BalPrefetcher(sortingEnabled, batchSize, fetchConcurrency)
        .prefetch(worldState, blockAccessList, SYNC_EXECUTOR)
        .join();

    assertCachedAccount(address1, accountData1);
    assertCachedAccount(address2, accountData2);
    worldState.close();
  }

  @ParameterizedTest
  @MethodSource("storageModes")
  public void prefetchStorageSlotsIntoCache(
      final StorageMode mode,
      final boolean sortingEnabled,
      final int batchSize,
      final int fetchConcurrency) {
    final Address address = Address.fromHexString("0x1111111111111111111111111111111111111111");
    final StorageSlotKey slot1 = new StorageSlotKey(UInt256.valueOf(1));
    final StorageSlotKey slot2 = new StorageSlotKey(UInt256.valueOf(2));
    final Bytes storageValue1 = Bytes.of(10);
    final Bytes storageValue2 = Bytes.of(20);

    final BonsaiWorldStateKeyValueStorage.Updater updater = baseStorage.updater();
    updater.putAccountInfoState(address.addressHash(), Bytes.of(1));
    updater.putStorageValueBySlotHash(address.addressHash(), slot1.getSlotHash(), storageValue1);
    updater.putStorageValueBySlotHash(address.addressHash(), slot2.getSlotHash(), storageValue2);
    updater.commit();
    clearCache();

    final BonsaiWorldState worldState = createWorldState(createStorage(mode));
    final BlockAccessList blockAccessList =
        new BlockAccessList(
            List.of(
                accountChanges(
                    address,
                    List.of(new BlockAccessList.SlotChanges(slot1, List.of())),
                    List.of(new BlockAccessList.SlotRead(slot2)))));

    new BalPrefetcher(sortingEnabled, batchSize, fetchConcurrency)
        .prefetch(worldState, blockAccessList, SYNC_EXECUTOR)
        .join();

    assertCachedStorage(address, slot1, storageValue1);
    assertCachedStorage(address, slot2, storageValue2);
    worldState.close();
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void prefetchDeduplicatesAccountAndStorageKeys(final boolean sortingEnabled) {
    final Address address = Address.fromHexString("0x1111111111111111111111111111111111111111");
    final StorageSlotKey slot = new StorageSlotKey(UInt256.valueOf(1));
    final RecordingBonsaiWorldStateKeyValueStorage recordingStorage =
        new RecordingBonsaiWorldStateKeyValueStorage(cacheManager);
    final BonsaiWorldState worldState = createWorldState(recordingStorage);
    final BlockAccessList blockAccessList =
        new BlockAccessList(
            List.of(
                accountChanges(
                    address,
                    List.of(
                        new BlockAccessList.SlotChanges(slot, List.of()),
                        new BlockAccessList.SlotChanges(slot, List.of())),
                    List.of(
                        new BlockAccessList.SlotRead(slot), new BlockAccessList.SlotRead(slot))),
                accountChanges(address, List.of(), List.of())));

    new BalPrefetcher(sortingEnabled, 0, 1)
        .prefetch(worldState, blockAccessList, SYNC_EXECUTOR)
        .join();

    assertThat(recordingStorage.accountKeys()).containsExactly(address.addressHash().getBytes());
    assertThat(recordingStorage.storageKeys())
        .containsExactly(
            Bytes.concatenate(address.addressHash().getBytes(), slot.getSlotHash().getBytes()));
    worldState.close();
  }

  private BonsaiWorldStateKeyValueStorage createStorage(final StorageMode mode) {
    return switch (mode) {
      case BASE -> baseStorage;
      case SNAPSHOT -> new BonsaiSnapshotWorldStateKeyValueStorage(baseStorage);
      case LAYER -> new BonsaiWorldStateLayerStorage(baseStorage);
    };
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

  private static BlockAccessList.AccountChanges accountChanges(
      final Address address,
      final List<BlockAccessList.SlotChanges> slotChanges,
      final List<BlockAccessList.SlotRead> slotReads) {
    return new BlockAccessList.AccountChanges(
        address, slotChanges, slotReads, List.of(), List.of(), List.of());
  }

  private void clearCache() {
    cacheManager.clear(ACCOUNT_INFO_STATE);
    cacheManager.clear(ACCOUNT_STORAGE_STORAGE);
  }

  private void assertCachedAccount(final Address address, final Bytes expectedValue) {
    assertThat(cacheManager.getCachedValue(ACCOUNT_INFO_STATE, address.addressHash().getBytes()))
        .isPresent()
        .get()
        .satisfies(
            value -> {
              assertThat(value.isRemoval()).isFalse();
              assertThat(value.getValue()).isEqualTo(expectedValue);
            });
  }

  private void assertCachedStorage(
      final Address address, final StorageSlotKey slot, final Bytes expectedValue) {
    final Bytes storageKey =
        Bytes.concatenate(address.addressHash().getBytes(), slot.getSlotHash().getBytes());
    assertThat(cacheManager.getCachedValue(ACCOUNT_STORAGE_STORAGE, storageKey))
        .isPresent()
        .get()
        .satisfies(
            value -> {
              assertThat(value.isRemoval()).isFalse();
              assertThat(value.getValue()).isEqualTo(expectedValue);
            });
  }

  private static class RecordingBonsaiWorldStateKeyValueStorage
      extends BonsaiWorldStateKeyValueStorage {

    private final List<Bytes> accountKeys = new ArrayList<>();
    private final List<Bytes> storageKeys = new ArrayList<>();

    RecordingBonsaiWorldStateKeyValueStorage(final VersionedCacheManager cacheManager) {
      super(
          new InMemoryKeyValueStorageProvider(),
          new NoOpMetricsSystem(),
          DataStorageConfiguration.DEFAULT_BONSAI_CONFIG,
          cacheManager);
    }

    @Override
    public List<Optional<Bytes>> getMultipleKeys(
        final SegmentIdentifier segmentIdentifier, final List<Bytes> keys) {
      if (ACCOUNT_INFO_STATE.equals(segmentIdentifier)) {
        accountKeys.addAll(keys);
      } else if (ACCOUNT_STORAGE_STORAGE.equals(segmentIdentifier)) {
        storageKeys.addAll(keys);
      }
      return keys.stream().map(__ -> Optional.<Bytes>empty()).toList();
    }

    private List<Bytes> accountKeys() {
      return accountKeys;
    }

    private List<Bytes> storageKeys() {
      return storageKeys;
    }
  }
}
