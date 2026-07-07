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
package org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.cache;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.trie.pathbased.common.provider.PathBasedWorldStateProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.PathBasedWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.WorldStateConfig;
import org.hyperledger.besu.evm.internal.EvmConfiguration;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.LongStream;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class PathBasedWorldStateCacheManagerTest {

  private static final int BLOCK_COUNT = 2000;
  private static final long HEAD = BLOCK_COUNT;
  private static final long WATERLINE = HEAD - PathBasedWorldStateCacheManager.RETAINED_LAYERS;

  private final BlockHeader[] headers = new BlockHeader[BLOCK_COUNT + 1];
  private final PathBasedWorldState[] worldStates = new PathBasedWorldState[BLOCK_COUNT + 1];
  private TestCacheManager cacheManager;

  @BeforeEach
  void setUp() {
    final Map<Hash, PathBasedCachedWorldStateView> worldStatesByHash = new ConcurrentHashMap<>();
    final PathBasedWorldStateKeyValueStorage mockStorage =
        Mockito.mock(PathBasedWorldStateKeyValueStorage.class);
    cacheManager = new TestCacheManager(mockStorage, worldStatesByHash);

    for (int i = 0; i <= BLOCK_COUNT; i++) {
      final BlockHeader header =
          new BlockHeaderTestFixture().number(i).stateRoot(uniqueStateRoot(i)).buildHeader();
      headers[i] = header;

      final PathBasedWorldState ws = Mockito.mock(PathBasedWorldState.class);
      Mockito.when(ws.isModifyingHeadWorldState()).thenReturn(true);
      Mockito.when(ws.getWorldStateStorage()).thenReturn(mockStorage);
      worldStates[i] = ws;
    }
  }

  @Test
  void stateRootCacheRetainsRecentAndEvictsStale() {
    // Insert first batch — well below the waterline capacity so no entries evicted yet.
    final int readPoint = 200;
    for (int i = 0; i <= readPoint; i++) {
      cacheManager.addCachedLayer(headers[i], headers[i].getStateRoot(), worldStates[i]);
    }

    // Perform reads on entries that will eventually fall below the final waterline.
    assertThat(cacheManager.getStorageByRootHash(uniqueStateRoot(0))).isPresent();
    assertThat(cacheManager.getStorageByRootHash(uniqueStateRoot(50))).isPresent();
    assertThat(cacheManager.getStorageByRootHash(uniqueStateRoot(100))).isPresent();

    // Insert remaining blocks.
    for (int i = readPoint + 1; i <= BLOCK_COUNT; i++) {
      cacheManager.addCachedLayer(headers[i], headers[i].getStateRoot(), worldStates[i]);
    }

    // Read blocks are evicted despite the earlier read.
    assertThat(cacheManager.getStorageByRootHash(uniqueStateRoot(0)))
        .describedAs(
            "block 0 read at block %d then evicted (below waterline %d)", readPoint, WATERLINE)
        .isEmpty();
    assertThat(cacheManager.getStorageByRootHash(uniqueStateRoot(50)))
        .describedAs(
            "block 50 read at block %d then evicted (below waterline %d)", readPoint, WATERLINE)
        .isEmpty();
    assertThat(cacheManager.getStorageByRootHash(uniqueStateRoot(100)))
        .describedAs(
            "block 100 read at block %d then evicted (below waterline %d)", readPoint, WATERLINE)
        .isEmpty();

    // All entries below waterline should also be evicted.
    LongStream.range(0, WATERLINE)
        .forEach(
            i ->
                assertThat(cacheManager.getStorageByRootHash(uniqueStateRoot(i)))
                    .describedAs("block %d (below waterline %d)", i, WATERLINE)
                    .isEmpty());

    // All entries at or above waterline should be present.
    LongStream.range(WATERLINE, HEAD + 1)
        .forEach(
            i ->
                assertThat(cacheManager.getStorageByRootHash(uniqueStateRoot(i)))
                    .describedAs("block %d (at/above waterline %d)", i, WATERLINE)
                    .isPresent());
  }

  private static Hash uniqueStateRoot(final long blockNumber) {
    return Hash.wrap(Bytes32.leftPad(Bytes.ofUnsignedLong(blockNumber)));
  }

  private static class TestCacheManager extends PathBasedWorldStateCacheManager {

    TestCacheManager(
        final PathBasedWorldStateKeyValueStorage storage,
        final Map<Hash, PathBasedCachedWorldStateView> map) {
      super(
          null,
          storage,
          map,
          EvmConfiguration.DEFAULT,
          WorldStateConfig.createStatefulConfigWithTrie());
    }

    @Override
    public PathBasedWorldState createWorldState(
        final PathBasedWorldStateProvider archive,
        final PathBasedWorldStateKeyValueStorage worldStateKeyValueStorage,
        final EvmConfiguration evmConfiguration) {
      throw new UnsupportedOperationException();
    }

    @Override
    public PathBasedWorldStateKeyValueStorage createLayeredKeyValueStorage(
        final PathBasedWorldStateKeyValueStorage worldStateKeyValueStorage) {
      throw new UnsupportedOperationException();
    }

    @Override
    public PathBasedWorldStateKeyValueStorage createSnapshotKeyValueStorage(
        final PathBasedWorldStateKeyValueStorage worldStateKeyValueStorage) {
      return worldStateKeyValueStorage;
    }
  }
}
