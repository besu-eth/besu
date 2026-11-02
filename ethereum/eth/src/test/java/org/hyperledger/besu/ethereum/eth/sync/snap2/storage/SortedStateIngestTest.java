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
package org.hyperledger.besu.ethereum.eth.sync.snap2.storage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_STORAGE_STORAGE;

import org.hyperledger.besu.ethereum.trie.RangeManager;
import org.hyperledger.besu.plugin.services.storage.SegmentIdentifier;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.plugin.services.storage.SortedWriter;
import org.hyperledger.besu.services.kvstore.SegmentedInMemoryKeyValueStorage;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class SortedStateIngestTest {

  private static final int RANGES = 64;

  private final RecordingStorage storage = new RecordingStorage();
  private final SortedIngestStats stats = new SortedIngestStats();
  private final Set<Bytes32> pendingStorage = Collections.synchronizedSet(new HashSet<>());
  private SortedStateIngest ingest;

  @AfterEach
  void close() {
    if (ingest != null) {
      ingest.close();
    }
  }

  @ParameterizedTest
  @ValueSource(longs = {Long.MAX_VALUE, 64 * 1024, 1024})
  void storesEverythingHandedOverInAnyOrderAndKeepsTheFilesSorted(final long memoryLimit) {
    final Random random = new Random(memoryLimit);
    final World world = World.random(random, 3_000);
    ingest = start(memoryLimit, world);

    // ranges complete in any order, each in several responses, storage arrives whenever
    final List<Integer> order = new ArrayList<>();
    for (int i = 0; i < RANGES; i++) {
      order.add(i);
    }
    Collections.shuffle(order, random);
    for (final int range : order) {
      handOverRange(world, range, random);
    }
    final List<Bytes32> withStorage = new ArrayList<>(world.storage.keySet());
    Collections.shuffle(withStorage, random);
    for (final Bytes32 account : withStorage) {
      handOverStorage(account, world.storage.get(account), random);
    }
    ingest.flush();

    assertStored(world);
    if (memoryLimit == Long.MAX_VALUE) {
      // with enough memory nothing is given up, and the runs are large enough for files
      assertThat(storage.files).isNotEmpty();
    }
  }

  @Test
  void writeAfterAFlushIsNotHiddenByALaterFile() {
    final Random random = new Random(1);
    final World world = World.random(random, 2_000);
    ingest = start(Long.MAX_VALUE, world);
    for (int range = RANGES - 1; range > 0; range--) {
      handOverRange(world, range, random);
    }
    // a pivot move rewrites an account that waits for the first range
    ingest.flush();
    final Bytes32 rewritten = world.accounts.lastKey();
    final Bytes newValue = Bytes.of(1, 2, 3);
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    tx.put(ACCOUNT_INFO_STATE, rewritten.toArrayUnsafe(), newValue.toArrayUnsafe());
    tx.commit();
    world.accounts.put(rewritten, newValue);

    handOverRange(world, 0, random);
    for (final Map.Entry<Bytes32, TreeMap<Bytes32, Bytes32>> account : world.storage.entrySet()) {
      handOverStorage(account.getKey(), account.getValue(), random);
    }
    ingest.flush();

    assertStored(world);
  }

  @Test
  void dropsTheWaitingSlotsOfARemovedStorage() {
    final Random random = new Random(2);
    final World world = World.random(random, 500);
    ingest = start(Long.MAX_VALUE, world);
    final Bytes32 removed = world.storage.firstKey();
    final TreeMap<Bytes32, Bytes32> slots = world.storage.remove(removed);
    // only part of the storage arrived when the account goes
    final Bytes32 firstSlot = slots.firstKey();
    ingest.storage(removed, "stream", List.of(firstSlot), List.of(slots.get(firstSlot)));
    ingest.storageRemoved(removed);
    pendingStorage.remove(removed);

    for (int range = 0; range < RANGES; range++) {
      handOverRange(world, range, random);
    }
    for (final Map.Entry<Bytes32, TreeMap<Bytes32, Bytes32>> account : world.storage.entrySet()) {
      handOverStorage(account.getKey(), account.getValue(), random);
    }
    ingest.flush();

    assertStored(world);
    assertThat(storage.get(ACCOUNT_STORAGE_STORAGE, slotKey(removed, firstSlot))).isEmpty();
  }

  private SortedStateIngest start(final long memoryLimit, final World world) {
    final SortedStateIngest started =
        new SortedStateIngest(
            storage, ACCOUNT_INFO_STATE, ACCOUNT_STORAGE_STORAGE, memoryLimit, stats);
    pendingStorage.addAll(world.storage.keySet());
    started.start(world.limits, pendingStorage::contains);
    return started;
  }

  private void handOverRange(final World world, final int range, final Random random) {
    final Bytes32 from = range == 0 ? RangeManager.MIN_RANGE : world.limits.get(range - 1);
    final Bytes32 limit = world.limits.get(range);
    final List<Map.Entry<Bytes32, Bytes>> accounts =
        new ArrayList<>(
            world.accounts.subMap(from, range == 0, limit, true).entrySet().stream().toList());
    // responses follow each other within the range
    int index = 0;
    do {
      final int size = Math.min(accounts.size() - index, 1 + random.nextInt(40));
      final List<Map.Entry<Bytes32, Bytes>> response = accounts.subList(index, index + size);
      index += size;
      ingest.accounts(
          limit,
          response.stream().map(Map.Entry::getKey).toList(),
          response.stream().map(Map.Entry::getValue).toList(),
          response.stream().map(Map.Entry::getKey).filter(world.storage::containsKey).toList(),
          index == accounts.size());
    } while (index < accounts.size());
  }

  // a storage comes in pieces from several streams, as with the chunks of a large storage
  private void handOverStorage(
      final Bytes32 account, final Map<Bytes32, Bytes32> slots, final Random random) {
    final List<Bytes32> keys = new ArrayList<>(slots.keySet());
    final int streams = 1 + random.nextInt(3);
    final List<List<Integer>> bounds = new ArrayList<>();
    final int per = Math.max(1, keys.size() / streams);
    for (int start = 0; start < keys.size(); start += per) {
      bounds.add(List.of(start, Math.min(keys.size(), start + per)));
    }
    Collections.shuffle(bounds, random);
    for (final List<Integer> bound : bounds) {
      final Object stream = new Object();
      for (int start = bound.get(0); start < bound.get(1); start += 7) {
        final List<Bytes32> piece = keys.subList(start, Math.min(bound.get(1), start + 7));
        ingest.storage(account, stream, piece, piece.stream().map(slots::get).toList());
      }
    }
    ingest.storageComplete(account);
    pendingStorage.remove(account);
  }

  private void assertStored(final World world) {
    assertThat(stats.outOfOrderEntries()).isZero();
    world.accounts.forEach(
        (hash, value) ->
            assertThat(storage.get(ACCOUNT_INFO_STATE, hash.toArrayUnsafe()).map(Bytes::wrap))
                .contains(value));
    world.storage.forEach(
        (account, slots) ->
            slots.forEach(
                (slot, value) ->
                    assertThat(
                            storage
                                .get(ACCOUNT_STORAGE_STORAGE, slotKey(account, slot))
                                .map(Bytes::wrap))
                        .contains(value)));
  }

  private static byte[] slotKey(final Bytes32 account, final Bytes32 slot) {
    return Bytes.concatenate(account, slot).toArrayUnsafe();
  }

  private record World(
      TreeMap<Bytes32, Bytes> accounts,
      TreeMap<Bytes32, TreeMap<Bytes32, Bytes32>> storage,
      List<Bytes32> limits) {

    static World random(final Random random, final int accountCount) {
      final TreeMap<Bytes32, Bytes> accounts = new TreeMap<>();
      final TreeMap<Bytes32, TreeMap<Bytes32, Bytes32>> storage = new TreeMap<>();
      for (int i = 0; i < accountCount; i++) {
        final Bytes32 hash = Bytes32.random(random);
        accounts.put(hash, Bytes.random(70, random));
        if (random.nextInt(4) == 0) {
          final TreeMap<Bytes32, Bytes32> slots = new TreeMap<>();
          final int count = random.nextInt(10) == 0 ? 2_000 : 1 + random.nextInt(30);
          for (int s = 0; s < count; s++) {
            slots.put(Bytes32.random(random), Bytes32.random(random));
          }
          storage.put(hash, slots);
        }
      }
      return new World(
          accounts,
          storage,
          List.copyOf(
              RangeManager.generateRanges(RangeManager.MIN_RANGE, RangeManager.MAX_RANGE, RANGES)
                  .values()));
    }
  }

  // an in-memory storage whose sorted files insist on ascending keys, as the RocksDB ones do
  private static final class RecordingStorage extends SegmentedInMemoryKeyValueStorage {
    private final List<SegmentIdentifier> files = Collections.synchronizedList(new ArrayList<>());

    @Override
    public SortedWriter startSortedWriter(final SegmentIdentifier segment) {
      final SegmentedKeyValueStorageTransaction tx = startTransaction();
      return new SortedWriter() {
        private byte[] last;

        @Override
        public void put(final byte[] key, final byte[] value) {
          assertThat(last == null || Arrays.compareUnsigned(last, key) < 0)
              .as("keys of a sorted file in ascending order")
              .isTrue();
          last = key;
          tx.put(segment, key, value);
        }

        @Override
        public void commit() {
          tx.commit();
          files.add(segment);
        }

        @Override
        public void rollback() {
          tx.rollback();
        }
      };
    }
  }
}
