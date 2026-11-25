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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_STORAGE_STORAGE;

import org.hyperledger.besu.ethereum.trie.RangeManager;
import org.hyperledger.besu.plugin.services.exception.StorageException;
import org.hyperledger.besu.plugin.services.storage.SegmentIdentifier;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.plugin.services.storage.SortedWriter;
import org.hyperledger.besu.services.kvstore.SegmentedInMemoryKeyValueStorage;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.util.concurrent.Uninterruptibles;
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

  @Test
  void closeEndsTheFilesNotYetStoredAndReleasesAWaitingFlush() throws Exception {
    final Random random = new Random(3);
    // the accounts and the slots are each enough for a file, both stored by the flush
    final World world = World.sized(random, 12_000, 20, 1_000);
    storage.holdCommits();
    ingest = start(Long.MAX_VALUE, world);
    for (int range = 0; range < RANGES; range++) {
      handOverRange(world, range, random);
    }
    for (final Map.Entry<Bytes32, TreeMap<Bytes32, Bytes32>> account : world.storage.entrySet()) {
      handOverStorage(account.getKey(), account.getValue(), random);
    }
    final AtomicReference<Throwable> flushFailure = new AtomicReference<>();
    final Thread flush =
        new Thread(
            () -> {
              try {
                ingest.flush();
              } catch (final RuntimeException e) {
                flushFailure.set(e);
              }
            });
    flush.start();
    // the flush waits for the file being stored, which waits for the commits to be released
    final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    while (flush.getState() != Thread.State.WAITING && System.nanoTime() < deadline) {
      Thread.onSpinWait();
    }
    final Thread release =
        new Thread(
            () -> {
              storage.awaitRollback();
              storage.releaseCommits();
            });
    release.start();

    ingest.close();
    flush.join(TimeUnit.SECONDS.toMillis(10));
    release.join();

    assertThat(flush.isAlive()).isFalse();
    assertThat(flushFailure.get()).isInstanceOf(CancellationException.class);
    assertThat(storage.rolledBack).isNotEmpty();
    // every writer ended, none still holds its file
    assertThat(storage.files.size() + storage.rolledBack.size()).isEqualTo(storage.started.get());
  }

  @Test
  void flushFailsWhenAFileIsNotStoredBecauseOfAnError() {
    final Random random = new Random(4);
    final World world = World.sized(random, 12_000, 20, 1_000);
    final OutOfMemoryError error = new OutOfMemoryError("while storing a file");
    storage.failCommitsWith(error);
    ingest = start(Long.MAX_VALUE, world);
    handOverAll(world, random);

    assertThatThrownBy(ingest::flush).isInstanceOf(StorageException.class).cause().isSameAs(error);
  }

  @Test
  void everyCloseWaitsForTheFileBeingStored() throws Exception {
    final Random random = new Random(5);
    // an account file and a storage file, the first held while being stored
    final World world = World.sized(random, 12_000, 20, 1_000);
    storage.holdCommits();
    ingest = start(Long.MAX_VALUE, world);
    handOverAll(world, random);
    final Thread flush =
        new Thread(
            () -> {
              try {
                ingest.flush();
              } catch (final RuntimeException e) {
                // cancelled by the close
              }
            });
    flush.start();
    storage.awaitCommitting();

    final Thread firstClose = new Thread(ingest::close);
    firstClose.start();
    storage.awaitRollback();
    // the next sync attempt closes again before it wipes the state
    final Thread secondClose = new Thread(ingest::close);
    secondClose.start();
    awaitParked(firstClose);
    awaitParked(secondClose);
    assertThat(firstClose.isAlive()).isTrue();
    assertThat(secondClose.isAlive()).isTrue();

    storage.releaseCommits();
    firstClose.join(TimeUnit.SECONDS.toMillis(10));
    secondClose.join(TimeUnit.SECONDS.toMillis(10));
    flush.join(TimeUnit.SECONDS.toMillis(10));
    assertThat(firstClose.isAlive()).isFalse();
    assertThat(secondClose.isAlive()).isFalse();
    assertThat(storage.files).hasSize(1);
    assertThat(storage.rolledBack).hasSize(1);
  }

  @Test
  void storesAPieceThatComesAfterAStorageWithFilesOfItsOwnCompleted() {
    final Random random = new Random(6);
    // one storage large enough to get files of its own
    final World world = World.sized(random, 500, 1, 2_000);
    ingest = start(64 * 1024, world);
    final Bytes32 account = world.storage.firstKey();
    final TreeMap<Bytes32, Bytes32> slots = world.storage.get(account);
    final Bytes32 late = slots.lastKey();
    handOverStorage(account, slots.headMap(late), random);
    ingest.storage(account, new Object(), List.of(late), List.of(slots.get(late)));

    for (int range = 0; range < RANGES; range++) {
      handOverRange(world, range, random);
    }
    ingest.flush();

    assertStored(world);
  }

  @Test
  void removesTheSlotsOfAPassedStorageThatAreStillInTheStorageFile() {
    final Random random = new Random(7);
    final World world = World.sized(random, 500, 20, 1_000);
    ingest = start(Long.MAX_VALUE, world);
    for (int range = 0; range < RANGES; range++) {
      handOverRange(world, range, random);
    }
    // in key order, each storage goes into the open storage file once complete
    for (final Map.Entry<Bytes32, TreeMap<Bytes32, Bytes32>> account : world.storage.entrySet()) {
      handOverStorage(account.getKey(), account.getValue(), random);
    }
    final Bytes32 removed = new ArrayList<>(world.storage.keySet()).get(10);
    world.storage.remove(removed);

    ingest.storageRemoved(removed);
    deleteSlots(removed);
    ingest.flush();

    assertStored(world);
    assertThat(slotsOf(removed)).isZero();
  }

  @Test
  void removingAStorageWithNothingInFilesDoesNotWaitForTheFiles() throws Exception {
    final Random random = new Random(8);
    final World world = World.sized(random, 12_000, 20, 1_000);
    storage.holdCommits();
    ingest = start(Long.MAX_VALUE, world);
    for (int range = 0; range < RANGES; range++) {
      handOverRange(world, range, random);
    }
    // an earlier storage that has not arrived holds the turn of the removed one back
    final Bytes32 removed = world.storage.lastKey();
    final Bytes32 holding = world.storage.lowerKey(removed);
    final TreeMap<Bytes32, Bytes32> removedSlots = world.storage.remove(removed);
    for (final Map.Entry<Bytes32, TreeMap<Bytes32, Bytes32>> account : world.storage.entrySet()) {
      if (!account.getKey().equals(holding)) {
        handOverStorage(account.getKey(), account.getValue(), random);
      }
    }
    final Bytes32 firstSlot = removedSlots.firstKey();
    ingest.storage(removed, "stream", List.of(firstSlot), List.of(removedSlots.get(firstSlot)));
    // the flush waits for the files, held while being stored
    final Thread flush = new Thread(ingest::flush);
    flush.start();
    storage.awaitCommitting();

    final Thread remove =
        new Thread(
            () -> {
              ingest.storageRemoved(removed);
              deleteSlots(removed);
            });
    remove.start();
    remove.join(TimeUnit.SECONDS.toMillis(5));
    final boolean removedWhileStoring = !remove.isAlive();
    storage.releaseCommits();
    remove.join(TimeUnit.SECONDS.toMillis(10));
    flush.join(TimeUnit.SECONDS.toMillis(10));
    pendingStorage.remove(removed);
    handOverStorage(holding, world.storage.get(holding), random);
    ingest.flush();

    assertThat(removedWhileStoring).isTrue();
    assertStored(world);
    assertThat(slotsOf(removed)).isZero();
  }

  @Test
  void writesTheRunsOfAStorageWithFilesOfItsOwnOnceTooMuchWaits() {
    final Random random = new Random(9);
    // a storage in more streams than the memory holds, none of them worth a file
    final World world = World.sized(random, 100, 1, 6_000);
    final long memoryLimit = 256 * 1024;
    ingest = start(memoryLimit, world);
    final Bytes32 account = world.storage.firstKey();
    final TreeMap<Bytes32, Bytes32> slots = world.storage.get(account);
    final List<Bytes32> keys = new ArrayList<>(slots.keySet());
    final int streams = 64;
    for (int stream = 0; stream < streams; stream++) {
      final List<Bytes32> chunk =
          keys.subList(stream * keys.size() / streams, (stream + 1) * keys.size() / streams);
      ingest.storage(account, new Object(), chunk, chunk.stream().map(slots::get).toList());
    }

    // what the runs held went through the regular write path before the storage completed
    assertThat(slotsOf(account)).isPositive();

    ingest.storageComplete(account);
    pendingStorage.remove(account);
    for (int range = 0; range < RANGES; range++) {
      handOverRange(world, range, random);
    }
    ingest.flush();
    assertStored(world);
  }

  @Test
  void givesUpTheStorageEverythingWaitsForBeforeTheRunsOfFilesOfTheirOwn() {
    final Random random = new Random(10);
    // a slow storage first, small storages behind it, and two large ones in several streams
    final TreeMap<Bytes32, Bytes> accounts = new TreeMap<>();
    for (int i = 0; i < 195; i++) {
      accounts.put(Bytes32.random(random), Bytes.random(70, random));
    }
    final List<Bytes32> hashes = new ArrayList<>(accounts.keySet());
    final Bytes32 slow = hashes.getFirst();
    final List<Bytes32> large = hashes.subList(hashes.size() - 2, hashes.size());
    final List<Bytes32> small = hashes.subList(1, hashes.size() - 2);
    final TreeMap<Bytes32, TreeMap<Bytes32, Bytes32>> storages = new TreeMap<>();
    for (final Bytes32 hash : hashes) {
      final int count = hash.equals(slow) ? 2_000 : large.contains(hash) ? 64_000 : 1_000;
      final TreeMap<Bytes32, Bytes32> slots = new TreeMap<>();
      for (int s = 0; s < count; s++) {
        slots.put(Bytes32.random(random), Bytes32.random(random));
      }
      storages.put(hash, slots);
    }
    final World world = new World(accounts, storages, World.rangeLimits());
    ingest = start(16 * 1024 * 1024, world);
    for (int range = 0; range < RANGES; range++) {
      handOverRange(world, range, random);
    }
    final TreeMap<Bytes32, Bytes32> slowSlots = storages.get(slow);
    final List<Bytes32> slowKeys = new ArrayList<>(slowSlots.keySet());
    final Object slowStream = new Object();
    handOver(slow, slowStream, slowKeys.subList(0, 100), slowSlots);

    // each round, a part of every stream of the large storages and twelve small storages
    final int streams = 4;
    final int rounds = 16;
    final Map<Bytes32, List<Object>> largeStreams = new HashMap<>();
    for (final Bytes32 hash : large) {
      final List<Object> objects = new ArrayList<>();
      for (int stream = 0; stream < streams; stream++) {
        objects.add(new Object());
      }
      largeStreams.put(hash, objects);
    }
    for (int round = 0; round < rounds; round++) {
      for (final Bytes32 hash : large) {
        final List<Bytes32> keys = new ArrayList<>(storages.get(hash).keySet());
        final int perStream = keys.size() / streams;
        final int perRound = perStream / rounds;
        for (int stream = 0; stream < streams; stream++) {
          final int from = stream * perStream + round * perRound;
          handOver(
              hash,
              largeStreams.get(hash).get(stream),
              keys.subList(from, from + perRound),
              storages.get(hash));
        }
      }
      for (final Bytes32 hash : small.subList(round * 12, (round + 1) * 12)) {
        handOverStorage(hash, storages.get(hash), random);
      }
    }
    handOver(slow, slowStream, slowKeys.subList(100, slowKeys.size()), slowSlots);
    for (final Bytes32 hash : List.of(slow, large.get(0), large.get(1))) {
      ingest.storageComplete(hash);
      pendingStorage.remove(hash);
    }
    ingest.flush();

    assertStored(world);
    // the runs of the large storages became files, none went through the regular write path
    final long regularSlots =
        large.stream()
            .flatMap(hash -> storages.get(hash).keySet().stream().map(slot -> slotKey(hash, slot)))
            .filter(key -> !storage.fileKeys.contains(Bytes.wrap(key)))
            .count();
    assertThat(regularSlots).isZero();
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

  private void handOverAll(final World world, final Random random) {
    for (int range = 0; range < RANGES; range++) {
      handOverRange(world, range, random);
    }
    for (final Map.Entry<Bytes32, TreeMap<Bytes32, Bytes32>> account : world.storage.entrySet()) {
      handOverStorage(account.getKey(), account.getValue(), random);
    }
  }

  // returns once the thread waits, or has ended
  private static void awaitParked(final Thread thread) {
    final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    while (System.nanoTime() < deadline) {
      final Thread.State state = thread.getState();
      if (state == Thread.State.WAITING
          || state == Thread.State.TIMED_WAITING
          || state == Thread.State.TERMINATED) {
        return;
      }
      Thread.onSpinWait();
    }
  }

  private void handOver(
      final Bytes32 account,
      final Object stream,
      final List<Bytes32> slots,
      final Map<Bytes32, Bytes32> values) {
    ingest.storage(account, stream, slots, slots.stream().map(values::get).toList());
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

  // as the sync storage removes a storage: the slots it reads
  private void deleteSlots(final Bytes32 account) {
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    storage
        .streamKeys(ACCOUNT_STORAGE_STORAGE)
        .filter(key -> Bytes.wrap(key, 0, Bytes32.SIZE).equals(account))
        .forEach(key -> tx.remove(ACCOUNT_STORAGE_STORAGE, key));
    tx.commit();
  }

  private long slotsOf(final Bytes32 account) {
    return storage
        .streamKeys(ACCOUNT_STORAGE_STORAGE)
        .filter(key -> Bytes.wrap(key, 0, Bytes32.SIZE).equals(account))
        .count();
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
      return new World(accounts, storage, rangeLimits());
    }

    // the first accounts have storage of the same size
    static World sized(
        final Random random, final int accountCount, final int storageCount, final int slotCount) {
      final TreeMap<Bytes32, Bytes> accounts = new TreeMap<>();
      final TreeMap<Bytes32, TreeMap<Bytes32, Bytes32>> storage = new TreeMap<>();
      for (int i = 0; i < accountCount; i++) {
        final Bytes32 hash = Bytes32.random(random);
        accounts.put(hash, Bytes.random(70, random));
        if (i < storageCount) {
          final TreeMap<Bytes32, Bytes32> slots = new TreeMap<>();
          for (int s = 0; s < slotCount; s++) {
            slots.put(Bytes32.random(random), Bytes32.random(random));
          }
          storage.put(hash, slots);
        }
      }
      return new World(accounts, storage, rangeLimits());
    }

    private static List<Bytes32> rangeLimits() {
      return List.copyOf(
          RangeManager.generateRanges(RangeManager.MIN_RANGE, RangeManager.MAX_RANGE, RANGES)
              .values());
    }
  }

  // an in-memory storage whose sorted files insist on ascending keys, as the RocksDB ones do
  private static final class RecordingStorage extends SegmentedInMemoryKeyValueStorage {
    private final List<SegmentIdentifier> files = Collections.synchronizedList(new ArrayList<>());
    // the keys stored through sorted files
    private final Set<Bytes> fileKeys = ConcurrentHashMap.newKeySet();
    private final List<SegmentIdentifier> rolledBack =
        Collections.synchronizedList(new ArrayList<>());
    private final AtomicInteger started = new AtomicInteger();
    private final CountDownLatch rollback = new CountDownLatch(1);
    private final CountDownLatch committing = new CountDownLatch(1);
    private volatile CountDownLatch commits = new CountDownLatch(0);
    private volatile Error commitError;

    void holdCommits() {
      commits = new CountDownLatch(1);
    }

    void releaseCommits() {
      commits.countDown();
    }

    void failCommitsWith(final Error error) {
      commitError = error;
    }

    void awaitRollback() {
      await(rollback);
    }

    void awaitCommitting() {
      await(committing);
    }

    private static void await(final CountDownLatch latch) {
      try {
        latch.await(10, TimeUnit.SECONDS);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    @Override
    public SortedWriter startSortedWriter(final SegmentIdentifier segment) {
      final SegmentedKeyValueStorageTransaction tx = startTransaction();
      started.incrementAndGet();
      return new SortedWriter() {
        private final List<Bytes> keys = new ArrayList<>();
        private byte[] last;

        @Override
        public void put(final byte[] key, final byte[] value) {
          assertThat(last == null || Arrays.compareUnsigned(last, key) < 0)
              .as("keys of a sorted file in ascending order")
              .isTrue();
          last = key;
          keys.add(Bytes.wrap(key));
          tx.put(segment, key, value);
        }

        @Override
        public void commit() {
          committing.countDown();
          // a commit cannot be interrupted, as with the RocksDB files
          Uninterruptibles.awaitUninterruptibly(commits);
          final Error error = commitError;
          if (error != null) {
            throw error;
          }
          tx.commit();
          fileKeys.addAll(keys);
          files.add(segment);
        }

        @Override
        public void rollback() {
          tx.rollback();
          rolledBack.add(segment);
          rollback.countDown();
        }
      };
    }
  }
}
