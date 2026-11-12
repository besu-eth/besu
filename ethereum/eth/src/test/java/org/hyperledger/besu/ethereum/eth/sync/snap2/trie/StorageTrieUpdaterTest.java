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
package org.hyperledger.besu.ethereum.eth.sync.snap2.trie;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.eth.sync.snap2.storage.Accounts;
import org.hyperledger.besu.ethereum.eth.sync.snap2.storage.SyncStateStorage;
import org.hyperledger.besu.ethereum.eth.sync.snap2.storage.SyncStorageFixture;
import org.hyperledger.besu.ethereum.eth.sync.snap2.storage.SyncStorageFixture.Format;
import org.hyperledger.besu.ethereum.eth.sync.snap2.storage.SyncStorageFixture.Storages;
import org.hyperledger.besu.ethereum.eth.sync.snap2.storage.SyncStorageFixture.World;
import org.hyperledger.besu.ethereum.trie.patricia.StoredMerklePatriciaTrie;
import org.hyperledger.besu.plugin.services.exception.StorageException;

import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Predicate;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/** Runs without workers: every update is applied on the test thread, by finish or applyNext. */
class StorageTrieUpdaterTest {

  private static final int NO_BUDGET = Integer.MAX_VALUE;

  // Bonsai, unless a test runs on each format
  private Storages storages = SyncStorageFixture.create(Format.BONSAI);

  @ParameterizedTest
  @EnumSource(Format.class)
  void appliesTheSlotsAMoveChangedToTheTrie(final Format format) {
    storages = SyncStorageFixture.create(format);
    final World world = SyncStorageFixture.randomWorld(new Random(8), 80);
    final Bytes32 account = largestStorage(world);
    final Bytes32 root = downloaded(world, account);
    final StorageTrieUpdater updater = updater(storages.sync(), NO_BUDGET);

    updater.changed(account, Hash.wrap(root), move(world, account, 1));
    updater.finish();

    assertThat(updater.updated()).isOne();
    assertStorageTrie(world, account);
  }

  // the moves are applied at the end, once per storage
  @Test
  void mergesTheMovesOfAStorageIntoOneUpdate() {
    final World world = SyncStorageFixture.randomWorld(new Random(11), 80);
    final Bytes32 account = largestStorage(world);
    final Bytes32 root = downloaded(world, account);
    final StorageTrieUpdater updater = updater(storages.sync(), NO_BUDGET);
    final Map<Bytes32, UInt256> first = move(world, account, 1);
    final Map<Bytes32, UInt256> second = move(world, account, 2);

    updater.changed(account, Hash.wrap(root), first);
    updater.changed(account, Accounts.UNKNOWN_STORAGE_ROOT, second);

    final Set<Bytes32> changedSlots = new HashSet<>(first.keySet());
    changedSlots.addAll(second.keySet());
    assertThat(updater.applyNext()).as("within the budget, updates wait for the end").isFalse();
    assertThat(updater.pendingStorages()).isOne();
    assertThat(updater.pendingSlots()).isEqualTo(changedSlots.size());
    updater.finish();
    assertThat(updater.updated()).isOne();
    assertStorageTrie(world, account);
  }

  @Test
  void startsAnUpdatePastTheMemoryBudget() {
    final World world = SyncStorageFixture.randomWorld(new Random(12), 80);
    final Bytes32 account = largestStorage(world);
    final Bytes32 root = downloaded(world, account);
    final Map<Bytes32, UInt256> changes = move(world, account, 1);
    final StorageTrieUpdater updater = updater(storages.sync(), changes.size() - 1);

    updater.changed(account, Hash.wrap(root), changes);

    assertThat(updater.applyNext()).isTrue();
    assertThat(updater.pendingStorages()).isZero();
    assertThat(updater.updated()).isOne();
    assertStorageTrie(world, account);
  }

  // a move during an update records the next one, which starts from the root the first gives
  @Test
  void appliesTheMovesOfAStorageOneAfterTheOther() {
    final World world = SyncStorageFixture.randomWorld(new Random(9), 80);
    final Bytes32 account = largestStorage(world);
    final Bytes32 root = downloaded(world, account);
    final AtomicReference<StorageTrieUpdater> updater = new AtomicReference<>();
    final AtomicBoolean startedTwice = new AtomicBoolean();
    updater.set(
        updater(
            onTheFirstRead(
                () -> {
                  // the account has an unknown root until the first update gives it
                  updater
                      .get()
                      .changed(account, Accounts.UNKNOWN_STORAGE_ROOT, move(world, account, 2));
                  startedTwice.set(updater.get().applyNext());
                }),
            0));

    updater.get().changed(account, Hash.wrap(root), move(world, account, 1));
    assertThat(updater.get().applyNext()).isTrue();

    assertThat(startedTwice).as("one update of a storage at a time").isFalse();
    assertThat(updater.get().updated()).isOne();
    assertThat(updater.get().pendingStorages()).isOne();
    // the next update is still to come
    assertThat(storageRoot(account)).isEqualTo(Accounts.UNKNOWN_STORAGE_ROOT);
    updater.get().finish();
    assertThat(updater.get().updated()).isEqualTo(2);
    assertStorageTrie(world, account);
  }

  // the move during the failed update has no root to start from, the final build makes it
  @Test
  void skipsTheNextUpdateOfAStorageWhoseUpdateFailed() {
    final World world = SyncStorageFixture.randomWorld(new Random(13), 80);
    final Bytes32 account = largestStorage(world);
    final Bytes32 root = downloaded(world, account);
    final AtomicReference<StorageTrieUpdater> updater = new AtomicReference<>();
    updater.set(
        updater(
            onTheFirstRead(
                () -> {
                  updater
                      .get()
                      .changed(account, Accounts.UNKNOWN_STORAGE_ROOT, move(world, account, 2));
                  throw new StorageException("disk failure");
                }),
            0));

    updater.get().changed(account, Hash.wrap(root), move(world, account, 1));
    assertThat(updater.get().applyNext()).isTrue();
    assertThat(updater.get().pendingStorages()).isOne();
    updater.get().finish();

    assertThat(updater.get().updated()).isZero();
    assertThat(storageRoot(account)).isEqualTo(Accounts.UNKNOWN_STORAGE_ROOT);
  }

  // once one update sees the sync stopped, no other starts and the roots stay unknown
  @Test
  void leavesTheRootsUnknownOnceTheSyncStopped() {
    final World world = SyncStorageFixture.randomWorld(new Random(19), 80);
    final List<Bytes32> accounts = largestStorages(world, 2);
    final Bytes32 firstRoot = downloaded(world, accounts.get(0));
    final Bytes32 secondRoot = downloaded(world, accounts.get(1));
    final StorageTrieUpdater updater =
        new StorageTrieUpdater(storages.sync(), action -> false, hash -> false, 0, 0);
    updater.changed(accounts.get(0), Hash.wrap(firstRoot), move(world, accounts.get(0), 1));
    updater.changed(accounts.get(1), Hash.wrap(secondRoot), move(world, accounts.get(1), 1));

    assertThat(updater.applyNext()).isTrue();

    assertThat(updater.pendingStorages()).isZero();
    assertThat(updater.applyNext()).isFalse();
    assertThat(updater.updated()).isZero();
    assertThat(accounts)
        .allSatisfy(
            account -> assertThat(storageRoot(account)).isEqualTo(Accounts.UNKNOWN_STORAGE_ROOT));
  }

  // a move after the stop is not recorded, and a stop waits a minute at most: the update in
  // progress commits no node and gives no root
  @Test
  void writesNothingOnceStoppedDuringAnUpdate() {
    final World world = SyncStorageFixture.randomWorld(new Random(20), 80);
    final Bytes32 account = largestStorage(world);
    final Bytes32 root = downloaded(world, account);
    final AtomicReference<StorageTrieUpdater> updater = new AtomicReference<>();
    updater.set(
        updater(
            onTheFirstRead(
                () -> {
                  updater.get().stop();
                  updater
                      .get()
                      .changed(account, Accounts.UNKNOWN_STORAGE_ROOT, move(world, account, 2));
                }),
            0));

    updater.get().changed(account, Hash.wrap(root), move(world, account, 1));
    final Bytes32 movedRoot = world.storageRoot(account);
    assertThat(updater.get().applyNext()).isTrue();

    assertThat(updater.get().updated()).isZero();
    assertThat(storageRoot(account)).isEqualTo(Accounts.UNKNOWN_STORAGE_ROOT);
    assertThat(storages.sync().hasStorageTrie(account, movedRoot)).isFalse();
  }

  // the writer commits on its own every few megabytes, so a stop is checked before each write
  @Test
  void writesNoMoreNodesOnceStoppedDuringACommit() {
    final SyncStateStorage.TrieWriter writer = spy(storages.sync().trieWriter());
    final StorageTrieUpdater updater = stoppedOnTheNode(writer, location -> true);

    assertThat(updater.applyNext()).isTrue();

    verify(writer).storageNode(any(), any(), any(), any());
    verify(writer, never()).commit();
    verify(writer).abort();
    assertThat(updater.updated()).isZero();
  }

  @Test
  void commitsNothingOnceStoppedOnTheRootNode() {
    final SyncStateStorage.TrieWriter writer = spy(storages.sync().trieWriter());
    final StorageTrieUpdater updater = stoppedOnTheNode(writer, Bytes::isEmpty);

    assertThat(updater.applyNext()).isTrue();

    verify(writer, never()).commit();
    verify(writer).abort();
  }

  @Test
  void needsAWorker() {
    assertThatThrownBy(
            () ->
                new StorageTrieUpdater(
                    storages.sync(), StorageTrieUpdaterTest::noMoves, hash -> false, 0))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void ignoresAStorageLeftToTheFinalBuild() {
    final World world = SyncStorageFixture.randomWorld(new Random(15), 80);
    final Bytes32 account = largestStorage(world);
    final Bytes32 root = downloaded(world, account);
    final Predicate<Bytes32> leftToTheFinalBuild = account::equals;
    final StorageTrieUpdater updater =
        new StorageTrieUpdater(
            storages.sync(), StorageTrieUpdaterTest::noMoves, leftToTheFinalBuild, 0, NO_BUDGET);

    updater.changed(account, Hash.wrap(root), move(world, account, 1));

    assertThat(updater.pendingStorages()).isZero();
    updater.finish();
    assertThat(updater.updated()).isZero();
  }

  // an unknown root with no update to wait for has no trie to change
  @Test
  void ignoresAMoveOnAnUnknownRoot() {
    final World world = SyncStorageFixture.randomWorld(new Random(16), 80);
    final Bytes32 account = largestStorage(world);
    downloaded(world, account);
    final StorageTrieUpdater updater = updater(storages.sync(), NO_BUDGET);

    updater.changed(account, Accounts.UNKNOWN_STORAGE_ROOT, move(world, account, 1));

    assertThat(updater.pendingStorages()).isZero();
  }

  @Test
  void leavesAStorageWithoutTrieToTheFinalBuild() {
    final World world = SyncStorageFixture.randomWorld(new Random(10), 80);
    final Bytes32 account = largestStorage(world);
    world.writeFlat(storages.sync());
    final StorageTrieUpdater updater = updater(storages.sync(), NO_BUDGET);

    updater.changed(account, Hash.wrap(world.storageRoot(account)), move(world, account, 1));
    // a second move finds the root unknown
    updater.changed(account, Accounts.UNKNOWN_STORAGE_ROOT, move(world, account, 2));
    updater.finish();

    assertThat(updater.updated()).isZero();
    assertThat(storageRoot(account)).isEqualTo(Accounts.UNKNOWN_STORAGE_ROOT);
  }

  @Test
  void dropsTheChangesWaitingOnStop() {
    final World world = SyncStorageFixture.randomWorld(new Random(17), 80);
    final Bytes32 account = largestStorage(world);
    final Bytes32 root = downloaded(world, account);
    final StorageTrieUpdater updater = updater(storages.sync(), NO_BUDGET);
    updater.changed(account, Hash.wrap(root), move(world, account, 1));

    updater.stop();
    updater.changed(account, Hash.wrap(root), move(world, account, 2));

    assertThat(updater.pendingStorages()).isZero();
    assertThat(updater.pendingSlots()).isZero();
    assertThat(updater.applyNext()).isFalse();
    assertThat(updater.updated()).isZero();
  }

  @Test
  void writesNoRootForAnAccountAMoveRemoved() {
    final World world = SyncStorageFixture.randomWorld(new Random(18), 80);
    final Bytes32 account = largestStorage(world);
    final Bytes32 root = downloaded(world, account);
    final StorageTrieUpdater updater = updater(storages.sync(), NO_BUDGET);
    updater.changed(account, Hash.wrap(root), move(world, account, 1));
    final SyncStateStorage.Batch batch = storages.sync().batch();
    batch.removeAccount(account);
    batch.commit();

    updater.finish();

    assertThat(updater.updated()).isOne();
    assertThat(storages.sync().getAccount(account)).isEmpty();
  }

  @Test
  void abortsTheRootBatchOfAFailedWrite() {
    final World world = SyncStorageFixture.randomWorld(new Random(21), 80);
    final Bytes32 account = largestStorage(world);
    final Bytes32 root = downloaded(world, account);
    final SyncStateStorage storage = spy(storages.sync());
    final SyncStateStorage.Batch batch = mock(SyncStateStorage.Batch.class);
    doReturn(batch).when(storage).batch();
    doThrow(new IllegalStateException("write failed")).when(batch).putAccount(any(), any());
    final StorageTrieUpdater updater = updater(storage, 0);
    updater.changed(account, Hash.wrap(root), move(world, account, 1));

    assertThat(updater.applyNext()).isTrue();
    // the error left the update to the final build
    assertThat(updater.updated()).isZero();
    verify(batch, never()).commit();
    verify(batch).abort();
  }

  private StorageTrieUpdater updater(final SyncStateStorage storage, final int budget) {
    return new StorageTrieUpdater(
        storage, StorageTrieUpdaterTest::noMoves, hash -> false, 0, budget);
  }

  // runs an action on an update's first read below the root, as a move or a stop during it does
  private SyncStateStorage onTheFirstRead(final Runnable action) {
    final SyncStateStorage storage = spy(storages.sync());
    final AtomicBoolean read = new AtomicBoolean();
    doAnswer(
            invocation -> {
              final Bytes location = invocation.getArgument(1);
              if (!location.isEmpty() && read.compareAndSet(false, true)) {
                action.run();
              }
              return invocation.callRealMethod();
            })
        .when(storage)
        .storageTrieNode(any(), any(), any());
    return storage;
  }

  // an updater with an update to apply, stopped as the update writes a node at such a location
  private StorageTrieUpdater stoppedOnTheNode(
      final SyncStateStorage.TrieWriter writer, final Predicate<Bytes> atLocation) {
    final World world = SyncStorageFixture.randomWorld(new Random(20), 80);
    final Bytes32 account = largestStorage(world);
    final Bytes32 root = downloaded(world, account);
    final SyncStateStorage storage = spy(storages.sync());
    doReturn(writer).when(storage).trieWriter();
    final StorageTrieUpdater updater = updater(storage, 0);
    doAnswer(
            invocation -> {
              if (atLocation.test(invocation.getArgument(1))) {
                updater.stop();
              }
              return invocation.callRealMethod();
            })
        .when(writer)
        .storageNode(any(), any(), any(), any());
    updater.changed(account, Hash.wrap(root), move(world, account, 1));
    return updater;
  }

  private static Bytes32 largestStorage(final World world) {
    return largestStorages(world, 1).getFirst();
  }

  private static List<Bytes32> largestStorages(final World world, final int count) {
    return world.storage.entrySet().stream()
        .sorted(
            Comparator.<Map.Entry<Bytes32, TreeMap<Bytes32, UInt256>>>comparingInt(
                    entry -> entry.getValue().size())
                .reversed())
        .limit(count)
        .map(Map.Entry::getKey)
        .toList();
  }

  // the storage as the download leaves it: its slots, its trie and its root in the account
  private Bytes32 downloaded(final World world, final Bytes32 account) {
    world.writeFlat(storages.sync());
    final Bytes32 root = world.storageRoot(account);
    final SyncStateStorage.TrieWriter writer = storages.sync().trieWriter();
    final StoredMerklePatriciaTrie<Bytes32, Bytes> trie =
        new StoredMerklePatriciaTrie<>(
            (location, hash) -> Optional.empty(), Function.identity(), Function.identity());
    world.storageLeaves(account).forEach(trie::put);
    trie.commit((location, hash, node) -> writer.storageNode(account, location, hash, node));
    writer.commit();
    final SyncStateStorage.Batch batch = storages.sync().batch();
    batch.putAccount(account, Accounts.encode(world.account(account)));
    batch.commit();
    return root;
  }

  // changes, adds and removes slots as a move does, leaving the root unknown; returns the changes
  private Map<Bytes32, UInt256> move(final World world, final Bytes32 account, final int seed) {
    final TreeMap<Bytes32, UInt256> slots = world.storage.get(account);
    final Map<Bytes32, UInt256> changes = new HashMap<>();
    changes.put(slots.firstKey(), UInt256.valueOf(7 + seed));
    changes.put(slots.lastKey(), UInt256.ZERO);
    changes.put(Bytes32.fromHexString("0x" + "ab".repeat(31) + "0" + seed), UInt256.valueOf(seed));
    final SyncStateStorage.Batch batch = storages.sync().batch();
    changes.forEach(
        (slot, value) -> {
          if (value.isZero()) {
            slots.remove(slot);
            batch.removeStorage(account, slot);
          } else {
            slots.put(slot, value);
            batch.putStorage(account, slot, value);
          }
        });
    batch.putAccount(
        account,
        Accounts.encode(
            Accounts.withStorageRoot(world.accounts.get(account), Accounts.UNKNOWN_STORAGE_ROOT)));
    batch.commit();
    return changes;
  }

  private Hash storageRoot(final Bytes32 account) {
    return Accounts.decode(storages.sync().getAccount(account).orElseThrow()).getStorageRoot();
  }

  // the account has the root of its slots, whose trie is stored whole
  private void assertStorageTrie(final World world, final Bytes32 account) {
    final Bytes32 root = world.storageRoot(account);
    assertThat(storageRoot(account).getBytes()).isEqualTo(root);
    assertThat(storages.storageTrie(account, root).entriesFrom(Bytes32.ZERO, Integer.MAX_VALUE))
        .isEqualTo(world.storageLeaves(account));
  }

  // no pivot move runs in these tests
  private static boolean noMoves(final Runnable action) {
    action.run();
    return true;
  }
}
