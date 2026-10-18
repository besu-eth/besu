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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.eth.sync.snap2.storage.Accounts;
import org.hyperledger.besu.ethereum.eth.sync.snap2.storage.SyncStateStorage;
import org.hyperledger.besu.ethereum.eth.sync.snap2.storage.SyncStorageFixture;
import org.hyperledger.besu.ethereum.eth.sync.snap2.storage.SyncStorageFixture.Format;
import org.hyperledger.besu.ethereum.eth.sync.snap2.storage.SyncStorageFixture.Storages;
import org.hyperledger.besu.ethereum.eth.sync.snap2.storage.SyncStorageFixture.World;
import org.hyperledger.besu.ethereum.trie.patricia.StoredMerklePatriciaTrie;

import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.function.Function;

import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

@Timeout(60)
class StorageTrieUpdaterTest {

  private Storages storages = SyncStorageFixture.create(Format.BONSAI);

  @ParameterizedTest
  @EnumSource(Format.class)
  void appliesTheSlotsAMoveChangedToTheTrie(final Format format) {
    storages = SyncStorageFixture.create(format);
    final World world = SyncStorageFixture.randomWorld(new Random(8), 80);
    final Bytes32 account = largestStorage(world);
    final Bytes32 root = downloaded(world, account);
    final StorageTrieUpdater updater =
        new StorageTrieUpdater(storages.sync(), StorageTrieUpdaterTest::noMoves, hash -> false, 2);

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
    final StorageTrieUpdater updater =
        new StorageTrieUpdater(storages.sync(), StorageTrieUpdaterTest::noMoves, hash -> false, 2);

    final Map<Bytes32, UInt256> first = move(world, account, 1);
    final Map<Bytes32, UInt256> second = move(world, account, 2);
    updater.changed(account, Hash.wrap(root), first);
    updater.changed(account, Accounts.UNKNOWN_STORAGE_ROOT, second);
    final Set<Bytes32> changedSlots = new HashSet<>(first.keySet());
    changedSlots.addAll(second.keySet());
    assertThat(updater.updated()).isZero();
    assertThat(updater.pendingStorages()).isOne();
    assertThat(updater.pendingSlots()).isEqualTo(changedSlots.size());
    updater.finish();

    assertThat(updater.updated()).isOne();
    assertStorageTrie(world, account);
  }

  // past the memory budget an update starts at once, and the next move waits for its root
  @Test
  void appliesTheMovesOfAStorageOneAfterTheOther() {
    final World world = SyncStorageFixture.randomWorld(new Random(9), 80);
    final Bytes32 account = largestStorage(world);
    final Bytes32 root = downloaded(world, account);
    final SyncStateStorage storage = spy(storages.sync());
    final CountDownLatch reading = new CountDownLatch(1);
    final CountDownLatch moved = new CountDownLatch(1);
    doAnswer(
            invocation -> {
              // the first update reads its trie once the second move is recorded
              reading.countDown();
              Uninterruptibles.awaitUninterruptibly(moved);
              return invocation.callRealMethod();
            })
        .when(storage)
        .storageTrieNode(any(), any(), any());
    final StorageTrieUpdater updater =
        new StorageTrieUpdater(storage, StorageTrieUpdaterTest::noMoves, hash -> false, 2, 0);

    updater.changed(account, Hash.wrap(root), move(world, account, 1));
    Uninterruptibles.awaitUninterruptibly(reading);
    // the account has an unknown root until the first update gives it
    updater.changed(account, Accounts.UNKNOWN_STORAGE_ROOT, move(world, account, 2));
    moved.countDown();
    updater.finish();

    assertThat(updater.updated()).isEqualTo(2);
    assertStorageTrie(world, account);
  }

  @Test
  void leavesAStorageWithoutTrieToTheFinalBuild() {
    final World world = SyncStorageFixture.randomWorld(new Random(10), 80);
    final Bytes32 account = largestStorage(world);
    world.writeFlat(storages.sync());
    final StorageTrieUpdater updater =
        new StorageTrieUpdater(storages.sync(), StorageTrieUpdaterTest::noMoves, hash -> false, 1);

    updater.changed(account, Hash.wrap(world.storageRoot(account)), move(world, account, 1));
    // a second move finds the root unknown
    updater.changed(account, Accounts.UNKNOWN_STORAGE_ROOT, move(world, account, 2));
    updater.finish();

    assertThat(updater.updated()).isZero();
    assertThat(storageRoot(account)).isEqualTo(Accounts.UNKNOWN_STORAGE_ROOT);
  }

  private static Bytes32 largestStorage(final World world) {
    return world.storage.entrySet().stream()
        .max(Comparator.comparingInt(entry -> entry.getValue().size()))
        .orElseThrow()
        .getKey();
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
