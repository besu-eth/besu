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
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.eth.sync.snap2.storage.Accounts;
import org.hyperledger.besu.ethereum.eth.sync.snap2.storage.SyncStateStorage;
import org.hyperledger.besu.ethereum.eth.sync.snap2.storage.SyncStorageFixture;
import org.hyperledger.besu.ethereum.eth.sync.snap2.storage.SyncStorageFixture.Format;
import org.hyperledger.besu.ethereum.eth.sync.snap2.storage.SyncStorageFixture.Storages;
import org.hyperledger.besu.ethereum.eth.sync.snap2.storage.SyncStorageFixture.World;
import org.hyperledger.besu.ethereum.trie.MerkleTrie;
import org.hyperledger.besu.ethereum.trie.RangeManager;
import org.hyperledger.besu.ethereum.trie.common.PatriciaTrieAccountValue;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import com.google.common.util.concurrent.MoreExecutors;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class WorldStateTrieBuilderTest {

  private static final int FIXES_PER_COMMIT = 3;

  @ParameterizedTest
  @EnumSource(Format.class)
  void buildsTheStateTrieFromTheFlatState(final Format format) {
    final Storages storages = SyncStorageFixture.create(format);
    final World world = SyncStorageFixture.randomWorld(new Random(1), 2000);
    world.writeFlat(storages.sync());

    final WorldStateTrieBuilder.Result result = builder(storages.sync(), hash -> false).build();

    assertThat(result.root()).isEqualTo(world.root());
    assertThat(Hash.hash(result.rootNode()).getBytes()).isEqualTo(result.root());
    assertThat(result.accounts()).isEqualTo(world.accounts.size());
    assertThat(result.slots())
        .isEqualTo(world.storage.values().stream().mapToLong(slots -> slots.size()).sum());

    // the written nodes can be read back by the storage format
    final MerkleTrie<Bytes32, Bytes> accountTrie = storages.accountTrie(result.root());
    world.accountLeaves().forEach((hash, leaf) -> assertThat(accountTrie.get(hash)).contains(leaf));
    world.storage.keySet().forEach(account -> assertStorageTrieStored(storages, world, account));
    assertFlatStorageRoots(storages.sync(), world);
  }

  @ParameterizedTest
  @EnumSource(Format.class)
  void keepsTheStorageTriesWrittenAndRebuildsTheChangedOnes(final Format format) {
    final Storages storages = SyncStorageFixture.create(format);
    final World world = SyncStorageFixture.randomWorld(new Random(7), 500);
    world.writeFlat(storages.sync());
    builder(storages.sync(), hash -> false).build();

    // a slot changes after its trie was written, as in a pivot move
    final Bytes32 changed = world.storage.firstKey();
    changeASlot(storages, world, changed);
    setStorageRoot(storages, changed, Accounts.UNKNOWN_STORAGE_ROOT);

    final WorldStateTrieBuilder.Result result = builder(storages.sync(), hash -> false).build();

    assertThat(result.root()).isEqualTo(world.root());
    assertThat(result.keptStorageTries()).isEqualTo(world.storage.size() - 1);
    assertThat(result.slots()).isEqualTo(world.storage.get(changed).size());
  }

  @ParameterizedTest
  @EnumSource(Format.class)
  void rebuildsTheStorageTriesTheDownloadLeft(final Format format) {
    final Storages storages = SyncStorageFixture.create(format);
    final World world = SyncStorageFixture.randomWorld(new Random(9), 500);
    world.writeFlat(storages.sync());
    builder(storages.sync(), hash -> false).build();

    // a slot of a storage the download completed without its trie, its root still known
    final Bytes32 left = world.storage.firstKey();
    changeASlot(storages, world, left);

    final WorldStateTrieBuilder.Result result = builder(storages.sync(), left::equals).build();

    assertThat(result.root()).isEqualTo(world.root());
    assertThat(result.slots()).isEqualTo(world.storage.get(left).size());
  }

  @ParameterizedTest
  @EnumSource(Format.class)
  void rebuildsAKnownStorageTrieWhoseRootNodeIsMissing(final Format format) {
    final Storages storages = SyncStorageFixture.create(format);
    final World world = SyncStorageFixture.randomWorld(new Random(11), 500);
    world.writeFlat(storages.sync());
    builder(storages.sync(), hash -> false).build();

    // a storage whose flat root is right, but whose trie at that root was never stored
    final Bytes32 lost = world.storage.firstKey();
    changeASlot(storages, world, lost);
    setStorageRoot(storages, lost, Hash.wrap(world.storageRoot(lost)));

    final WorldStateTrieBuilder.Result result = builder(storages.sync(), hash -> false).build();

    assertThat(result.root()).isEqualTo(world.root());
    assertThat(result.slots()).isEqualTo(world.storage.get(lost).size());
    assertStorageTrieStored(storages, world, lost);
  }

  @Test
  void removesTheSlotsOfAnAccountMissingFromTheFlatState() {
    final Storages storages = SyncStorageFixture.create(Format.BONSAI);
    final World world = SyncStorageFixture.randomWorld(new Random(12), 200);
    world.writeFlat(storages.sync());
    // slots left by an account the flat state no longer holds
    final Bytes32 missing = Bytes32.fromHexString("0x" + "5a".repeat(32));
    assertThat(world.accounts).doesNotContainKey(missing);
    final SyncStateStorage.Batch batch = storages.sync().batch();
    batch.putStorage(missing, Bytes32.ZERO, UInt256.ONE);
    batch.putStorage(missing, Bytes32.fromHexString("0x" + "01".repeat(32)), UInt256.valueOf(2));
    batch.commit();
    assertThat(slotsWithoutAccount(storages.sync())).hasSize(2);

    final WorldStateTrieBuilder.Result result = builder(storages.sync(), hash -> false).build();

    assertThat(result.root()).isEqualTo(world.root());
    assertThat(slotsWithoutAccount(storages.sync())).isEmpty();
  }

  @ParameterizedTest
  @EnumSource(Format.class)
  void buildsSmallStates(final Format format) {
    for (int size : new int[] {0, 1, 2, 5}) {
      final Storages storages = SyncStorageFixture.create(format);
      final World world = SyncStorageFixture.randomWorld(new Random(size), size);
      world.writeFlat(storages.sync());

      final WorldStateTrieBuilder.Result result = builder(storages.sync(), hash -> false).build();

      assertThat(result.root()).isEqualTo(world.root());
      assertThat(Hash.hash(result.rootNode()).getBytes()).isEqualTo(result.root());
    }
  }

  @Test
  void commitsTheFixesInFullBatches() {
    final Storages storages = SyncStorageFixture.create(Format.BONSAI);
    final World world = SyncStorageFixture.randomWorld(new Random(5), 2000);
    world.writeFlat(storages.sync());
    final SyncStateStorage storage = spy(storages.sync());

    final WorldStateTrieBuilder.Result result = builder(storage, hash -> false).build();

    assertThat(result.root()).isEqualTo(world.root());
    // every flat root is stale, so each partition fixes all its accounts
    final long batches =
        world.accounts.keySet().stream()
            .collect(Collectors.groupingBy(hash -> hash.get(0), Collectors.counting()))
            .values()
            .stream()
            .mapToLong(fixes -> (fixes + FIXES_PER_COMMIT - 1) / FIXES_PER_COMMIT)
            .sum();
    verify(storage, times((int) batches)).batch();
    assertFlatStorageRoots(storage, world);
  }

  @Test
  void commitsTheStorageNodesBeforeTheRootsThatPointToThem() {
    final Storages storages = SyncStorageFixture.create(Format.BONSAI);
    final World world = SyncStorageFixture.randomWorld(new Random(8), 2000);
    world.writeFlat(storages.sync());
    final SyncStateStorage storage = spy(storages.sync());
    final List<Bytes32> checked = new ArrayList<>();
    doAnswer(
            invocation -> {
              final SyncStateStorage.Batch batch = spy(storages.sync().batch());
              final List<Bytes32> fixed = new ArrayList<>();
              doAnswer(
                      put -> {
                        fixed.add(put.getArgument(0));
                        put.callRealMethod();
                        return null;
                      })
                  .when(batch)
                  .putAccount(any(), any());
              // a crash right after this commit keeps these roots: their tries must be stored
              doAnswer(
                      commit -> {
                        commit.callRealMethod();
                        fixed.stream()
                            .filter(world.storage::containsKey)
                            .forEach(
                                account -> {
                                  assertStorageTrieStored(storages, world, account);
                                  checked.add(account);
                                });
                        return null;
                      })
                  .when(batch)
                  .commit();
              return batch;
            })
        .when(storage)
        .batch();

    assertThat(builder(storage, hash -> false).build().root()).isEqualTo(world.root());
    assertThat(checked).hasSize(world.storage.size());
  }

  @Test
  void throwsWhenCancelledOnceThePartitionsAreDone() {
    final Storages storages = SyncStorageFixture.create(Format.BONSAI);
    final World world = SyncStorageFixture.randomWorld(new Random(4), 100);
    world.writeFlat(storages.sync());
    final SyncStateStorage storage = spy(storages.sync());
    final WorldStateTrieBuilder builder = builder(storage, hash -> false);
    // cancelled once the last partition read its accounts, past its last check
    doAnswer(
            invocation -> {
              invocation.callRealMethod();
              final Bytes32 from = invocation.getArgument(0);
              if ((from.get(0) & 0xff) == 255) {
                builder.cancel();
              }
              return null;
            })
        .when(storage)
        .scan(any(), any(), any());

    assertThatThrownBy(builder::build).isInstanceOf(CancellationException.class);
  }

  @Test
  void stopsThePartitionsWhenTheBuildFailsOnItsOwnThread() {
    final Storages storages = SyncStorageFixture.create(Format.BONSAI);
    final World world = SyncStorageFixture.randomWorld(new Random(6), 2000);
    world.writeFlat(storages.sync());
    final SyncStateStorage storage = spy(storages.sync());
    // the writer of the top of the account trie, created first, fails on its first node
    final IllegalStateException diskFailure = new IllegalStateException("disk failure");
    final SyncStateStorage.TrieWriter topWriter = mock(SyncStateStorage.TrieWriter.class);
    doThrow(diskFailure).when(topWriter).accountNode(any(), any(), any());
    doReturn(topWriter).doCallRealMethod().when(storage).trieWriter();
    final OnDemandExecutor partitions = new OnDemandExecutor();

    assertThatThrownBy(
            () ->
                new WorldStateTrieBuilder(
                        storage, hash -> false, () -> partitions, FIXES_PER_COMMIT)
                    .build())
        .isSameAs(diskFailure);
    // the partitions the build did not wait for run now, as on other threads, and write nothing
    final long ran = partitions.tasks.stream().filter(FutureTask::isDone).count();
    assertThat(ran).isBetween(1L, 255L);
    partitions.tasks.forEach(FutureTask::run);
    world.accounts.keySet().stream()
        .filter(hash -> (hash.get(0) & 0xff) >= ran)
        .forEach(
            hash ->
                assertThat(Accounts.decode(storage.getAccount(hash).orElseThrow()).getStorageRoot())
                    .isEqualTo(Accounts.UNKNOWN_STORAGE_ROOT));
  }

  @Test
  void aFailedPartitionStopsTheOthersAndAbortsItsFixes() {
    final Storages storages = SyncStorageFixture.create(Format.BONSAI);
    final World world = SyncStorageFixture.randomWorld(new Random(3), 2000);
    world.writeFlat(storages.sync());
    final SyncStateStorage storage = spy(storages.sync());
    final List<SyncStateStorage.Batch> batches = new ArrayList<>();
    doAnswer(
            invocation -> {
              final SyncStateStorage.Batch batch = spy(storages.sync().batch());
              batches.add(batch);
              return batch;
            })
        .when(storage)
        .batch();
    // the first partition fails once it read its accounts, its last fixes not committed
    final IllegalStateException diskFailure = new IllegalStateException("disk failure");
    final Set<Integer> completed = new HashSet<>();
    doAnswer(
            invocation -> {
              invocation.callRealMethod();
              final Bytes32 from = invocation.getArgument(0);
              final int prefix = from.get(0) & 0xff;
              if (prefix == 0) {
                throw diskFailure;
              }
              completed.add(prefix);
              return null;
            })
        .when(storage)
        .scan(any(), any(), any());

    assertThatThrownBy(() -> builder(storage, hash -> false).build()).isSameAs(diskFailure);
    // the other partitions stop at their first account, before any fix
    final Set<Integer> withAccounts =
        world.accounts.keySet().stream()
            .map(hash -> hash.get(0) & 0xff)
            .collect(Collectors.toSet());
    assertThat(completed).doesNotContainAnyElementsOf(withAccounts);
    // so the last batch is the failed partition's
    verify(batches.getLast(), never()).commit();
    verify(batches.getLast()).abort();
  }

  // a slot of the account changes in the flat state and in the world, as in a pivot move
  private static void changeASlot(
      final Storages storages, final World world, final Bytes32 account) {
    final Bytes32 slot = world.storage.get(account).firstKey();
    world.storage.get(account).put(slot, UInt256.valueOf(424242));
    final SyncStateStorage.Batch batch = storages.sync().batch();
    batch.putStorage(account, slot, UInt256.valueOf(424242));
    batch.commit();
  }

  private static void setStorageRoot(
      final Storages storages, final Bytes32 account, final Hash storageRoot) {
    final PatriciaTrieAccountValue flat =
        Accounts.decode(storages.sync().getAccount(account).orElseThrow());
    final SyncStateStorage.Batch batch = storages.sync().batch();
    batch.putAccount(account, Accounts.encode(Accounts.withStorageRoot(flat, storageRoot)));
    batch.commit();
  }

  // every slot of the account reads back from its stored storage trie
  private static void assertStorageTrieStored(
      final Storages storages, final World world, final Bytes32 account) {
    final MerkleTrie<Bytes32, Bytes> trie =
        storages.storageTrie(account, world.storageRoot(account));
    world.storageLeaves(account).forEach((slot, leaf) -> assertThat(trie.get(slot)).contains(leaf));
  }

  // every flat account carries the storage root of the world
  private static void assertFlatStorageRoots(final SyncStateStorage storage, final World world) {
    for (final Bytes32 hash : world.accounts.keySet()) {
      final PatriciaTrieAccountValue flat = Accounts.decode(storage.getAccount(hash).orElseThrow());
      assertThat(flat.getStorageRoot().getBytes()).isEqualTo(world.storageRoot(hash));
    }
  }

  // the slots a scan gives outside the accounts, all of which it skips
  private static List<Bytes32> slotsWithoutAccount(final SyncStateStorage storage) {
    final List<Bytes32> slots = new ArrayList<>();
    storage.scan(
        RangeManager.MIN_RANGE,
        RangeManager.MAX_RANGE,
        new SyncStateStorage.ScanVisitor() {
          @Override
          public boolean onAccount(final Bytes32 accountHash, final Bytes account) {
            return false;
          }

          @Override
          public void onSlot(final Bytes32 accountHash, final Bytes32 slotHash, final Bytes value) {
            slots.add(slotHash);
          }
        });
    return slots;
  }

  // the partitions run one after the other on the test thread, with small batches of fixes
  private static WorldStateTrieBuilder builder(
      final SyncStateStorage storage, final Predicate<Bytes32> trieLeft) {
    return new WorldStateTrieBuilder(
        storage, trieLeft, MoreExecutors::newDirectExecutorService, FIXES_PER_COMMIT);
  }

  // runs each task on the test thread once the build waits for it, the others when the test says
  private static final class OnDemandExecutor extends AbstractExecutorService {
    private final List<FutureTask<?>> tasks = new ArrayList<>();

    @Override
    protected <T> RunnableFuture<T> newTaskFor(final Callable<T> callable) {
      final FutureTask<T> task =
          new FutureTask<>(callable) {
            @Override
            public T get() throws InterruptedException, ExecutionException {
              run();
              return super.get();
            }
          };
      tasks.add(task);
      return task;
    }

    @Override
    public void execute(final Runnable command) {}

    @Override
    public void shutdown() {}

    @Override
    public List<Runnable> shutdownNow() {
      return List.of();
    }

    @Override
    public boolean isShutdown() {
      return false;
    }

    @Override
    public boolean isTerminated() {
      return true;
    }

    @Override
    public boolean awaitTermination(final long timeout, final TimeUnit unit) {
      return true;
    }
  }
}
