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
package org.hyperledger.besu.ethereum.trie.immutabletree;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.hyperledger.besu.ethereum.trie.MerkleTrie;
import org.hyperledger.besu.ethereum.trie.MerkleTrieException;
import org.hyperledger.besu.ethereum.trie.patricia.ParallelStoredMerklePatriciaTrie;
import org.hyperledger.besu.ethereum.trie.patricia.StoredMerklePatriciaTrie;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ImmutableTreeCacheTest {

  private static final ForkJoinPool POOL = new ForkJoinPool(4);

  private final Random random = new Random(1);
  private PathBasedNodeStore store;
  private List<Bytes> keys;
  private Bytes32 storedRoot;

  @AfterAll
  static void shutdown() {
    POOL.shutdownNow();
  }

  @BeforeEach
  void setUp() {
    store = new PathBasedNodeStore();
    keys = new ArrayList<>();
    final StoredMerklePatriciaTrie<Bytes, Bytes> besu =
        store.besuTrie(MerkleTrie.EMPTY_TRIE_NODE_HASH);
    for (int i = 0; i < 1000; i++) {
      final Bytes key = bytes(32);
      keys.add(key);
      besu.put(key, bytes(1 + random.nextInt(40)));
    }
    besu.commit(store);
    storedRoot = besu.getRootHash();
    store.resetReads();
  }

  private Bytes bytes(final int size) {
    final byte[] bytes = new byte[size];
    random.nextBytes(bytes);
    return Bytes.wrap(bytes);
  }

  private static ImmutableTreeCache cache(final int pruneAfterBlocks, final long maxBytes) {
    return new ImmutableTreeCache(
        new ImmutableTreeCacheConfig(pruneAfterBlocks, 2, maxBytes, 1, 1000));
  }

  private ImmutableTreeMerkleTrie trie(final ImmutableTreeCache cache, final Bytes32 root) {
    return new ImmutableTreeMerkleTrie(
        cache, TreeKind.STATE, root, store, POOL, ImmutableTreeMerkleTrie.RootListener.NONE);
  }

  @Test
  void openingARootLoadsTheRootNodeOnly() {
    final ImmutableTreeCache cache = cache(512, 64 << 20);
    final TreeHandle handle = cache.open(TreeKind.STATE, storedRoot, cache.newSession(store));

    assertThat(store.reads()).isEqualTo(1);
    final BranchTreeNode root = (BranchTreeNode) handle.root();
    for (int i = 0; i < 16; i++) {
      assertThat(root.child(i)).isInstanceOf(StoredTreeNode.class);
    }
    // registered: opening again loads nothing and yields the same tree
    assertThat(cache.open(TreeKind.STATE, storedRoot, cache.newSession(store))).isSameAs(handle);
    assertThat(store.reads()).isEqualTo(1);
    assertThat(cache.stats().cachedOpens()).isEqualTo(1);
    assertThat(cache.stats().storedOpens()).isEqualTo(1);
  }

  @Test
  void missingRootFailsWithTheNodeLocation() {
    final ImmutableTreeCache cache = cache(512, 64 << 20);
    final Bytes32 unknown = Bytes32.random();
    assertThatThrownBy(() -> cache.open(TreeKind.STATE, unknown, cache.newSession(store)))
        .isInstanceOf(MerkleTrieException.class)
        .satisfies(
            e -> {
              assertThat(((MerkleTrieException) e).getHash()).isEqualTo(unknown);
              assertThat(((MerkleTrieException) e).getLocation()).isEqualTo(Bytes.EMPTY);
            });
  }

  @Test
  void updatesCopyOnWriteAndRootsCoexist() {
    final ImmutableTreeCache cache = cache(512, 64 << 20);

    final ImmutableTreeMerkleTrie first = trie(cache, storedRoot);
    final ImmutableTreeMerkleTrie second = trie(cache, storedRoot);
    final Bytes value1 = bytes(20);
    final Bytes value2 = bytes(20);
    first.put(keys.get(0), value1);
    first.remove(keys.get(1));
    second.put(keys.get(0), value2);
    second.put(bytes(32), value2);

    final Bytes32 root1 = first.getRootHash();
    final Bytes32 root2 = second.getRootHash();
    final TreeHandle base = cache.lookup(TreeKind.STATE, storedRoot).orElseThrow();
    final TreeHandle handle1 = cache.register(TreeKind.STATE, first.currentRoot());
    final TreeHandle handle2 = cache.register(TreeKind.STATE, second.currentRoot());

    // Both forks and their base are registered and each still reads as before
    assertThat(cache.stats().stateRoots()).isEqualTo(3);
    assertThat(trie(cache, storedRoot).get(keys.get(0)))
        .isEqualTo(store.besuTrie(storedRoot).get(keys.get(0)));
    assertThat(trie(cache, storedRoot).get(keys.get(1))).isPresent();
    assertThat(trie(cache, root1).get(keys.get(0))).contains(value1);
    assertThat(trie(cache, root1).get(keys.get(1))).isEmpty();
    assertThat(trie(cache, root2).get(keys.get(0))).contains(value2);
    assertThat(base.root().hash()).isEqualTo(storedRoot);
    assertThat(handle1.rootHash()).isEqualTo(root1);
    assertThat(handle2.rootHash()).isEqualTo(root2);

    // Roots match Besu's for the same updates
    final StoredMerklePatriciaTrie<Bytes, Bytes> besu = store.besuTrie(storedRoot);
    besu.put(keys.get(0), value1);
    besu.remove(keys.get(1));
    assertThat(root1).isEqualTo(besu.getRootHash());

    // The unchanged part of the tree is shared: the forks reuse the base's untouched children,
    // loaded ones by reference and stored ones through the same compact hash slots
    final BranchTreeNode baseRoot = (BranchTreeNode) base.root();
    final BranchTreeNode forkRoot = (BranchTreeNode) handle1.root();
    int shared = 0;
    for (int i = 0; i < 16; i++) {
      final MaterializedTreeNode loaded = baseRoot.loadedChild(i);
      if (loaded != null
          ? loaded == forkRoot.loadedChild(i)
          : baseRoot.isCompactChild(i) && forkRoot.isCompactChild(i)) {
        shared++;
      }
    }
    assertThat(shared).isGreaterThanOrEqualTo(13);
  }

  @Test
  void registeringAnExistingRootKeepsTheRegisteredTree() {
    final ImmutableTreeCache cache = cache(512, 64 << 20);
    final TreeHandle opened = cache.open(TreeKind.STATE, storedRoot, cache.newSession(store));
    final TreeNode sameContent = TreeSession.create(store, 0).loadRoot(storedRoot);
    assertThat(cache.register(TreeKind.STATE, sameContent)).isSameAs(opened);
    // kinds are separate namespaces
    assertThat(cache.register(TreeKind.STORAGE, sameContent)).isNotSameAs(opened);
  }

  @Test
  void headAndNewPayloadBindingsKeepRootsWhileForksExpire() {
    final ImmutableTreeCache cache = cache(512, 64 << 20);
    cache.advanceBlock(10, false);
    final TreeHandle headRoot = cache.open(TreeKind.STATE, storedRoot, cache.newSession(store));
    cache.setHead(headRoot);

    final ImmutableTreeMerkleTrie payload = trie(cache, storedRoot);
    payload.put(keys.get(3), bytes(8));
    payload.getRootHash();
    final TreeHandle payloadRoot = cache.register(TreeKind.STATE, payload.currentRoot());
    cache.setNewPayload(payloadRoot);

    final ImmutableTreeMerkleTrie fork = trie(cache, storedRoot);
    fork.put(keys.get(4), bytes(8));
    fork.getRootHash();
    final TreeHandle forkRoot = cache.register(TreeKind.STATE, fork.currentRoot());

    cache.advanceBlock(20, false);
    cache.prune();
    assertThat(cache.lookup(TreeKind.STATE, storedRoot)).containsSame(headRoot);
    assertThat(cache.lookup(TreeKind.STATE, payloadRoot.rootHash())).containsSame(payloadRoot);
    assertThat(cache.lookup(TreeKind.STATE, forkRoot.rootHash())).isEmpty();
    assertThat(cache.stats().head()).isEqualTo(storedRoot);
    assertThat(cache.stats().newPayload()).isEqualTo(payloadRoot.rootHash());

    // The payload becomes the head: the old head is a fork now and expires
    cache.setHead(payloadRoot);
    cache.advanceBlock(30, false);
    cache.prune();
    assertThat(cache.lookup(TreeKind.STATE, storedRoot)).isEmpty();
    assertThat(cache.lookup(TreeKind.STATE, payloadRoot.rootHash())).containsSame(payloadRoot);
    assertThat(cache.boundState(TreeRole.HEAD)).containsSame(payloadRoot);
  }

  @Test
  void storageBindingsFollowTheAccountAndExpireWhenIdle() {
    final ImmutableTreeCache cache = cache(16, 64 << 20);
    final Bytes32 account = Bytes32.random();
    final Bytes32 otherAccount = Bytes32.random();
    final TreeHandle storage = cache.open(TreeKind.STORAGE, storedRoot, cache.newSession(store));
    cache.bindStorage(TreeRole.HEAD, account, storage);
    // identical storage in another account shares the tree
    cache.bindStorage(TreeRole.HEAD, otherAccount, storage);
    assertThat(cache.boundStorage(TreeRole.HEAD, otherAccount)).containsSame(storage);

    final ImmutableTreeMerkleTrie update =
        new ImmutableTreeMerkleTrie(
            cache,
            TreeKind.STORAGE,
            storedRoot,
            store,
            null,
            ImmutableTreeMerkleTrie.RootListener.NONE);
    update.put(keys.get(0), bytes(4));
    update.getRootHash();
    final TreeHandle updated = cache.register(TreeKind.STORAGE, update.currentRoot());
    cache.bindStorage(TreeRole.HEAD, account, updated);

    cache.advanceBlock(10, false);
    cache.prune();
    // still bound through the other account
    assertThat(cache.lookup(TreeKind.STORAGE, storedRoot)).containsSame(storage);

    cache.bindStorage(TreeRole.HEAD, otherAccount, null);
    cache.advanceBlock(14, false);
    cache.prune();
    assertThat(cache.lookup(TreeKind.STORAGE, storedRoot)).isEmpty();
    assertThat(cache.lookup(TreeKind.STORAGE, updated.rootHash())).containsSame(updated);

    // bound but idle for longer than the prune window: dropped with its binding
    cache.advanceBlock(40, false);
    cache.prune();
    assertThat(cache.boundStorage(TreeRole.HEAD, account)).isEmpty();
    assertThat(cache.lookup(TreeKind.STORAGE, updated.rootHash())).isEmpty();
  }

  @Test
  void lockedRootsAreNeitherDroppedNorPruned() {
    final ImmutableTreeCache cache = cache(4, 64 << 20);
    cache.advanceBlock(1, false);
    final ImmutableTreeMerkleTrie reader = trie(cache, storedRoot);
    for (final Bytes key : keys.subList(0, 50)) {
      reader.get(key);
    }
    final TreeHandle handle = cache.lookup(TreeKind.STATE, storedRoot).orElseThrow();
    final long loaded = countLoaded(handle.root());
    assertThat(loaded).isGreaterThan(50);

    handle.acquire(1);
    cache.advanceBlock(100, false);
    final ImmutableTreeCache.PruneStats locked = cache.prune();
    assertThat(locked.lockedRoots()).isEqualTo(1);
    assertThat(cache.lookup(TreeKind.STATE, storedRoot)).containsSame(handle);
    assertThat(countLoaded(handle.root())).isEqualTo(loaded);

    handle.release();
    cache.setHead(handle);
    cache.prune();
    assertThat(cache.lookup(TreeKind.STATE, storedRoot)).containsSame(handle);
    // untouched for longer than the window: everything below the root is unloaded
    assertThat(countLoaded(handle.root())).isEqualTo(1);
  }

  @Test
  void prunedNodesReloadThroughTheNextSession() {
    final ImmutableTreeCache cache = cache(4, 64 << 20);
    cache.advanceBlock(1, false);
    final TreeHandle handle = cache.open(TreeKind.STATE, storedRoot, cache.newSession(store));
    cache.setHead(handle);
    final ImmutableTreeMerkleTrie reader = trie(cache, storedRoot);
    final Bytes key = keys.get(7);
    final Optional<Bytes> expected = store.besuTrie(storedRoot).get(key);
    assertThat(reader.get(key)).isEqualTo(expected);

    cache.advanceBlock(10, false);
    final ImmutableTreeCache.PruneStats stats = cache.prune();
    assertThat(stats.unloadedSubtrees()).isPositive();

    store.resetReads();
    assertThat(trie(cache, storedRoot).get(key)).isEqualTo(expected);
    assertThat(store.reads()).isPositive();
    assertThat(handle.root().hash()).isEqualTo(storedRoot);
  }

  @Test
  void decodedBranchesKeepStoredChildrenCompactly() {
    final ImmutableTreeCache cache = cache(4, 64 << 20);
    cache.advanceBlock(1, false);
    final TreeHandle base = cache.open(TreeKind.STATE, storedRoot, cache.newSession(store));
    cache.setHead(base);
    final BranchTreeNode baseRoot = (BranchTreeNode) base.root();
    for (int i = 0; i < 16; i++) {
      assertThat(baseRoot.isCompactChild(i)).isTrue();
    }

    // a read loads the child into its slot
    final Bytes key = keys.get(0);
    final int nibble = (key.get(0) & 0xff) >>> 4;
    trie(cache, storedRoot).get(key);
    assertThat(baseRoot.loadedChild(nibble)).isNotNull();

    // an update copies the slots: untouched children stay compact, the updated one is new
    final PathBasedNodeStore updatedStore = store.copy();
    final ImmutableTreeMerkleTrie update =
        new ImmutableTreeMerkleTrie(
            cache,
            TreeKind.STATE,
            storedRoot,
            updatedStore,
            null,
            ImmutableTreeMerkleTrie.RootListener.NONE);
    update.put(key, Bytes.of(1, 2, 3));
    update.commit(updatedStore);
    final TreeHandle updated = cache.register(TreeKind.STATE, update.currentRoot());
    cache.setNewPayload(updated);
    final BranchTreeNode updatedRoot = (BranchTreeNode) updated.root();
    for (int i = 0; i < 16; i++) {
      assertThat(updatedRoot.isCompactChild(i)).isEqualTo(i != nibble);
    }

    // pruning unloads into the compact slot where the hash matches, a placeholder otherwise
    cache.advanceBlock(20, false);
    cache.prune();
    assertThat(baseRoot.isCompactChild(nibble)).isTrue();
    assertThat(updatedRoot.isCompactChild(nibble)).isFalse();
    assertThat(updatedRoot.isStoredChild(nibble)).isTrue();

    // each root reads back through a store consistent with it
    final ImmutableTreeMerkleTrie updatedReader =
        new ImmutableTreeMerkleTrie(
            cache,
            TreeKind.STATE,
            updated.rootHash(),
            updatedStore,
            null,
            ImmutableTreeMerkleTrie.RootListener.NONE);
    assertThat(updatedReader.get(key)).contains(Bytes.of(1, 2, 3));
    assertThat(trie(cache, storedRoot).get(key)).isEqualTo(store.besuTrie(storedRoot).get(key));
    assertThat(updatedStore.besuTrie(Bytes32.wrap(updated.rootHash())).get(key))
        .contains(Bytes.of(1, 2, 3));
  }

  @Test
  void recentlyUsedPathsSurvivePruning() {
    final ImmutableTreeCache cache = cache(4, 64 << 20);
    cache.advanceBlock(1, false);
    final TreeHandle handle = cache.open(TreeKind.STATE, storedRoot, cache.newSession(store));
    cache.setHead(handle);
    for (final Bytes key : keys.subList(0, 100)) {
      trie(cache, storedRoot).get(key);
    }
    cache.advanceBlock(8, false);
    final Bytes hot = keys.get(0);
    trie(cache, storedRoot).get(hot);
    cache.prune();

    store.resetReads();
    trie(cache, storedRoot).get(hot);
    assertThat(store.reads()).isZero();
    trie(cache, storedRoot).get(keys.get(50));
    assertThat(store.reads()).isPositive();
  }

  @Test
  void heapBudgetShortensThePruneWindow() {
    final long budget = 150_000;
    final ImmutableTreeCache cache = cache(512, budget);
    cache.advanceBlock(1, false);
    final TreeHandle handle = cache.open(TreeKind.STATE, storedRoot, cache.newSession(store));
    cache.setHead(handle);
    for (int block = 1; block <= 40; block++) {
      cache.advanceBlock(block, false);
      for (final Bytes key : keys.subList(block * 20, block * 20 + 20)) {
        trie(cache, storedRoot).get(key);
      }
    }
    final long loadedBefore = countLoaded(handle.root());
    final ImmutableTreeCache.PruneStats stats = cache.prune();
    assertThat(stats.estimatedBytes()).isLessThanOrEqualTo(budget);
    assertThat(stats.windowBlocks()).isLessThan(40);
    assertThat(countLoaded(handle.root()))
        .isEqualTo(stats.materializedNodes())
        .isLessThan(loadedBefore);
    // the most recent block's paths are kept, the head root is never dropped
    store.resetReads();
    trie(cache, storedRoot).get(keys.get(40 * 20));
    assertThat(store.reads()).isZero();
    assertThat(cache.lookup(TreeKind.STATE, storedRoot)).containsSame(handle);
  }

  @Test
  void pruneTriggerIgnoresBoundRoots() throws InterruptedException {
    final ImmutableTreeCache cache =
        new ImmutableTreeCache(new ImmutableTreeCacheConfig(512, 2, 64 << 20, 1000, 3));
    // many bound storage roots: the head's accounts, kept as long as they are used
    for (int i = 0; i < 20; i++) {
      final ImmutableTreeMerkleTrie storage =
          new ImmutableTreeMerkleTrie(
              cache,
              TreeKind.STORAGE,
              MerkleTrie.EMPTY_TRIE_NODE_HASH,
              store,
              null,
              ImmutableTreeMerkleTrie.RootListener.NONE);
      storage.put(bytes(32), bytes(8));
      storage.getRootHash();
      cache.bindStorage(
          TreeRole.HEAD, Bytes32.random(), cache.register(TreeKind.STORAGE, storage.currentRoot()));
    }
    Thread.sleep(200);
    assertThat(cache.stats().lastPrune().block()).isEqualTo(-1);

    // unbound roots beyond the cap do start one
    for (int i = 0; i < 5; i++) {
      final ImmutableTreeMerkleTrie fork =
          new ImmutableTreeMerkleTrie(
              cache,
              TreeKind.STATE,
              MerkleTrie.EMPTY_TRIE_NODE_HASH,
              store,
              null,
              ImmutableTreeMerkleTrie.RootListener.NONE);
      fork.put(bytes(32), bytes(8));
      fork.getRootHash();
      cache.register(TreeKind.STATE, fork.currentRoot());
    }
    final long deadline = System.currentTimeMillis() + 5_000;
    while (cache.stats().lastPrune().block() == -1 && System.currentTimeMillis() < deadline) {
      Thread.sleep(10);
    }
    assertThat(cache.stats().lastPrune().block()).isNotEqualTo(-1);
    // a registration after that run may have started another: settle before counting
    cache.prune();
    assertThat(cache.stats().stateRoots()).isLessThanOrEqualTo(3);
    assertThat(cache.stats().storageRoots()).isEqualTo(20);
    assertThat(cache.stats().headStorageBindings()).isEqualTo(20);
  }

  @Test
  void startingAtTheHeadBlockKeepsTheFirstBlockWarm() {
    final ImmutableTreeCache cache = cache(512, 64 << 20);
    cache.startAt(10_000);
    final TreeHandle handle = cache.open(TreeKind.STATE, storedRoot, cache.newSession(store));
    cache.setHead(handle);
    for (final Bytes key : keys.subList(0, 50)) {
      trie(cache, storedRoot).get(key);
    }
    final long loaded = countLoaded(handle.root());
    cache.advanceBlock(10_001, true);
    final ImmutableTreeCache.PruneStats stats = cache.prune();
    assertThat(stats.unloadedSubtrees()).isZero();
    assertThat(countLoaded(handle.root())).isEqualTo(loaded);
  }

  @Test
  void bindingRegistersADroppedRootAgainButNotAfterAClear() {
    final ImmutableTreeCache cache = cache(512, 64 << 20);
    cache.advanceBlock(1, false);
    final TreeHandle handle = cache.open(TreeKind.STATE, storedRoot, cache.newSession(store));
    cache.advanceBlock(100, false);
    cache.prune();
    assertThat(cache.lookup(TreeKind.STATE, storedRoot)).isEmpty();

    // bound after it expired: registered again, so it can be opened and pruned
    cache.setHead(handle);
    assertThat(cache.lookup(TreeKind.STATE, storedRoot)).containsSame(handle);
    assertThat(cache.boundState(TreeRole.HEAD)).containsSame(handle);

    // a root from before a clear stands for nodes that may be gone from storage
    cache.clear();
    cache.setNewPayload(handle);
    assertThat(cache.boundState(TreeRole.NEW_PAYLOAD)).isEmpty();
    assertThat(cache.lookup(TreeKind.STATE, storedRoot)).isEmpty();
  }

  @Test
  void commitsMatchBesuAcrossBlocks() {
    final ImmutableTreeCache cache = cache(512, 64 << 20);
    final PathBasedNodeStore besuStore = store.copy();
    Bytes32 root = storedRoot;
    final AtomicReference<TreeNode> computed = new AtomicReference<>();
    for (int block = 1; block <= 20; block++) {
      cache.advanceBlock(block, false);
      final ImmutableTreeMerkleTrie trie =
          new ImmutableTreeMerkleTrie(
              cache,
              TreeKind.STATE,
              root,
              store,
              block % 2 == 0 ? POOL : null,
              (t, node, committed) -> computed.set(node));
      final ParallelStoredMerklePatriciaTrie<Bytes, Bytes> besu =
          new ParallelStoredMerklePatriciaTrie<>(
              besuStore, root, Function.identity(), Function.identity());
      for (int i = 0; i < 1 + random.nextInt(80); i++) {
        final Bytes key =
            random.nextInt(4) == 0 ? bytes(32) : keys.get(random.nextInt(keys.size()));
        switch (random.nextInt(3)) {
          case 0 -> {
            trie.remove(key);
            besu.remove(key);
          }
          case 1 -> {
            final Bytes value = bytes(1 + random.nextInt(40));
            trie.putDeferred(key, prior -> Optional.of(value));
            besu.putDeferred(key, prior -> Optional.of(value));
          }
          default -> {
            final Bytes value = bytes(1 + random.nextInt(40));
            trie.put(key, value);
            besu.put(key, value);
          }
        }
      }
      trie.commit(store);
      besu.commit(besuStore);
      assertThat(trie.getRootHash()).isEqualTo(besu.getRootHash());
      root = trie.getRootHash();
      cache.setHead(cache.register(TreeKind.STATE, computed.get()));
      assertThat(store.reachableNodes(root)).isEqualTo(besuStore.reachableNodes(root));
    }
    // the base of every block after the first came from the cache
    assertThat(cache.stats().cachedOpens()).isGreaterThanOrEqualTo(19);
  }

  @Test
  void deferredMergesSeeEarlierStagedWrites() {
    final ImmutableTreeCache cache = cache(512, 64 << 20);
    final ImmutableTreeMerkleTrie trie = trie(cache, storedRoot);
    final Bytes key = keys.get(0);
    final Bytes staged = Bytes.of(1, 2, 3);
    final AtomicInteger calls = new AtomicInteger();
    trie.put(key, staged);
    trie.putDeferred(
        key,
        prior -> {
          calls.incrementAndGet();
          assertThat(prior).contains(staged);
          return Optional.of(Bytes.concatenate(prior.get(), Bytes.of(4)));
        });
    assertThat(calls.get()).isZero();
    assertThat(trie.get(key)).contains(Bytes.of(1, 2, 3, 4));
    assertThat(calls.get()).isEqualTo(1);
  }

  @Test
  void rangeReadsDoNotLoadIntoTheSharedTree() {
    final ImmutableTreeCache cache = cache(512, 64 << 20);
    final ImmutableTreeMerkleTrie trie = trie(cache, storedRoot);
    final Map<Bytes32, Bytes> all = trie.entriesFrom(Bytes32.ZERO, Integer.MAX_VALUE);
    assertThat(all)
        .hasSize(1000)
        .isEqualTo(store.besuTrie(storedRoot).entriesFrom(Bytes32.ZERO, Integer.MAX_VALUE));
    final TreeHandle handle = cache.lookup(TreeKind.STATE, storedRoot).orElseThrow();
    assertThat(countLoaded(handle.root())).isEqualTo(1);
  }

  @Test
  void clearDropsEverything() {
    final ImmutableTreeCache cache = cache(512, 64 << 20);
    final TreeHandle handle = cache.open(TreeKind.STATE, storedRoot, cache.newSession(store));
    cache.setHead(handle);
    cache.bindStorage(
        TreeRole.NEW_PAYLOAD, Bytes32.ZERO, cache.register(TreeKind.STORAGE, handle.root()));
    cache.clear();
    assertThat(cache.stats().stateRoots()).isZero();
    assertThat(cache.stats().storageRoots()).isZero();
    assertThat(cache.boundState(TreeRole.HEAD)).isEmpty();
    assertThat(cache.boundStorage(TreeRole.NEW_PAYLOAD, Bytes32.ZERO)).isEmpty();
  }

  private static long countLoaded(final TreeNode node) {
    return switch (node) {
      case BranchTreeNode branch -> {
        long count = 1;
        for (int i = 0; i < 16; i++) {
          count += countLoaded(branch.child(i));
        }
        yield count;
      }
      case ExtensionTreeNode extension -> 1 + countLoaded(extension.child());
      case LeafTreeNode ignored -> 1;
      default -> 0;
    };
  }
}
