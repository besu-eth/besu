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

import org.hyperledger.besu.ethereum.trie.MerkleTrie;
import org.hyperledger.besu.ethereum.trie.NodeLoader;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Predicate;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A cache of immutable Merkle Patricia trees, shared by every state root computation.
 *
 * <p>Roots are registered by {@link TreeKey} (root hash and {@link TreeKind}). Any number of them
 * can be alive at once and they share every subtree they have in common, so keeping the root of a
 * block and of its child costs only the nodes the child changed. A root is opened from the
 * registry, or else by loading its root node alone; the rest of the tree is loaded as traversals
 * touch it.
 *
 * <p>Retention is decided by bindings. The state root of the head world state is bound as {@link
 * TreeRole#HEAD}, the one of the latest payload validation as {@link TreeRole#NEW_PAYLOAD}, and the
 * storage roots each of them computed are bound per account. Bound roots stay while they are in
 * use. Other roots - the roots a computation started from, rolled world states, simulations, block
 * building - are forks: they are dropped a few blocks after their last use, so the cache never pins
 * more than a short window of history.
 *
 * <p>Pruning, run in the background as the head advances, drops expired roots and unloads the nodes
 * of the remaining ones that no traversal has touched for {@link
 * ImmutableTreeCacheConfig#pruneAfterBlocks()} blocks, replacing them by stored children. If the
 * estimated heap of what is left exceeds {@link ImmutableTreeCacheConfig#maxCachedBytes()}, it
 * shortens that window until it fits, and past a window of zero drops the least recently used
 * roots. Roots locked by a running traversal are skipped. Pruning finds what to drop without the
 * lock that guards bindings, and takes it only for short batches of removals, so the block import
 * binding its roots does not wait for a scan.
 */
public final class ImmutableTreeCache {

  private static final Logger LOG = LoggerFactory.getLogger(ImmutableTreeCache.class);

  // One thread for every cache: prune runs are rare and short
  private static final ExecutorService PRUNE_EXECUTOR =
      Executors.newSingleThreadExecutor(
          runnable -> {
            final Thread thread = new Thread(runnable, "besu-immutable-tree-prune");
            thread.setDaemon(true);
            return thread;
          });

  // Pruning holds the binding lock for at most this many removals at a time
  private static final int LOCK_BATCH = 256;

  private final ImmutableTreeCacheConfig config;
  private final ConcurrentHashMap<TreeKey, TreeHandle> roots = new ConcurrentHashMap<>();

  // Bindings change under bindingLock. The fields and maps are safe to read without it, which is
  // how pruning scans them before taking the lock to act.
  private final Object bindingLock = new Object();
  private volatile TreeHandle head;
  private volatile TreeHandle newPayload;
  private final ConcurrentHashMap<Bytes32, TreeHandle> headStorage = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<Bytes32, TreeHandle> newPayloadStorage =
      new ConcurrentHashMap<>();
  private final AtomicInteger boundRoots = new AtomicInteger();
  // Incremented by clear(): handles from before a clear are never bound again
  private final AtomicInteger generation = new AtomicInteger();

  private final AtomicLong currentBlock = new AtomicLong();
  private final AtomicLong lastPruneBlock = new AtomicLong();
  private final AtomicBoolean pruneScheduled = new AtomicBoolean();
  private final AtomicInteger walkEpochs = new AtomicInteger();
  private final LongAdder cachedOpens = new LongAdder();
  private final LongAdder storedOpens = new LongAdder();
  private volatile PruneStats lastPrune = PruneStats.NONE;

  /**
   * Creates a cache.
   *
   * @param config its limits
   */
  public ImmutableTreeCache(final ImmutableTreeCacheConfig config) {
    this.config = config;
  }

  /**
   * Returns the limits of this cache.
   *
   * @return the config
   */
  public ImmutableTreeCacheConfig config() {
    return config;
  }

  /**
   * Returns the latest block the cache was advanced to.
   *
   * @return the block number
   */
  public long currentBlock() {
    return currentBlock.get();
  }

  /**
   * Starts the cache at a block, the head's when the node starts: nodes are stamped with it from
   * the first block on, and the first prune is due {@link
   * ImmutableTreeCacheConfig#pruneIntervalBlocks()} blocks later.
   *
   * @param blockNumber the block number
   */
  public void startAt(final long blockNumber) {
    currentBlock.accumulateAndGet(blockNumber, Math::max);
    lastPruneBlock.accumulateAndGet(blockNumber, Math::max);
  }

  /**
   * Creates a session stamping the nodes it visits with the current block.
   *
   * @param loader loads missing nodes; must be consistent with the roots the session works on
   * @return a new session
   */
  public TreeSession newSession(final NodeLoader loader) {
    return TreeSession.create(loader, (int) Math.min(currentBlock.get(), Integer.MAX_VALUE));
  }

  /**
   * Opens a root: returns its handle if it is registered, otherwise loads its root node - and only
   * that - through the session and registers it as a fork.
   *
   * @param kind the kind of trie
   * @param rootHash the root hash
   * @param session the session opening it
   * @return the handle, or null for the empty root, which needs none
   */
  public TreeHandle open(final TreeKind kind, final Bytes32 rootHash, final TreeSession session) {
    if (rootHash.equals(MerkleTrie.EMPTY_TRIE_NODE_HASH)) {
      return null;
    }
    final TreeKey key = new TreeKey(rootHash, kind);
    final long block = currentBlock.get();
    final TreeHandle cached = roots.get(key);
    if (cached != null) {
      cached.touch(block);
      cachedOpens.increment();
      return cached;
    }
    final TreeNode root = session.loadRoot(rootHash);
    storedOpens.increment();
    final TreeHandle opened = new TreeHandle(key, root, block, generation.get());
    final TreeHandle raced = roots.putIfAbsent(key, opened);
    return raced != null ? raced : opened;
  }

  /**
   * Returns the handle of a registered root.
   *
   * @param kind the kind of trie
   * @param rootHash the root hash
   * @return the handle, if registered
   */
  public Optional<TreeHandle> lookup(final TreeKind kind, final Bytes32 rootHash) {
    return Optional.ofNullable(roots.get(new TreeKey(rootHash, kind)));
  }

  /**
   * Registers a computed root. If the root is registered already, the existing handle is kept, so
   * one tree stands for each root.
   *
   * @param kind the kind of trie
   * @param root the root node, hashed
   * @return the handle, or null for the empty root
   */
  public TreeHandle register(final TreeKind kind, final TreeNode root) {
    if (root instanceof StoredTreeNode) {
      throw new IllegalArgumentException("Only loaded roots can be registered");
    }
    final Bytes32 rootHash = root.hash();
    if (rootHash.equals(MerkleTrie.EMPTY_TRIE_NODE_HASH)) {
      return null;
    }
    final TreeKey key = new TreeKey(rootHash, kind);
    final long block = currentBlock.get();
    final int currentGeneration = generation.get();
    final TreeHandle handle =
        roots.computeIfAbsent(key, k -> new TreeHandle(k, root, block, currentGeneration));
    handle.touch(block);
    if (roots.size() - boundRoots.get() > config.maxUnboundRoots()) {
      // e.g. payload validations or simulations while the head does not move
      schedulePrune();
    }
    return handle;
  }

  /**
   * Binds the state root of the head world state.
   *
   * @param handle the state root handle, or null for the empty state
   */
  public void setHead(final TreeHandle handle) {
    checkKind(handle, TreeKind.STATE);
    synchronized (bindingLock) {
      final TreeHandle bound = registered(handle);
      rebind(head, bound);
      head = bound;
    }
  }

  /**
   * Binds the state root of the latest payload validation.
   *
   * @param handle the state root handle, or null for the empty state
   */
  public void setNewPayload(final TreeHandle handle) {
    checkKind(handle, TreeKind.STATE);
    synchronized (bindingLock) {
      final TreeHandle bound = registered(handle);
      rebind(newPayload, bound);
      newPayload = bound;
    }
  }

  /**
   * Binds the storage root of an account for a role, replacing the previous one.
   *
   * @param role the role
   * @param accountHash the account
   * @param handle the storage root handle, or null to unbind (empty storage)
   */
  public void bindStorage(final TreeRole role, final Bytes32 accountHash, final TreeHandle handle) {
    checkKind(handle, TreeKind.STORAGE);
    synchronized (bindingLock) {
      final Map<Bytes32, TreeHandle> bindings = storageBindings(role);
      final TreeHandle bound = registered(handle);
      final TreeHandle previous =
          bound == null ? bindings.remove(accountHash) : bindings.put(accountHash, bound);
      rebind(previous, bound);
    }
  }

  /**
   * Returns the state root bound to a role.
   *
   * @param role the role
   * @return the handle, if bound
   */
  public Optional<TreeHandle> boundState(final TreeRole role) {
    return Optional.ofNullable(role == TreeRole.HEAD ? head : newPayload);
  }

  /**
   * Returns the storage root bound to an account for a role.
   *
   * @param role the role
   * @param accountHash the account
   * @return the handle, if bound
   */
  public Optional<TreeHandle> boundStorage(final TreeRole role, final Bytes32 accountHash) {
    return Optional.ofNullable(storageBindings(role).get(accountHash));
  }

  private ConcurrentHashMap<Bytes32, TreeHandle> storageBindings(final TreeRole role) {
    return role == TreeRole.HEAD ? headStorage : newPayloadStorage;
  }

  /**
   * Returns the registered handle of a root about to be bound, under the binding lock. A root
   * dropped between its registration and now is registered again, so it stays prunable and can be
   * opened; if it was registered again in the meantime, that handle is bound instead. Roots from
   * before a {@link #clear()} are not bound at all.
   */
  private TreeHandle registered(final TreeHandle handle) {
    if (handle == null || handle.generation() != generation.get()) {
      return null;
    }
    final TreeHandle existing = roots.putIfAbsent(handle.key(), handle);
    return existing != null ? existing : handle;
  }

  /** Moves one binding from {@code previous} to {@code next}, under the binding lock. */
  private void rebind(final TreeHandle previous, final TreeHandle next) {
    if (previous == next) {
      return;
    }
    if (previous != null && previous.bindings.decrementAndGet() == 0) {
      boundRoots.decrementAndGet();
    }
    if (next != null && next.bindings.getAndIncrement() == 0) {
      boundRoots.incrementAndGet();
    }
  }

  private static void checkKind(final TreeHandle handle, final TreeKind kind) {
    if (handle != null && handle.kind() != kind) {
      throw new IllegalArgumentException("Expected a " + kind + " root, got " + handle);
    }
  }

  /**
   * Moves the cache to a block. Nodes visited from now on are stamped with it. Advancing the head
   * schedules a prune run every {@link ImmutableTreeCacheConfig#pruneIntervalBlocks()} blocks.
   *
   * @param blockNumber the block number
   * @param isHead whether the head world state reached this block
   */
  public void advanceBlock(final long blockNumber, final boolean isHead) {
    final long block = currentBlock.accumulateAndGet(blockNumber, Math::max);
    if (isHead && block - lastPruneBlock.get() >= config.pruneIntervalBlocks()) {
      schedulePrune();
    }
  }

  private void schedulePrune() {
    if (pruneScheduled.compareAndSet(false, true)) {
      PRUNE_EXECUTOR.execute(
          () -> {
            try {
              prune();
            } catch (final RuntimeException e) {
              LOG.warn("Immutable tree cache prune failed", e);
            } finally {
              pruneScheduled.set(false);
            }
          });
    }
  }

  /** Drops every root and binding. */
  public void clear() {
    synchronized (bindingLock) {
      generation.incrementAndGet();
      head = null;
      newPayload = null;
      headStorage.clear();
      newPayloadStorage.clear();
      roots.values().forEach(handle -> handle.bindings.set(0));
      roots.clear();
      boundRoots.set(0);
    }
    LOG.debug("Immutable tree cache cleared");
  }

  /**
   * Returns the current size of the cache and the outcome of the last prune.
   *
   * @return the stats
   */
  public Stats stats() {
    int stateRoots = 0;
    int storageRoots = 0;
    for (final TreeKey key : roots.keySet()) {
      if (key.kind() == TreeKind.STATE) {
        stateRoots++;
      } else {
        storageRoots++;
      }
    }
    final TreeHandle headRoot = head;
    final TreeHandle newPayloadRoot = newPayload;
    return new Stats(
        stateRoots,
        storageRoots,
        headRoot == null ? null : headRoot.rootHash(),
        newPayloadRoot == null ? null : newPayloadRoot.rootHash(),
        headStorage.size(),
        newPayloadStorage.size(),
        cachedOpens.sum(),
        storedOpens.sum(),
        lastPrune);
  }

  // ---------------------------------------------------------------------------------------------
  // Pruning

  /**
   * Drops expired roots and unloads the nodes nothing has used for the prune window, or a shorter
   * one if the cache is over its heap budget. Runs in the background after head advances; callable
   * directly.
   *
   * @return what the run did
   */
  public synchronized PruneStats prune() {
    final long startNanos = System.nanoTime();
    final long block = currentBlock.get();
    lastPruneBlock.set(block);

    final int droppedBindings = dropIdleStorageBindings(block);
    int droppedRoots = dropExpiredRoots(block);

    final long budget = config.maxCachedBytes();
    long window = config.pruneAfterBlocks();
    Walk walk = unloadStale(block, window);
    long unloaded = walk.unloaded;
    if (walk.estimatedBytes > budget) {
      // Traversals touch nodes top down, so unloading with a shorter window keeps about the nodes
      // used within it: pick the longest window whose nodes fit
      window = walk.longestWindowWithin(budget);
      walk = unloadStale(block, window);
      unloaded += walk.unloaded;
      while (walk.estimatedBytes > budget && window > 0) {
        window /= 2;
        walk = unloadStale(block, window);
        unloaded += walk.unloaded;
      }
    }
    if (walk.estimatedBytes > budget) {
      // Only nodes used in the current block are left, and every root keeps its root node loaded
      droppedRoots += dropLeastRecentlyUsed(walk.estimatedBytes - budget);
      walk = unloadStale(block, window);
      unloaded += walk.unloaded;
    }

    final PruneStats stats =
        new PruneStats(
            block,
            droppedRoots,
            droppedBindings,
            unloaded,
            walk.materialized,
            walk.stored,
            walk.estimatedBytes,
            window,
            walk.lockedRoots,
            roots.size(),
            (System.nanoTime() - startNanos) / 1_000_000);
    lastPrune = stats;
    LOG.debug("Immutable tree cache pruned: {}", stats);
    return stats;
  }

  /** Drops the storage bindings not used for the prune window. */
  private int dropIdleStorageBindings(final long block) {
    final Predicate<TreeHandle> idle =
        handle ->
            !handle.isLocked() && block - handle.lastAccessBlock() > config.pruneAfterBlocks();
    int dropped = 0;
    for (final TreeRole role : TreeRole.values()) {
      dropped += unbindStorage(storageBindings(role), idle);
    }
    return dropped;
  }

  /**
   * Removes the bindings whose root matches, checking again under the binding lock, which it takes
   * for a batch of removals at a time.
   */
  private int unbindStorage(
      final ConcurrentHashMap<Bytes32, TreeHandle> bindings, final Predicate<TreeHandle> matches) {
    final List<Map.Entry<Bytes32, TreeHandle>> matching = new ArrayList<>();
    bindings.forEach(
        (account, handle) -> {
          if (matches.test(handle)) {
            matching.add(Map.entry(account, handle));
          }
        });
    int removed = 0;
    for (int from = 0; from < matching.size(); from += LOCK_BATCH) {
      synchronized (bindingLock) {
        for (final Map.Entry<Bytes32, TreeHandle> binding :
            matching.subList(from, Math.min(matching.size(), from + LOCK_BATCH))) {
          final TreeHandle handle = binding.getValue();
          if (matches.test(handle) && bindings.remove(binding.getKey(), handle)) {
            rebind(handle, null);
            removed++;
          }
        }
      }
    }
    return removed;
  }

  /** Drops unbound roots unused for the fork retention, and the oldest beyond the cap. */
  private int dropExpiredRoots(final long block) {
    final List<TreeHandle> dropping = new ArrayList<>();
    final List<TreeHandle> unbound = new ArrayList<>();
    for (final TreeHandle handle : roots.values()) {
      if (handle.bindings.get() > 0 || handle.isLocked()) {
        continue;
      }
      if (block - handle.lastAccessBlock() > config.forkRetentionBlocks()) {
        dropping.add(handle);
      } else {
        unbound.add(handle);
      }
    }
    if (unbound.size() > config.maxUnboundRoots()) {
      unbound.sort(Comparator.comparingLong(TreeHandle::lastAccessBlock));
      dropping.addAll(unbound.subList(0, unbound.size() - config.maxUnboundRoots()));
    }
    return removeUnbound(dropping);
  }

  /**
   * Drops the least recently used roots, with their storage bindings, until the heap the last walk
   * attributed to them covers {@code excessBytes}. The head and new payload state roots are kept.
   */
  private int dropLeastRecentlyUsed(final long excessBytes) {
    final TreeHandle headRoot = head;
    final TreeHandle newPayloadRoot = newPayload;
    final List<TreeHandle> candidates = new ArrayList<>();
    for (final TreeHandle handle : roots.values()) {
      if (handle != headRoot && handle != newPayloadRoot && !handle.isLocked()) {
        candidates.add(handle);
      }
    }
    candidates.sort(Comparator.comparingLong(TreeHandle::lastAccessBlock));
    final Set<TreeHandle> dropping = Collections.newSetFromMap(new IdentityHashMap<>());
    long freed = 0;
    for (final TreeHandle handle : candidates) {
      if (freed >= excessBytes) {
        break;
      }
      dropping.add(handle);
      freed += Math.max(handle.walkBytes, 1);
    }
    for (final TreeRole role : TreeRole.values()) {
      unbindStorage(storageBindings(role), dropping::contains);
    }
    return removeUnbound(new ArrayList<>(dropping));
  }

  /**
   * Removes roots that are still unbound, unlocked and neither head nor new payload, checking under
   * the binding lock, which it takes for a batch of removals at a time.
   */
  private int removeUnbound(final List<TreeHandle> handles) {
    int removed = 0;
    for (int from = 0; from < handles.size(); from += LOCK_BATCH) {
      synchronized (bindingLock) {
        for (final TreeHandle handle :
            handles.subList(from, Math.min(handles.size(), from + LOCK_BATCH))) {
          if (handle.bindings.get() == 0
              && !handle.isLocked()
              && handle != head
              && handle != newPayload
              && roots.remove(handle.key(), handle)) {
            removed++;
          }
        }
      }
    }
    return removed;
  }

  private static final class Walk {
    private final long block;
    // Estimated heap of what is left, by age in blocks of the node it belongs to
    private final long[] bytesByAge;
    long materialized;
    long stored;
    long unloaded;
    long estimatedBytes;
    int lockedRoots;

    Walk(final long block, final long window) {
      this.block = block;
      this.bytesByAge = new long[(int) Math.min(window, Integer.MAX_VALUE - 1) + 1];
    }

    void add(final int lastAccess, final long bytes) {
      final long age = Math.max(0, block - lastAccess);
      bytesByAge[(int) Math.min(age, bytesByAge.length - 1)] += bytes;
      estimatedBytes += bytes;
    }

    /** The longest window whose nodes fit in the budget, 0 if not even the newest ones do. */
    long longestWindowWithin(final long budget) {
      long total = 0;
      for (int age = 0; age < bytesByAge.length; age++) {
        total += bytesByAge[age];
        if (total > budget) {
          return Math.max(0L, age - 1L);
        }
      }
      return bytesByAge.length - 1L;
    }
  }

  /**
   * Walks every unlocked root once, unloading loaded children last used more than {@code window}
   * blocks ago. A shared subtree is walked once however many roots reach it.
   */
  private Walk unloadStale(final long block, final long window) {
    final Walk walk = new Walk(block, window);
    final int epoch = walkEpochs.incrementAndGet();
    final long threshold = block - window;
    final int stampThreshold =
        (int) Math.max(Integer.MIN_VALUE, Math.min(threshold, Integer.MAX_VALUE));
    final PathStack path = new PathStack();
    for (final TreeHandle handle : roots.values()) {
      if (handle.isLocked()) {
        walk.lockedRoots++;
        handle.walkBytes = 0;
        continue;
      }
      final long before = walk.estimatedBytes;
      walkNode(handle.root(), path, stampThreshold, epoch, walk);
      handle.walkBytes = walk.estimatedBytes - before;
    }
    return walk;
  }

  private static void walkNode(
      final TreeNode node,
      final PathStack path,
      final int threshold,
      final int epoch,
      final Walk walk) {
    if (!(node instanceof MaterializedTreeNode materialized) || materialized.visitEpoch == epoch) {
      return;
    }
    materialized.visitEpoch = epoch;
    walk.materialized++;
    // stored children are accounted to the node holding them
    final int lastAccess = materialized.lastAccess;
    walk.add(lastAccess, estimatedSize(materialized));
    if (node instanceof BranchTreeNode branch) {
      Bytes location = null;
      for (int i = 0; i < TreePaths.RADIX; i++) {
        final MaterializedTreeNode loaded = branch.loadedChild(i);
        if (loaded == null) {
          if (branch.isStoredChild(i)) {
            walk.stored++;
            walk.add(lastAccess, branch.isCompactChild(i) ? COMPACT_SIZE : STORED_SIZE);
          }
        } else if (loaded.lastAccess < threshold && loaded.isReferencedByHash()) {
          if (location == null) {
            location = path.toBytes();
          }
          if (branch.swapChild(
              i, loaded, new StoredTreeNode(location, TreePaths.nibble(i), loaded.hash()))) {
            walk.unloaded++;
            walk.stored++;
            walk.add(lastAccess, branch.isCompactChild(i) ? COMPACT_SIZE : STORED_SIZE);
          }
        } else {
          path.push(i);
          walkNode(loaded, path, threshold, epoch, walk);
          path.pop(1);
        }
      }
    } else if (node instanceof ExtensionTreeNode extension) {
      final TreeNode child = extension.child();
      if (child instanceof StoredTreeNode) {
        walk.stored++;
        walk.add(lastAccess, STORED_SIZE);
      } else if (child instanceof MaterializedTreeNode loaded) {
        if (loaded.lastAccess < threshold && loaded.isReferencedByHash()) {
          if (extension.swapChild(
              loaded, new StoredTreeNode(path.toBytes(), extension.path(), loaded.hash()))) {
            walk.unloaded++;
            walk.stored++;
            walk.add(lastAccess, STORED_SIZE);
          }
        } else {
          path.push(extension.path());
          walkNode(loaded, path, threshold, epoch, walk);
          path.pop(extension.path().size());
        }
      }
    }
  }

  // Heap footprint for the budget and the logs, from object layouts with compressed pointers:
  // node, hash, slot array, the shared array of stored hashes (counted per branch, although copies
  // share it) and the byte payloads. Compact slots live in that array; a placeholder object costs
  // more. Calibrated against the measured heap of a cached 300,000 account trie.
  private static final long HASH_SIZE = 72;
  private static final long STORED_SIZE = 32 + HASH_SIZE;
  private static final long COMPACT_SIZE = 0;

  private static long estimatedSize(final MaterializedTreeNode node) {
    return switch (node) {
      case BranchTreeNode branch ->
          40 + HASH_SIZE + 80 + (branch.hasStoredHashes() ? 16 + 16 * 32 + 48 : 0);
      case ExtensionTreeNode extension -> 40 + HASH_SIZE + 40 + extension.path().size();
      case LeafTreeNode leaf -> 40 + HASH_SIZE + 80 + leaf.path().size() + leaf.value().size();
    };
  }

  /** The nibble path of the node being walked. */
  private static final class PathStack {
    private byte[] nibbles = new byte[128];
    private int depth;

    void push(final int nibble) {
      ensureCapacity(depth + 1);
      nibbles[depth++] = (byte) nibble;
    }

    void push(final Bytes segment) {
      ensureCapacity(depth + segment.size());
      for (int i = 0; i < segment.size(); i++) {
        nibbles[depth++] = segment.get(i);
      }
    }

    void pop(final int count) {
      depth -= count;
    }

    Bytes toBytes() {
      return Bytes.wrap(Arrays.copyOf(nibbles, depth));
    }

    private void ensureCapacity(final int size) {
      if (size > nibbles.length) {
        nibbles = Arrays.copyOf(nibbles, Math.max(size, nibbles.length * 2));
      }
    }
  }

  /**
   * Size of the cache.
   *
   * @param stateRoots registered state roots
   * @param storageRoots registered storage roots
   * @param head the state root bound as head, or null
   * @param newPayload the state root bound as new payload, or null
   * @param headStorageBindings accounts with a storage root bound for the head
   * @param newPayloadStorageBindings accounts with a storage root bound for the new payload
   * @param cachedOpens opens served from the registry
   * @param storedOpens opens that loaded the root node from storage
   * @param lastPrune the last prune run
   */
  public record Stats(
      int stateRoots,
      int storageRoots,
      Bytes32 head,
      Bytes32 newPayload,
      int headStorageBindings,
      int newPayloadStorageBindings,
      long cachedOpens,
      long storedOpens,
      PruneStats lastPrune) {}

  /**
   * Outcome of a prune run.
   *
   * @param block the block it ran at
   * @param droppedRoots roots dropped from the registry
   * @param droppedBindings idle storage bindings dropped
   * @param unloadedSubtrees loaded subtrees replaced by stored children
   * @param materializedNodes loaded nodes left, counted once however many roots share them
   * @param storedPlaceholders stored children left
   * @param estimatedBytes estimated heap of what is left, the figure the budget applies to
   * @param windowBlocks the prune window that was applied, after any shortening
   * @param lockedRoots roots skipped because a traversal held them
   * @param registeredRoots roots left in the registry
   * @param durationMillis how long the run took
   */
  public record PruneStats(
      long block,
      int droppedRoots,
      int droppedBindings,
      long unloadedSubtrees,
      long materializedNodes,
      long storedPlaceholders,
      long estimatedBytes,
      long windowBlocks,
      int lockedRoots,
      int registeredRoots,
      long durationMillis) {

    static final PruneStats NONE = new PruneStats(-1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);

    @Override
    public String toString() {
      return String.format(
          "block=%d droppedRoots=%d droppedStorageBindings=%d unloadedSubtrees=%d"
              + " loadedNodes=%d storedChildren=%d estimatedMiB=%d window=%d lockedRoots=%d"
              + " registeredRoots=%d took=%dms",
          block,
          droppedRoots,
          droppedBindings,
          unloadedSubtrees,
          materializedNodes,
          storedPlaceholders,
          estimatedBytes >> 20,
          windowBlocks,
          lockedRoots,
          registeredRoots,
          durationMillis);
    }
  }
}
