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

import static com.google.common.base.Preconditions.checkNotNull;
import static org.hyperledger.besu.ethereum.trie.CompactEncoding.bytesToPath;

import org.hyperledger.besu.ethereum.trie.CommitVisitor;
import org.hyperledger.besu.ethereum.trie.MerkleTrie;
import org.hyperledger.besu.ethereum.trie.Node;
import org.hyperledger.besu.ethereum.trie.NodeLoader;
import org.hyperledger.besu.ethereum.trie.NodeUpdater;
import org.hyperledger.besu.ethereum.trie.PathNodeVisitor;
import org.hyperledger.besu.ethereum.trie.Proof;
import org.hyperledger.besu.ethereum.trie.TrieIterator;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/**
 * A {@link MerkleTrie} whose root is computed by copy-on-write updates of an immutable tree.
 *
 * <p>The trie starts from a base root, opened through an {@link ImmutableTreeCache} (so a root an
 * earlier computation left in the cache is reused with everything it has loaded) or, without a
 * cache, by loading the root node. Writes are staged, as in Besu's parallel trie, and applied in
 * one pass by {@link #getRootHash()}, {@link #commit(NodeUpdater)} or a read. The base tree is
 * never modified; the new root shares every untouched subtree with it.
 *
 * <p>{@link #commit(NodeUpdater)} writes exactly the nodes this trie created, and never loads a
 * node to do so. After each computed root the {@link RootListener} is told, so the caller can
 * register it once it knows the root is valid.
 *
 * <p>Methods taking Besu node visitors are not supported: this trie has no Besu nodes.
 */
public class ImmutableTreeMerkleTrie implements MerkleTrie<Bytes, Bytes> {

  /** Receives the roots a trie computes. */
  @FunctionalInterface
  public interface RootListener {

    /** Ignores every root. */
    RootListener NONE = (trie, root, committed) -> {};

    /**
     * Called with each computed root.
     *
     * @param trie the trie that computed it
     * @param root the root node, hashed
     * @param committed whether its nodes were just committed
     */
    void onRoot(ImmutableTreeMerkleTrie trie, TreeNode root, boolean committed);
  }

  private static final Comparator<TreeUpdate> BY_PATH = (a, b) -> comparePaths(a.path(), b.path());

  private final ImmutableTreeCache cache;
  private final TreeKind kind;
  private final Bytes32 baseRootHash;
  private final ForkJoinPool pool;
  private final RootListener listener;
  private final Map<Bytes, TreeUpdate> pending = new ConcurrentHashMap<>();

  private TreeSession session;
  private TreeHandle baseHandle;
  private boolean baseWasCached;
  private TreeNode root;

  /**
   * Creates a trie.
   *
   * @param cache the cache to open the base root through, or null to load it directly
   * @param kind the kind of trie, which the cache registers roots under
   * @param baseRootHash the root to start from
   * @param loader loads stored nodes; must be consistent with the base root
   * @param pool the pool updates, hashing and commits run in, or null to stay in the caller
   * @param listener told about every computed root
   */
  public ImmutableTreeMerkleTrie(
      final ImmutableTreeCache cache,
      final TreeKind kind,
      final Bytes32 baseRootHash,
      final NodeLoader loader,
      final ForkJoinPool pool,
      final RootListener listener) {
    this.cache = cache;
    this.kind = kind;
    this.baseRootHash = baseRootHash;
    this.pool = pool;
    this.listener = listener;
    this.session = cache != null ? cache.newSession(loader) : TreeSession.create(loader, 0);
  }

  /**
   * Creates a trie outside any cache.
   *
   * @param loader loads stored nodes
   * @param baseRootHash the root to start from
   * @return the trie
   */
  public static ImmutableTreeMerkleTrie standalone(
      final NodeLoader loader, final Bytes32 baseRootHash) {
    return new ImmutableTreeMerkleTrie(
        null, TreeKind.STATE, baseRootHash, loader, null, RootListener.NONE);
  }

  // ---------------------------------------------------------------------------------------------
  // Writes are staged

  @Override
  public void put(final Bytes key, final Bytes value) {
    checkNotNull(key);
    checkNotNull(value);
    final Bytes path = bytesToPath(key);
    pending.put(path, TreeUpdate.put(path, value));
  }

  @Override
  public void putPath(final Bytes path, final Bytes value) {
    checkNotNull(path);
    checkNotNull(value);
    pending.put(path, TreeUpdate.put(path, value));
  }

  @Override
  public void remove(final Bytes key) {
    checkNotNull(key);
    final Bytes path = bytesToPath(key);
    pending.put(path, TreeUpdate.remove(path));
  }

  /**
   * Stages a merge: {@code merger} is called once, while the update is applied, with the value the
   * key has at that point, which includes earlier staged writes to the same key.
   */
  @Override
  public void putDeferred(
      final Bytes key, final Function<Optional<Bytes>, Optional<Bytes>> merger) {
    checkNotNull(key);
    final Bytes path = bytesToPath(key);
    final TreeUpdate merge = TreeUpdate.merge(path, merger);
    pending.compute(
        path,
        (p, previous) ->
            previous == null
                ? merge
                : TreeUpdate.merge(
                    path,
                    prior ->
                        merger.apply(
                            Optional.ofNullable(previous.applyToValue(prior.orElse(null))))));
  }

  // ---------------------------------------------------------------------------------------------
  // Root computation and commit

  @Override
  public Bytes32 getRootHash() {
    final TreeNode computed = traverse(this::applyPending);
    final Bytes32 hash = computed.hash();
    listener.onRoot(this, computed, false);
    return hash;
  }

  @Override
  public void commit(final NodeUpdater nodeUpdater) {
    final TreeNode committed =
        traverse(
            () -> {
              final TreeNode updated = applyPending();
              updated.hash();
              TreeOps.commit(session, updated, nodeUpdater, pool);
              return updated;
            });
    listener.onRoot(this, committed, true);
    // What was committed is no longer dirty for later commits
    session = session.renew();
  }

  /**
   * Returns the root after the staged writes.
   *
   * @return the root node
   */
  public TreeNode currentRoot() {
    return traverse(this::applyPending);
  }

  private TreeNode applyPending() {
    if (!pending.isEmpty()) {
      final TreeUpdate[] updates = pending.values().toArray(new TreeUpdate[0]);
      pending.clear();
      Arrays.sort(updates, BY_PATH);
      root = BatchUpdater.apply(session, root, updates, pool);
    }
    return root;
  }

  // ---------------------------------------------------------------------------------------------
  // Reads

  @Override
  public Optional<Bytes> get(final Bytes key) {
    checkNotNull(key);
    return getPath(bytesToPath(key));
  }

  @Override
  public Optional<Bytes> getPath(final Bytes path) {
    checkNotNull(path);
    return traverse(() -> TreeOps.get(session, applyPending(), path));
  }

  @Override
  public Proof<Bytes> getValueWithProof(final Bytes key) {
    checkNotNull(key);
    return traverse(() -> TreeOps.proof(session, applyPending(), bytesToPath(key)));
  }

  /**
   * Reads the current root without applying staged writes, like Besu's parallel trie. Nodes read
   * here are not kept loaded in the shared tree, since range reads mostly serve one-off scans.
   */
  @Override
  public Map<Bytes32, Bytes> entriesFrom(final Bytes32 startKeyHash, final int limit) {
    return traverse(() -> TreeOps.entriesFrom(session, root, startKeyHash, limit, false));
  }

  // ---------------------------------------------------------------------------------------------
  // Opening and locking

  private <T> T traverse(final Supplier<T> action) {
    final TreeHandle handle = open();
    if (handle != null) {
      handle.acquire(cache.currentBlock());
    }
    try {
      return action.get();
    } finally {
      if (handle != null) {
        handle.release();
      }
    }
  }

  private TreeHandle open() {
    if (root == null) {
      if (baseRootHash.equals(EMPTY_TRIE_NODE_HASH)) {
        root = EmptyTreeNode.INSTANCE;
      } else if (cache != null) {
        final long loadsBefore = session.loadCount();
        baseHandle = cache.open(kind, baseRootHash, session);
        // this trie's session is its own, so it loaded the root exactly when it was not cached
        baseWasCached = session.loadCount() == loadsBefore;
        root = baseHandle.root();
      } else {
        root = session.loadRoot(baseRootHash);
      }
    }
    return baseHandle;
  }

  /**
   * Returns the kind of trie.
   *
   * @return the kind
   */
  public TreeKind kind() {
    return kind;
  }

  /**
   * Returns the root this trie started from.
   *
   * @return the base root hash
   */
  public Bytes32 baseRootHash() {
    return baseRootHash;
  }

  /**
   * Whether the base root was already registered when this trie opened it.
   *
   * @return true if the base came from the cache
   */
  public boolean isBaseCached() {
    return baseWasCached;
  }

  /**
   * Returns how many nodes this trie loaded from its loader.
   *
   * @return the number of loads
   */
  public long loadCount() {
    return session.loadCount();
  }

  // ---------------------------------------------------------------------------------------------
  // Besu node visitors: not supported

  @Override
  public void put(final Bytes key, final PathNodeVisitor<Bytes> putVisitor) {
    throw unsupported("put with a node visitor");
  }

  @Override
  public void removePath(final Bytes path, final PathNodeVisitor<Bytes> removeVisitor) {
    throw unsupported("removePath with a node visitor");
  }

  @Override
  public void commit(final NodeUpdater nodeUpdater, final CommitVisitor<Bytes> commitVisitor) {
    throw unsupported("commit with a commit visitor");
  }

  @Override
  public Map<Bytes32, Bytes> entriesFrom(final Function<Node<Bytes>, Map<Bytes32, Bytes>> handler) {
    throw unsupported("entriesFrom with a node handler");
  }

  @Override
  public void visitAll(final Consumer<Node<Bytes>> nodeConsumer) {
    throw unsupported("visitAll");
  }

  @Override
  public CompletableFuture<Void> visitAll(
      final Consumer<Node<Bytes>> nodeConsumer, final ExecutorService executorService) {
    throw unsupported("visitAll");
  }

  @Override
  public void visitLeafs(final TrieIterator.LeafHandler<Bytes> handler) {
    throw unsupported("visitLeafs");
  }

  private static UnsupportedOperationException unsupported(final String operation) {
    return new UnsupportedOperationException(
        operation + " is not supported by immutable trees, which have no Besu nodes");
  }

  /** Orders nibble paths lexicographically, the order of keys in the trie. */
  static int comparePaths(final Bytes a, final Bytes b) {
    final int size = Math.min(a.size(), b.size());
    for (int i = 0; i < size; i++) {
      final int diff = Integer.compare(a.get(i) & 0xff, b.get(i) & 0xff);
      if (diff != 0) {
        return diff;
      }
    }
    return Integer.compare(a.size(), b.size());
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "[" + kind + ", base " + baseRootHash + "]";
  }
}
