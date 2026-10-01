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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.trie.MerkleTrie;
import org.hyperledger.besu.ethereum.trie.NodeLoader;
import org.hyperledger.besu.ethereum.trie.immutabletree.ImmutableTreeCache;
import org.hyperledger.besu.ethereum.trie.immutabletree.ImmutableTreeMerkleTrie;
import org.hyperledger.besu.ethereum.trie.immutabletree.ImmutableTreeMerkleTrie.RootListener;
import org.hyperledger.besu.ethereum.trie.immutabletree.TreeHandle;
import org.hyperledger.besu.ethereum.trie.immutabletree.TreeKind;
import org.hyperledger.besu.ethereum.trie.immutabletree.TreeNode;
import org.hyperledger.besu.ethereum.trie.immutabletree.TreeRole;
import org.hyperledger.besu.plugin.data.BlockHeader;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ForkJoinPool;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Connects a Bonsai world state to the {@link ImmutableTreeCache}: builds the tries its state root
 * computations run on, and registers the roots they produce once the computation succeeded.
 *
 * <p>A persist or a frozen root recompute {@link #begin(BlockHeader) begins} collecting. The
 * account trie and each storage trie report their root when they are hashed or committed. If the
 * persist succeeds, {@link #finish} registers the state root and binds it to the role of the world
 * state: {@link TreeRole#HEAD} for the head world state, {@link TreeRole#NEW_PAYLOAD} for a frozen
 * one (payload validation), none for a rolled layer. The storage roots are bound per account to the
 * same role. Frozen root recomputes (block building, simulations) register their roots as forks,
 * bound to no role. If the computation fails, including on a state root mismatch, {@link
 * #discard()} drops everything, so a root that does not match its block header is never registered.
 * A block rejected after its state was persisted, for example for a wrong receipts root, keeps its
 * roots registered: they describe their state correctly, and the role's next binding replaces them.
 *
 * <p>Tries for frontier receipt roots report nothing: their intermediate roots are never
 * registered.
 */
public class BonsaiImmutableTrees {

  private static final Logger LOG = LoggerFactory.getLogger(BonsaiImmutableTrees.class);

  /**
   * A root computed by a trie.
   *
   * @param trie the trie that computed it
   * @param root the root node
   */
  public record Computed(ImmutableTreeMerkleTrie trie, TreeNode root) {}

  /**
   * The roots of one computation, handed from a background computation to the world state that
   * persists its result.
   *
   * @param state the account trie root, or null if none was computed
   * @param storage the storage roots by account hash
   */
  public record Results(Computed state, Map<Bytes32, Computed> storage) {}

  private final ImmutableTreeCache cache;
  private final Map<Bytes32, Computed> storage = new ConcurrentHashMap<>();
  private volatile boolean collecting;
  private volatile Computed state;

  /**
   * Creates the connection.
   *
   * @param cache the node's immutable tree cache
   */
  public BonsaiImmutableTrees(final ImmutableTreeCache cache) {
    this.cache = cache;
  }

  /**
   * Returns the cache.
   *
   * @return the cache
   */
  public ImmutableTreeCache cache() {
    return cache;
  }

  /**
   * Creates the account trie of a state root computation.
   *
   * @param rootHash the world state root to start from
   * @param loader loads account trie nodes, consistent with the root
   * @param pool the pool to update and hash in, or null
   * @return the trie
   */
  public MerkleTrie<Bytes, Bytes> accountTrie(
      final Bytes32 rootHash, final NodeLoader loader, final ForkJoinPool pool) {
    return new ImmutableTreeMerkleTrie(
        cache,
        TreeKind.STATE,
        rootHash,
        loader,
        pool,
        (trie, root, committed) -> {
          if (collecting) {
            state = new Computed(trie, root);
          }
        });
  }

  /**
   * Creates the storage trie of an account for a state root computation.
   *
   * @param accountHash the account
   * @param rootHash the storage root to start from
   * @param loader loads the account's storage trie nodes, consistent with the root
   * @param pool the pool to update and hash in, or null
   * @return the trie
   */
  public MerkleTrie<Bytes, Bytes> storageTrie(
      final Hash accountHash,
      final Bytes32 rootHash,
      final NodeLoader loader,
      final ForkJoinPool pool) {
    final Bytes32 account = Bytes32.wrap(accountHash.getBytes());
    return new ImmutableTreeMerkleTrie(
        cache,
        TreeKind.STORAGE,
        rootHash,
        loader,
        pool,
        (trie, root, committed) -> {
          if (collecting) {
            storage.put(account, new Computed(trie, root));
          }
        });
  }

  /**
   * Creates a trie for frontier receipt roots, updated one transaction at a time. Its roots are
   * never registered.
   *
   * @param kind the kind of trie
   * @param rootHash the root to start from
   * @param loader loads its nodes, consistent with the root
   * @return the trie
   */
  public MerkleTrie<Bytes, Bytes> frontierTrie(
      final TreeKind kind, final Bytes32 rootHash, final NodeLoader loader) {
    return new ImmutableTreeMerkleTrie(cache, kind, rootHash, loader, null, RootListener.NONE);
  }

  /**
   * Starts collecting the roots of a computation. The cache moves to the block first, so the nodes
   * the computation touches are stamped with it.
   *
   * @param blockHeader the block being persisted, or null for a root recompute
   */
  public void begin(final BlockHeader blockHeader) {
    if (blockHeader != null) {
      cache.advanceBlock(blockHeader.getNumber(), false);
    }
    clearResults();
    collecting = true;
  }

  /** Stops collecting and drops what was collected. */
  public void discard() {
    collecting = false;
    clearResults();
  }

  /**
   * Stops collecting and returns what was collected, for another world state to register.
   *
   * @return the collected roots
   */
  public Results drain() {
    collecting = false;
    final Results results = new Results(state, Map.copyOf(storage));
    clearResults();
    return results;
  }

  /**
   * Adds roots collected by another world state's computation, whose result this one persists.
   *
   * @param results the roots
   */
  public void adopt(final Results results) {
    if (collecting && results != null) {
      if (results.state() != null) {
        state = results.state();
      }
      storage.putAll(results.storage());
    }
  }

  private void clearResults() {
    state = null;
    storage.clear();
  }

  /**
   * Registers the collected roots after the computation was persisted.
   *
   * @param role the role of the world state, or null for a fork
   * @param blockHeader the persisted block, or null for a root recompute
   * @param persistedRoot the state root the world state now has
   */
  public void finish(final TreeRole role, final BlockHeader blockHeader, final Hash persistedRoot) {
    if (!collecting) {
      return;
    }
    final Computed stateResult = state;
    final Map<Bytes32, Computed> storageResults = Map.copyOf(storage);
    discard();
    if (stateResult == null) {
      return;
    }
    try {
      register(role, blockHeader, persistedRoot, stateResult, storageResults);
    } catch (final RuntimeException e) {
      // The cache only saves work: never fail the block over it
      LOG.warn("Failed to register immutable tree roots, clearing the cache", e);
      cache.clear();
    }
  }

  private void register(
      final TreeRole role,
      final BlockHeader blockHeader,
      final Hash persistedRoot,
      final Computed stateResult,
      final Map<Bytes32, Computed> storageResults) {
    final Bytes32 stateRoot = stateResult.root().hash();
    if (!persistedRoot.getBytes().equals(stateRoot)) {
      LOG.warn(
          "Immutable tree state root {} differs from the persisted root {}, not registering it",
          stateRoot,
          persistedRoot);
      return;
    }
    final TreeHandle stateHandle = cache.register(TreeKind.STATE, stateResult.root());
    long storageBasesCached = 0;
    long storageLoads = 0;
    for (final Map.Entry<Bytes32, Computed> entry : storageResults.entrySet()) {
      final TreeHandle storageHandle = cache.register(TreeKind.STORAGE, entry.getValue().root());
      if (role != null) {
        cache.bindStorage(role, entry.getKey(), storageHandle);
      }
      if (entry.getValue().trie().isBaseCached()) {
        storageBasesCached++;
      }
      storageLoads += entry.getValue().trie().loadCount();
    }
    if (role == TreeRole.HEAD) {
      cache.setHead(stateHandle);
    } else if (role == TreeRole.NEW_PAYLOAD) {
      cache.setNewPayload(stateHandle);
    }
    if (blockHeader != null) {
      cache.advanceBlock(blockHeader.getNumber(), role == TreeRole.HEAD);
    }

    final long basesCached = storageBasesCached;
    final long loads = storageLoads;
    LOG.atDebug()
        .setMessage(
            "Immutable tree {} state root {} for block {} computed from base {} (base cached: {},"
                + " {} account trie nodes loaded); {} storage roots ({} bases cached, {} storage"
                + " trie nodes loaded); cache: {}")
        .addArgument(() -> role == null ? "fork" : role)
        .addArgument(stateRoot)
        .addArgument(() -> blockHeader == null ? "-" : Long.toString(blockHeader.getNumber()))
        .addArgument(() -> stateResult.trie().baseRootHash())
        .addArgument(() -> stateResult.trie().isBaseCached())
        .addArgument(() -> stateResult.trie().loadCount())
        .addArgument(storageResults::size)
        .addArgument(basesCached)
        .addArgument(loads)
        .addArgument(this::describeCache)
        .log();
  }

  private String describeCache() {
    final ImmutableTreeCache.Stats stats = cache.stats();
    return String.format(
        "%d state roots, %d storage roots, head %s, new payload %s, storage bindings %d head / %d"
            + " new payload, opens %d cached / %d from storage, last prune [%s]",
        stats.stateRoots(),
        stats.storageRoots(),
        stats.head(),
        stats.newPayload(),
        stats.headStorageBindings(),
        stats.newPayloadStorageBindings(),
        stats.cachedOpens(),
        stats.storedOpens(),
        stats.lastPrune());
  }
}
