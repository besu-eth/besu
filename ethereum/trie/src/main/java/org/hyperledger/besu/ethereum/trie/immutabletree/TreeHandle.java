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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.tuweni.bytes.Bytes32;

/**
 * A root registered in an {@link ImmutableTreeCache}.
 *
 * <p>The root node is always loaded; its descendants are loaded as traversals touch them. A
 * traversal holds the handle's lock while it runs, and the cache neither evicts a locked root nor
 * prunes nodes below it.
 */
public final class TreeHandle {

  private final TreeKey key;
  private final TreeNode root;
  private final int generation;
  private final AtomicInteger locks = new AtomicInteger();
  private final AtomicLong lastAccessBlock;

  /**
   * Number of role bindings referencing this handle. Changed under the cache's binding lock; read
   * without it by pruning, which checks again under the lock before acting.
   */
  final AtomicInteger bindings = new AtomicInteger();

  /** Heap attributed to this root by the last prune walk. Only used by the pruning thread. */
  long walkBytes;

  TreeHandle(final TreeKey key, final TreeNode root, final long block, final int generation) {
    this.key = key;
    this.root = root;
    this.generation = generation;
    this.lastAccessBlock = new AtomicLong(block);
  }

  /**
   * Returns the key this root is registered under.
   *
   * @return the key
   */
  public TreeKey key() {
    return key;
  }

  /**
   * Returns the root hash.
   *
   * @return the root hash
   */
  public Bytes32 rootHash() {
    return key.rootHash();
  }

  /**
   * Returns the kind of trie.
   *
   * @return the kind
   */
  public TreeKind kind() {
    return key.kind();
  }

  /**
   * Returns the root node, loaded.
   *
   * @return the root node
   */
  public TreeNode root() {
    return root;
  }

  /** The registry generation this handle was created in; a cleared cache starts a new one. */
  int generation() {
    return generation;
  }

  /**
   * Takes the traversal lock. Must be paired with {@link #release()}.
   *
   * @param block the current block, recorded as the last access
   */
  public void acquire(final long block) {
    locks.incrementAndGet();
    touch(block);
  }

  /** Releases the traversal lock. */
  public void release() {
    final int remaining = locks.decrementAndGet();
    assert remaining >= 0 : "Released an unlocked tree handle";
  }

  /**
   * Whether a traversal currently holds the lock.
   *
   * @return true if locked
   */
  public boolean isLocked() {
    return locks.get() > 0;
  }

  long lastAccessBlock() {
    return lastAccessBlock.get();
  }

  void touch(final long block) {
    lastAccessBlock.accumulateAndGet(block, Math::max);
  }

  @Override
  public String toString() {
    return key.toString();
  }
}
