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

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Limits of an {@link ImmutableTreeCache}.
 *
 * @param pruneAfterBlocks loaded nodes not traversed for this many blocks are unloaded, and storage
 *     roots bound to a role but not used for this many blocks are dropped
 * @param forkRetentionBlocks roots bound to no role are dropped once unused for this many blocks
 * @param maxCachedBytes estimated heap the cached trees may use; above it pruning shortens its
 *     window until they fit, then drops the least recently used roots
 * @param pruneIntervalBlocks the head advances this many blocks between prune runs
 * @param maxUnboundRoots the most roots bound to no role that are kept; registering more starts a
 *     prune, which drops the least recently used ones beyond that
 */
public record ImmutableTreeCacheConfig(
    int pruneAfterBlocks,
    int forkRetentionBlocks,
    long maxCachedBytes,
    int pruneIntervalBlocks,
    int maxUnboundRoots) {

  /** Default number of blocks after which untouched nodes are unloaded. */
  public static final int DEFAULT_PRUNE_AFTER_BLOCKS = 512;

  /** Default heap budget of the cached trees: 512 MiB. */
  public static final long DEFAULT_MAX_CACHED_BYTES = 512L * 1024 * 1024;

  /** The defaults. */
  public static final ImmutableTreeCacheConfig DEFAULT =
      new ImmutableTreeCacheConfig(
          DEFAULT_PRUNE_AFTER_BLOCKS, 8, DEFAULT_MAX_CACHED_BYTES, 8, 50_000);

  /**
   * Validates and creates the limits.
   *
   * @param pruneAfterBlocks see {@link #pruneAfterBlocks()}
   * @param forkRetentionBlocks see {@link #forkRetentionBlocks()}
   * @param maxCachedBytes see {@link #maxCachedBytes()}
   * @param pruneIntervalBlocks see {@link #pruneIntervalBlocks()}
   * @param maxUnboundRoots see {@link #maxUnboundRoots()}
   */
  public ImmutableTreeCacheConfig(
      final int pruneAfterBlocks,
      final int forkRetentionBlocks,
      final long maxCachedBytes,
      final int pruneIntervalBlocks,
      final int maxUnboundRoots) {
    checkArgument(pruneAfterBlocks >= 1, "pruneAfterBlocks must be at least 1");
    checkArgument(forkRetentionBlocks >= 1, "forkRetentionBlocks must be at least 1");
    checkArgument(maxCachedBytes >= 1, "maxCachedBytes must be at least 1");
    checkArgument(pruneIntervalBlocks >= 1, "pruneIntervalBlocks must be at least 1");
    checkArgument(maxUnboundRoots >= 0, "maxUnboundRoots must not be negative");
    this.pruneAfterBlocks = pruneAfterBlocks;
    this.forkRetentionBlocks = forkRetentionBlocks;
    this.maxCachedBytes = maxCachedBytes;
    this.pruneIntervalBlocks = pruneIntervalBlocks;
    this.maxUnboundRoots = maxUnboundRoots;
  }

  /**
   * Returns these limits with another prune window and heap budget.
   *
   * @param pruneAfterBlocks the prune window in blocks
   * @param maxCachedBytes the heap budget in bytes
   * @return the new limits
   */
  public ImmutableTreeCacheConfig withLimits(
      final int pruneAfterBlocks, final long maxCachedBytes) {
    return new ImmutableTreeCacheConfig(
        pruneAfterBlocks,
        forkRetentionBlocks,
        maxCachedBytes,
        pruneIntervalBlocks,
        maxUnboundRoots);
  }
}
