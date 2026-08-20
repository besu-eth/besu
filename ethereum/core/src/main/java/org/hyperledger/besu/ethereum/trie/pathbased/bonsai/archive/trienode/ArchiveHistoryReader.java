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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reconstructs a trie node's RLP as of a target block via a bounded backward walk over {@link
 * ArchiveNodeHistoryStore}. Finds the nearest FULL checkpoint at or before the target, collects
 * interleaved DIFF entries, and applies them forward.
 */
public final class ArchiveHistoryReader {

  private static final Logger LOG = LoggerFactory.getLogger(ArchiveHistoryReader.class);

  /**
   * Interval, in blocks, at which the writer ({@link ArchiveTrieNodeStrategy}) forces a FULL entry
   * instead of a DIFF, bounding how far this reader ever needs to walk backward to find one.
   */
  public static final int CHECKPOINT_INTERVAL = 16;

  /**
   * Maximum number of backward steps this reader will take looking for a FULL checkpoint before
   * giving up. Must be at least as large as the longest DIFF run {@link ArchiveTrieNodeStrategy}
   * can produce (currently {@code CHECKPOINT_INTERVAL - 1}), or legitimate history becomes
   * unreachable.
   */
  public static final int MAX_BACKWARD_WALK_STEPS = CHECKPOINT_INTERVAL;

  private final ArchiveNodeHistoryStore historyStore;

  /**
   * Creates a reader backed by the given history store.
   *
   * @param historyStore the store to read archived trie-node history from
   */
  public ArchiveHistoryReader(final ArchiveNodeHistoryStore historyStore) {
    this.historyStore = Objects.requireNonNull(historyStore, "historyStore must not be null");
  }

  /**
   * Reconstructs the trie node's RLP as of {@code targetBlock} for the given natural key.
   *
   * @param naturalKey the node's natural key, from {@link ArchiveNodeKey#account} or {@link
   *     ArchiveNodeKey#storage}
   * @param targetBlock the block to reconstruct the node as of; must be >= 0
   * @return the node's RLP at or before {@code targetBlock}, or empty if the node did not exist or
   *     was deleted by then, or if no FULL checkpoint could be found within {@link
   *     #MAX_BACKWARD_WALK_STEPS} backward steps
   * @throws IllegalArgumentException if {@code targetBlock} is negative
   */
  public Optional<Bytes> nodeAt(final Bytes naturalKey, final long targetBlock) {
    Objects.requireNonNull(naturalKey, "naturalKey must not be null");
    if (targetBlock < 0) {
      throw new IllegalArgumentException("targetBlock must be >= 0, got " + targetBlock);
    }

    final Optional<ArchiveNodeHistoryStore.HistoryEntry> anchorEntryOpt =
        historyStore.getLatestBefore(naturalKey, targetBlock);
    if (anchorEntryOpt.isEmpty() || anchorEntryOpt.get().codecEntry().isDeletion()) {
      return Optional.empty();
    }
    final ArchiveNodeHistoryStore.HistoryEntry anchorEntry = anchorEntryOpt.get();
    if (anchorEntry.codecEntry().isFull()) {
      return Optional.of(anchorEntry.codecEntry().fullNode());
    }
    return reconstructFromDiffChain(naturalKey, anchorEntry, targetBlock);
  }

  /**
   * Walks backward from {@code anchorEntry} (itself a DIFF) collecting diffs until a FULL
   * checkpoint turns up, then applies them forward. Gives up after {@link #MAX_BACKWARD_WALK_STEPS}
   * steps, on running out of history, or on hitting a deletion tombstone.
   */
  private Optional<Bytes> reconstructFromDiffChain(
      final Bytes naturalKey,
      final ArchiveNodeHistoryStore.HistoryEntry anchorEntry,
      final long targetBlock) {
    final List<Bytes> diffs = new ArrayList<>();
    diffs.add(anchorEntry.rawEntryBytes());
    long walkBlock = anchorEntry.block();

    for (int steps = 0; walkBlock > 0 && steps < MAX_BACKWARD_WALK_STEPS; steps++) {
      final Optional<ArchiveNodeHistoryStore.HistoryEntry> prevOpt =
          historyStore.getLatestBefore(naturalKey, walkBlock - 1);
      if (prevOpt.isEmpty()) {
        break;
      }
      final ArchiveNodeHistoryStore.HistoryEntry prev = prevOpt.get();
      if (prev.codecEntry().isDeletion()) {
        LOG.warn(
            "unexpected deletion tombstone mid-walk for key {} at block {} (before block {})",
            naturalKey,
            prev.block(),
            targetBlock);
        return Optional.empty();
      }
      if (prev.codecEntry().isFull()) {
        Collections.reverse(diffs);
        return Optional.of(ArchiveTrieNodeCodec.reconstruct(prev.rawEntryBytes(), diffs));
      }
      diffs.add(prev.rawEntryBytes());
      walkBlock = prev.block();
    }

    LOG.warn(
        "no FULL checkpoint found within {} backward steps for key {} at block {}",
        MAX_BACKWARD_WALK_STEPS,
        naturalKey,
        targetBlock);
    return Optional.empty();
  }
}
