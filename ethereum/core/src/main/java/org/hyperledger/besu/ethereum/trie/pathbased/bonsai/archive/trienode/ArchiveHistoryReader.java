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
   *     was deleted by then, or if the diff chain is inconsistent (no FULL checkpoint found where
   *     the stored counter indicates)
   * @throws IllegalArgumentException if {@code targetBlock} is negative
   */
  public Optional<Bytes> nodeAt(final Bytes naturalKey, final long targetBlock) {
    Objects.requireNonNull(naturalKey, "naturalKey must not be null");
    if (targetBlock < 0) {
      throw new IllegalArgumentException("targetBlock must be >= 0, got " + targetBlock);
    }

    final Optional<ArchiveNodeHistoryStore.HistoryEntry> maybeAnchorEntry =
        historyStore.getLatestBefore(naturalKey, targetBlock);
    if (maybeAnchorEntry.isEmpty() || maybeAnchorEntry.get().codecEntry().isDeletion()) {
      return Optional.empty();
    }
    final ArchiveNodeHistoryStore.HistoryEntry anchorEntry = maybeAnchorEntry.get();
    if (anchorEntry.codecEntry().isFull()) {
      return Optional.of(anchorEntry.codecEntry().fullNode());
    }
    return reconstructFromDiffChain(naturalKey, anchorEntry, targetBlock);
  }

  /**
   * Walks backward from {@code anchorEntry} (itself a DIFF) exactly {@code anchorEntry.counter()}
   * steps — the distance to the FULL checkpoint recorded by the writer — then applies the collected
   * diffs forward. The counter is a single unsigned byte, so the walk is inherently bounded to at
   * most {@link ArchiveNodeHistoryStore#MAX_COUNTER} steps. Treats any mismatch between the counter
   * and the actual chain (a missing entry, an unexpected deletion, or no FULL at the expected
   * point) as corruption.
   */
  private Optional<Bytes> reconstructFromDiffChain(
      final Bytes naturalKey,
      final ArchiveNodeHistoryStore.HistoryEntry anchorEntry,
      final long targetBlock) {
    final int stepsToFull = anchorEntry.counter();
    final List<Bytes> diffs = new ArrayList<>(stepsToFull);
    diffs.add(anchorEntry.rawEntryBytes());
    long walkBlock = anchorEntry.block();

    for (int step = 0; step < stepsToFull && walkBlock > 0; step++) {
      final Optional<ArchiveNodeHistoryStore.HistoryEntry> prevOpt =
          historyStore.getLatestBefore(naturalKey, walkBlock - 1);
      if (prevOpt.isEmpty()) {
        LOG.warn(
            "expected a FULL checkpoint {} steps back for key {} but history ran out before block {} (target block {})",
            stepsToFull,
            naturalKey,
            walkBlock,
            targetBlock);
        return Optional.empty();
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
        "expected a FULL checkpoint within {} steps for key {} but none was found (target block {})",
        stepsToFull,
        naturalKey,
        targetBlock);
    return Optional.empty();
  }
}
