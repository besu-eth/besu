/*
 * Copyright contributors to Hyperledger Besu.
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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive;

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
 * TrieNodeHistoryStore}. No index dependency — see the design spec's termination-guarantee
 * rationale for why the walk is bounded by construction, not by empirical tuning.
 */
public final class TrieNodeHistoryReader {

  private static final Logger LOG = LoggerFactory.getLogger(TrieNodeHistoryReader.class);

  /** Uniform checkpoint interval for all non-root nodes (root is always FULL). */
  public static final int CHECKPOINT_INTERVAL = 16;

  /** Corrupt-data guard only; should never bind in correct operation (see design spec). */
  public static final int MAX_BACKWARD_WALK_STEPS = CHECKPOINT_INTERVAL;

  private final TrieNodeHistoryStore historyStore;

  public TrieNodeHistoryReader(final TrieNodeHistoryStore historyStore) {
    this.historyStore = Objects.requireNonNull(historyStore, "historyStore must not be null");
  }

  public Optional<Bytes> nodeAt(final Bytes naturalKey, final long targetBlock) {
    Objects.requireNonNull(naturalKey, "naturalKey must not be null");
    if (targetBlock < 0) {
      throw new IllegalArgumentException("targetBlock must be >= 0, got " + targetBlock);
    }

    final Optional<TrieNodeHistoryStore.HistoryEntry> bStarOpt =
        historyStore.getLatestBefore(naturalKey, targetBlock);
    if (bStarOpt.isEmpty()) {
      return Optional.empty();
    }
    final TrieNodeHistoryStore.HistoryEntry bStar = bStarOpt.get();
    if (bStar.codecEntry().isDeletion()) {
      return Optional.empty();
    }
    if (bStar.codecEntry().isFull()) {
      return Optional.of(bStar.codecEntry().fullNode());
    }

    final List<Bytes> diffsDescending = new ArrayList<>();
    diffsDescending.add(bStar.rawEntryBytes());
    long walkBlock = bStar.block();
    Bytes fullEntry = null;
    int steps = 0;

    while (walkBlock > 0 && steps < MAX_BACKWARD_WALK_STEPS) {
      steps++;
      final Optional<TrieNodeHistoryStore.HistoryEntry> prevOpt =
          historyStore.getLatestBefore(naturalKey, walkBlock - 1);
      if (prevOpt.isEmpty()) {
        break;
      }
      final TrieNodeHistoryStore.HistoryEntry prev = prevOpt.get();
      if (prev.codecEntry().isDeletion()) {
        return Optional.empty();
      }
      if (prev.codecEntry().isFull()) {
        fullEntry = prev.rawEntryBytes();
        break;
      }
      diffsDescending.add(prev.rawEntryBytes());
      walkBlock = prev.block();
    }

    if (fullEntry == null) {
      LOG.warn(
          "no FULL checkpoint found within {} backward steps for key {} at block {}",
          MAX_BACKWARD_WALK_STEPS,
          naturalKey,
          targetBlock);
      return Optional.empty();
    }

    Collections.reverse(diffsDescending);
    return Optional.of(ArchiveTrieNodeCodec.reconstruct(fullEntry, diffsDescending));
  }
}
