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

import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode.ArchiveNodeHistoryStore.ArchivedBlock;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode.TrieNodeArchiveStep.ArchiveGapException;

import java.io.Closeable;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Keeps the trie-node archive growing with the chain by archiving blocks a fixed distance behind
 * the head, where they can no longer be reorged out: {@code maxLayersToLoad} behind on chains with
 * finality, and up to the head itself on chains without it, where every imported block is final.
 */
public final class TrailingTrieNodeArchiver implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(TrailingTrieNodeArchiver.class);

  private final Blockchain blockchain;
  private final ArchiveNodeHistoryStore historyStore;
  private final TrieNodeArchiveStep step;
  private final long maxLayersToLoad;
  private final ExecutorService executor;
  private final AtomicBoolean running = new AtomicBoolean();
  private OptionalLong observerId = OptionalLong.empty();
  private volatile boolean halted;
  private volatile boolean closed;

  public TrailingTrieNodeArchiver(
      final Blockchain blockchain,
      final ArchiveNodeHistoryStore historyStore,
      final TrieNodeArchiveStep step,
      final long maxLayersToLoad,
      final ExecutorService executor) {
    this.blockchain = blockchain;
    this.historyStore = historyStore;
    this.step = step;
    this.maxLayersToLoad = maxLayersToLoad;
    this.executor = executor;
  }

  public synchronized void start() {
    if (closed) {
      return;
    }
    observerId =
        OptionalLong.of(
            blockchain.observeBlockAdded(
                event -> {
                  if (event.isNewCanonicalHead()) {
                    scheduleCatchUpIfBehind();
                  }
                }));
    scheduleCatchUpIfBehind();
  }

  @VisibleForTesting
  static long targetFor(final long head, final boolean hasFinality, final long maxLayersToLoad) {
    return hasFinality ? Math.max(0, head - maxLayersToLoad) : head;
  }

  @VisibleForTesting
  boolean isHalted() {
    return halted;
  }

  private long target() {
    return targetFor(
        blockchain.getChainHeadBlockNumber(),
        blockchain.getFinalized().isPresent(),
        maxLayersToLoad);
  }

  private void scheduleCatchUpIfBehind() {
    if (halted || closed || lastCovered().map(last -> last >= target()).orElse(true)) {
      return;
    }
    if (!running.compareAndSet(false, true)) {
      return;
    }
    try {
      executor.execute(this::catchUp);
    } catch (final RejectedExecutionException e) {
      running.set(false);
    }
  }

  private void catchUp() {
    final long headAtStart = blockchain.getChainHeadBlockNumber();
    boolean progressed = false;
    try {
      progressed = archiveToTarget();
    } catch (final ArchiveGapException e) {
      halted = true;
      LOG.error("Trie-node archiver halted until restart: {}", e.getMessage());
    } catch (final RuntimeException e) {
      if (!closed) {
        LOG.error("Trie-node archiver failed; retrying on the next block", e);
      }
    } finally {
      running.set(false);
    }
    if (progressed || blockchain.getChainHeadBlockNumber() > headAtStart) {
      scheduleCatchUpIfBehind();
    }
  }

  private boolean archiveToTarget() {
    final Optional<Long> from = lastCovered();
    if (from.isEmpty()) {
      return false;
    }
    step.archiveRange(from.get(), target(), () -> !closed);
    final long after = lastCovered().orElse(from.get());
    if (after > from.get()) {
      LOG.debug("Trie-node archiver archived blocks {}..{}", from.get() + 1, after);
      return true;
    }
    return false;
  }

  private Optional<Long> lastCovered() {
    return historyStore.getArchivedBlock().map(ArchivedBlock::number);
  }

  @Override
  public synchronized void close() {
    closed = true;
    observerId.ifPresent(blockchain::removeObserver);
    observerId = OptionalLong.empty();
    executor.shutdownNow();
    try {
      if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
        LOG.warn("Trie-node archiver did not terminate within 10 seconds");
      }
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
