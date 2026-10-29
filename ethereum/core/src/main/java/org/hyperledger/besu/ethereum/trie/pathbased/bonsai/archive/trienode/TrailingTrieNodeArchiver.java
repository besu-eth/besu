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
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode.ArchiveCoverageTracker.Coverage;
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
 * the head. On chains with finality it archives in batches of 32 blocks (at most half of {@code
 * maxLayersToLoad}), targeting a batch plus some slack inside the {@code maxLayersToLoad} window
 * the live path serves, so the archive overlaps that window. On chains without finality, where
 * every imported block is final, it stays one block behind the head, so it never archives the block
 * being imported.
 */
public final class TrailingTrieNodeArchiver implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(TrailingTrieNodeArchiver.class);
  private static final long FINALITY_BATCH = 32;

  private final Blockchain blockchain;
  private final ArchiveCoverageTracker coverage;
  private final TrieNodeArchiveStep step;
  private final long maxLayersToLoad;
  private final ExecutorService executor;
  private final AtomicBoolean running = new AtomicBoolean();
  private OptionalLong observerId = OptionalLong.empty();
  private volatile boolean halted;
  private volatile boolean closed;

  public TrailingTrieNodeArchiver(
      final Blockchain blockchain,
      final ArchiveCoverageTracker coverage,
      final TrieNodeArchiveStep step,
      final long maxLayersToLoad,
      final ExecutorService executor) {
    this.blockchain = blockchain;
    this.coverage = coverage;
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
                    scheduleIfBatchReady();
                  }
                }));
    scheduleIfBatchReady();
  }

  @VisibleForTesting
  long targetFor(final long head, final boolean hasFinality) {
    final long batch = minBatchFor(hasFinality);
    final long lag = hasFinality ? Math.max(1, maxLayersToLoad - batch - batch / 4) : 1;
    return Math.max(0, head - lag);
  }

  @VisibleForTesting
  long minBatchFor(final boolean hasFinality) {
    // capped so the target stays at least half the live window behind the head
    return hasFinality ? Math.max(1, Math.min(FINALITY_BATCH, maxLayersToLoad / 2)) : 1;
  }

  @VisibleForTesting
  boolean isHalted() {
    return halted;
  }

  private long target() {
    return targetFor(blockchain.getChainHeadBlockNumber(), blockchain.getFinalized().isPresent());
  }

  private boolean hasBatchToArchive() {
    final long minBatch = minBatchFor(blockchain.getFinalized().isPresent());
    return lastCovered().map(last -> target() - last >= minBatch).orElse(false);
  }

  private void scheduleIfBatchReady() {
    if (halted || closed || !hasBatchToArchive()) {
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
      halt(e.getMessage());
    } catch (final RuntimeException e) {
      if (!closed) {
        LOG.error("Trie-node archiver failed; retrying on the next block", e);
      }
    } finally {
      running.set(false);
    }
    if ((progressed || blockchain.getChainHeadBlockNumber() > headAtStart) && hasBatchToArchive()) {
      scheduleIfBatchReady();
    }
  }

  private boolean archiveToTarget() {
    final Coverage tip = coverage.getCoverage().orElseThrow();
    if (haltIfCannotContinue(tip)) {
      return false;
    }
    step.archiveRange(tip.lastBlock(), target(), () -> !closed);
    return lastCovered().orElse(tip.lastBlock()) > tip.lastBlock();
  }

  private boolean haltIfCannotContinue(final Coverage tip) {
    if (blockchain
        .getBlockHashByNumber(tip.lastBlock())
        .filter(tip.lastBlockHash()::equals)
        .isEmpty()) {
      halt(
          String.format(
              "the archived proof history includes block %d, which is no longer canonical or "
                  + "its header is missing, so capture cannot continue and proof history needs a "
                  + "resync",
              tip.lastBlock()));
      return true;
    }
    final long depth = blockchain.getChainHeadBlockNumber() - tip.lastBlock();
    if (depth > 2 * maxLayersToLoad) {
      halt(
          String.format(
              "the proof archive is %d blocks behind the head, more than the supported 2 x %d "
                  + "rollback, so it cannot catch up and proof history needs a resync",
              depth, maxLayersToLoad));
      return true;
    }
    return false;
  }

  private void halt(final String reason) {
    halted = true;
    LOG.error("Trie-node archiver halted until restart: {}", reason);
  }

  private Optional<Long> lastCovered() {
    return coverage.getCoverage().map(Coverage::lastBlock);
  }

  @Override
  public synchronized void close() {
    closed = true;
    observerId.ifPresent(blockchain::removeObserver);
    observerId = OptionalLong.empty();
    executor.shutdownNow();
    try {
      if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
        LOG.warn("Trie-node archiver did not terminate within 30 seconds and may still be running");
      }
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
