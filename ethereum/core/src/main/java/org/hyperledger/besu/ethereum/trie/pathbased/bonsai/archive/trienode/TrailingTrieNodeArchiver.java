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
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode.ArchiveNodeHistoryStore.ArchivedBlock;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode.TrieLogReplayArchiver.UnarchivableBlockException;

import java.io.Closeable;
import java.util.OptionalLong;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongSupplier;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Archives trie nodes in the background, trailing the chain head. On each new chain head it walks
 * from the last archived block up to {@code target}, archiving one block at a time with a {@link
 * TrieLogReplayArchiver}.
 *
 * <p>A pass ends early if a reorg moves the chain, and the next pass picks up from the last
 * archived block. If a block can't be archived, it stops until restart.
 */
public final class TrailingTrieNodeArchiver implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(TrailingTrieNodeArchiver.class);

  private final Blockchain blockchain;
  private final ArchiveNodeHistoryStore historyStore;
  private final TrieLogReplayArchiver replayArchiver;
  private final LongSupplier target;
  private final ExecutorService executor;
  private final AtomicBoolean running = new AtomicBoolean();
  private OptionalLong observerId = OptionalLong.empty();
  private volatile boolean halted;
  private volatile boolean closed;

  /**
   * @param target the highest block that may be archived: final, and no higher than the persisted
   *     head
   */
  public TrailingTrieNodeArchiver(
      final Blockchain blockchain,
      final ArchiveNodeHistoryStore historyStore,
      final TrieLogReplayArchiver replayArchiver,
      final LongSupplier target,
      final ExecutorService executor) {
    this.blockchain = blockchain;
    this.historyStore = historyStore;
    this.replayArchiver = replayArchiver;
    this.target = target;
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
                    schedule();
                  }
                }));
    schedule();
  }

  @VisibleForTesting
  boolean isHalted() {
    return halted;
  }

  private boolean hasWork() {
    return historyStore
        .getArchivedBlock()
        .map(last -> target.getAsLong() > last.number())
        .orElse(false);
  }

  private void schedule() {
    if (halted || closed || !hasWork() || !running.compareAndSet(false, true)) {
      return;
    }
    try {
      executor.execute(this::catchUp);
    } catch (final RejectedExecutionException e) {
      running.set(false);
    }
  }

  private void catchUp() {
    boolean completed = false;
    try {
      archiveToTarget();
      completed = true;
    } catch (final UnarchivableBlockException e) {
      halt(e.getMessage());
    } catch (final RuntimeException e) {
      if (!closed) {
        LOG.error("Trie-node archiver failed; retrying on the next block", e);
      }
    } finally {
      running.set(false);
    }
    if (completed) {
      // blocks may have become final while this pass ran
      schedule();
    }
  }

  @VisibleForTesting
  void archiveToTarget() {
    // Read the target first: inline capture has then settled every block up to it, so it never
    // writes a block this pass archives.
    final long to = target.getAsLong();
    final ArchivedBlock last = historyStore.getArchivedBlock().orElseThrow();
    BlockHeader parent =
        blockchain
            .getBlockHeader(last.number())
            .filter(header -> header.getBlockHash().equals(last.hash()))
            .orElseThrow(
                () ->
                    new UnarchivableBlockException(
                        String.format(
                            "archived block %d (%s) is no longer canonical, so proof history"
                                + " needs a resync",
                            last.number(), last.hash())));
    for (long number = last.number() + 1; number <= to && !closed; number++) {
      final BlockHeader block = blockchain.getBlockHeader(number).orElse(null);
      // a reorg moved the chain under us; the next pass rechecks the archived block
      if (block == null || !block.getParentHash().equals(parent.getBlockHash())) {
        return;
      }
      replayArchiver.archive(parent, block);
      parent = block;
    }
  }

  private void halt(final String reason) {
    halted = true;
    LOG.error("Trie-node archiver stopped until restart: {}", reason);
  }

  @Override
  public synchronized void close() {
    closed = true;
    observerId.ifPresent(blockchain::removeObserver);
    observerId = OptionalLong.empty();
    executor.shutdownNow();
    try {
      if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
        LOG.warn("Trie-node archiver did not stop within 30 seconds");
      }
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    } finally {
      replayArchiver.close();
    }
  }
}
