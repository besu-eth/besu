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

import static com.google.common.base.Preconditions.checkState;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.plugin.data.BlockHeader;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;
import org.apache.tuweni.bytes.Bytes;

/**
 * Persists the computed history entries to {@code TRIE_BRANCH_STORAGE_ARCHIVE}: the capture pool
 * commits them while the block is persisted, and the block's coverage commits before the block.
 */
public class ArchiveTrieNodeWriter implements Closeable {

  private record CaptureRequest(
      Bytes naturalKey, Bytes location, long block, Bytes newNode, Bytes priorNode) {}

  // latestCounter is empty for a deletion, which removes the key's latest counter
  private record EncodedEntry(
      Bytes naturalKey, Bytes historyKey, Bytes codecEntry, Optional<Integer> latestCounter) {}

  private static class CaptureBuffer {
    final long block;
    final Hash blockHash;
    final boolean chainContiguous;
    final List<CaptureRequest> pendingRequests = new ArrayList<>();
    final List<Future<?>> inFlight = new ArrayList<>();

    CaptureBuffer(final long block, final Hash blockHash, final boolean chainContiguous) {
      this.block = block;
      this.blockHash = blockHash;
      this.chainContiguous = chainContiguous;
    }
  }

  private static final int BATCH_SIZE = 64;

  private final ArchiveNodeHistoryStore historyStore;
  private final ArchiveCoverageTracker coverageTracker;
  private final ExecutorService capturePool;
  private final int shallowCheckpointInterval;
  private final int deepCheckpointInterval;

  // One buffer per open transaction, keyed by identity; removed on commit or rollback.
  private final ConcurrentHashMap<SegmentedKeyValueStorageTransaction, CaptureBuffer> buffers =
      new ConcurrentHashMap<>();

  // Each batch of a block's entries and index updates commits in a transaction of its own on the
  // capture pool, and the block's coverage in another, all before the block's.
  private final Supplier<SegmentedKeyValueStorageTransaction> archiveTransactions;

  public ArchiveTrieNodeWriter(
      final ArchiveNodeHistoryStore historyStore,
      final ArchiveCoverageTracker coverageTracker,
      final ExecutorService capturePool,
      final int shallowCheckpointInterval,
      final int deepCheckpointInterval,
      final Supplier<SegmentedKeyValueStorageTransaction> archiveTransactions) {
    this.historyStore = Objects.requireNonNull(historyStore, "historyStore must not be null");
    this.coverageTracker =
        Objects.requireNonNull(coverageTracker, "coverageTracker must not be null");
    this.capturePool = Objects.requireNonNull(capturePool, "capturePool must not be null");
    this.shallowCheckpointInterval = shallowCheckpointInterval;
    this.deepCheckpointInterval = deepCheckpointInterval;
    this.archiveTransactions = Objects.requireNonNull(archiveTransactions);
  }

  @VisibleForTesting
  int checkpointIntervalForDepth(final int locationSizeBytes) {
    return locationSizeBytes <= 2 ? shallowCheckpointInterval : deepCheckpointInterval;
  }

  /**
   * Opens {@code transaction}'s capture buffer for {@code blockHeader}'s block. Entries diff
   * against prior history only when coverage ends at {@code block - 1}; otherwise every entry is
   * written FULL.
   *
   * @param transaction the open transaction that will commit this block's writes
   * @param blockHeader the header of the block being imported
   * @throws IllegalStateException if {@code transaction} already has a begun block
   */
  public void beginBlock(
      final SegmentedKeyValueStorageTransaction transaction, final BlockHeader blockHeader) {
    final long block = blockHeader.getNumber();
    final boolean chainContiguous =
        coverageTracker.getCoverage().map(c -> c.lastBlock() == block - 1).orElse(false);
    checkState(
        buffers.putIfAbsent(
                transaction, new CaptureBuffer(block, blockHeader.getBlockHash(), chainContiguous))
            == null,
        "Block already begun on this transaction");
  }

  /**
   * Captures a trie-node write for async history-entry computation.
   *
   * @param naturalKey the archive key for this node (account or storage form)
   * @param location the nibble-path location in bytes, used to select the checkpoint interval
   * @param block the block number being imported
   * @param newNode the new RLP-encoded node value, or {@code null} if the node is being deleted
   * @param priorNode the previous RLP-encoded node value, or {@code null} if this is a creation
   * @param transaction the open transaction that will commit this block's writes
   * @throws IllegalStateException if {@link #beginBlock} was not called for {@code transaction}
   */
  void capture(
      final Bytes naturalKey,
      final Bytes location,
      final long block,
      final Bytes newNode,
      final Bytes priorNode,
      final SegmentedKeyValueStorageTransaction transaction) {
    final CaptureBuffer buf = buffers.get(transaction);
    if (buf == null) {
      throw new IllegalStateException(
          "trie-node capture for block " + block + " on a transaction with no begun block");
    }
    synchronized (buf) {
      if (buf.block != block) {
        throw new IllegalStateException(
            "trie-node capture buffer holds block "
                + buf.block
                + " but its transaction received a write for block "
                + block);
      }
      buf.pendingRequests.add(new CaptureRequest(naturalKey, location, block, newNode, priorNode));
      if (buf.pendingRequests.size() >= BATCH_SIZE) {
        dispatchBatch(buf);
      }
    }
  }

  private void dispatchBatch(final CaptureBuffer buf) {
    final List<CaptureRequest> batch = List.copyOf(buf.pendingRequests);
    buf.pendingRequests.clear();
    buf.inFlight.add(capturePool.submit(() -> encodeAndCommit(batch, buf.chainContiguous)));
  }

  // Runs on the capture pool: encodes the batch, then commits its entries and index updates in a
  // transaction of its own, so the pool threads share the write work.
  private void encodeAndCommit(final List<CaptureRequest> batch, final boolean chainContiguous) {
    final List<EncodedEntry> entries = encodeBatch(batch, chainContiguous);
    final SegmentedKeyValueStorageTransaction writeTx = archiveTransactions.get();
    try {
      for (final EncodedEntry entry : entries) {
        historyStore.putEncoded(writeTx, entry.historyKey(), entry.codecEntry());
        entry
            .latestCounter()
            .ifPresentOrElse(
                counter -> historyStore.putLatestCounter(writeTx, entry.naturalKey(), counter),
                () -> historyStore.removeLatestCounter(writeTx, entry.naturalKey()));
      }
    } catch (final RuntimeException | Error e) {
      writeTx.rollback();
      throw e;
    }
    writeTx.commit();
  }

  // Looks up the latest counters of the batch's updates in one multiget; creations, deletions and
  // non-contiguous blocks are written FULL.
  private List<EncodedEntry> encodeBatch(
      final List<CaptureRequest> batch, final boolean chainContiguous) {
    final Map<Bytes, Integer> priorCounters =
        chainContiguous
            ? historyStore.getLatestCounters(
                batch.stream()
                    .filter(r -> r.priorNode() != null && r.newNode() != null)
                    .map(CaptureRequest::naturalKey)
                    .toList())
            : Map.of();
    return batch.stream()
        .map(r -> encodeHistoryEntry(r, Optional.ofNullable(priorCounters.get(r.naturalKey()))))
        .toList();
  }

  private EncodedEntry encodeHistoryEntry(
      final CaptureRequest request, final Optional<Integer> priorCounter) {
    final Bytes priorNode = request.priorNode();
    final Bytes newNode = request.newNode();

    if (priorNode == null || newNode == null) {
      return createEncodedEntry(request, 0, ArchiveTrieNodeCodec.encodeDiff(priorNode, newNode));
    }

    if (priorCounter.isPresent()) {
      final int counter = priorCounter.get() + 1;
      if (counter < checkpointIntervalForDepth(request.location().size())) {
        final Bytes codecEntry = ArchiveTrieNodeCodec.encodeDiff(priorNode, newNode);
        // encodeDiff may fall back to FULL when the patch is no smaller than the node
        final int effectiveCounter = ArchiveTrieNodeEntry.isFullEncoded(codecEntry) ? 0 : counter;
        return createEncodedEntry(request, effectiveCounter, codecEntry);
      }
    }
    return createEncodedEntry(request, 0, ArchiveTrieNodeCodec.encodeFull(newNode));
  }

  private EncodedEntry createEncodedEntry(
      final CaptureRequest request, final int counter, final Bytes codecEntry) {
    return new EncodedEntry(
        request.naturalKey(),
        ArchiveNodeKey.historyKey(request.naturalKey(), request.block()),
        codecEntry,
        request.newNode() == null ? Optional.empty() : Optional.of(counter));
  }

  /**
   * Waits for the block's history entries, which the capture pool commits batch by batch, then
   * commits the block's coverage in a transaction of its own. The coverage is the commit point:
   * entries left by a block that fails before it are overwritten when the block is imported again.
   */
  public void onBeforeCommit(final SegmentedKeyValueStorageTransaction transaction) {
    final CaptureBuffer buf = buffers.remove(transaction);
    if (buf == null) {
      return;
    }
    awaitEntries(buf);
    final SegmentedKeyValueStorageTransaction coverageTx = archiveTransactions.get();
    try {
      coverageTracker.record(coverageTx, buf.block, buf.blockHash);
    } catch (final RuntimeException | Error e) {
      coverageTx.rollback();
      throw e;
    }
    coverageTx.commit();
  }

  // Waits for every batch, so none is still writing when this fails, then rethrows the first
  // failure.
  private void awaitEntries(final CaptureBuffer buf) {
    final List<Future<?>> batches;
    synchronized (buf) {
      if (!buf.pendingRequests.isEmpty()) {
        dispatchBatch(buf);
      }
      batches = List.copyOf(buf.inFlight);
    }
    Throwable failure = null;
    for (final Future<?> batch : batches) {
      try {
        batch.get();
      } catch (final InterruptedException e) {
        // batches that have not started never run; the ones that have finish on their own
        batches.forEach(b -> b.cancel(false));
        Thread.currentThread().interrupt();
        throw new RuntimeException("interrupted while flushing trie-node captures", e);
      } catch (final ExecutionException e) {
        if (failure == null) {
          failure = e.getCause();
        }
      }
    }
    if (failure != null) {
      throw new RuntimeException("trie-node capture failed", failure);
    }
  }

  public void onRollback(final SegmentedKeyValueStorageTransaction transaction) {
    buffers.remove(transaction);
  }

  @Override
  public void close() {
    capturePool.shutdown();
    try {
      if (!capturePool.awaitTermination(5, TimeUnit.SECONDS)) {
        capturePool.shutdownNow();
      }
    } catch (final InterruptedException e) {
      capturePool.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }
}
