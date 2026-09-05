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

import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import org.apache.tuweni.bytes.Bytes;

/**
 * Persists the computed history entries to {@code TRIE_BRANCH_STORAGE_ARCHIVE} before each block
 * commit.
 */
public class ArchiveTrieNodeWriter implements Closeable {

  private record CaptureRequest(
      Bytes naturalKey, Bytes location, long block, Bytes newNode, Bytes priorNode) {}

  private record EncodedEntry(Bytes historyKey, Bytes storedValue) {}

  private static class CaptureBuffer {
    final long block;
    final boolean chainContiguous;
    final List<CaptureRequest> pendingRequests = new ArrayList<>();
    final List<Future<List<EncodedEntry>>> inFlight = new ArrayList<>();

    CaptureBuffer(final long block, final boolean chainContiguous) {
      this.block = block;
      this.chainContiguous = chainContiguous;
    }
  }

  private static final int BATCH_SIZE = 64;

  /**
   * Returns the checkpoint interval for a node at the given nibble-path depth (in {@code location}
   * bytes).
   *
   * <ul>
   *   <li>depth 0 (root) → the configured shallow interval (root and shallow share the same
   *       interval)
   *   <li>depth 1–2 → the configured shallow interval
   *   <li>depth ≥ 3 → the configured deep interval
   * </ul>
   *
   * @param locationSizeBytes the trie node's {@code location.size()} in bytes
   * @return the mutation interval at which a FULL entry is emitted
   */
  @VisibleForTesting
  int checkpointIntervalForDepth(final int locationSizeBytes) {
    return locationSizeBytes <= 2 ? shallowCheckpointInterval : deepCheckpointInterval;
  }

  private final ArchiveNodeHistoryStore historyStore;
  private final ArchiveCoverageTracker coverageTracker;
  private final ExecutorService capturePool;
  private final int shallowCheckpointInterval;
  private final int deepCheckpointInterval;

  // One buffer per open transaction, keyed by identity; removed on commit or rollback.
  private final ConcurrentHashMap<SegmentedKeyValueStorageTransaction, CaptureBuffer> buffers =
      new ConcurrentHashMap<>();
  private volatile long lastArchivedBlock = -1L;

  public ArchiveTrieNodeWriter(
      final ArchiveNodeHistoryStore historyStore,
      final ArchiveCoverageTracker coverageTracker,
      final ExecutorService capturePool,
      final int shallowCheckpointInterval,
      final int deepCheckpointInterval) {
    this.historyStore = Objects.requireNonNull(historyStore, "historyStore must not be null");
    this.coverageTracker =
        Objects.requireNonNull(coverageTracker, "coverageTracker must not be null");
    this.capturePool = Objects.requireNonNull(capturePool, "capturePool must not be null");
    this.shallowCheckpointInterval = shallowCheckpointInterval;
    this.deepCheckpointInterval = deepCheckpointInterval;
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
   */
  void capture(
      final Bytes naturalKey,
      final Bytes location,
      final long block,
      final Bytes newNode,
      final Bytes priorNode,
      final SegmentedKeyValueStorageTransaction transaction) {
    final CaptureBuffer buf =
        buffers.computeIfAbsent(
            transaction, tx -> new CaptureBuffer(block, block == lastArchivedBlock + 1L));
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
    buf.inFlight.add(
        capturePool.submit(
            () -> batch.stream().map(r -> encodeHistoryEntry(r, buf.chainContiguous)).toList()));
  }

  private EncodedEntry encodeHistoryEntry(
      final CaptureRequest request, final boolean chainContiguous) {
    final Bytes priorNode = request.priorNode();
    final Bytes newNode = request.newNode();

    if (priorNode == null || newNode == null) {
      return createEncodedEntry(request, 0, ArchiveTrieNodeCodec.encodeDiff(priorNode, newNode));
    }

    if (request.block() != 0L && chainContiguous) {
      final Optional<ArchiveNodeHistoryStore.HistoryEntry> priorEntry =
          historyStore.getLatestBefore(request.naturalKey(), request.block() - 1L);
      if (priorEntry.isPresent() && !priorEntry.get().codecEntry().isDeletion()) {
        final int counter = priorEntry.get().counter() + 1;
        if (counter < checkpointIntervalForDepth(request.location().size())) {
          final Bytes codecEntry = ArchiveTrieNodeCodec.encodeDiff(priorNode, newNode);
          // encodeDiff may fall back to FULL when the patch is no smaller than the node
          final int effectiveCounter = ArchiveTrieNodeEntry.isFullEncoded(codecEntry) ? 0 : counter;
          return createEncodedEntry(request, effectiveCounter, codecEntry);
        }
      }
    }
    return createEncodedEntry(request, 0, ArchiveTrieNodeCodec.encodeFull(newNode));
  }

  private EncodedEntry createEncodedEntry(
      final CaptureRequest request, final int counter, final Bytes codecEntry) {
    return new EncodedEntry(
        ArchiveNodeKey.historyKey(request.naturalKey(), request.block()),
        ArchiveNodeHistoryStore.encodeStoredValue(counter, codecEntry));
  }

  public void onBeforeCommit(final SegmentedKeyValueStorageTransaction transaction) {
    final CaptureBuffer buf = buffers.remove(transaction);
    if (buf == null) {
      return;
    }
    synchronized (buf) {
      if (!buf.pendingRequests.isEmpty()) {
        dispatchBatch(buf);
      }
      try {
        for (final Future<List<EncodedEntry>> future : buf.inFlight) {
          for (final EncodedEntry entry : future.get()) {
            historyStore.putEncoded(transaction, entry.historyKey(), entry.storedValue());
          }
        }
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException("interrupted while flushing trie-node captures", e);
      } catch (final ExecutionException e) {
        throw new RuntimeException("trie-node capture failed", e.getCause());
      }
      coverageTracker.record(transaction, buf.block);
      lastArchivedBlock = buf.block;
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
