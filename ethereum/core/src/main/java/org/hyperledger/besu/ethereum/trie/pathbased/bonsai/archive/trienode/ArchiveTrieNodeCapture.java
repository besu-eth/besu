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

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/**
 * Captures trie-node writes as async tasks and persists the computed history entries to {@code
 * TRIE_BRANCH_STORAGE_ARCHIVE} before each block commit.
 *
 * <p>Per-thread isolation is achieved via a {@link ThreadLocal}: each calling thread owns its own
 * {@link CaptureBuffer}. {@link #onBeforeCommit} and {@link #onRollback} check {@code
 * owningTransaction} identity before acting, so a mid-block rollback from {@link
 * org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.BonsaiWorldStateUpdateAccumulator}
 * (which shares the import thread but carries a different transaction) is correctly ignored.
 */
public class ArchiveTrieNodeCapture {

  private record CaptureRequest(
      Bytes naturalKey,
      Bytes location,
      long block,
      Hash accountHash,
      Bytes32 nodeHash,
      Bytes newNode,
      Bytes priorNode) {}

  private record CaptureResult(Bytes historyKey, Bytes storedValue) {}

  private static class CaptureBuffer {
    final SegmentedKeyValueStorageTransaction owningTransaction;
    final long block;
    final boolean chainContiguous;
    final List<CaptureRequest> pendingRequests = new ArrayList<>();
    final List<Future<List<CaptureResult>>> inFlight = new ArrayList<>();

    CaptureBuffer(
        final SegmentedKeyValueStorageTransaction owningTransaction,
        final long block,
        final boolean chainContiguous) {
      this.owningTransaction = owningTransaction;
      this.block = block;
      this.chainContiguous = chainContiguous;
    }
  }

  private static final int CAPTURE_CHUNK_SIZE = 64;

  private final ArchiveNodeHistoryStore historyStore;
  private final ArchiveCoverageTracker coverageTracker;
  private final ExecutorService capturePool;

  // Instance-level (not static) so multiple instances in tests don't share slots.
  @SuppressWarnings("ThreadLocalUsage")
  private final ThreadLocal<CaptureBuffer> threadLocalBuffer = new ThreadLocal<>();

  // Written and read only in onBeforeCommit, always on the same calling thread.
  private long lastArchivedBlock = -1L;

  public ArchiveTrieNodeCapture(
      final ArchiveNodeHistoryStore historyStore,
      final ArchiveCoverageTracker coverageTracker,
      final ExecutorService capturePool) {
    this.historyStore = historyStore;
    this.coverageTracker = coverageTracker;
    this.capturePool = capturePool;
  }

  void enqueue(
      final Bytes naturalKey,
      final Bytes location,
      final long block,
      final Hash accountHash,
      final Bytes32 nodeHash,
      final Bytes newNode,
      final Bytes priorNode,
      final SegmentedKeyValueStorageTransaction transaction) {
    CaptureBuffer buf = threadLocalBuffer.get();
    if (buf == null) {
      buf = new CaptureBuffer(transaction, block, block == lastArchivedBlock + 1L);
      threadLocalBuffer.set(buf);
    } else if (buf.block != block) {
      throw new IllegalStateException(
          "trie-node capture buffer holds block "
              + buf.block
              + " but received a write for block "
              + block
              + " — previous block was neither flushed nor discarded");
    }
    buf.pendingRequests.add(
        new CaptureRequest(naturalKey, location, block, accountHash, nodeHash, newNode, priorNode));
    if (buf.pendingRequests.size() >= CAPTURE_CHUNK_SIZE) {
      submitChunk(buf);
    }
  }

  private void submitChunk(final CaptureBuffer buf) {
    final List<CaptureRequest> chunk = List.copyOf(buf.pendingRequests);
    buf.pendingRequests.clear();
    buf.inFlight.add(
        capturePool.submit(
            () ->
                chunk.stream()
                    .flatMap(r -> computeCapture(r, buf.chainContiguous).stream())
                    .toList()));
  }

  private Optional<CaptureResult> computeCapture(
      final CaptureRequest request, final boolean chainContiguous) {
    final Bytes priorNode = request.priorNode();
    final Bytes newNode = request.newNode();

    if (priorNode == null || newNode == null) {
      return Optional.of(result(request, 0, ArchiveTrieNodeCodec.encodeDiff(priorNode, newNode)));
    }

    if (!request.location().isEmpty() && request.block() != 0L && chainContiguous) {
      final Optional<ArchiveNodeHistoryStore.HistoryEntry> priorEntry =
          historyStore.getLatestBefore(request.naturalKey(), request.block() - 1L);
      if (priorEntry.isPresent() && !priorEntry.get().codecEntry().isDeletion()) {
        final int counter = priorEntry.get().counter() + 1;
        if (counter < ArchiveHistoryReader.CHECKPOINT_INTERVAL) {
          return Optional.of(
              result(request, counter, ArchiveTrieNodeCodec.encodeDiff(priorNode, newNode)));
        }
      }
    }
    return Optional.of(result(request, 0, ArchiveTrieNodeCodec.encodeFull(newNode)));
  }

  private static CaptureResult result(
      final CaptureRequest request, final int counter, final Bytes codecEntry) {
    return new CaptureResult(
        ArchiveNodeKey.historyKey(request.naturalKey(), request.block()),
        ArchiveNodeHistoryStore.encodeStoredValue(counter, codecEntry));
  }

  public void onBeforeCommit(final SegmentedKeyValueStorageTransaction transaction) {
    final CaptureBuffer buf = threadLocalBuffer.get();
    if (buf == null || buf.owningTransaction != transaction) {
      return;
    }
    threadLocalBuffer.remove();
    if (!buf.pendingRequests.isEmpty()) {
      submitChunk(buf);
    }
    try {
      for (final Future<List<CaptureResult>> future : buf.inFlight) {
        for (final CaptureResult r : future.get()) {
          historyStore.putEncoded(transaction, r.historyKey(), r.storedValue());
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

  public void onRollback(final SegmentedKeyValueStorageTransaction transaction) {
    final CaptureBuffer buf = threadLocalBuffer.get();
    if (buf == null || buf.owningTransaction != transaction) {
      return;
    }
    threadLocalBuffer.remove();
  }
}
