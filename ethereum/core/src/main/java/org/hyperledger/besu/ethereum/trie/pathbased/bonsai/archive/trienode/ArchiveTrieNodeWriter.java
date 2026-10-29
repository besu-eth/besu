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
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
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

import com.google.common.annotations.VisibleForTesting;
import org.apache.tuweni.bytes.Bytes;

/**
 * Persists the computed history entries to {@code TRIE_BRANCH_STORAGE_ARCHIVE} before each block
 * commit.
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
    final List<Future<List<EncodedEntry>>> inFlight = new ArrayList<>();

    CaptureBuffer(final long block, final Hash blockHash, final boolean chainContiguous) {
      this.block = block;
      this.blockHash = blockHash;
      this.chainContiguous = chainContiguous;
    }
  }

  private static final int BATCH_SIZE = 64;

  private final ArchiveNodeHistoryStore historyStore;
  private final ExecutorService capturePool;
  private final int shallowCheckpointInterval;
  private final int deepCheckpointInterval;

  // One buffer per open transaction, keyed by identity; removed on commit or rollback.
  private final ConcurrentHashMap<SegmentedKeyValueStorageTransaction, CaptureBuffer> buffers =
      new ConcurrentHashMap<>();

  // When present, each block's entries and coverage commit in their own transaction on it.
  private final Optional<SegmentedKeyValueStorage> ownCommitStorage;

  public ArchiveTrieNodeWriter(
      final ArchiveNodeHistoryStore historyStore,
      final ExecutorService capturePool,
      final int shallowCheckpointInterval,
      final int deepCheckpointInterval,
      final Optional<SegmentedKeyValueStorage> ownCommitStorage) {
    this.historyStore = Objects.requireNonNull(historyStore, "historyStore must not be null");
    this.capturePool = Objects.requireNonNull(capturePool, "capturePool must not be null");
    this.shallowCheckpointInterval = shallowCheckpointInterval;
    this.deepCheckpointInterval = deepCheckpointInterval;
    this.ownCommitStorage = Objects.requireNonNull(ownCommitStorage);
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
    final boolean chainContiguous = historyStore.getArchivedBlockNumber() == block - 1;
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
    buf.inFlight.add(capturePool.submit(() -> encodeBatch(batch, buf.chainContiguous)));
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

  public void onBeforeCommit(final SegmentedKeyValueStorageTransaction transaction) {
    final CaptureBuffer buf = buffers.remove(transaction);
    if (buf == null) {
      return;
    }
    if (ownCommitStorage.isEmpty()) {
      writeBlock(buf, transaction);
      return;
    }
    final SegmentedKeyValueStorageTransaction ownTx =
        ownCommitStorage.get().startLowPriorityTransaction();
    try {
      writeBlock(buf, ownTx);
    } catch (final RuntimeException | Error e) {
      ownTx.rollback();
      throw e;
    }
    ownTx.commit();
  }

  private void writeBlock(
      final CaptureBuffer buf, final SegmentedKeyValueStorageTransaction writeTx) {
    synchronized (buf) {
      if (!buf.pendingRequests.isEmpty()) {
        dispatchBatch(buf);
      }
      try {
        for (final Future<List<EncodedEntry>> future : buf.inFlight) {
          for (final EncodedEntry entry : future.get()) {
            historyStore.putEncoded(writeTx, entry.historyKey(), entry.codecEntry());
            entry
                .latestCounter()
                .ifPresentOrElse(
                    counter -> historyStore.putLatestCounter(writeTx, entry.naturalKey(), counter),
                    () -> historyStore.removeLatestCounter(writeTx, entry.naturalKey()));
          }
        }
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException("interrupted while flushing trie-node captures", e);
      } catch (final ExecutionException e) {
        throw new RuntimeException("trie-node capture failed", e.getCause());
      }
      historyStore.saveArchiveProgress(writeTx, buf.block, buf.blockHash);
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
