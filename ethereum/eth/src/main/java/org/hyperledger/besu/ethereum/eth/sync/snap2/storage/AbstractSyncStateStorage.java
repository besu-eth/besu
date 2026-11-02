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
package org.hyperledger.besu.ethereum.eth.sync.snap2.storage;

import org.hyperledger.besu.ethereum.trie.RangeManager;
import org.hyperledger.besu.ethereum.trie.TrieBytes;
import org.hyperledger.besu.plugin.services.storage.SegmentIdentifier;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Stream;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/** Flat state in two segments, accounts keyed by hash and slots by both, and the trie writer. */
abstract class AbstractSyncStateStorage implements SyncStateStorage {

  // bounds the write batch a writer keeps before committing
  static final int BYTES_PER_COMMIT = 8 * 1024 * 1024;
  private static final int DELETES_PER_COMMIT = 10_000;
  private static final byte[] ZEROS = new byte[32];
  private static final byte[] ONES = RangeManager.MAX_RANGE.toArray();

  protected final SegmentedKeyValueStorage flat;
  protected final SegmentIdentifier accountSegment;
  protected final SegmentIdentifier storageSegment;

  protected AbstractSyncStateStorage(
      final SegmentedKeyValueStorage flat,
      final SegmentIdentifier accountSegment,
      final SegmentIdentifier storageSegment) {
    this.flat = flat;
    this.accountSegment = accountSegment;
    this.storageSegment = storageSegment;
  }

  @Override
  public Optional<Bytes> getAccount(final Bytes32 accountHash) {
    return flat.get(accountSegment, accountHash.toArrayUnsafe()).map(Bytes::wrap);
  }

  @Override
  public void scan(final Bytes32 from, final Bytes32 to, final ScanVisitor visitor) {
    try (Stream<Pair<byte[], byte[]>> accounts =
            flat.streamFromKey(accountSegment, from.toArrayUnsafe(), to.toArrayUnsafe());
        SlotCursor slots =
            new SlotCursor(concat(from.toArrayUnsafe(), ZEROS), concat(to.toArrayUnsafe(), ONES))) {
      final Iterator<Pair<byte[], byte[]>> accountIterator = accounts.iterator();
      while (accountIterator.hasNext()) {
        final Pair<byte[], byte[]> account = accountIterator.next();
        final byte[] accountKey = account.getKey();
        visitSlotsBefore(slots.iterator, accountKey, visitor);
        if (visitor.onAccount(Bytes32.wrap(accountKey), Bytes.wrap(account.getValue()))) {
          visitSlotsUpTo(slots.iterator, accountKey, visitor);
        } else {
          slots.skip(accountKey);
        }
      }
      visitSlotsUpTo(slots.iterator, ONES, visitor);
    }
  }

  // the slots of a scan, which can jump over the slots of an account without reading them
  private final class SlotCursor implements AutoCloseable {
    private final byte[] end;
    private Stream<Pair<byte[], byte[]>> stream;
    private PeekingIterator<Pair<byte[], byte[]>> iterator;

    private SlotCursor(final byte[] start, final byte[] end) {
      this.end = end;
      open(start);
    }

    private void open(final byte[] start) {
      stream =
          Arrays.compareUnsigned(start, end) > 0
              ? Stream.empty()
              : flat.streamFromKey(storageSegment, start, end);
      iterator = Iterators.peekingIterator(stream.iterator());
    }

    private void skip(final byte[] accountKey) {
      if (!iterator.hasNext() || compareAccount(iterator.peek().getKey(), accountKey) != 0) {
        return;
      }
      stream.close();
      final Optional<Bytes32> next = RangeManager.incrementBytes32(Bytes32.wrap(accountKey));
      if (next.isEmpty()) {
        stream = Stream.empty();
        iterator = Iterators.peekingIterator(Collections.emptyIterator());
      } else {
        open(concat(next.get().toArrayUnsafe(), ZEROS));
      }
    }

    @Override
    public void close() {
      stream.close();
    }
  }

  @Override
  public void deleteStorage(final Bytes32 accountHash) {
    final byte[] prefix = accountHash.toArrayUnsafe();
    try (Stream<Pair<byte[], byte[]>> slots =
        flat.streamFromKey(storageSegment, concat(prefix, ZEROS), concat(prefix, ONES))) {
      final Iterator<Pair<byte[], byte[]>> it = slots.iterator();
      while (it.hasNext()) {
        write(
            batch -> {
              for (int deletes = 0; deletes < DELETES_PER_COMMIT && it.hasNext(); deletes++) {
                batch.removeStorage(accountHash, Bytes32.wrap(it.next().getKey(), 32));
              }
            });
      }
    }
  }

  @Override
  public boolean hasStorageTrie(final Bytes32 accountHash, final Bytes32 storageRoot) {
    return storageTrieNode(accountHash, Bytes.EMPTY, storageRoot).isPresent();
  }

  /** A batch whose abort does nothing once its commit has started, or after an abort. */
  protected abstract static class FlatBatch implements Batch {
    private boolean completed;

    @Override
    public final void commit() {
      completed = true;
      commitWrites();
    }

    // a completed transaction refuses its rollback, even one whose commit failed
    @Override
    public final void abort() {
      if (!completed) {
        completed = true;
        dropWrites();
      }
    }

    /** Writes the batch. */
    protected abstract void commitWrites();

    /** Drops the writes. */
    protected abstract void dropWrites();
  }

  /** Trie nodes put through one updater of the format, until committed or aborted. */
  protected interface NodeBatch {
    void putAccountNode(Bytes location, Bytes32 hash, Bytes rlp);

    void putStorageNode(Bytes32 accountHash, Bytes location, Bytes32 hash, Bytes rlp);

    void commit();

    void abort();
  }

  /** Starts a batch of trie nodes. */
  protected abstract NodeBatch nodeBatch();

  @Override
  public TrieWriter trieWriter() {
    return new TrieWriter() {
      private NodeBatch nodes;
      private long pendingBytes;

      @Override
      public void accountNode(final Bytes location, final Bytes32 hash, final Bytes rlp) {
        nodes().putAccountNode(location, hash, rlp);
        commitWhenFull(rlp);
      }

      @Override
      public void storageNode(
          final Bytes32 accountHash, final Bytes location, final Bytes32 hash, final Bytes rlp) {
        nodes().putStorageNode(accountHash, location, hash, rlp);
        commitWhenFull(rlp);
      }

      @Override
      public void commit() {
        end(NodeBatch::commit);
      }

      @Override
      public void abort() {
        end(NodeBatch::abort);
      }

      private NodeBatch nodes() {
        if (nodes == null) {
          nodes = nodeBatch();
        }
        return nodes;
      }

      // the node and about 32 bytes for its key, whatever the format
      private void commitWhenFull(final Bytes rlp) {
        pendingBytes += Bytes32.SIZE + rlp.size();
        if (pendingBytes >= BYTES_PER_COMMIT) {
          commit();
        }
      }

      private void end(final Consumer<NodeBatch> ending) {
        if (nodes != null) {
          try {
            ending.accept(nodes);
          } finally {
            nodes = null;
            pendingBytes = 0;
          }
        }
      }
    };
  }

  /** Returns the key of a slot in the storage segment. */
  protected static byte[] slotKey(final Bytes32 accountHash, final Bytes32 slotHash) {
    return TrieBytes.concat(accountHash, slotHash).toArrayUnsafe();
  }

  // visits the slots of the accounts before the given one
  private static void visitSlotsBefore(
      final PeekingIterator<Pair<byte[], byte[]>> slots,
      final byte[] accountKey,
      final ScanVisitor visitor) {
    while (slots.hasNext() && compareAccount(slots.peek().getKey(), accountKey) < 0) {
      visitSlot(slots.next(), visitor);
    }
  }

  // visits the slots of the accounts up to the given one, included
  private static void visitSlotsUpTo(
      final PeekingIterator<Pair<byte[], byte[]>> slots,
      final byte[] accountKey,
      final ScanVisitor visitor) {
    while (slots.hasNext() && compareAccount(slots.peek().getKey(), accountKey) <= 0) {
      visitSlot(slots.next(), visitor);
    }
  }

  private static void visitSlot(final Pair<byte[], byte[]> slot, final ScanVisitor visitor) {
    final byte[] key = slot.getKey();
    // each key is a fresh array
    visitor.onSlot(Bytes32.wrap(key, 0), Bytes32.wrap(key, 32), Bytes.wrap(slot.getValue()));
  }

  // compares the account of a slot key with an account key
  private static int compareAccount(final byte[] slotKey, final byte[] accountKey) {
    return Arrays.compareUnsigned(slotKey, 0, 32, accountKey, 0, 32);
  }

  private static byte[] concat(final byte[] a, final byte[] b) {
    return com.google.common.primitives.Bytes.concat(a, b);
  }
}
