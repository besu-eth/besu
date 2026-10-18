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
import org.hyperledger.besu.plugin.services.storage.SegmentIdentifier;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/** Flat state kept in two segments: accounts keyed by hash and slots keyed by both hashes. */
abstract class AbstractSyncStateStorage implements SyncStateStorage {

  // bounds the write batch a writer keeps before committing
  protected static final int BYTES_PER_COMMIT = 8 * 1024 * 1024;
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
        visitSlots(slots.iterator, accountKey, false, visitor);
        if (visitor.onAccount(Bytes32.wrap(accountKey), Bytes.wrap(account.getValue()))) {
          visitSlots(slots.iterator, accountKey, true, visitor);
        } else {
          slots.skip(accountKey);
        }
      }
      visitSlots(slots.iterator, ONES, true, visitor);
    }
  }

  @Override
  public void forEachSlotValue(final Bytes32 accountHash, final BiConsumer<Bytes32, Bytes> slots) {
    final byte[] prefix = accountHash.toArrayUnsafe();
    try (Stream<Pair<byte[], byte[]>> stream =
        flat.streamFromKey(storageSegment, concat(prefix, ZEROS), concat(prefix, ONES))) {
      stream.forEach(
          slot ->
              slots.accept(
                  Bytes32.wrap(Arrays.copyOfRange(slot.getKey(), 32, 64)),
                  Bytes.wrap(slot.getValue())));
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
      if (!iterator.hasNext()
          || Arrays.compareUnsigned(iterator.peek().getKey(), 0, 32, accountKey, 0, 32) != 0) {
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
      Batch batch = batch();
      int pending = 0;
      for (final Iterator<Pair<byte[], byte[]>> it = slots.iterator(); it.hasNext(); ) {
        batch.removeStorage(accountHash, Bytes32.wrap(it.next().getKey(), 32));
        if (++pending == DELETES_PER_COMMIT) {
          batch.commit();
          batch = batch();
          pending = 0;
        }
      }
      batch.commit();
    }
  }

  // visits slots of accounts before the given one, or of the given account when inclusive
  private static void visitSlots(
      final PeekingIterator<Pair<byte[], byte[]>> slots,
      final byte[] accountKey,
      final boolean inclusive,
      final ScanVisitor visitor) {
    while (slots.hasNext()) {
      final byte[] key = slots.peek().getKey();
      final int cmp = Arrays.compareUnsigned(key, 0, 32, accountKey, 0, 32);
      if (cmp > 0 || (cmp == 0 && !inclusive)) {
        return;
      }
      final Pair<byte[], byte[]> slot = slots.next();
      visitor.onSlot(
          Bytes32.wrap(Arrays.copyOfRange(key, 0, 32)),
          Bytes32.wrap(Arrays.copyOfRange(key, 32, 64)),
          Bytes.wrap(slot.getValue()));
    }
  }

  /** Returns a slot value as the format stores it. */
  protected abstract Bytes storedSlot(Bytes32 value);

  /** The accounts and slots of a batch, written to the flat segments. */
  protected abstract class FlatBatch implements Batch {
    private final SegmentedKeyValueStorageTransaction tx;

    protected FlatBatch(final SegmentedKeyValueStorageTransaction tx) {
      this.tx = tx;
    }

    @Override
    public void putAccount(final Bytes32 accountHash, final Bytes account) {
      tx.put(accountSegment, accountHash.toArrayUnsafe(), account.toArrayUnsafe());
    }

    @Override
    public void removeAccount(final Bytes32 accountHash) {
      tx.remove(accountSegment, accountHash.toArrayUnsafe());
    }

    @Override
    public void putStorage(final Bytes32 accountHash, final Bytes32 slotHash, final Bytes32 value) {
      tx.put(storageSegment, slotKey(accountHash, slotHash), storedSlot(value).toArrayUnsafe());
    }

    @Override
    public void removeStorage(final Bytes32 accountHash, final Bytes32 slotHash) {
      tx.remove(storageSegment, slotKey(accountHash, slotHash));
    }
  }

  protected static byte[] slotKey(final Bytes32 accountHash, final Bytes32 slotHash) {
    return concat(accountHash.toArrayUnsafe(), slotHash.toArrayUnsafe());
  }

  private static byte[] concat(final byte[] a, final byte[] b) {
    final byte[] out = new byte[a.length + b.length];
    System.arraycopy(a, 0, out, 0, a.length);
    System.arraycopy(b, 0, out, a.length, b.length);
    return out;
  }
}
