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

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Optional;
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
        final Batch batch = batch();
        try {
          for (int deletes = 0; deletes < DELETES_PER_COMMIT && it.hasNext(); deletes++) {
            batch.removeStorage(accountHash, Bytes32.wrap(it.next().getKey(), 32));
          }
        } catch (final RuntimeException e) {
          batch.abort();
          throw e;
        }
        // outside the try: a transaction whose commit has started refuses its rollback
        batch.commit();
      }
    }
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
