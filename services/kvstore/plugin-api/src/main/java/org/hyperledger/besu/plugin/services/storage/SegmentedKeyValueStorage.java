/*
 * Copyright ConsenSys AG.
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
package org.hyperledger.besu.plugin.services.storage;

import org.hyperledger.besu.plugin.services.exception.StorageException;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.tuweni.bytes.Bytes;

/** Service provided by Besu to facilitate persistent data storage. */
public interface SegmentedKeyValueStorage extends Closeable {

  /**
   * Get the value from the associated segment and key.
   *
   * @param segment the segment
   * @param key Index into persistent data repository.
   * @return The value persisted at the key index.
   * @throws StorageException the storage exception
   */
  Optional<byte[]> get(SegmentIdentifier segment, byte[] key) throws StorageException;

  /**
   * Get the values from the associated segment and keys.
   *
   * <p>Default implementation loops {@link #get(SegmentIdentifier, byte[])}. Storage backends that
   * support batched reads must override this — {@code RocksDBColumnarKeyValueStorage} uses RocksDB
   * {@code multiGetAsList}, and layered / in-memory stores provide their own batch paths. Callers
   * always go through this method; they never need to special-case RocksDB.
   *
   * @param segment the segment
   * @param keys indexes into the persistent data repository
   * @return the values persisted at the key indexes, in the same order as {@code keys}
   * @throws StorageException the storage exception
   */
  default List<Optional<byte[]>> multiget(final SegmentIdentifier segment, final List<byte[]> keys)
      throws StorageException {
    final List<Optional<byte[]>> results = new ArrayList<>(keys.size());
    for (final byte[] key : keys) {
      results.add(get(segment, key));
    }
    return results;
  }

  /**
   * Finds the key and corresponding value that is "nearest before" the specified key. "Nearest
   * before" is defined as the closest key that is either exactly matching the supplied key or
   * lexicographically before it.
   *
   * @param segmentIdentifier The segment to scan for the nearest key.
   * @param key The key for which we are searching for the nearest match before it.
   * @return An Optional of NearestKeyValue, wrapping the matched key and its corresponding value,
   *     if found.
   * @throws StorageException If an error occurs during the retrieval process.
   */
  Optional<NearestKeyValue> getNearestBefore(final SegmentIdentifier segmentIdentifier, Bytes key)
      throws StorageException;

  /**
   * Finds the key and corresponding value that is "nearest after" the specified key. "Nearest
   * after" is defined as the closest key that is either exactly matching the supplied key or
   * lexicographically after it.
   *
   * <p>This method aims to find the next key in sequence after the provided key, considering the
   * order of keys within the specified segment. It is particularly useful for iterating over keys
   * in a sorted manner starting from a given key.
   *
   * @param segmentIdentifier The segment to scan for the nearest key.
   * @param key The key for which we are searching for the nearest match after it.
   * @return An Optional of NearestKeyValue, wrapping the matched key and its corresponding value,
   *     if found.
   * @throws StorageException If an error occurs during the retrieval process.
   */
  Optional<NearestKeyValue> getNearestAfter(final SegmentIdentifier segmentIdentifier, Bytes key)
      throws StorageException;

  /**
   * Contains key.
   *
   * @param segment the segment
   * @param key the key
   * @return the boolean
   * @throws StorageException the storage exception
   */
  default boolean containsKey(final SegmentIdentifier segment, final byte[] key)
      throws StorageException {
    return get(segment, key).isPresent();
  }

  /**
   * Begins a transaction. Returns a transaction object that can be updated and committed.
   *
   * @return An object representing the transaction.
   * @throws StorageException the storage exception
   */
  SegmentedKeyValueStorageTransaction startTransaction() throws StorageException;

  /**
   * Begins a transaction with low write priority. On RocksDB-backed storage this sets {@code
   * WriteOptions.low_pri = true}, which causes RocksDB to throttle this transaction's writes more
   * aggressively than normal writes when compaction back-pressure builds.
   *
   * <p>Non-RocksDB implementations fall back to {@link #startTransaction()}.
   *
   * @return An object representing the transaction.
   * @throws StorageException the storage exception
   */
  default SegmentedKeyValueStorageTransaction startLowPriorityTransaction()
      throws StorageException {
    return startTransaction();
  }

  /**
   * Begins a transaction committed as one write batch, for writers that never conflict: it skips
   * the conflict checks of an optimistic transaction. Storage that locks keys, such as a
   * pessimistic RocksDB transaction database, still locks them at commit. Storage without such
   * checks falls back to {@link #startTransaction()}.
   *
   * @return An object representing the transaction.
   * @throws StorageException the storage exception
   */
  default SegmentedKeyValueStorageTransaction startBatchTransaction() throws StorageException {
    return startTransaction();
  }

  /**
   * Tells the storage that a large amount of data is about to be loaded into the segments, mostly
   * through {@link #startSortedWriter}. Until the returned load is closed the storage may hold back
   * the background work that reorganises the segments: during the load that work rewrites entries
   * over and over that are about to be put where they belong anyway.
   *
   * <p>Reads and regular writes keep working during the load, but may be slower than usual. A
   * segment is in one bulk load at a time, until that load has ended for it. The default
   * implementation does nothing.
   *
   * @param segmentIdentifiers the segments that are loaded
   * @return the bulk load, which has to be closed when the data is in
   * @throws IllegalStateException if one of the segments is already in a bulk load
   */
  default SegmentBulkLoad startBulkLoad(final List<SegmentIdentifier> segmentIdentifiers) {
    return () -> {};
  }

  /**
   * Starts a bulk load as {@link #startBulkLoad(List)} does, which also takes in large values
   * through regular writes in no particular key order, each written once. Until the load is closed
   * the storage may keep those values apart from their keys, so that reorganising their segments
   * moves the keys only. The values written meanwhile stay apart. The default implementation does
   * not.
   *
   * @param segmentIdentifiers the segments that are loaded
   * @param separateValueSegments the segments of large values written in no particular key order,
   *     none of them also in {@code segmentIdentifiers}
   * @return the bulk load, which has to be closed when the data is in
   */
  default SegmentBulkLoad startBulkLoad(
      final List<SegmentIdentifier> segmentIdentifiers,
      final List<SegmentIdentifier> separateValueSegments) {
    return startBulkLoad(segmentIdentifiers);
  }

  /**
   * Starts adding entries to a segment in increasing key order, written to a file then added to the
   * segment at once, without going through the memtable and its compactions. Storage that cannot
   * add files falls back to a batch transaction.
   *
   * @param segment the segment the entries are added to
   * @return the writer, not shared between threads
   * @throws StorageException problem encountered when starting the writer
   */
  default SortedWriter startSortedWriter(final SegmentIdentifier segment) throws StorageException {
    final SegmentedKeyValueStorageTransaction transaction = startBatchTransaction();
    return new SortedWriter() {
      private byte[] last;

      @Override
      public void put(final byte[] key, final byte[] value) {
        if (last != null && Arrays.compareUnsigned(key, last) <= 0) {
          throw new StorageException("keys must be added in increasing order");
        }
        last = key;
        transaction.put(segment, key, value);
      }

      @Override
      public void commit() {
        transaction.commit();
      }

      @Override
      public void rollback() {
        transaction.rollback();
      }
    };
  }

  /**
   * Returns a stream of all keys for the segment.
   *
   * @param segmentIdentifier The segment identifier whose keys we want to stream.
   * @return A stream of all keys in the specified segment.
   */
  Stream<Pair<byte[], byte[]>> stream(final SegmentIdentifier segmentIdentifier);

  /**
   * Returns a stream of key-value pairs starting from the specified key. This method is used to
   * retrieve a stream of data from the storage, starting from the given key. If no data is
   * available from the specified key onwards, an empty stream is returned.
   *
   * @param segmentIdentifier The segment identifier whose keys we want to stream.
   * @param startKey The key from which the stream should start.
   * @return A stream of key-value pairs starting from the specified key.
   */
  Stream<Pair<byte[], byte[]>> streamFromKey(
      final SegmentIdentifier segmentIdentifier, final byte[] startKey);

  /**
   * Returns a stream of key-value pairs starting from the specified key, ending at the specified
   * key. This method is used to retrieve a stream of data from the storage, starting from the given
   * key. If no data is available from the specified key onwards, an empty stream is returned.
   *
   * @param segmentIdentifier The segment identifier whose keys we want to stream.
   * @param startKey The key from which the stream should start.
   * @param endKey The key at which the stream should stop.
   * @return A stream of key-value pairs starting from the specified key.
   */
  Stream<Pair<byte[], byte[]>> streamFromKey(
      final SegmentIdentifier segmentIdentifier, final byte[] startKey, final byte[] endKey);

  /**
   * Stream keys.
   *
   * @param segmentIdentifier the segment identifier
   * @return the stream
   */
  Stream<byte[]> streamKeys(final SegmentIdentifier segmentIdentifier);

  /**
   * Delete the value corresponding to the given key in the given segment if a write lock can be
   * instantly acquired on the underlying storage. Do nothing otherwise.
   *
   * @param segmentIdentifier The segment identifier whose keys we want to stream.
   * @param key The key to delete.
   * @return false if the lock on the underlying storage could not be instantly acquired, true
   *     otherwise
   * @throws StorageException any problem encountered during the deletion attempt.
   */
  boolean tryDelete(SegmentIdentifier segmentIdentifier, byte[] key) throws StorageException;

  /**
   * Gets all keys that matches condition.
   *
   * @param segmentIdentifier the segment identifier
   * @param returnCondition the return condition
   * @return set of result
   */
  Set<byte[]> getAllKeysThat(
      SegmentIdentifier segmentIdentifier, Predicate<byte[]> returnCondition);

  /**
   * Gets all values from keys that matches condition.
   *
   * @param segmentIdentifier the segment identifier
   * @param returnCondition the return condition
   * @return the set of result
   */
  Set<byte[]> getAllValuesFromKeysThat(
      final SegmentIdentifier segmentIdentifier, Predicate<byte[]> returnCondition);

  /**
   * Clear.
   *
   * @param segmentIdentifier the segment identifier
   */
  void clear(SegmentIdentifier segmentIdentifier);

  /**
   * Replaces the whole content of a segment. Every entry is handed to the function and replaced by
   * the value it returns, or dropped when that is null, and the given entries are added on top.
   *
   * <p>The function may be called from several threads at once and in any order of the keys, so it
   * has to be thread safe and must not depend on the entries it was called with before.
   *
   * <p>The rewrite needs the segment to itself. Nothing else may read or write the segment until
   * the call returns: a reader could find it empty or partly filled.
   *
   * <p>The segment ends up with either its previous content or all of the new one, also when the
   * process stops half way, in which case the implementation finishes the rewrite when the storage
   * is opened again. After a call that throws, the segment must not be used before the call was
   * repeated or the storage reopened. Both finish a rewrite that had begun to replace the segment
   * instead of starting another, so the function is never applied to an entry twice.
   *
   * @param segmentIdentifier the segment identifier
   * @param transform maps the key and value of an entry to its new value, or to null to drop it
   * @param additions entries put after the existing ones have been rewritten
   */
  default void rewrite(
      final SegmentIdentifier segmentIdentifier,
      final BiFunction<byte[], byte[], byte[]> transform,
      final List<Pair<byte[], byte[]>> additions) {
    final SegmentedKeyValueStorageTransaction transaction = startTransaction();
    try (final Stream<Pair<byte[], byte[]>> entries = stream(segmentIdentifier)) {
      entries.forEach(
          entry -> {
            final byte[] value = transform.apply(entry.getKey(), entry.getValue());
            if (value == null) {
              transaction.remove(segmentIdentifier, entry.getKey());
            } else {
              transaction.put(segmentIdentifier, entry.getKey(), value);
            }
          });
      additions.forEach(
          entry -> transaction.put(segmentIdentifier, entry.getKey(), entry.getValue()));
    } catch (final RuntimeException e) {
      // a transaction that is never committed keeps what the implementation holds for it
      transaction.rollback();
      throw e;
    }
    transaction.commit();
  }

  /**
   * Whether the underlying storage is closed.
   *
   * @return boolean indicating whether the underlying storage is closed.
   */
  boolean isClosed();

  /**
   * record type used to wrap responses from getNearestTo, includes the matched key and the value.
   *
   * @param key the matched (nearest) key
   * @param value the corresponding value
   */
  record NearestKeyValue(Bytes key, Optional<byte[]> value) {

    /**
     * Convenience method to map the Optional value to Bytes.
     *
     * @return Optional of Bytes.
     */
    public Optional<Bytes> wrapBytes() {
      return value.map(Bytes::wrap);
    }
  }
}
