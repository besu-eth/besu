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
import java.util.Optional;
import java.util.Set;
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
