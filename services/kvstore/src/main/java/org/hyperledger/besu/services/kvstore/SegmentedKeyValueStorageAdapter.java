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
package org.hyperledger.besu.services.kvstore;

import org.hyperledger.besu.plugin.services.exception.StorageException;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorageTransaction;
import org.hyperledger.besu.plugin.services.storage.SegmentIdentifier;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;

import java.io.IOException;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** This class will adapt a SegmentedKeyValueStorage to a KeyValueStorage instance. */
public class SegmentedKeyValueStorageAdapter implements KeyValueStorage {

  private static final Logger LOG = LoggerFactory.getLogger(SegmentedKeyValueStorageAdapter.class);
  private final SegmentIdentifier segmentIdentifier;

  /** The storage to wrap. */
  protected final SegmentedKeyValueStorage storage;

  /**
   * Instantiates a new Segmented key value storage adapter for a single segment.
   *
   * @param segmentIdentifier the segmentIdentifier to wrap as a KeyValueStorage
   * @param storage the storage
   */
  public SegmentedKeyValueStorageAdapter(
      final SegmentIdentifier segmentIdentifier, final SegmentedKeyValueStorage storage) {
    this.segmentIdentifier = segmentIdentifier;
    this.storage = storage;
  }

  SegmentedKeyValueStorage getSegmentedStore() {
    return this.storage;
  }

  @Override
  public void clear() {
    throwIfClosed();
    storage.clear(segmentIdentifier);
  }

  @Override
  public boolean containsKey(final byte[] key) throws StorageException {
    throwIfClosed();
    return storage.containsKey(segmentIdentifier, key);
  }

  @Override
  public Optional<byte[]> get(final byte[] key) throws StorageException {
    throwIfClosed();
    return storage.get(segmentIdentifier, key);
  }

  @Override
  public Set<byte[]> getAllKeysThat(final Predicate<byte[]> returnCondition) {
    throwIfClosed();
    return storage.getAllKeysThat(segmentIdentifier, returnCondition);
  }

  @Override
  public Set<byte[]> getAllValuesFromKeysThat(final Predicate<byte[]> returnCondition) {
    throwIfClosed();
    return storage.getAllValuesFromKeysThat(segmentIdentifier, returnCondition);
  }

  @Override
  public Stream<Pair<byte[], byte[]>> stream() {
    throwIfClosed();
    return storage.stream(segmentIdentifier);
  }

  @Override
  public Stream<Pair<byte[], byte[]>> streamFromKey(final byte[] startKeyHash)
      throws StorageException {
    return storage.streamFromKey(segmentIdentifier, startKeyHash);
  }

  @Override
  public Stream<Pair<byte[], byte[]>> streamFromKey(final byte[] startKey, final byte[] endKey) {
    return storage.streamFromKey(segmentIdentifier, startKey, endKey);
  }

  @Override
  public Stream<byte[]> streamKeys() {
    throwIfClosed();
    return storage.streamKeys(segmentIdentifier);
  }

  @Override
  public boolean tryDelete(final byte[] key) {
    throwIfClosed();
    return storage.tryDelete(segmentIdentifier, key);
  }

  @Override
  public void close() throws IOException {
    storage.close();
  }

  @Override
  public KeyValueStorageTransaction startTransaction() throws StorageException {
    return new KeyValueStorageTransactionAdapter(segmentIdentifier, storage);
  }

  @Override
  public boolean isClosed() {
    return storage.isClosed();
  }

  private void throwIfClosed() {
    if (storage.isClosed()) {
      LOG.error("Attempting to use a closed Storage instance.");
      throw new StorageException("Storage has been closed");
    }
  }

  /** This class will adapt a SegmentedKeyValueStorageTransaction to a KeyValueStorageTransaction */
  public static class KeyValueStorageTransactionAdapter implements KeyValueStorageTransaction {
    private final SegmentedKeyValueStorageTransaction segmentedTransaction;
    private final SegmentIdentifier segmentIdentifier;
    private final SegmentedKeyValueStorage storage;

    /**
     * Instantiates a new Key value storage transaction adapter.
     *
     * @param segmentIdentifier the segmentIdentifier to use for the wrapped transaction
     * @param storage the storage
     */
    public KeyValueStorageTransactionAdapter(
        final SegmentIdentifier segmentIdentifier, final SegmentedKeyValueStorage storage) {
      this(segmentIdentifier, storage, storage.startTransaction());
    }

    private KeyValueStorageTransactionAdapter(
        final SegmentIdentifier segmentIdentifier,
        final SegmentedKeyValueStorage storage,
        final SegmentedKeyValueStorageTransaction segmentedTransaction) {
      this.segmentedTransaction = segmentedTransaction;
      this.segmentIdentifier = segmentIdentifier;
      this.storage = storage;
    }

    /**
     * Returns a transaction on another storage that is committed or rolled back together with this
     * one, which is only possible when both storages adapt a segment of the same segmented storage.
     * Commit or roll back this transaction, not the returned one.
     *
     * @param otherStorage the storage to write to as part of this transaction
     * @return the joined transaction, empty if the storages do not share a segmented storage
     */
    public Optional<KeyValueStorageTransaction> joinedTransactionFor(
        final KeyValueStorage otherStorage) {
      if (otherStorage instanceof SegmentedKeyValueStorageAdapter adapter
          && adapter.storage == storage) {
        return Optional.of(
            new KeyValueStorageTransactionAdapter(
                adapter.segmentIdentifier, storage, segmentedTransaction));
      }
      return Optional.empty();
    }

    @Override
    public void put(final byte[] key, final byte[] value) {
      segmentedTransaction.put(segmentIdentifier, key, value);
    }

    @Override
    public void remove(final byte[] key) {
      segmentedTransaction.remove(segmentIdentifier, key);
    }

    @Override
    public void commit() throws StorageException {
      segmentedTransaction.commit();
    }

    @Override
    public void rollback() {
      segmentedTransaction.rollback();
    }

    @Override
    public void close() {
      segmentedTransaction.close();
    }
  }
}
