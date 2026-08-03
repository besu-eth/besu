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
package org.hyperledger.besu.ethereum.mainnet.slowblock;

import org.hyperledger.besu.plugin.services.exception.StorageException;
import org.hyperledger.besu.plugin.services.storage.SegmentIdentifier;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.plugin.services.storage.SnappableKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SnappedKeyValueStorage;

import java.io.IOException;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.tuweni.bytes.Bytes;

/**
 * Counts point reads that cross the storage boundary, for slow-block cache-miss metrics.
 *
 * <p>Installed once, around the process-wide storage instance, because there is no per-block seam
 * to hook: which block a read belongs to is decided by {@link SlowBlockDiskReadCounters} from the
 * calling thread. Only installed when slow-block tracing is enabled, so a node with the feature off
 * runs entirely undecorated.
 *
 * <p>Implements {@link SnappableKeyValueStorage} unconditionally: bonsai snapshots cast the
 * composed storage to it, so a decorator that dropped the interface would break snapshotting.
 * Snapshots are wrapped in turn, keeping the read counting in place for snapshot-backed world
 * states.
 */
public class ReadMeteredSegmentedKeyValueStorage implements SnappableKeyValueStorage {

  private final SegmentedKeyValueStorage delegate;

  /**
   * Wraps a storage instance.
   *
   * @param delegate the storage to meter
   */
  public ReadMeteredSegmentedKeyValueStorage(final SegmentedKeyValueStorage delegate) {
    this.delegate = delegate;
  }

  @Override
  public Optional<byte[]> get(final SegmentIdentifier segment, final byte[] key)
      throws StorageException {
    SlowBlockDiskReadCounters.recordRead(segment);
    return delegate.get(segment, key);
  }

  @Override
  public boolean containsKey(final SegmentIdentifier segment, final byte[] key)
      throws StorageException {
    SlowBlockDiskReadCounters.recordRead(segment);
    return delegate.containsKey(segment, key);
  }

  @Override
  public Optional<NearestKeyValue> getNearestBefore(
      final SegmentIdentifier segmentIdentifier, final Bytes key) throws StorageException {
    SlowBlockDiskReadCounters.recordRead(segmentIdentifier);
    return delegate.getNearestBefore(segmentIdentifier, key);
  }

  @Override
  public Optional<NearestKeyValue> getNearestAfter(
      final SegmentIdentifier segmentIdentifier, final Bytes key) throws StorageException {
    SlowBlockDiskReadCounters.recordRead(segmentIdentifier);
    return delegate.getNearestAfter(segmentIdentifier, key);
  }

  @Override
  public SnappedKeyValueStorage takeSnapshot() {
    if (delegate instanceof SnappableKeyValueStorage snappable) {
      return new MeteredSnapshot(snappable.takeSnapshot());
    }
    throw new UnsupportedOperationException(
        "Underlying storage does not support snapshots: " + delegate.getClass().getName());
  }

  @Override
  public SegmentedKeyValueStorageTransaction startTransaction() throws StorageException {
    return delegate.startTransaction();
  }

  @Override
  public SegmentedKeyValueStorageTransaction startLowPriorityTransaction() throws StorageException {
    // RocksDB overrides this for write throttling, so it must not fall back to startTransaction.
    return delegate.startLowPriorityTransaction();
  }

  @Override
  public Stream<Pair<byte[], byte[]>> stream(final SegmentIdentifier segmentIdentifier) {
    return delegate.stream(segmentIdentifier);
  }

  @Override
  public Stream<Pair<byte[], byte[]>> streamFromKey(
      final SegmentIdentifier segmentIdentifier, final byte[] startKey) {
    return delegate.streamFromKey(segmentIdentifier, startKey);
  }

  @Override
  public Stream<Pair<byte[], byte[]>> streamFromKey(
      final SegmentIdentifier segmentIdentifier, final byte[] startKey, final byte[] endKey) {
    return delegate.streamFromKey(segmentIdentifier, startKey, endKey);
  }

  @Override
  public Stream<byte[]> streamKeys(final SegmentIdentifier segmentIdentifier) {
    return delegate.streamKeys(segmentIdentifier);
  }

  @Override
  public boolean tryDelete(final SegmentIdentifier segmentIdentifier, final byte[] key)
      throws StorageException {
    return delegate.tryDelete(segmentIdentifier, key);
  }

  @Override
  public Set<byte[]> getAllKeysThat(
      final SegmentIdentifier segmentIdentifier, final Predicate<byte[]> returnCondition) {
    return delegate.getAllKeysThat(segmentIdentifier, returnCondition);
  }

  @Override
  public Set<byte[]> getAllValuesFromKeysThat(
      final SegmentIdentifier segmentIdentifier, final Predicate<byte[]> returnCondition) {
    return delegate.getAllValuesFromKeysThat(segmentIdentifier, returnCondition);
  }

  @Override
  public void clear(final SegmentIdentifier segmentIdentifier) {
    delegate.clear(segmentIdentifier);
  }

  @Override
  public boolean isClosed() {
    return delegate.isClosed();
  }

  @Override
  public void close() throws IOException {
    delegate.close();
  }

  /** A metered snapshot, so snapshot-backed world states keep counting their reads. */
  private static class MeteredSnapshot extends ReadMeteredSegmentedKeyValueStorage
      implements SnappedKeyValueStorage {

    private final SnappedKeyValueStorage snapshot;

    MeteredSnapshot(final SnappedKeyValueStorage snapshot) {
      super(snapshot);
      this.snapshot = snapshot;
    }

    @Override
    public SegmentedKeyValueStorageTransaction getSnapshotTransaction() {
      return snapshot.getSnapshotTransaction();
    }
  }
}
