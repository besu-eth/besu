/*
 * Copyright contributors to Hyperledger Besu.
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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat;

import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ARCHIVE_ACCOUNT_INDEX;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ARCHIVE_STORAGE_INDEX;

import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;

import java.nio.charset.StandardCharsets;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.bouncycastle.util.Arrays;

/**
 * Writes block numbers into the archive index segments. The index maps (naturalKey + rangeId) →
 * sorted list of block numbers at which the account/slot changed.
 *
 * <p>Block ranges (of size {@link #RANGE_SIZE}) keep individual index entries small and bounded,
 * even for hot accounts that change every block.
 */
public class ArchiveIndexWriter {

  /** Number of blocks covered by a single index range entry. */
  public static final long RANGE_SIZE = 100_000L;

  /** Metadata key used to store the last block whose index entries are contiguously complete. */
  public static final byte[] LAST_INDEXED_BLOCK_KEY =
      "lastIndexedBlock".getBytes(StandardCharsets.UTF_8);

  private ArchiveIndexWriter() {}

  /**
   * Appends {@code blockNumber} to the account index entry for the given {@code accountHash} within
   * the appropriate range. Must be called inside an already-open transaction.
   *
   * @param storage the underlying storage (used for read-before-write)
   * @param tx the open transaction to write into
   * @param accountHash raw 32-byte account address hash
   * @param blockNumber the block at which this account changed
   */
  public static void appendAccountToIndex(
      final SegmentedKeyValueStorage storage,
      final SegmentedKeyValueStorageTransaction tx,
      final byte[] accountHash,
      final long blockNumber) {
    final long rangeId = blockNumber / RANGE_SIZE;
    final byte[] indexKey = buildAccountIndexKey(accountHash, rangeId);
    final byte[] existing = storage.get(ARCHIVE_ACCOUNT_INDEX, indexKey).orElse(new byte[0]);
    tx.put(ARCHIVE_ACCOUNT_INDEX, indexKey, appendBlockNumber(existing, blockNumber));
  }

  /**
   * Appends {@code blockNumber} to the storage index entry for the given {@code naturalKey}
   * (accountHash ++ slotHash) within the appropriate range.
   *
   * @param storage the underlying storage (used for read-before-write)
   * @param tx the open transaction to write into
   * @param naturalKey raw 64-byte key (accountHash ++ slotHash)
   * @param blockNumber the block at which this storage slot changed
   */
  public static void appendStorageToIndex(
      final SegmentedKeyValueStorage storage,
      final SegmentedKeyValueStorageTransaction tx,
      final byte[] naturalKey,
      final long blockNumber) {
    final long rangeId = blockNumber / RANGE_SIZE;
    final byte[] indexKey = buildStorageIndexKey(naturalKey, rangeId);
    final byte[] existing = storage.get(ARCHIVE_STORAGE_INDEX, indexKey).orElse(new byte[0]);
    tx.put(ARCHIVE_STORAGE_INDEX, indexKey, appendBlockNumber(existing, blockNumber));
  }

  /**
   * Attempts to advance the {@code lastIndexedBlock} pointer by one. Only advances if {@code
   * blockNumber} equals the current {@code lastIndexedBlock + 1}, ensuring contiguous coverage from
   * block 0. This guarantees that a block listed in the index implies all prior blocks are also
   * indexed.
   *
   * @param storage the underlying storage
   * @param blockNumber the block just fully indexed
   */
  public static void tryAdvanceLastIndexedBlock(
      final SegmentedKeyValueStorage storage, final long blockNumber) {
    final long current = getLastIndexedBlock(storage).orElse(-1L);
    if (blockNumber == current + 1) {
      final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
      tx.put(
          ARCHIVE_ACCOUNT_INDEX,
          LAST_INDEXED_BLOCK_KEY,
          Bytes.ofUnsignedLong(blockNumber).toArrayUnsafe());
      tx.commit();
    }
  }

  /**
   * Returns the latest block for which the index is known to be contiguously complete.
   *
   * @param storage the underlying storage
   * @return the last indexed block number, or empty if none
   */
  public static Optional<Long> getLastIndexedBlock(final SegmentedKeyValueStorage storage) {
    return storage
        .get(ARCHIVE_ACCOUNT_INDEX, LAST_INDEXED_BLOCK_KEY)
        .map(Bytes::wrap)
        .map(Bytes::toLong);
  }

  /**
   * Builds the index key for an account: {@code accountHash (32 bytes) ++ rangeId (8 bytes)}.
   *
   * @param accountHash raw 32-byte account address hash
   * @param rangeId the range identifier
   * @return the composite index key
   */
  public static byte[] buildAccountIndexKey(final byte[] accountHash, final long rangeId) {
    return Bytes.concatenate(Bytes.of(accountHash), Bytes.ofUnsignedLong(rangeId)).toArrayUnsafe();
  }

  /**
   * Builds the index key for a storage slot: {@code naturalKey (64 bytes) ++ rangeId (8 bytes)}.
   *
   * @param naturalKey raw 64-byte key (accountHash ++ slotHash)
   * @param rangeId the range identifier
   * @return the composite index key
   */
  public static byte[] buildStorageIndexKey(final byte[] naturalKey, final long rangeId) {
    return Bytes.concatenate(Bytes.of(naturalKey), Bytes.ofUnsignedLong(rangeId)).toArrayUnsafe();
  }

  private static byte[] appendBlockNumber(final byte[] existing, final long blockNumber) {
    return Arrays.concatenate(existing, Bytes.ofUnsignedLong(blockNumber).toArrayUnsafe());
  }
}
