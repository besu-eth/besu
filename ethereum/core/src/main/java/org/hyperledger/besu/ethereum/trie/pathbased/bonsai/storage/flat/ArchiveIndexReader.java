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

import org.hyperledger.besu.plugin.services.storage.SegmentIdentifier;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;

import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;

/**
 * Reads from the archive index to resolve historical queries without a seekForPrev scan.
 *
 * <p>The index stores, per (naturalKey, rangeId), a sorted list of block numbers at which the
 * account or storage slot changed. A binary search over this list finds the exact block ≤ the
 * target, enabling an O(1) direct key lookup instead of an O(N) seek.
 *
 * <p>If the index is not yet complete for the queried block range, callers should fall back to the
 * existing {@code getNearestBefore} / seekForPrev path.
 */
public class ArchiveIndexReader {

  private ArchiveIndexReader() {}

  /**
   * Returns {@code true} if the index has been built contiguously up to and including {@code
   * targetBlock}. A {@code true} result means:
   *
   * <ul>
   *   <li>An index hit → the returned block is the exact block to look up.
   *   <li>An index miss → the account/slot did not change in the range, so the value is not present
   *       at this block (definitive empty).
   * </ul>
   *
   * @param storage the underlying storage
   * @param targetBlock the block we are querying
   * @return whether the index covers this block
   */
  public static boolean isIndexComplete(
      final SegmentedKeyValueStorage storage, final long targetBlock) {
    return ArchiveIndexWriter.getLastIndexedBlock(storage)
        .map(last -> last >= targetBlock)
        .orElse(false);
  }

  /**
   * Binary-searches the account index for the largest block ≤ {@code targetBlock} at which {@code
   * accountHash} changed.
   *
   * @param storage the underlying storage
   * @param accountHash raw 32-byte account address hash
   * @param targetBlock the block we are querying
   * @return the exact block to use for a direct lookup, or empty if the account had no change in
   *     this range
   */
  public static Optional<Long> findExactAccountBlock(
      final SegmentedKeyValueStorage storage, final byte[] accountHash, final long targetBlock) {
    final long rangeId = targetBlock / ArchiveIndexWriter.RANGE_SIZE;
    final byte[] indexKey = ArchiveIndexWriter.buildAccountIndexKey(accountHash, rangeId);
    return findExactBlock(storage, ARCHIVE_ACCOUNT_INDEX, indexKey, targetBlock);
  }

  /**
   * Binary-searches the storage index for the largest block ≤ {@code targetBlock} at which {@code
   * naturalKey} (accountHash ++ slotHash) changed.
   *
   * @param storage the underlying storage
   * @param naturalKey raw 64-byte key (accountHash ++ slotHash)
   * @param targetBlock the block we are querying
   * @return the exact block to use for a direct lookup, or empty if the slot had no change in this
   *     range
   */
  public static Optional<Long> findExactStorageBlock(
      final SegmentedKeyValueStorage storage, final byte[] naturalKey, final long targetBlock) {
    final long rangeId = targetBlock / ArchiveIndexWriter.RANGE_SIZE;
    final byte[] indexKey = ArchiveIndexWriter.buildStorageIndexKey(naturalKey, rangeId);
    return findExactBlock(storage, ARCHIVE_STORAGE_INDEX, indexKey, targetBlock);
  }

  /**
   * Performs a binary search over the sorted block-number list stored at {@code indexKey} in {@code
   * indexSegment}, returning the largest value ≤ {@code targetBlock}.
   *
   * <p>Block numbers are stored as a contiguous array of 8-byte big-endian longs in ascending
   * order.
   *
   * @param storage the underlying storage
   * @param indexSegment the segment to look in ({@code ARCHIVE_ACCOUNT_INDEX} or {@code
   *     ARCHIVE_STORAGE_INDEX})
   * @param indexKey the pre-built composite index key
   * @param targetBlock the block we are querying
   * @return the largest stored block ≤ targetBlock, or empty
   */
  private static Optional<Long> findExactBlock(
      final SegmentedKeyValueStorage storage,
      final SegmentIdentifier indexSegment,
      final byte[] indexKey,
      final long targetBlock) {
    final Optional<byte[]> blockListOpt = storage.get(indexSegment, indexKey);
    if (blockListOpt.isEmpty()) {
      return Optional.empty();
    }
    final byte[] blockList = blockListOpt.get();
    final int count = blockList.length / Long.BYTES;
    if (count == 0) {
      return Optional.empty();
    }

    // Binary search: find the rightmost index where blockList[i] <= targetBlock
    int lo = 0;
    int hi = count - 1;
    int result = -1;
    while (lo <= hi) {
      final int mid = (lo + hi) >>> 1;
      final long midBlock = Bytes.wrap(blockList, mid * Long.BYTES, Long.BYTES).toLong();
      if (midBlock <= targetBlock) {
        result = mid;
        lo = mid + 1;
      } else {
        hi = mid - 1;
      }
    }

    if (result < 0) {
      return Optional.empty();
    }
    return Optional.of(Bytes.wrap(blockList, result * Long.BYTES, Long.BYTES).toLong());
  }
}
