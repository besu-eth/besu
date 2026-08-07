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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive;

import org.apache.tuweni.bytes.Bytes;

/**
 * Utility methods for constructing and deconstructing keys for the archive trie node CFs.
 *
 * <h2>Key encoding</h2>
 *
 * <p>Natural keys include a 1-byte length prefix before the location segment. This prevents a
 * prefix-collision bug in {@link TrieNodeHistoryStore#getLatestBefore}: without the prefix, a
 * shallower location like {@code [0x0e]} and a deeper one like {@code [0x0e, 0x00]} share the same
 * byte prefix, causing the deeper path's genesis entry to sort lexicographically <em>between</em>
 * the shallower path's genesis entry and a later block's seek key — making {@code getNearestBefore}
 * return the wrong entry. With the 1-byte length prefix the first byte differs ({@code 0x01} vs
 * {@code 0x02}), so the entries for different locations never interleave.
 *
 * <p><strong>Schema note:</strong> this length prefix is a breaking change relative to any data
 * written without it. Existing history data must be wiped when this version is first deployed.
 */
public final class ArchiveNodeKey {

  private ArchiveNodeKey() {}

  /**
   * Returns the natural key for an account-trie node: {@code [len: 1 byte] ‖ location}.
   *
   * <p>The 1-byte length prefix guarantees that no two account natural keys are byte-prefixes of
   * each other (see class-level javadoc), which is required for correct {@code seekForPrev}
   * behaviour in {@link TrieNodeHistoryStore}.
   */
  public static Bytes account(final Bytes location) {
    if (location.size() > 255) {
      throw new IllegalArgumentException(
          "account location too long for 1-byte length prefix: " + location.size());
    }
    return Bytes.concatenate(Bytes.of((byte) location.size()), location);
  }

  /**
   * Returns the natural key for a storage-trie node: {@code accountHash(32) ‖ [len: 1 byte] ‖
   * location}.
   *
   * <p>The account hash prefix (fixed 32 bytes) keeps different accounts separate. The 1-byte
   * location-length prefix prevents the same prefix-collision issue within a single account's
   * storage trie (see {@link #account}).
   */
  public static Bytes storage(final Bytes accountHash, final Bytes location) {
    if (accountHash.size() != 32) {
      throw new IllegalArgumentException(
          "accountHash must be exactly 32 bytes, got " + accountHash.size());
    }
    if (location.size() > 255) {
      throw new IllegalArgumentException(
          "storage location too long for 1-byte length prefix: " + location.size());
    }
    return Bytes.concatenate(accountHash, Bytes.of((byte) location.size()), location);
  }

  /** Constructs a history-CF key: {@code naturalKey ‖ block(8 bytes BE)}. */
  public static Bytes historyKey(final Bytes naturalKey, final long block) {
    return Bytes.concatenate(naturalKey, Bytes.ofUnsignedLong(block));
  }

  /** Extracts the block number from a history key (the trailing 8 big-endian bytes). */
  public static long blockFromHistoryKey(final Bytes historyKey) {
    if (historyKey.size() < 8) {
      throw new IllegalArgumentException(
          "historyKey too short: expected >= 8 bytes, got " + historyKey.size());
    }
    return historyKey.getLong(historyKey.size() - 8);
  }

  /** Extracts the natural key prefix from a history key (all bytes except the trailing 8). */
  public static Bytes naturalKeyFromHistoryKey(final Bytes historyKey) {
    if (historyKey.size() < 8) {
      throw new IllegalArgumentException(
          "historyKey too short: expected >= 8 bytes, got " + historyKey.size());
    }
    return historyKey.slice(0, historyKey.size() - 8);
  }
}
