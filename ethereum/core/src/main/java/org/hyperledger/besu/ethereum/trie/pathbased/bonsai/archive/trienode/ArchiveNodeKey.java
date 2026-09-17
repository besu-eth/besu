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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode;

import org.apache.tuweni.bytes.Bytes;

/** Utility class for constructing natural keys and history keys for archive trie nodes. */
public final class ArchiveNodeKey {

  public static final int BLOCK_SUFFIX_BYTES = 8;

  private ArchiveNodeKey() {}

  /**
   * Natural key for an account trie node: {@code [len:1B]‖location}.
   *
   * <p>The 1-byte length prefix disambiguate keys that would otherwise be byte-prefixes of each
   * other (e.g. location {@code [0x0e]} vs {@code [0x0e, 0x00]}), ensuring {@code getNearestBefore}
   * never confuses entries from one node with those of a deeper node.
   */
  public static Bytes account(final Bytes location) {
    if (location.size() > 255) {
      throw new IllegalArgumentException(
          "account location too long for 1-byte length prefix: " + location.size());
    }
    return Bytes.concatenate(Bytes.of((byte) location.size()), location);
  }

  /**
   * Natural key for a storage trie node: {@code accountHash(32)‖[len:1B]‖location}.
   *
   * <p>The 32-byte account hash prefix separates storage tries of different accounts.
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

  /** History key: {@code naturalKey‖block(8B big-endian)}. */
  public static Bytes historyKey(final Bytes naturalKey, final long block) {
    return Bytes.concatenate(naturalKey, Bytes.ofUnsignedLong(block));
  }

  /** Extracts the block number from the bytes of a history key. */
  public static long blockFromHistoryKey(final Bytes historyKey) {
    if (historyKey.size() < BLOCK_SUFFIX_BYTES) {
      throw new IllegalArgumentException(
          "historyKey too short: expected >= "
              + BLOCK_SUFFIX_BYTES
              + " bytes, got "
              + historyKey.size());
    }
    return historyKey.getLong(historyKey.size() - BLOCK_SUFFIX_BYTES);
  }

  /**
   * Extracts the natural key (everything but the last {@value #BLOCK_SUFFIX_BYTES} bytes) from a
   * history key.
   */
  public static Bytes naturalKeyFromHistoryKey(final Bytes historyKey) {
    if (historyKey.size() < BLOCK_SUFFIX_BYTES) {
      throw new IllegalArgumentException(
          "historyKey too short: expected >= "
              + BLOCK_SUFFIX_BYTES
              + " bytes, got "
              + historyKey.size());
    }
    return historyKey.slice(0, historyKey.size() - BLOCK_SUFFIX_BYTES);
  }
}
