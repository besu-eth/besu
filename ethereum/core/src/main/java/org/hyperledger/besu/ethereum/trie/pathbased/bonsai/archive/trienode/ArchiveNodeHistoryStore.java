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

import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE_ARCHIVE;

import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage.NearestKeyValue;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;

import java.util.Objects;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stores historical trie node values in the archive store, keyed by a combination of the natural
 * key and the block number at which the value was valid. For a given natural key and target block,
 * returns the latest value at or before that block.
 *
 * <p>Wire format per stored value: {@code [counter: 1 unsigned byte] ‖ [ArchiveTrieNodeCodec
 * entry]}.
 */
public final class ArchiveNodeHistoryStore {

  private static final Logger LOG = LoggerFactory.getLogger(ArchiveNodeHistoryStore.class);

  // The diff-chain counter is stored as a single unsigned byte, so its maximum value is 255
  public static final int MAX_COUNTER = 0xFF;

  private final SegmentedKeyValueStorage storage;

  /**
   * Creates a store backed by the given segmented key-value storage.
   *
   * @param storage the storage to read and write archive trie-node history through
   */
  public ArchiveNodeHistoryStore(final SegmentedKeyValueStorage storage) {
    this.storage = Objects.requireNonNull(storage, "storage must not be null");
  }

  /**
   * Builds the stored wire value: {@code [counter: 1 byte] ‖ codecEntry}.
   *
   * @param counter distance since the last FULL entry for this natural key (0 = this entry is FULL)
   * @param codecEntry the {@link ArchiveTrieNodeCodec} entry bytes to store
   * @return the wire-format bytes to pass to {@link #putEncoded}
   * @throws IllegalArgumentException if {@code counter} does not fit in an unsigned byte (0-255)
   */
  public static Bytes encodeStoredValue(final int counter, final Bytes codecEntry) {
    if (counter < 0 || counter > MAX_COUNTER) {
      throw new IllegalArgumentException("counter must fit in 1 unsigned byte: " + counter);
    }
    return Bytes.concatenate(Bytes.of((byte) counter), codecEntry);
  }

  /**
   * Writes a pre-built history entry.
   *
   * @param tx the transaction to write through
   * @param historyKey the key, which must come from {@link ArchiveNodeKey#historyKey}
   * @param storedValue the value, which must come from {@link #encodeStoredValue}
   */
  public void putEncoded(
      final SegmentedKeyValueStorageTransaction tx,
      final Bytes historyKey,
      final Bytes storedValue) {
    tx.put(TRIE_BRANCH_STORAGE_ARCHIVE, historyKey.toArrayUnsafe(), storedValue.toArrayUnsafe());
  }

  /**
   * Returns the latest history entry at or before {@code block} for the given natural key, if one
   * exists.
   *
   * @param naturalKey the node's natural key, from {@link ArchiveNodeKey#account} or {@link
   *     ArchiveNodeKey#storage}
   * @param block the target block; the returned entry's block is at or before this
   * @return the matching entry, or empty if none exists at or before {@code block}
   */
  public Optional<HistoryEntry> getLatestBefore(final Bytes naturalKey, final long block) {
    final Bytes seekKey = ArchiveNodeKey.historyKey(naturalKey, block);
    return storage
        .getNearestBefore(TRIE_BRANCH_STORAGE_ARCHIVE, seekKey)
        .filter(nearest -> naturalKeyMatches(naturalKey, nearest.key()))
        .flatMap(this::decodeNearest);
  }

  private Optional<HistoryEntry> decodeNearest(final NearestKeyValue nearest) {
    final long block = ArchiveNodeKey.blockFromHistoryKey(nearest.key());
    return nearest.wrapBytes().flatMap(storedValue -> decodeStoredValue(storedValue, block));
  }

  /**
   * Returns true if the foundKey is a history key for the same natural key as the given naturalKey.
   * This is used to filter out history keys that are for different natural keys when searching for
   * the latest value before a given block.
   */
  private boolean naturalKeyMatches(final Bytes naturalKey, final Bytes foundKey) {
    return foundKey.size() >= naturalKey.size() + ArchiveNodeKey.BLOCK_SUFFIX_BYTES
        && ArchiveNodeKey.naturalKeyFromHistoryKey(foundKey).equals(naturalKey);
  }

  private Optional<HistoryEntry> decodeStoredValue(final Bytes storedValue, final long block) {
    if (storedValue.size() < 2) {
      LOG.warn(
          "corrupt archive entry at block {}: stored value too short ({} bytes), skipping",
          block,
          storedValue.size());
      return Optional.empty();
    }
    final int counter = Byte.toUnsignedInt(storedValue.get(0));
    final Bytes rawEntryBytes = storedValue.slice(1);
    try {
      return Optional.of(
          new HistoryEntry(
              counter, ArchiveTrieNodeCodec.decode(rawEntryBytes), rawEntryBytes, block));
    } catch (final IllegalArgumentException e) {
      LOG.warn("corrupt archive entry at block {}: {}, skipping", block, e.getMessage());
      return Optional.empty();
    }
  }

  /**
   * Decoded, typed view of a stored history entry.
   *
   * @param counter distance since the last FULL entry for this natural key (0 = this entry is FULL)
   * @param codecEntry the decoded codec entry
   * @param rawEntryBytes the codec entry bytes, unmodified — fed back into {@link
   *     ArchiveTrieNodeCodec#reconstruct} without re-encoding
   * @param block the block number this entry was written at
   */
  public record HistoryEntry(
      int counter, ArchiveTrieNodeEntry codecEntry, Bytes rawEntryBytes, long block) {}
}
