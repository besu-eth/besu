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

import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_ARCHIVE_LATEST;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE_ARCHIVE;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage.NearestKeyValue;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stores historical trie node values in the archive store, keyed by a combination of the natural
 * key and the block number at which the value was valid. For a given natural key and target block,
 * returns the latest value at or before that block.
 *
 * <p>Each stored value is an {@link ArchiveTrieNodeCodec} entry. The diff counter of each key's
 * latest entry (the number of DIFFs since its last FULL) is kept separately in {@code
 * TRIE_BRANCH_STORAGE_ARCHIVE_LATEST}.
 */
public final class ArchiveNodeHistoryStore {

  private static final Logger LOG = LoggerFactory.getLogger(ArchiveNodeHistoryStore.class);

  // The latest diff counter is stored as a single unsigned byte, so its maximum value is 255
  public static final int MAX_COUNTER = 0xFF;

  /** The last block whose trie nodes are archived; history covers {@code [0, number]}. */
  public record ArchivedBlock(long number, Hash hash) {}

  // Last archived block number (8 bytes) and hash (32 bytes); coverage always starts at block 0.
  static final byte[] ARCHIVE_PROGRESS_KEY =
      "ARCHIVE_TRIE_PROGRESS".getBytes(StandardCharsets.UTF_8);

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
   * Returns the last block whose trie nodes are archived; history covers {@code [0, block]}.
   *
   * @return the last archived block, or {@code -1} if none
   */
  public long getArchivedBlockNumber() {
    return getArchivedBlock().map(ArchivedBlock::number).orElse(-1L);
  }

  /**
   * Returns the last archived block's number and hash, read together.
   *
   * @return the last archived block, or empty if none
   */
  public Optional<ArchivedBlock> getArchivedBlock() {
    return storage
        .get(TRIE_BRANCH_STORAGE_ARCHIVE, ARCHIVE_PROGRESS_KEY)
        .filter(raw -> raw.length == 40)
        .map(Bytes::wrap)
        .map(b -> new ArchivedBlock(b.getLong(0), Hash.wrap(Bytes32.wrap(b.slice(8, 32)))));
  }

  /**
   * Records {@code block} as the last archived block, atomically with the block's history entries.
   *
   * @param tx the transaction committing the block's history entries
   * @param block the archived block number
   * @param blockHash the archived block's hash
   */
  public void saveArchiveProgress(
      final SegmentedKeyValueStorageTransaction tx, final long block, final Hash blockHash) {
    tx.put(
        TRIE_BRANCH_STORAGE_ARCHIVE,
        ARCHIVE_PROGRESS_KEY,
        Bytes.concatenate(Bytes.ofUnsignedLong(block), blockHash.getBytes()).toArrayUnsafe());
  }

  /**
   * Writes a history entry.
   *
   * @param tx the transaction to write through
   * @param historyKey the key, which must come from {@link ArchiveNodeKey#historyKey}
   * @param codecEntry the {@link ArchiveTrieNodeCodec} entry bytes to store
   */
  public void putEncoded(
      final SegmentedKeyValueStorageTransaction tx,
      final Bytes historyKey,
      final Bytes codecEntry) {
    tx.put(TRIE_BRANCH_STORAGE_ARCHIVE, historyKey.toArrayUnsafe(), codecEntry.toArrayUnsafe());
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
        .flatMap(nearest -> decodeNearest(nearest, naturalKey));
  }

  private Optional<HistoryEntry> decodeNearest(
      final NearestKeyValue nearest, final Bytes naturalKey) {
    final long block = ArchiveNodeKey.blockFromHistoryKey(nearest.key());
    return nearest
        .wrapBytes()
        .flatMap(storedValue -> decodeStoredValue(storedValue, naturalKey, block));
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

  private Optional<HistoryEntry> decodeStoredValue(
      final Bytes storedValue, final Bytes naturalKey, final long block) {
    if (storedValue.isEmpty()) {
      LOG.error(
          "corrupt archive entry for key {} at block {}: empty value, skipping", naturalKey, block);
      return Optional.empty();
    }
    try {
      return Optional.of(
          new HistoryEntry(ArchiveTrieNodeCodec.decode(storedValue), storedValue, block));
    } catch (final IllegalArgumentException e) {
      LOG.error(
          "corrupt archive entry for key {} at block {}: {}, skipping",
          naturalKey,
          block,
          e.getMessage());
      return Optional.empty();
    }
  }

  /**
   * Returns the diff counter of each natural key's latest history entry. A key is absent when it
   * has no latest entry, or its latest entry is a deletion.
   *
   * @param naturalKeys the natural keys to look up
   * @return the counters of the keys that have one
   */
  public Map<Bytes, Integer> getLatestCounters(final List<Bytes> naturalKeys) {
    final List<Optional<byte[]>> values =
        storage.multiget(
            TRIE_BRANCH_ARCHIVE_LATEST, naturalKeys.stream().map(Bytes::toArrayUnsafe).toList());
    final Map<Bytes, Integer> counters = new HashMap<>();
    for (int i = 0; i < naturalKeys.size(); i++) {
      final Bytes naturalKey = naturalKeys.get(i);
      values.get(i).ifPresent(v -> counters.put(naturalKey, Byte.toUnsignedInt(v[0])));
    }
    return counters;
  }

  /**
   * Records {@code counter} as the diff counter of {@code naturalKey}'s latest history entry.
   *
   * @param tx the transaction to write through
   * @param naturalKey the node's natural key
   * @param counter the latest entry's counter (0 = FULL)
   */
  public void putLatestCounter(
      final SegmentedKeyValueStorageTransaction tx, final Bytes naturalKey, final int counter) {
    tx.put(TRIE_BRANCH_ARCHIVE_LATEST, naturalKey.toArrayUnsafe(), new byte[] {(byte) counter});
  }

  /**
   * Removes {@code naturalKey}'s latest counter, after its latest history entry became a deletion.
   *
   * @param tx the transaction to write through
   * @param naturalKey the node's natural key
   */
  public void removeLatestCounter(
      final SegmentedKeyValueStorageTransaction tx, final Bytes naturalKey) {
    tx.remove(TRIE_BRANCH_ARCHIVE_LATEST, naturalKey.toArrayUnsafe());
  }

  /**
   * Decoded, typed view of a stored history entry.
   *
   * @param codecEntry the decoded codec entry
   * @param rawEntryBytes the codec entry bytes, unmodified — fed back into {@link
   *     ArchiveTrieNodeCodec#reconstruct} without re-encoding
   * @param block the block number this entry was written at
   */
  public record HistoryEntry(ArchiveTrieNodeEntry codecEntry, Bytes rawEntryBytes, long block) {}
}
