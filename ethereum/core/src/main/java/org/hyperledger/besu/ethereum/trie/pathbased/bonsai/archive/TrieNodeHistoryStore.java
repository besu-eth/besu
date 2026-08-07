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

import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE_ARCHIVE;

import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;

import java.util.Objects;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;

/**
 * Point storage for {@code TRIE_BRANCH_STORAGE_ARCHIVE}. Owns the wire format: {@code
 * [distanceSinceFull: 1 byte] ‖ [ArchiveTrieNodeCodec entry]}. The counter is a
 * TrieNodeHistoryStore-level concern (see the design spec's rationale) — the codec never sees it.
 */
public final class TrieNodeHistoryStore {

  private final SegmentedKeyValueStorage storage;

  public TrieNodeHistoryStore(final SegmentedKeyValueStorage storage) {
    this.storage = Objects.requireNonNull(storage, "storage must not be null");
  }

  /** Builds the stored wire value: {@code [counter: 1 byte] ‖ codecEntry}. */
  public static Bytes encodeStoredValue(final int counter, final Bytes codecEntry) {
    return Bytes.concatenate(Bytes.of((byte) counter), codecEntry);
  }

  /** Writes a pre-built history entry. Key must come from {@link ArchiveNodeKey#historyKey}. */
  public void putEncoded(
      final SegmentedKeyValueStorageTransaction tx,
      final Bytes historyKey,
      final Bytes storedValue) {
    tx.put(TRIE_BRANCH_STORAGE_ARCHIVE, historyKey.toArrayUnsafe(), storedValue.toArrayUnsafe());
  }

  public Optional<HistoryEntry> getLatestBefore(final Bytes naturalKey, final long block) {
    final Bytes seekKey = ArchiveNodeKey.historyKey(naturalKey, block);
    // NearestKeyValue(Bytes key, Optional<byte[]> value) —
    // plugin-api/.../SegmentedKeyValueStorage.java:202.
    // value() is itself Optional: a matched key with no value shouldn't occur for this CF (every
    // put() always writes a non-empty value), but is filtered defensively rather than assumed.
    return storage
        .getNearestBefore(TRIE_BRANCH_STORAGE_ARCHIVE, seekKey)
        .filter(nearest -> naturalKeyMatches(naturalKey, nearest.key()))
        .flatMap(
            nearest ->
                nearest
                    .value()
                    .map(
                        rawValue ->
                            decodeStoredValue(
                                Bytes.wrap(rawValue),
                                ArchiveNodeKey.blockFromHistoryKey(nearest.key()))));
  }

  /**
   * {@code getNearestBefore} searches the whole column family, so the single key it returns may
   * belong to a different natural key entirely; this filter rejects those.
   *
   * <p>Why the filter is normally sufficient: entries for one natural key sort contiguously (the
   * natural key is the prefix), so the greatest key at or before {@code naturalKey ‖ block} is one
   * of this key's own entries whenever any exists at or before {@code block}; otherwise it belongs
   * to a lexicographically smaller natural key and is correctly rejected here.
   *
   * <p><strong>Known limitation:</strong> natural keys are variable length, so one can be a byte
   * prefix of another (e.g. account location {@code 0x01} and a deeper location {@code 0x01 00…}).
   * When the longer key's entry sorts between our target and our own latest entry, this filter
   * rejects it and {@code getLatestBefore} returns empty even though a valid earlier entry for the
   * queried key exists — a missed reconstruction, not a wrong one (the fail-closed hash check in
   * the fail-closed hash check would still reject any bad node). It requires a location that is a
   * strict prefix of another location with colliding block-suffix bytes, so it is rare but not
   * impossible. Fixing it properly needs an unambiguous key encoding (e.g. a fixed-width
   * natural-key length field); that is a schema change and is deliberately out of scope here.
   * Record it in the PR description.
   */
  private static boolean naturalKeyMatches(final Bytes naturalKey, final Bytes foundKey) {
    return foundKey.size() >= naturalKey.size()
        && ArchiveNodeKey.naturalKeyFromHistoryKey(foundKey).equals(naturalKey);
  }

  private static HistoryEntry decodeStoredValue(final Bytes storedValue, final long block) {
    final int counter = Byte.toUnsignedInt(storedValue.get(0));
    final Bytes rawEntryBytes = storedValue.slice(1);
    return new HistoryEntry(
        counter, ArchiveTrieNodeCodec.decode(rawEntryBytes), rawEntryBytes, block);
  }

  /** Decoded, typed view of a stored history entry, used by both the write and read paths. */
  public record HistoryEntry(
      int counter, ArchiveTrieNodeEntry codecEntry, Bytes rawEntryBytes, long block) {}
}
