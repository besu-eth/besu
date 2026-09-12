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

import static org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode.ArchiveTrieNodeEntry.DELETION;
import static org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode.ArchiveTrieNodeEntry.DIFF;
import static org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode.ArchiveTrieNodeEntry.FULL;

import java.util.List;
import java.util.Objects;

import org.apache.tuweni.bytes.Bytes;

/**
 * Codec for {@link ArchiveTrieNodeEntry} instances. Provides methods to encode/decode entries and
 * reconstruct a node's bytes from a FULL entry and a list of DIFF entries.
 *
 * <p>A DIFF entry's body is a binary patch produced by {@link BinaryDiffCodec}, which documents the
 * op wire format. This class owns the entry framing (metadata byte, creation/deletion lifecycle)
 * and the storage policy around the patch.
 *
 * <p>If the patch body would be at least as large as the new node, {@link #encodeDiff} falls back
 * to a FULL entry (via {@link #encodeFull}), bounding the worst case. These mid-chain FULL entries
 * act as checkpoints: readers that use {@code isFull()} will stop reconstruction there and return
 * the full node directly rather than applying further diffs.
 */
public final class ArchiveTrieNodeCodec {

  private ArchiveTrieNodeCodec() {}

  /** Layout: {@code [FULL]} ‖ {@code nodeBytes}. */
  public static Bytes encodeFull(final Bytes nodeBytes) {
    Objects.requireNonNull(nodeBytes, "nodeBytes must not be null");
    return Bytes.concatenate(Bytes.of(FULL), nodeBytes);
  }

  /**
   * Encodes the diff from {@code oldNode} to {@code newNode} as a binary patch entry. Returns a
   * {@code FULL} entry when {@code oldNode} is null (creation), a {@code DELETION} tombstone when
   * {@code newNode} is null (deletion), or a {@code FULL} entry when the patch body would be at
   * least as large as the new node.
   */
  public static Bytes encodeDiff(final Bytes oldNode, final Bytes newNode) {
    if (oldNode == null && newNode == null) {
      throw new IllegalArgumentException("encodeDiff: both old and new nodes are null");
    } else if (oldNode == null) {
      return encodeFull(newNode);
    } else if (newNode == null) {
      return Bytes.of(DELETION);
    }

    final Bytes patch = BinaryDiffCodec.encode(oldNode, newNode);
    if (patch.size() >= newNode.size()) {
      return encodeFull(newNode);
    }
    return Bytes.concatenate(Bytes.of(DIFF), patch);
  }

  /**
   * Decodes a raw codec entry (as produced by {@link #encodeFull} or {@link #encodeDiff}) into a
   * typed {@link ArchiveTrieNodeEntry}.
   *
   * @param entry the encoded bytes; must be at least 1 byte (the metadata byte)
   * @return the decoded entry
   * @throws IllegalArgumentException if {@code entry} is null, empty, or carries an unknown
   *     metadata byte
   */
  public static ArchiveTrieNodeEntry decode(final Bytes entry) {
    Objects.requireNonNull(entry, "entry must not be null");
    if (entry.isEmpty()) {
      throw new IllegalArgumentException("Entry must be at least 1 byte (metadata byte)");
    }
    final byte metadata = entry.get(0);
    if (!isKnownMetadata(metadata)) {
      throw new IllegalArgumentException(
          String.format("Unknown archive trie-node entry metadata byte: 0x%02X", metadata));
    }
    return new ArchiveTrieNodeEntry(metadata, entry.slice(1));
  }

  private static boolean isKnownMetadata(final byte metadata) {
    return metadata == FULL || metadata == DIFF || metadata == DELETION;
  }

  /**
   * Reconstructs a node by applying each diff entry's patch to the base FULL node in order.
   *
   * @param fullEntry a FULL codec entry (from {@link #encodeFull}), not a DIFF or deletion
   * @param diffEntries zero or more DIFF entries (not standalone FULL, not deletion) in ascending
   *     block order
   * @return the reconstructed node bytes after all diffs are applied
   * @throws IllegalArgumentException if {@code fullEntry} is not FULL, or any diff entry is a
   *     standalone FULL or a deletion tombstone
   */
  public static Bytes reconstruct(final Bytes fullEntry, final List<Bytes> diffEntries) {
    Objects.requireNonNull(fullEntry, "fullEntry must not be null");
    Objects.requireNonNull(diffEntries, "diffEntries must not be null");
    final ArchiveTrieNodeEntry base = decode(fullEntry);
    if (!base.isFull()) {
      throw new IllegalArgumentException("reconstruct: fullEntry must be a FULL entry");
    }

    Bytes node = base.fullNode();
    for (final Bytes diffEntry : diffEntries) {
      final ArchiveTrieNodeEntry entry = decode(diffEntry);
      if (entry.isDeletion()) {
        throw new IllegalArgumentException(
            "reconstruct: diff list must not contain deletion entries");
      }
      if (entry.isFull()) {
        throw new IllegalArgumentException(
            "reconstruct: diff list must not contain standalone FULL entries");
      }
      node = BinaryDiffCodec.apply(node, entry.patchBody());
    }
    return node;
  }
}
