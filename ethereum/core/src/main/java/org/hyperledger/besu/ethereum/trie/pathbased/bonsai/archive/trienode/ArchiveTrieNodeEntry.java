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

/**
 * The decoded, typed view of one archived trie-node history entry, produced by {@link
 * ArchiveTrieNodeCodec#decode(Bytes)}. One of three shapes — FULL ({@link #fullNode()}), a binary
 * patch DIFF ({@link #patchBody()}), or a deletion tombstone — selected by the predicates below.
 * Callers must check the relevant predicate before calling a shape-specific accessor; calling the
 * wrong one throws {@link IllegalStateException}.
 */
public final class ArchiveTrieNodeEntry {

  public static final byte DIFF = 0x00;
  public static final byte FULL = 0x01;
  public static final byte DELETION = 0x02;

  private final byte metadata;
  private final Bytes body;

  ArchiveTrieNodeEntry(final byte metadata, final Bytes body) {
    this.metadata = metadata;
    this.body = body;
  }

  public boolean isFull() {
    return metadata == FULL;
  }

  /**
   * Returns true if the first byte of an encoded (not yet decoded) entry indicates a FULL entry.
   *
   * @param encodedEntry a codec-produced entry; must be non-null and non-empty
   * @throws IllegalArgumentException if {@code encodedEntry} is empty
   */
  public static boolean isFullEncoded(final Bytes encodedEntry) {
    if (encodedEntry.isEmpty()) {
      throw new IllegalArgumentException("encodedEntry must be at least 1 byte");
    }
    return encodedEntry.get(0) == FULL;
  }

  public boolean isDeletion() {
    return metadata == DELETION;
  }

  /**
   * Returns the full node bytes. Valid when {@link #isFull()} is true.
   *
   * @throws IllegalStateException if called on a diff or deletion entry
   */
  public Bytes fullNode() {
    if (!isFull()) {
      throw new IllegalStateException("fullNode() called on a non-full entry");
    }
    return body;
  }

  /**
   * Returns the raw binary patch body (COPY/SKIP/INSERT op sequence). Only valid when {@link
   * #isFull()} and {@link #isDeletion()} are both false.
   *
   * @throws IllegalStateException if called on a full or deletion entry
   */
  public Bytes patchBody() {
    if (isFull() || isDeletion()) {
      throw new IllegalStateException("patchBody() called on a non-diff entry");
    }
    return body;
  }
}
