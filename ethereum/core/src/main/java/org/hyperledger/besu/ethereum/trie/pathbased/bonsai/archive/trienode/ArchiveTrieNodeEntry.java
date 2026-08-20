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

  public static final byte ENTRY_DIFF = 0b0000_0000;
  public static final byte ENTRY_FULL = 0b0000_0001;
  public static final byte CREATION = 0b0001_0000;
  public static final byte DELETION = 0b0010_0000;

  private final byte metadata;
  private final Bytes body;

  ArchiveTrieNodeEntry(final byte metadata, final Bytes body) {
    this.metadata = metadata;
    this.body = body;
  }

  public boolean isFull() {
    return (metadata & ENTRY_FULL) != 0;
  }

  public boolean isCreation() {
    return (metadata & CREATION) != 0;
  }

  public boolean isDeletion() {
    return (metadata & DELETION) != 0;
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
