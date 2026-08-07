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

import org.hyperledger.besu.ethereum.rlp.RLP;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;

/**
 * The decoded, typed view of one archived trie-node history entry, produced by {@link
 * ArchiveTrieNodeCodec#decode(Bytes)}. An instance is one of three shapes, distinguished by the
 * predicates below: FULL (the complete node RLP, via {@link #fullNode()}), a branch or short-node
 * DIFF (a structural delta against the prior version), or a deletion tombstone (no body at all).
 * Only the codec constructs instances and only it knows which accessors are valid for a given
 * metadata byte — callers must check the relevant predicate before calling a shape-specific
 * accessor; calling the wrong one throws {@link IllegalStateException}.
 */
public final class ArchiveTrieNodeEntry {

  /** bit0: full node RLP follows; if clear this is a diff entry. */
  public static final byte ENTRY_FULL = 0x01;

  /** bit1: the node is a branch (17-item RLP list); clear = extension or leaf (2-item). */
  public static final byte NODE_IS_BRANCH = 0x02;

  /** bit2: (short node diff) the path segment (key) changed relative to the previous version. */
  public static final byte KEY_CHANGED = 0x04;

  /** bit3: (short/branch node diff) the embedded value changed relative to the previous version. */
  public static final byte VALUE_CHANGED = 0x08;

  /** bit4: this node was created at this block — there is no prior version to diff against. */
  public static final byte CREATION = 0x10;

  /** bit5: this node was deleted at this block (tombstone entry). */
  public static final byte DELETION = 0x20;

  /**
   * bit6: (branch node diff) exactly one child slot changed; its index follows as a single byte
   * instead of the 2-byte {@code childMask} — the common case, since a single key update touches
   * exactly one child slot in each branch node along its path to the root.
   */
  public static final byte SINGLE_CHILD_CHANGED = 0x40;

  /** Number of child slots in a branch node. */
  static final int BRANCH_CHILDREN = 16;

  private final byte metadata;
  private final Bytes body;

  ArchiveTrieNodeEntry(final byte metadata, final Bytes body) {
    this.metadata = metadata;
    this.body = body;
  }

  public boolean isFull() {
    return (metadata & ENTRY_FULL) != 0;
  }

  public boolean isDeletion() {
    return (metadata & DELETION) != 0;
  }

  public boolean isBranchNode() {
    return (metadata & NODE_IS_BRANCH) != 0;
  }

  public Bytes fullNode() {
    if (!isFull()) {
      throw new IllegalStateException("fullNode() called on a diff entry");
    }
    return body;
  }

  public Map<Integer, Bytes> changedChildRefs() {
    requireBranchDiff("changedChildRefs()");
    if (isSingleChildChanged()) {
      final int index = Byte.toUnsignedInt(body.get(0));
      return Map.of(index, readRlpItem(1));
    }
    final int mask = readChildMask();
    int offset = 2;
    final Map<Integer, Bytes> result = new LinkedHashMap<>();
    for (int i = 0; i < BRANCH_CHILDREN; i++) {
      if ((mask & (1 << i)) != 0) {
        final Bytes childRlp = readRlpItem(offset);
        result.put(i, childRlp);
        offset += childRlp.size();
      }
    }
    return Collections.unmodifiableMap(result);
  }

  public Optional<Bytes> changedValue() {
    requireBranchDiff("changedValue()");
    if ((metadata & VALUE_CHANGED) == 0) {
      return Optional.empty();
    }
    final int offset = offsetAfterChildRefs();
    final int len = Byte.toUnsignedInt(body.get(offset));
    return Optional.of(body.slice(offset + 1, len));
  }

  private void requireBranchDiff(final String methodName) {
    if (isFull() || !isBranchNode()) {
      throw new IllegalStateException(methodName + " called on a non-branch DIFF entry");
    }
  }

  private boolean isSingleChildChanged() {
    return (metadata & SINGLE_CHILD_CHANGED) != 0;
  }

  private int readChildMask() {
    final int hi = Byte.toUnsignedInt(body.get(0));
    final int lo = Byte.toUnsignedInt(body.get(1));
    return (hi << 8) | lo;
  }

  /**
   * Child refs are stored as raw, self-delimiting RLP (their own header encodes their length), so
   * no external length prefix is needed — {@link RLP#input} parses exactly one item starting at
   * {@code offset} and ignores anything after it, which is how the caller learns each item's
   * consumed byte count ({@code .size()}) to advance to the next one.
   */
  private Bytes readRlpItem(final int offset) {
    return RLP.input(body.slice(offset)).readAsRlp().raw();
  }

  private int offsetAfterChildRefs() {
    if (isSingleChildChanged()) {
      return 1 + readRlpItem(1).size();
    }
    final int mask = readChildMask();
    int offset = 2;
    for (int i = 0; i < BRANCH_CHILDREN; i++) {
      if ((mask & (1 << i)) != 0) {
        offset += readRlpItem(offset).size();
      }
    }
    return offset;
  }

  public Optional<Bytes> changedKey() {
    requireShortDiff("changedKey()");
    if ((metadata & KEY_CHANGED) == 0) {
      return Optional.empty();
    }
    return Optional.of(readShortField(0));
  }

  public Optional<Bytes> changedShortNodeValue() {
    requireShortDiff("changedShortNodeValue()");
    if ((metadata & VALUE_CHANGED) == 0) {
      return Optional.empty();
    }
    // Value is stored as raw, self-delimiting RLP (no external length prefix, unlike the key
    // field) — it is always the last field present, so no further offset bookkeeping is needed.
    final int keyFieldSize = ((metadata & KEY_CHANGED) != 0) ? (2 + readShortFieldLength(0)) : 0;
    return Optional.of(readRlpItem(keyFieldSize));
  }

  private void requireShortDiff(final String methodName) {
    if (isFull() || isBranchNode() || isDeletion()) {
      throw new IllegalStateException(methodName + " called on a non-short-node DIFF entry");
    }
  }

  private Bytes readShortField(final int offset) {
    final int len = readShortFieldLength(offset);
    return body.slice(offset + 2, len);
  }

  private int readShortFieldLength(final int offset) {
    final int hi = Byte.toUnsignedInt(body.get(offset));
    final int lo = Byte.toUnsignedInt(body.get(offset + 1));
    return (hi << 8) | lo;
  }
}
