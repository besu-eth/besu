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

import static org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.ArchiveTrieNodeEntry.BRANCH_CHILDREN;
import static org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.ArchiveTrieNodeEntry.CREATION;
import static org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.ArchiveTrieNodeEntry.DELETION;
import static org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.ArchiveTrieNodeEntry.ENTRY_FULL;
import static org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.ArchiveTrieNodeEntry.KEY_CHANGED;
import static org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.ArchiveTrieNodeEntry.NODE_IS_BRANCH;
import static org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.ArchiveTrieNodeEntry.SINGLE_CHILD_CHANGED;
import static org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.ArchiveTrieNodeEntry.VALUE_CHANGED;

import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.rlp.RLPInput;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;

/**
 * Encodes/decodes a trie node's history entry as FULL (complete node RLP), DIFF (structural delta
 * vs. the prior version), or a deletion tombstone. Pure codec: no I/O, no storage dependency.
 *
 * <p>The decoded, typed view of an entry is {@link ArchiveTrieNodeEntry}, which also owns the
 * metadata-byte bit-layout constants (wire format), since it represents "what does an entry look
 * like" — this class owns "how do you produce/reconstruct one."
 *
 * <h2>Metadata byte (first byte of every entry)</h2>
 *
 * <pre>
 * bit0  ENTRY_FULL    (0x01)  1 = full node RLP follows; 0 = diff entry
 * bit1  NODE_IS_BRANCH(0x02)  1 = branch (17-item list); 0 = extension/leaf (2-item short node)
 * bit2  KEY_CHANGED   (0x04)  (short node) path segment changed
 * bit3  VALUE_CHANGED (0x08)  (short/branch) embedded value changed
 * bit4  CREATION      (0x10)  node created at this block (no prior version)
 * bit5  DELETION      (0x20)  node deleted at this block (tombstone; no body follows)
 * bit6  SINGLE_CHILD_CHANGED (0x40) (branch diff) exactly one child slot changed; its index
 *                            follows as 1 byte instead of the 2-byte childMask
 * bit7  reserved
 * </pre>
 *
 * <h2>Field framing</h2>
 *
 * <p>Branch child refs and short-node values are captured as raw RLP ({@code readAsRlp().raw()}) —
 * already self-delimiting, since an RLP item's own header encodes its length — so neither carries
 * an external length prefix; decode re-parses them with the RLP reader to recover both the content
 * and how many bytes it occupied. Branch terminal values and short-node paths are captured as
 * decoded byte payloads ({@code readBytes()}), which are not self-delimiting, so those still carry
 * an explicit length prefix (1 byte for branch values, 2 bytes for short-node paths, matching their
 * pre-existing size limits).
 */
public final class ArchiveTrieNodeCodec {

  /** RLP list arity of a short node (extension or leaf): {@code [encodedPath, value]}. */
  private static final int SHORT_NODE_ARITY = 2;

  /**
   * RLP list arity of a branch node: {@link ArchiveTrieNodeEntry#BRANCH_CHILDREN} child slots (one
   * per hex nibble, 0-F) plus one terminal value slot.
   */
  private static final int BRANCH_NODE_ARITY = BRANCH_CHILDREN + 1;

  /**
   * Largest length a 2-byte big-endian length prefix can represent ({@code 2^16 - 1}) — the size
   * ceiling for the short-node path field, the only field still framed this way (see the class
   * javadoc's "Field framing" section).
   */
  private static final int SHORT_NODE_PATH_MAX_LENGTH = 0xFFFF;

  private ArchiveTrieNodeCodec() {}

  /** Layout: {@code [ENTRY_FULL]} ‖ {@code nodeRlp}. */
  public static Bytes encodeFull(final Bytes nodeRlp) {
    Objects.requireNonNull(nodeRlp, "nodeRlp must not be null");
    return Bytes.concatenate(Bytes.of(ENTRY_FULL), nodeRlp);
  }

  public static Bytes encodeDiff(final Bytes oldNodeRlp, final Bytes newNodeRlp) {
    if (oldNodeRlp == null && newNodeRlp == null) {
      throw new IllegalArgumentException("encodeDiff: both old and new node RLPs are null");
    }
    if (oldNodeRlp == null) {
      return Bytes.concatenate(Bytes.of((byte) (ENTRY_FULL | CREATION)), newNodeRlp);
    }
    if (newNodeRlp == null) {
      return Bytes.of(DELETION);
    }
    final int oldArity = nodeArity(oldNodeRlp);
    final int newArity = nodeArity(newNodeRlp);
    if (oldArity != newArity) {
      return encodeFull(newNodeRlp);
    }
    if (oldArity == BRANCH_NODE_ARITY) {
      return encodeBranchDiff(oldNodeRlp, newNodeRlp);
    }
    return encodeShortDiff(oldNodeRlp, newNodeRlp);
  }

  static int nodeArity(final Bytes nodeRlp) {
    final RLPInput in = RLP.input(nodeRlp);
    final int count = in.enterList();
    if (count != SHORT_NODE_ARITY && count != BRANCH_NODE_ARITY) {
      throw new IllegalArgumentException(
          "Expected a "
              + SHORT_NODE_ARITY
              + "-item short node or "
              + BRANCH_NODE_ARITY
              + "-item branch node RLP list, got "
              + count
              + " items");
    }
    return count;
  }

  public static ArchiveTrieNodeEntry decode(final Bytes entry) {
    Objects.requireNonNull(entry, "entry must not be null");
    if (entry.isEmpty()) {
      throw new IllegalArgumentException("Entry must be at least 1 byte (metadata byte)");
    }
    return new ArchiveTrieNodeEntry(entry.get(0), entry.slice(1));
  }

  private static Bytes encodeBranchDiff(final Bytes oldNodeRlp, final Bytes newNodeRlp) {
    final BranchFields oldFields = parseBranchFields(oldNodeRlp);
    final BranchFields newFields = parseBranchFields(newNodeRlp);

    int childMask = 0;
    int changedCount = 0;
    int soleChangedIndex = -1;
    for (int i = 0; i < BRANCH_CHILDREN; i++) {
      if (!oldFields.children()[i].equals(newFields.children()[i])) {
        childMask |= (1 << i);
        changedCount++;
        soleChangedIndex = i;
      }
    }
    final boolean valueChanged = !oldFields.value().equals(newFields.value());

    byte metadata = NODE_IS_BRANCH;
    if (valueChanged) {
      metadata |= VALUE_CHANGED;
    }

    final List<Bytes> parts = new ArrayList<>();
    if (changedCount == 1) {
      // Common case: a single key update touches exactly one child slot in each branch node
      // along its path — encode the index directly instead of spending 2 bytes on a bitmask.
      parts.add(Bytes.of((byte) (metadata | SINGLE_CHILD_CHANGED)));
      parts.add(Bytes.of((byte) soleChangedIndex));
      parts.add(newFields.children()[soleChangedIndex]); // raw RLP, self-delimiting
    } else {
      parts.add(Bytes.of(metadata));
      parts.add(Bytes.of((byte) ((childMask >> 8) & 0xFF), (byte) (childMask & 0xFF)));
      for (int i = 0; i < BRANCH_CHILDREN; i++) {
        if ((childMask & (1 << i)) != 0) {
          parts.add(newFields.children()[i]); // raw RLP, self-delimiting
        }
      }
    }
    if (valueChanged) {
      parts.add(frameBranchValue(newFields.value()));
    }
    return Bytes.concatenate(parts.toArray(new Bytes[0]));
  }

  /**
   * Frames the branch terminal value (decoded payload, not self-delimiting) with a 1-byte length
   * prefix.
   */
  private static Bytes frameBranchValue(final Bytes value) {
    final int len = value.size();
    if (len > 255) {
      throw new IllegalArgumentException("Branch value too large for 1-byte length prefix: " + len);
    }
    return Bytes.concatenate(Bytes.of((byte) len), value);
  }

  private static BranchFields parseBranchFields(final Bytes nodeRlp) {
    final RLPInput in = RLP.input(nodeRlp);
    final int count = in.enterList();
    if (count != BRANCH_NODE_ARITY) {
      throw new IllegalArgumentException(
          "Expected " + BRANCH_NODE_ARITY + "-item branch node RLP list, got " + count);
    }
    final Bytes[] children = new Bytes[BRANCH_CHILDREN];
    for (int i = 0; i < BRANCH_CHILDREN; i++) {
      children[i] = in.readAsRlp().raw();
    }
    final Bytes value = in.nextIsNull() ? readNullAsEmpty(in) : in.readBytes();
    in.leaveList();
    return new BranchFields(children, value);
  }

  private static Bytes readNullAsEmpty(final RLPInput in) {
    in.skipNext();
    return Bytes.EMPTY;
  }

  private record BranchFields(Bytes[] children, Bytes value) {}

  private static Bytes encodeShortDiff(final Bytes oldNodeRlp, final Bytes newNodeRlp) {
    final ShortFields oldFields = parseShortFields(oldNodeRlp);
    final ShortFields newFields = parseShortFields(newNodeRlp);
    final boolean keyChanged = !oldFields.path.equals(newFields.path);
    final boolean valueChanged = !oldFields.valueRlp.equals(newFields.valueRlp);

    byte metadata = 0;
    if (keyChanged) metadata |= KEY_CHANGED;
    if (valueChanged) metadata |= VALUE_CHANGED;

    final List<Bytes> parts = new ArrayList<>();
    parts.add(Bytes.of(metadata));
    if (keyChanged) parts.add(frameShortField(newFields.path));
    if (valueChanged) parts.add(newFields.valueRlp); // raw RLP, self-delimiting, no length prefix
    return Bytes.concatenate(parts.toArray(new Bytes[0]));
  }

  /**
   * Frames the short-node path (decoded payload, not self-delimiting) with a 2-byte length prefix.
   */
  private static Bytes frameShortField(final Bytes field) {
    final int len = field.size();
    if (len > SHORT_NODE_PATH_MAX_LENGTH) {
      throw new IllegalArgumentException(
          "Short-node path too large for 2-byte length prefix: " + len);
    }
    return Bytes.concatenate(Bytes.of((byte) ((len >> 8) & 0xFF), (byte) (len & 0xFF)), field);
  }

  private static ShortFields parseShortFields(final Bytes nodeRlp) {
    final RLPInput in = RLP.input(nodeRlp);
    final int count = in.enterList();
    if (count != 2) {
      throw new IllegalArgumentException("Expected 2-item short node RLP list, got " + count);
    }
    final Bytes path = in.readBytes();
    final Bytes valueRlp = in.readAsRlp().raw();
    in.leaveList();
    return new ShortFields(path, valueRlp);
  }

  private record ShortFields(Bytes path, Bytes valueRlp) {}

  public static Bytes reconstruct(final Bytes fullEntry, final List<Bytes> diffEntries) {
    Objects.requireNonNull(fullEntry, "fullEntry must not be null");
    Objects.requireNonNull(diffEntries, "diffEntries must not be null");
    final ArchiveTrieNodeEntry base = decode(fullEntry);
    if (!base.isFull()) {
      throw new IllegalArgumentException("reconstruct: fullEntry must be a FULL entry");
    }
    final Bytes baseNode = base.fullNode();
    return nodeArity(baseNode) == BRANCH_NODE_ARITY
        ? reconstructBranch(baseNode, diffEntries)
        : reconstructShort(baseNode, diffEntries);
  }

  private static Bytes reconstructBranch(final Bytes baseNode, final List<Bytes> diffEntries) {
    final BranchFields base = parseBranchFields(baseNode);
    final Bytes[] children = Arrays.copyOf(base.children(), BRANCH_CHILDREN);
    Bytes value = base.value();
    for (final Bytes diffEntry : diffEntries) {
      final ArchiveTrieNodeEntry entry = decode(diffEntry);
      requireDiffEntry(entry);
      if (!entry.isBranchNode()) {
        throw new IllegalArgumentException(
            "reconstruct type mismatch: base is branch, diff is not");
      }
      for (final Map.Entry<Integer, Bytes> e : entry.changedChildRefs().entrySet()) {
        children[e.getKey()] = e.getValue();
      }
      final Optional<Bytes> newVal = entry.changedValue();
      if (newVal.isPresent()) {
        value = newVal.get();
      }
    }

    final BytesValueRLPOutput rlpOutput = new BytesValueRLPOutput();
    rlpOutput.startList();
    for (int i = 0; i < BRANCH_CHILDREN; i++) {
      rlpOutput.writeRaw(children[i]);
    }
    rlpOutput.writeBytes(value);
    rlpOutput.endList();
    return rlpOutput.encoded();
  }

  private static Bytes reconstructShort(final Bytes baseNode, final List<Bytes> diffEntries) {
    final ShortFields base = parseShortFields(baseNode);
    Bytes path = base.path;
    Bytes valueRlp = base.valueRlp;
    for (final Bytes diffEntry : diffEntries) {
      final ArchiveTrieNodeEntry entry = decode(diffEntry);
      requireDiffEntry(entry);
      if (entry.isBranchNode()) {
        throw new IllegalArgumentException(
            "reconstruct type mismatch: base is short, diff is branch");
      }
      final Optional<Bytes> newPath = entry.changedKey();
      if (newPath.isPresent()) {
        path = newPath.get();
      }
      final Optional<Bytes> newVal = entry.changedShortNodeValue();
      if (newVal.isPresent()) {
        valueRlp = newVal.get();
      }
    }

    final BytesValueRLPOutput rlpOutput = new BytesValueRLPOutput();
    rlpOutput.startList();
    rlpOutput.writeBytes(path);
    rlpOutput.writeRaw(valueRlp);
    rlpOutput.endList();
    return rlpOutput.encoded();
  }

  private static void requireDiffEntry(final ArchiveTrieNodeEntry entry) {
    if (entry.isFull() || entry.isDeletion()) {
      throw new IllegalArgumentException("reconstruct expects DIFF entries only");
    }
  }
}
