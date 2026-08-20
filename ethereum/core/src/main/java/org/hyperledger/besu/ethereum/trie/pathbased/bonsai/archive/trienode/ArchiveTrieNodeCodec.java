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

import static org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode.ArchiveTrieNodeEntry.CREATION;
import static org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode.ArchiveTrieNodeEntry.DELETION;
import static org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode.ArchiveTrieNodeEntry.ENTRY_DIFF;
import static org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode.ArchiveTrieNodeEntry.ENTRY_FULL;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.apache.tuweni.bytes.Bytes;

/**
 * Codec for {@link ArchiveTrieNodeEntry} instances. Provides methods to encode/decode entries and
 * reconstruct a node's bytes from a FULL entry and a list of DIFF entries.
 *
 * <p>DIFF entries are encoded as a sequence of binary COPY/SKIP/INSERT/REPLACE ops applied to the
 * previous node's bytes:
 *
 * <ul>
 *   <li>COPY(n) — emit n bytes from the old node at current old_pos, advance old_pos
 *   <li>SKIP(n) — advance old_pos by n, no output
 *   <li>INSERT(n) — followed by n bytes of new data, emit them (old_pos unchanged)
 *   <li>REPLACE(n) — followed by n bytes of new data, emit them AND advance old_pos by n
 * </ul>
 *
 * <p>REPLACE collapses the INSERT+SKIP pair used for same-length changed regions. After all ops,
 * any remaining bytes in the old node are implicitly appended (zero-cost trailing suffix). Each op
 * is 2 bytes: byte1 bits[7:6] = type (0=COPY, 1=SKIP, 2=INSERT, 3=REPLACE), byte1 bits[5:0] and
 * byte2 = 14-bit length (max 16383). This format is trie-structure agnostic: it works for MPT, PBT
 * (EIP-8297), or any future encoding without modification.
 *
 * <p>If the patch body would be at least as large as the new node, {@link #encodeDiff} falls back
 * to a FULL entry (via {@link #encodeFull}), bounding the worst case. These mid-chain FULL entries
 * act as checkpoints: readers that use {@code isFull()} will stop reconstruction there and return
 * the full node directly rather than applying further diffs.
 */
public final class ArchiveTrieNodeCodec {

  private static final int OP_COPY = 0;
  private static final int OP_SKIP = 1;
  private static final int OP_INSERT = 2;
  private static final int OP_REPLACE = 3;
  private static final int OP_HEADER_SIZE = 2;
  private static final int OP_TYPE_SHIFT = 6;
  private static final int OP_LENGTH_HIGH_MASK = 0x3F;
  private static final int OP_MAX_LENGTH = 0x3FFF; // 14 bits (2-byte op format)

  /**
   * Minimum matching run required to accept a re-synchronisation anchor in {@link
   * #encodePatchAligned}. Trie realignment points (child slots, hash refs) are ≥ 32 bytes, so 4
   * rejects spurious 1-in-4-billion coincidental matches while still anchoring every real boundary.
   */
  private static final int RESYNC_MATCH_MIN = 4;

  /**
   * Cap on the (skipOld + insNew) distance {@link #findResync} searches before giving up and
   * consuming the remainder as one edit. Real trie realignments happen within a child slot (≤ 33
   * bytes); beyond this window the data has genuinely diverged and a single spanning edit (bounded
   * by the FULL fallback in {@link #encodeDiff}) is the right answer.
   */
  private static final int RESYNC_MAX_RADIUS = 256;

  private ArchiveTrieNodeCodec() {}

  /** Layout: {@code [ENTRY_FULL]} ‖ {@code nodeBytes}. */
  public static Bytes encodeFull(final Bytes nodeBytes) {
    Objects.requireNonNull(nodeBytes, "nodeBytes must not be null");
    return Bytes.concatenate(Bytes.of(ENTRY_FULL), nodeBytes);
  }

  /**
   * Encodes the diff from {@code oldNode} to {@code newNode} as a binary patch entry. Returns a
   * {@code ENTRY_FULL | CREATION} entry when {@code oldNode} is null (creation), a {@code DELETION}
   * tombstone when {@code newNode} is null (deletion), or a {@code ENTRY_FULL} entry when the patch
   * body would be at least as large as the new node.
   */
  public static Bytes encodeDiff(final Bytes oldNode, final Bytes newNode) {
    if (oldNode == null && newNode == null) {
      throw new IllegalArgumentException("encodeDiff: both old and new nodes are null");
    } else if (oldNode == null) {
      return Bytes.concatenate(Bytes.of((byte) (ENTRY_FULL | CREATION)), newNode);
    } else if (newNode == null) {
      return Bytes.of(DELETION);
    }

    final Bytes patch = encodePatch(oldNode, newNode);
    if (patch.size() >= newNode.size()) {
      return encodeFull(newNode);
    }
    return Bytes.concatenate(Bytes.of(ENTRY_DIFF), patch);
  }

  public static ArchiveTrieNodeEntry decode(final Bytes entry) {
    Objects.requireNonNull(entry, "entry must not be null");
    if (entry.isEmpty()) {
      throw new IllegalArgumentException("Entry must be at least 1 byte (metadata byte)");
    }
    return new ArchiveTrieNodeEntry(entry.get(0), entry.slice(1));
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
      node = applyPatch(node, entry.patchBody());
    }
    return node;
  }

  /**
   * Dispatches to {@link #encodePatchMultiRun} for same-length arrays (the common case for
   * hash-to-hash trie node changes) or {@link #encodePatchAligned} for different-length arrays.
   */
  private static Bytes encodePatch(final Bytes old, final Bytes newNode) {
    if (old.size() == newNode.size()) {
      return encodePatchMultiRun(old, newNode);
    }
    return encodePatchAligned(old, newNode);
  }

  /**
   * Greedy multi-run encoder for same-length arrays. Emits one COPY+REPLACE pair per contiguous run
   * of changed bytes. The trailing common suffix is left to the implicit copy in {@link
   * #applyPatch}.
   */
  private static Bytes encodePatchMultiRun(final Bytes old, final Bytes newNode) {
    final int nodeLength = old.size();
    final List<Bytes> parts = new ArrayList<>();
    int position = 0;
    while (position < nodeLength) {
      // COPY phase: advance over the matching run.
      final int copyStart = position;
      position += matchRun(old, position, newNode, position);
      if (position >= nodeLength) break; // trailing suffix: implicit copy in applyPatch handles it
      if (position > copyStart) {
        appendNoDataOp(parts, OP_COPY, position - copyStart);
      }

      // DIFF phase: advance until RESYNC_MATCH_MIN or more matching bytes.
      final int diffStart = position;
      while (position < nodeLength
          && matchRun(old, position, newNode, position) < RESYNC_MATCH_MIN) {
        position++;
      }
      if (position > diffStart) {
        appendDataOp(parts, OP_REPLACE, newNode.slice(diffStart, position - diffStart));
      }
    }
    return Bytes.concatenate(parts.toArray(new Bytes[0]));
  }

  /**
   * Length-tolerant multi-region encoder for different-length arrays. Walks {@code old} and {@code
   * newNode} with two cursors, emitting a COPY for each run of matching bytes and — at each
   * divergence — the minimal REPLACE/INSERT/SKIP edit needed to reach the next re-synchronisation
   * point ({@link #findResync}). Unlike a single prefix/suffix diff, this copies unchanged regions
   * that sit <em>between</em> two changes (e.g. the untouched child slots between two changed
   * children of a branch node) instead of re-storing them inside one spanning INSERT — matching the
   * density of a structure-aware child-mask diff without any knowledge of the node's structure.
   *
   * <p>The trailing common suffix is left to the implicit copy in {@link #applyPatch}. Ops larger
   * than {@link #OP_MAX_LENGTH} are split into chunks.
   */
  private static Bytes encodePatchAligned(final Bytes old, final Bytes newNode) {
    final int oldLen = old.size();
    final int newLen = newNode.size();
    final List<Bytes> parts = new ArrayList<>();
    int oldPos = 0;
    int newPos = 0;

    while (oldPos < oldLen || newPos < newLen) {
      // COPY: emit a matching run at the current aligned position.
      final int matchLen = matchRun(old, oldPos, newNode, newPos);
      if (matchLen > 0) {
        if (oldPos + matchLen == oldLen && newPos + matchLen == newLen) {
          break; // trailing common suffix: implicit copy in applyPatch handles it
        }
        appendNoDataOp(parts, OP_COPY, matchLen);
        oldPos += matchLen;
        newPos += matchLen;
        continue;
      }

      final Resync resync = findResync(old, oldPos, newNode, newPos);
      appendResyncEdit(parts, newNode, newPos, resync);
      oldPos += resync.skipOld();
      newPos += resync.insertNew();
    }
    return Bytes.concatenate(parts.toArray(new Bytes[0]));
  }

  /** Emits the REPLACE, INSERT, and SKIP operations that consume one divergent region. */
  private static void appendResyncEdit(
      final List<Bytes> parts, final Bytes newNode, final int newPos, final Resync resync) {
    final int replaceLen = Math.min(resync.skipOld(), resync.insertNew());
    if (replaceLen > 0) {
      appendDataOp(parts, OP_REPLACE, newNode.slice(newPos, replaceLen));
    }
    if (resync.insertNew() > replaceLen) {
      appendDataOp(
          parts, OP_INSERT, newNode.slice(newPos + replaceLen, resync.insertNew() - replaceLen));
    }
    if (resync.skipOld() > replaceLen) {
      appendNoDataOp(parts, OP_SKIP, resync.skipOld() - replaceLen);
    }
  }

  /**
   * Finds the nearest re-synchronisation point after a divergence at {@code (oldPos, newPos)}.
   * Returns the number of old bytes to drop and new bytes to add to reach a run of at least {@link
   * #RESYNC_MATCH_MIN} matching bytes (or the shared end of both arrays).
   *
   * <p>Candidates are searched by ascending total distance {@code d = skipOld + insertNew}, and
   * within each distance from the most balanced split outward — so an equal-length substitution
   * (encoded as a tight REPLACE) is preferred over an insertion/deletion interpretation of the same
   * change. If no anchor is found within {@link #RESYNC_MAX_RADIUS}, the remainder of both arrays
   * is consumed as a single edit.
   */
  private static Resync findResync(
      final Bytes old, final int oldPos, final Bytes newNode, final int newPos) {
    final int maxOld = old.size() - oldPos;
    final int maxNew = newNode.size() - newPos;
    final int maxDistance = Math.min(maxOld + maxNew, RESYNC_MAX_RADIUS);
    for (int totalDistance = 1; totalDistance <= maxDistance; totalDistance++) {
      // Visit all (skipOld, insertNew) pairs with skipOld+insertNew==totalDistance, from most
      // balanced outward. Imbalance must share parity with totalDistance so the halves are ints.
      for (int imbalance = (totalDistance & 1); imbalance <= totalDistance; imbalance += 2) {
        final int skipOld = (totalDistance + imbalance) / 2;
        final int insertNew = (totalDistance - imbalance) / 2;
        if (skipOld <= maxOld
            && insertNew <= maxNew
            && isResyncAnchor(old, oldPos + skipOld, newNode, newPos + insertNew)) {
          return new Resync(skipOld, insertNew);
        }
        // Mirror: insert-heavy counterpart (skipOld=insertNew, insertNew=skipOld).
        if (imbalance > 0
            && insertNew <= maxOld
            && skipOld <= maxNew
            && isResyncAnchor(old, oldPos + insertNew, newNode, newPos + skipOld)) {
          return new Resync(insertNew, skipOld);
        }
      }
    }
    return new Resync(maxOld, maxNew);
  }

  private record Resync(int skipOld, int insertNew) {}

  /**
   * True if old and new re-synchronise at the given positions: at least {@link #RESYNC_MATCH_MIN}
   * bytes match, or the matching run reaches the end of both arrays together (a genuine common
   * suffix shorter than the threshold).
   */
  private static boolean isResyncAnchor(
      final Bytes old, final int oldPos, final Bytes newNode, final int newPos) {
    final int matchLen = matchRun(old, oldPos, newNode, newPos);
    return matchLen >= RESYNC_MATCH_MIN
        || (matchLen > 0 && oldPos + matchLen == old.size() && newPos + matchLen == newNode.size());
  }

  /**
   * Length of the matching run starting at {@code (oldPos, newPos)}, bounded by both array ends.
   */
  private static int matchRun(
      final Bytes old, final int oldPos, final Bytes newNode, final int newPos) {
    int matchLen = 0;
    while (oldPos + matchLen < old.size()
        && newPos + matchLen < newNode.size()
        && old.get(oldPos + matchLen) == newNode.get(newPos + matchLen)) {
      matchLen++;
    }
    return matchLen;
  }

  /** Emits a COPY or SKIP op (no data), splitting lengths above {@link #OP_MAX_LENGTH}. */
  private static void appendNoDataOp(final List<Bytes> parts, final int opType, final int length) {
    int remaining = length;
    while (remaining > 0) {
      final int chunk = Math.min(remaining, OP_MAX_LENGTH);
      parts.add(encodeOp(opType, chunk));
      remaining -= chunk;
    }
  }

  /** Emits a REPLACE or INSERT op followed by its data, splitting above {@link #OP_MAX_LENGTH}. */
  private static void appendDataOp(final List<Bytes> parts, final int opType, final Bytes data) {
    int off = 0;
    int remaining = data.size();
    while (remaining > 0) {
      final int chunk = Math.min(remaining, OP_MAX_LENGTH);
      parts.add(encodeOp(opType, chunk));
      parts.add(data.slice(off, chunk));
      off += chunk;
      remaining -= chunk;
    }
  }

  /** Applies a binary patch body to {@code base}, producing the reconstructed node. */
  private static Bytes applyPatch(final Bytes base, final Bytes patchBody) {
    final List<Bytes> outputParts = new ArrayList<>();
    int oldPos = 0;
    int patchPos = 0;

    while (patchPos < patchBody.size()) {
      if (patchPos + OP_HEADER_SIZE > patchBody.size()) {
        throw new IllegalArgumentException("truncated op at position " + patchPos);
      }
      // 2-byte op: [2b type][6b lenHi][8b lenLo]
      final int first = Byte.toUnsignedInt(patchBody.get(patchPos++));
      final int second = Byte.toUnsignedInt(patchBody.get(patchPos++));
      final int opType = first >> OP_TYPE_SHIFT;
      final int length = ((first & OP_LENGTH_HIGH_MASK) << Byte.SIZE) | second;

      switch (opType) {
        case OP_COPY -> {
          if (oldPos + length > base.size()) {
            throw new IllegalArgumentException("COPY length overruns base node");
          }
          outputParts.add(base.slice(oldPos, length));
          oldPos += length;
        }
        case OP_SKIP -> {
          oldPos += length;
          if (oldPos > base.size()) {
            throw new IllegalArgumentException("SKIP length overruns base node");
          }
        }
        case OP_INSERT -> {
          if (patchPos + length > patchBody.size()) {
            throw new IllegalArgumentException("INSERT length overruns patch body");
          }
          outputParts.add(patchBody.slice(patchPos, length));
          patchPos += length;
        }
        case OP_REPLACE -> {
          if (patchPos + length > patchBody.size()) {
            throw new IllegalArgumentException("REPLACE data overruns patch body");
          }
          if (oldPos + length > base.size()) {
            throw new IllegalArgumentException("REPLACE length overruns base node");
          }
          outputParts.add(patchBody.slice(patchPos, length));
          patchPos += length;
          oldPos += length;
        }
        default -> throw new IllegalArgumentException("unknown patch op type: " + opType);
      }
    }

    // Implicit: copy remaining old bytes (the common suffix)
    if (oldPos < base.size()) {
      outputParts.add(base.slice(oldPos));
    }

    return Bytes.concatenate(outputParts.toArray(new Bytes[0]));
  }

  private static Bytes encodeOp(final int type, final int length) {
    if (length < 0 || length > OP_MAX_LENGTH) {
      throw new IllegalArgumentException(
          "patch op length out of range [0, " + OP_MAX_LENGTH + "]: " + length);
    }
    return Bytes.of(
        (byte) ((type << OP_TYPE_SHIFT) | (length >> Byte.SIZE)), (byte) (length & 0xFF));
  }
}
