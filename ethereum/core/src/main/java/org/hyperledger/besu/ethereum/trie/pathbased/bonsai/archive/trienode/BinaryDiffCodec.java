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

import java.util.ArrayList;
import java.util.List;

import org.apache.tuweni.bytes.Bytes;

/**
 * Generic binary diff codec: encodes the byte-level difference between a base array and a target
 * array as a sequence of COPY/SKIP/INSERT/REPLACE ops, and applies such a patch to reconstruct the
 * target from the base.
 *
 * <ul>
 *   <li>COPY(n) — emit n bytes from the base at current base_pos, advance base_pos
 *   <li>SKIP(n) — advance base_pos by n, no output
 *   <li>INSERT(n) — followed by n bytes of target data, emit them (base_pos unchanged)
 *   <li>REPLACE(n) — followed by n bytes of target data, emit them AND advance base_pos by n
 * </ul>
 *
 * <p>REPLACE collapses the INSERT+SKIP pair used for same-length changed regions. After all ops,
 * any remaining bytes in the base are implicitly appended (zero-cost trailing suffix). Each op is 2
 * bytes: byte1 bits[7:6] = type (0=COPY, 1=SKIP, 2=INSERT, 3=REPLACE), byte1 bits[5:0] and byte2 =
 * 14-bit length (max 16383). This format is trie-structure agnostic: it works for MPT, PBT
 * (EIP-8297), or any future encoding without modification.
 *
 * <p>A patch is always correct but not always worth storing: {@link #encode} may return a patch at
 * least as large as the target (e.g. when every byte differs). Callers decide the fallback — {@link
 * ArchiveTrieNodeCodec#encodeDiff} stores a FULL entry in that case.
 */
public final class BinaryDiffCodec {

  private static final byte OP_COPY = 0x00;
  private static final byte OP_SKIP = 0x01;
  private static final byte OP_INSERT = 0x02;
  private static final byte OP_REPLACE = 0x03;

  private static final int OP_HEADER_SIZE = 2;
  private static final int OP_TYPE_SHIFT = 6;
  private static final int OP_LENGTH_HIGH_MASK = 0x3F;
  private static final int OP_MAX_LENGTH = 0x3FFF; // 14 bits (2-byte op format)
  private static final int RESYNC_MATCH_MIN = 4;
  // 64 covers all real MPT patterns (single/dual child-presence toggles peak at edit distance 68);
  // rarer multi-toggle cases fall through to the (maxBase, maxTarget) fallback and encodeDiff →
  // FULL.
  private static final int RESYNC_MAX_RADIUS = 64;

  private BinaryDiffCodec() {}

  /** Encodes the difference between {@code base} and {@code target} as a binary patch. */
  public static Bytes encode(final Bytes base, final Bytes target) {
    if (base.size() == target.size()) {
      return encodeSameLength(base, target);
    }
    return encodeRealigning(base, target);
  }

  /** Applies a binary patch body to {@code base}, producing the reconstructed bytes. */
  public static Bytes apply(final Bytes base, final Bytes patchBody) {
    final List<Bytes> outputParts = new ArrayList<>();
    int basePos = 0;
    int patchPos = 0;

    while (patchPos < patchBody.size()) {
      if (patchPos + OP_HEADER_SIZE > patchBody.size()) {
        throw new IllegalArgumentException("truncated op at position " + patchPos);
      }
      // 2-byte op: [2b type][6b lenHi][8b lenLo]
      final int firstHeaderByte = Byte.toUnsignedInt(patchBody.get(patchPos++));
      final int secondHeaderByte = Byte.toUnsignedInt(patchBody.get(patchPos++));
      final byte opType = (byte) (firstHeaderByte >> OP_TYPE_SHIFT);
      final int length = ((firstHeaderByte & OP_LENGTH_HIGH_MASK) << Byte.SIZE) | secondHeaderByte;

      switch (opType) {
        case OP_COPY -> {
          if (basePos + length > base.size()) {
            throw new IllegalArgumentException("COPY length overruns base");
          }
          outputParts.add(base.slice(basePos, length));
          basePos += length;
        }
        case OP_SKIP -> {
          basePos += length;
          if (basePos > base.size()) {
            throw new IllegalArgumentException("SKIP length overruns base");
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
          if (basePos + length > base.size()) {
            throw new IllegalArgumentException("REPLACE length overruns base");
          }
          outputParts.add(patchBody.slice(patchPos, length));
          patchPos += length;
          basePos += length;
        }
        default -> throw new IllegalArgumentException("unknown patch op type: " + opType);
      }
    }

    // Implicit: copy remaining base bytes (the common suffix)
    if (basePos < base.size()) {
      outputParts.add(base.slice(basePos));
    }

    return Bytes.concatenate(outputParts.toArray(new Bytes[0]));
  }

  /**
   * Greedy encoder for same-length arrays. Emits one COPY+REPLACE pair per contiguous run of
   * changed bytes. The trailing common suffix is left to the implicit copy in {@link #apply}.
   */
  private static Bytes encodeSameLength(final Bytes base, final Bytes target) {
    final int length = base.size();
    final List<Bytes> parts = new ArrayList<>();
    int position = 0;
    while (position < length) {
      // COPY phase: advance over the matching run.
      final int copyStart = position;
      position += matchRunLength(base, position, target, position);
      if (position >= length) break; // trailing suffix: implicit copy in apply handles it
      if (position > copyStart) {
        appendNoDataOp(parts, OP_COPY, position - copyStart);
      }

      // DIFF phase: advance until RESYNC_MATCH_MIN or more matching bytes.
      final int diffStart = position;
      while (position < length
          && matchRunLength(base, position, target, position) < RESYNC_MATCH_MIN) {
        position++;
      }
      if (position > diffStart) {
        appendDataOp(parts, OP_REPLACE, target.slice(diffStart, position - diffStart));
      }
    }
    return Bytes.concatenate(parts.toArray(new Bytes[0]));
  }

  /**
   * Length-tolerant multi-region encoder for different-length arrays. Walks {@code base} and {@code
   * target} with two cursors, emitting a COPY for each run of matching bytes and — at each
   * divergence — the minimal REPLACE/INSERT/SKIP edit needed to reach the next re-synchronisation
   * point ({@link #findResync}). Unlike a single prefix/suffix diff, this copies unchanged regions
   * that sit <em>between</em> two changes (e.g. the untouched child slots between two changed
   * children of a branch node) instead of re-storing them inside one spanning INSERT — matching the
   * density of a structure-aware child-mask diff without any knowledge of the node's structure.
   *
   * <p>The trailing common suffix is left to the implicit copy in {@link #apply}. Ops larger than
   * {@link #OP_MAX_LENGTH} are split into chunks.
   */
  private static Bytes encodeRealigning(final Bytes base, final Bytes target) {
    final int baseLen = base.size();
    final int targetLen = target.size();
    final List<Bytes> parts = new ArrayList<>();
    int basePos = 0;
    int targetPos = 0;

    while (basePos < baseLen || targetPos < targetLen) {
      // COPY: emit a matching run at the current aligned position.
      final int matchLen = matchRunLength(base, basePos, target, targetPos);
      if (matchLen > 0) {
        if (basePos + matchLen == baseLen && targetPos + matchLen == targetLen) {
          break; // trailing common suffix: implicit copy in apply handles it
        }
        appendNoDataOp(parts, OP_COPY, matchLen);
        basePos += matchLen;
        targetPos += matchLen;
        continue;
      }

      final Resync resync = findResync(base, basePos, target, targetPos);
      appendResyncEdit(parts, target, targetPos, resync);
      basePos += resync.skipOld();
      targetPos += resync.insertNew();
    }
    return Bytes.concatenate(parts.toArray(new Bytes[0]));
  }

  /** Emits the REPLACE, INSERT, and SKIP operations that consume one divergent region. */
  private static void appendResyncEdit(
      final List<Bytes> parts, final Bytes target, final int targetPos, final Resync resync) {
    final int replaceLen = Math.min(resync.skipOld(), resync.insertNew());
    if (replaceLen > 0) {
      appendDataOp(parts, OP_REPLACE, target.slice(targetPos, replaceLen));
    }
    if (resync.insertNew() > replaceLen) {
      appendDataOp(
          parts, OP_INSERT, target.slice(targetPos + replaceLen, resync.insertNew() - replaceLen));
    }
    if (resync.skipOld() > replaceLen) {
      appendNoDataOp(parts, OP_SKIP, resync.skipOld() - replaceLen);
    }
  }

  /**
   * Finds the nearest re-synchronisation point after a divergence at {@code (basePos, targetPos)}.
   * Returns the number of base bytes to drop and target bytes to add to reach a run of at least
   * {@link #RESYNC_MATCH_MIN} matching bytes (or the shared end of both arrays).
   *
   * <p>Candidates are searched by ascending total distance {@code d = skipOld + insertNew}, and
   * within each distance from the most balanced split outward — so an equal-length substitution
   * (encoded as a tight REPLACE) is preferred over an insertion/deletion interpretation of the same
   * change. If no anchor is found within {@link #RESYNC_MAX_RADIUS}, the remainder of both arrays
   * is consumed as a single edit.
   */
  private static Resync findResync(
      final Bytes base, final int basePos, final Bytes target, final int targetPos) {
    final int maxBase = base.size() - basePos;
    final int maxTarget = target.size() - targetPos;
    final int maxDistance = Math.min(maxBase + maxTarget, RESYNC_MAX_RADIUS);
    for (int totalDistance = 1; totalDistance <= maxDistance; totalDistance++) {
      // Visit all (skipOld, insertNew) pairs with skipOld+insertNew==totalDistance, from most
      // balanced outward. Imbalance must share parity with totalDistance so the halves are ints.
      for (int imbalance = (totalDistance & 1); imbalance <= totalDistance; imbalance += 2) {
        final int skipOld = (totalDistance + imbalance) / 2;
        final int insertNew = (totalDistance - imbalance) / 2;
        if (skipOld <= maxBase
            && insertNew <= maxTarget
            && isResyncAnchor(base, basePos + skipOld, target, targetPos + insertNew)) {
          return new Resync(skipOld, insertNew);
        }
        // Mirror: insert-heavy counterpart (skipOld=insertNew, insertNew=skipOld).
        if (imbalance > 0
            && insertNew <= maxBase
            && skipOld <= maxTarget
            && isResyncAnchor(base, basePos + insertNew, target, targetPos + skipOld)) {
          return new Resync(insertNew, skipOld);
        }
      }
    }
    return new Resync(maxBase, maxTarget);
  }

  private record Resync(int skipOld, int insertNew) {}

  /**
   * True if base and target re-synchronise at the given positions: at least {@link
   * #RESYNC_MATCH_MIN} bytes match, or the matching run reaches the end of both arrays together (a
   * genuine common suffix shorter than the threshold).
   */
  private static boolean isResyncAnchor(
      final Bytes base, final int basePos, final Bytes target, final int targetPos) {
    final int matchLen = matchRunLength(base, basePos, target, targetPos);
    return matchLen >= RESYNC_MATCH_MIN
        || (matchLen > 0
            && basePos + matchLen == base.size()
            && targetPos + matchLen == target.size());
  }

  /**
   * Length of the matching run starting at {@code (basePos, targetPos)}, bounded by both array
   * ends.
   */
  private static int matchRunLength(
      final Bytes base, final int basePos, final Bytes target, final int targetPos) {
    int matchLen = 0;
    while (basePos + matchLen < base.size()
        && targetPos + matchLen < target.size()
        && base.get(basePos + matchLen) == target.get(targetPos + matchLen)) {
      matchLen++;
    }
    return matchLen;
  }

  /** Emits a COPY or SKIP op (no data), splitting lengths above {@link #OP_MAX_LENGTH}. */
  private static void appendNoDataOp(final List<Bytes> parts, final byte opType, final int length) {
    int remaining = length;
    while (remaining > 0) {
      final int chunk = Math.min(remaining, OP_MAX_LENGTH);
      parts.add(encodeOpHeader(opType, chunk));
      remaining -= chunk;
    }
  }

  /** Emits a REPLACE or INSERT op followed by its data, splitting above {@link #OP_MAX_LENGTH}. */
  private static void appendDataOp(final List<Bytes> parts, final byte opType, final Bytes data) {
    int off = 0;
    while (off < data.size()) {
      final int chunk = Math.min(data.size() - off, OP_MAX_LENGTH);
      parts.add(encodeOpHeader(opType, chunk));
      parts.add(data.slice(off, chunk));
      off += chunk;
    }
  }

  private static Bytes encodeOpHeader(final byte type, final int length) {
    // 2-byte op: [ tt llllll ] [ llllllll ] — 2-bit type, 14-bit length (big-endian)
    return Bytes.of((byte) ((type << OP_TYPE_SHIFT) | (length >> Byte.SIZE)), (byte) length);
  }
}
