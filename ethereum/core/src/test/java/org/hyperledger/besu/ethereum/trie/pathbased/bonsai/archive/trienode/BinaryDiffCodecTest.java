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

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;

class BinaryDiffCodecTest {

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private static Bytes node(final int... bytes) {
    final byte[] b = new byte[bytes.length];
    for (int i = 0; i < bytes.length; i++) {
      b[i] = (byte) bytes[i];
    }
    return Bytes.wrap(b);
  }

  private static Bytes fill(final int len, final int value) {
    final byte[] b = new byte[len];
    java.util.Arrays.fill(b, (byte) value);
    return Bytes.wrap(b);
  }

  /** Encodes old→new and asserts apply reproduces new byte-exactly, returning the patch. */
  private static Bytes encodeAndRoundTrip(final Bytes old, final Bytes newBytes) {
    final Bytes patch = BinaryDiffCodec.encode(old, newBytes);
    assertThat(BinaryDiffCodec.apply(old, patch)).isEqualTo(newBytes);
    return patch;
  }

  /**
   * Counts ops of a given type in a patch body. Each op is 2 bytes: bits[7:6]=type,
   * bits[13:0]=length.
   */
  private static int countOps(final byte[] body, final int wantedType) {
    int count = 0;
    int pos = 0;
    while (pos < body.length) {
      final int first = body[pos++] & 0xFF;
      final int second = body[pos++] & 0xFF;
      final int type = first >> 6;
      final int length = ((first & 0x3F) << 8) | second;
      if (type == wantedType) {
        count++;
      }
      if (type == 2 || type == 3) { // INSERT / REPLACE carry data
        pos += length;
      }
    }
    return count;
  }

  // ---------------------------------------------------------------------------
  // Basic properties
  // ---------------------------------------------------------------------------

  @Test
  void identicalArraysProduceEmptyPatch() {
    // The whole array is the implicit trailing suffix: zero ops needed.
    final Bytes n = fill(40, 0xAB);
    final Bytes patch = encodeAndRoundTrip(n, n);
    assertThat(patch.isEmpty()).isTrue();
  }

  @Test
  void applyEmptyPatchReturnsBaseByteExact() {
    final Bytes n = node(0x01, 0x02, 0x03);
    assertThat(BinaryDiffCodec.apply(n, Bytes.EMPTY)).isEqualTo(n);
  }

  @Test
  void encodeProducesKnownWireBytes() {
    // old: 40 bytes of 0x01; new: same with byte 20 = 0xFF
    // prefix=20, REPLACE(1, 0xFF), suffix=19 implicit
    // COPY(20):    type=0, len=20  → [0x00, 0x14]
    // REPLACE(1):  type=3, len=1   → [0xC0, 0x01], then data [0xFF]
    final Bytes oldNode = fill(40, 0x01);
    final byte[] newArr = oldNode.toArray();
    newArr[20] = (byte) 0xFF;
    final Bytes patch = encodeAndRoundTrip(oldNode, Bytes.wrap(newArr));
    assertThat(patch.toArray()).isEqualTo(new byte[] {0x00, 0x14, (byte) 0xC0, 0x01, (byte) 0xFF});
  }

  // ---------------------------------------------------------------------------
  // Same-length (multi-run) encoder
  // ---------------------------------------------------------------------------

  @Test
  void roundTripSingleByteChange() {
    final Bytes old = Bytes.concatenate(node(0xAA, 0xBB, 0xCC), fill(7, 0x00));
    final Bytes newNode = Bytes.concatenate(node(0xAA, 0xFF, 0xCC), fill(7, 0x00));
    encodeAndRoundTrip(old, newNode);
  }

  @Test
  void roundTripHashSizedBlockChange() {
    // Simulates a 32-byte hash ref changing inside a larger node (dominant trie change pattern).
    final Bytes prefix = fill(3, 0x01);
    final Bytes suffix = fill(5, 0x02);
    final Bytes old = Bytes.concatenate(prefix, fill(32, 0x00), suffix);
    final Bytes newNode = Bytes.concatenate(prefix, fill(32, 0xAA), suffix);
    final Bytes patch = encodeAndRoundTrip(old, newNode);
    assertThat(patch.size()).isLessThan(newNode.size()); // beats storing the node outright
  }

  @Test
  void roundTripPrefixOnlyChange() {
    // First byte changes; suffix is long and unchanged.
    final Bytes old = Bytes.concatenate(node(0x01), fill(50, 0xBB));
    final Bytes newNode = Bytes.concatenate(node(0x02), fill(50, 0xBB));
    encodeAndRoundTrip(old, newNode);
  }

  @Test
  void roundTripSuffixOnlyChange() {
    // Last byte changes; prefix is long and unchanged.
    final Bytes old = Bytes.concatenate(fill(50, 0xBB), node(0x01));
    final Bytes newNode = Bytes.concatenate(fill(50, 0xBB), node(0x02));
    encodeAndRoundTrip(old, newNode);
  }

  @Test
  void encodeMultipleNonAdjacentChangesProducesMultipleRuns() {
    // 20-byte arrays: bytes 2 and 14 differ; 11-byte gap between them satisfies RESYNC_MATCH_MIN
    final byte[] oldBytes = new byte[20];
    final byte[] newBytes = new byte[20];
    oldBytes[2] = 0x01;
    newBytes[2] = 0x02;
    oldBytes[14] = 0x03;
    newBytes[14] = 0x04;

    final Bytes patch = encodeAndRoundTrip(Bytes.wrap(oldBytes), Bytes.wrap(newBytes));
    // Two separate REPLACE ops (one per changed region, not one spanning both).
    assertThat(countOps(patch.toArray(), 3)).isEqualTo(2);
  }

  // ---------------------------------------------------------------------------
  // Length-tolerant multi-region alignment (different-length arrays)
  // ---------------------------------------------------------------------------

  @Test
  void alignedEncoderCopiesUnchangedRegionBetweenTwoChangesInsteadOfSpanningInsert() {
    // Two changes with an unchanged region between them, where an overall length change forces the
    // different-length (aligned) path. A single prefix/suffix diff would swallow the middle 0x33
    // run inside one spanning INSERT; the aligned encoder must COPY it instead.
    final Bytes a = fill(20, 0x11); // leading unchanged
    final Bytes h1Old = fill(32, 0x00); // changed hash (substitution)
    final Bytes h1New = fill(32, 0xAA);
    final Bytes mid = fill(20, 0x33); // interior UNCHANGED region
    final Bytes h2Old = node(0x80); // empty child slot (1 byte)
    final Bytes h2New = Bytes.concatenate(node(0xA0), fill(32, 0x77)); // becomes present (33 bytes)
    final Bytes c = fill(20, 0x55); // trailing unchanged

    final Bytes old = Bytes.concatenate(a, h1Old, mid, h2Old, c);
    final Bytes newNode = Bytes.concatenate(a, h1New, mid, h2New, c);

    final Bytes patch = encodeAndRoundTrip(old, newNode);
    assertThat(patch.size()).isLessThan(newNode.size()); // genuine multi-region diff win

    // The structural win: at least two COPY ops (leading region + interior 0x33 region), proving
    // the unchanged middle was copied rather than re-stored inside one spanning INSERT.
    assertThat(countOps(patch.toArray(), 0)).isGreaterThanOrEqualTo(2);
  }

  @Test
  void alignedEncoderInteriorInsertionIsFarSmallerThanNodeSize() {
    // A 32-byte block inserted into the middle, unchanged data on both sides.
    final Bytes head = fill(60, 0x11);
    final Bytes tail = fill(60, 0x22);
    final Bytes old = Bytes.concatenate(head, tail);
    final Bytes newNode = Bytes.concatenate(head, fill(32, 0x33), tail);

    final Bytes patch = encodeAndRoundTrip(old, newNode);
    // Patch stores only the 32 inserted bytes + a few op bytes, not the 152-byte node.
    assertThat(patch.size()).isLessThan(44);
  }

  @Test
  void alignedEncoderInteriorDeletionRoundTrips() {
    // A 32-byte block removed from the middle, unchanged data on both sides.
    final Bytes head = fill(60, 0x11);
    final Bytes tail = fill(60, 0x22);
    final Bytes old = Bytes.concatenate(head, fill(32, 0x33), tail);
    final Bytes newNode = Bytes.concatenate(head, tail);

    final Bytes patch = encodeAndRoundTrip(old, newNode);
    // Deletion stores no new data — just COPY + SKIP ops — so the patch is tiny.
    assertThat(patch.size()).isLessThan(11);
    assertThat(countOps(patch.toArray(), 1)).isGreaterThanOrEqualTo(1); // ≥1 SKIP
  }

  @Test
  void alignedEncoderMixedSubstitutionAndInsertionRoundTrips() {
    // Substitution (same length) AND insertion (length change) in the same node — exercises both
    // the balanced-resync (REPLACE) and asymmetric-resync (INSERT) paths in one patch.
    final Bytes a = fill(15, 0x11);
    final Bytes subOld = fill(32, 0x00);
    final Bytes subNew = fill(32, 0xAA);
    final Bytes b = fill(15, 0x33);
    final Bytes insNew = fill(40, 0x77);
    final Bytes c = fill(15, 0x55);

    final Bytes old = Bytes.concatenate(a, subOld, b, c);
    final Bytes newNode = Bytes.concatenate(a, subNew, b, insNew, c);
    encodeAndRoundTrip(old, newNode);
  }

  @Test
  void alignedEncoderPureTrailingInsertionRoundTrips() {
    final Bytes old = fill(50, 0x11);
    final Bytes newNode = Bytes.concatenate(old, fill(20, 0x22));
    encodeAndRoundTrip(old, newNode);
  }

  @Test
  void alignedEncoderPureTrailingDeletionRoundTrips() {
    final Bytes newNode = fill(50, 0x11);
    final Bytes old = Bytes.concatenate(newNode, fill(20, 0x22));
    encodeAndRoundTrip(old, newNode);
  }

  @Test
  void alignedEncoderLeadingInsertionRoundTrips() {
    final Bytes shared = fill(50, 0x11);
    final Bytes newNode = Bytes.concatenate(fill(20, 0x22), shared);
    encodeAndRoundTrip(shared, newNode);
  }

  @Test
  void alignedEncoderThreeChangedRegionsRoundTripsAndStaysCompact() {
    // Simulates three changed children in a branch node with a presence change (length delta),
    // separated by unchanged children. The worst case the aligned encoder was built to fix.
    final Bytes header = fill(3, 0x0F);
    final Bytes childPresent = Bytes.concatenate(node(0xA0), fill(32, 0xC1)); // 33 bytes
    final Bytes childEmpty = node(0x80); // 1 byte

    // old: [hdr][c0][c1-empty][c2][c3][c4]
    final Bytes c0 = childPresent;
    final Bytes c1old = childEmpty;
    final Bytes c2 = Bytes.concatenate(node(0xA0), fill(32, 0xC2));
    final Bytes c3old = Bytes.concatenate(node(0xA0), fill(32, 0xC3));
    final Bytes c4 = Bytes.concatenate(node(0xA0), fill(32, 0xC4));

    // new: c1 becomes present (length change), c3's hash changes (substitution)
    final Bytes c1new = Bytes.concatenate(node(0xA0), fill(32, 0xB1));
    final Bytes c3new = Bytes.concatenate(node(0xA0), fill(32, 0xB3));

    final Bytes old = Bytes.concatenate(header, c0, c1old, c2, c3old, c4);
    final Bytes newNode = Bytes.concatenate(header, c0, c1new, c2, c3new, c4);

    final Bytes patch = encodeAndRoundTrip(old, newNode);

    // Only the two changed children (~65 bytes of data) plus op overhead — nowhere near a spanning
    // INSERT of the whole c1..c3 span (~101 bytes) or the full node (~170 bytes).
    assertThat(patch.size()).isLessThan(89);
    // Unchanged c2 (between the two changes) must be COPYed, not re-stored: ≥2 COPY ops.
    assertThat(countOps(patch.toArray(), 0)).isGreaterThanOrEqualTo(2);
  }
}
