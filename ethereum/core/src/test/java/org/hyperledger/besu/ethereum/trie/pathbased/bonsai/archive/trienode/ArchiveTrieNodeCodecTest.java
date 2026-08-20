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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;

class ArchiveTrieNodeCodecTest {

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

  // ---------------------------------------------------------------------------
  // encodeFull
  // ---------------------------------------------------------------------------

  @Test
  void encodeFullRoundTrips() {
    final Bytes n = node(0xAA, 0xBB, 0xCC);
    final ArchiveTrieNodeEntry e = ArchiveTrieNodeCodec.decode(ArchiveTrieNodeCodec.encodeFull(n));
    assertThat(e.isFull()).isTrue();
    assertThat(e.isDeletion()).isFalse();
    assertThat(e.fullNode()).isEqualTo(n);
  }

  // ---------------------------------------------------------------------------
  // encodeDiff — lifecycle cases
  // ---------------------------------------------------------------------------

  @Test
  void encodeDiffNullOldIsCreationFull() {
    final Bytes n = node(0x01, 0x02);
    final ArchiveTrieNodeEntry e =
        ArchiveTrieNodeCodec.decode(ArchiveTrieNodeCodec.encodeDiff(null, n));
    assertThat(e.isFull()).isTrue();
    assertThat(e.isCreation()).isTrue();
    assertThat(e.fullNode()).isEqualTo(n);
  }

  @Test
  void encodeDiffNullNewIsDeletionTombstoneWithNoBody() {
    final Bytes n = node(0x01, 0x02);
    final Bytes entry = ArchiveTrieNodeCodec.encodeDiff(n, null);
    assertThat(entry.size()).isEqualTo(1);
    assertThat(ArchiveTrieNodeCodec.decode(entry).isDeletion()).isTrue();
  }

  @Test
  void encodeDiffBothNullThrows() {
    assertThatThrownBy(() -> ArchiveTrieNodeCodec.encodeDiff(null, null))
        .isInstanceOf(IllegalArgumentException.class);
  }

  // ---------------------------------------------------------------------------
  // encodeDiff — binary patch properties
  // ---------------------------------------------------------------------------

  @Test
  void encodeDiffIdenticalNodesProducesNonFullDiff() {
    // A no-op diff: same bytes in and out, but NOT a FULL entry (it encodes as a zero-op patch).
    final Bytes n = fill(40, 0xAB);
    final ArchiveTrieNodeEntry e =
        ArchiveTrieNodeCodec.decode(ArchiveTrieNodeCodec.encodeDiff(n, n));
    assertThat(e.isFull()).isFalse();
    assertThat(e.isDeletion()).isFalse();
  }

  @Test
  void encodeDiffProducesEntrySmallerthanNodeForSingleByteChange() {
    // Dominant trie-node case: one byte changes in a 36-byte node.
    final byte[] old = new byte[36];
    final byte[] newBytes = old.clone();
    newBytes[18] = (byte) 0xFF;
    final Bytes diffEntry = ArchiveTrieNodeCodec.encodeDiff(Bytes.wrap(old), Bytes.wrap(newBytes));
    assertThat(diffEntry.size()).isLessThan(36 + 1); // must beat FULL
  }

  @Test
  void encodeDiffFallsBackToFullWhenPatchExceedsNodeSize() {
    // Every byte differs → patch body (REPLACE) exceeds new node size → FULL fallback.
    final Bytes old = fill(8, 0x11);
    final Bytes newNode = fill(8, 0x22);
    // REPLACE(8 bytes) = 2 (op header) + 8 data = 10 bytes > 8 → FULL
    final ArchiveTrieNodeEntry e =
        ArchiveTrieNodeCodec.decode(ArchiveTrieNodeCodec.encodeDiff(old, newNode));
    assertThat(e.isFull()).isTrue();
    assertThat(e.fullNode()).isEqualTo(newNode);
  }

  // ---------------------------------------------------------------------------
  // encodeDiff — length-tolerant multi-region alignment (different-length arrays)
  // ---------------------------------------------------------------------------

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

    final Bytes diffEntry = ArchiveTrieNodeCodec.encodeDiff(old, newNode);
    final ArchiveTrieNodeEntry diff = ArchiveTrieNodeCodec.decode(diffEntry);
    assertThat(diff.isFull()).isFalse(); // genuine multi-region DIFF, not a FULL fallback

    // Round-trip byte-exact.
    assertThat(
            ArchiveTrieNodeCodec.reconstruct(
                ArchiveTrieNodeCodec.encodeFull(old), List.of(diffEntry)))
        .isEqualTo(newNode);

    // The structural win: at least two COPY ops (leading region + interior 0x33 region), proving
    // the unchanged middle was copied rather than re-stored inside one spanning INSERT.
    assertThat(countOps(diff.patchBody().toArray(), 0)).isGreaterThanOrEqualTo(2);
  }

  @Test
  void alignedEncoderInteriorInsertionIsFarSmallerThanNodeSize() {
    // A 32-byte block inserted into the middle, unchanged data on both sides.
    final Bytes head = fill(60, 0x11);
    final Bytes tail = fill(60, 0x22);
    final Bytes old = Bytes.concatenate(head, tail);
    final Bytes newNode = Bytes.concatenate(head, fill(32, 0x33), tail);

    final Bytes diffEntry = ArchiveTrieNodeCodec.encodeDiff(old, newNode);
    final ArchiveTrieNodeEntry diff = ArchiveTrieNodeCodec.decode(diffEntry);
    assertThat(diff.isFull()).isFalse();
    // Patch stores only the 32 inserted bytes + a few op bytes, not the 152-byte node.
    assertThat(diffEntry.size()).isLessThan(45);
    assertThat(
            ArchiveTrieNodeCodec.reconstruct(
                ArchiveTrieNodeCodec.encodeFull(old), List.of(diffEntry)))
        .isEqualTo(newNode);
  }

  @Test
  void alignedEncoderInteriorDeletionRoundTrips() {
    // A 32-byte block removed from the middle, unchanged data on both sides.
    final Bytes head = fill(60, 0x11);
    final Bytes tail = fill(60, 0x22);
    final Bytes old = Bytes.concatenate(head, fill(32, 0x33), tail);
    final Bytes newNode = Bytes.concatenate(head, tail);

    final Bytes diffEntry = ArchiveTrieNodeCodec.encodeDiff(old, newNode);
    final ArchiveTrieNodeEntry diff = ArchiveTrieNodeCodec.decode(diffEntry);
    assertThat(diff.isFull()).isFalse();
    // Deletion stores no new data — just COPY + SKIP ops — so the patch is tiny.
    assertThat(diffEntry.size()).isLessThan(12);
    assertThat(countOps(diff.patchBody().toArray(), 1)).isGreaterThanOrEqualTo(1); // ≥1 SKIP
    assertThat(
            ArchiveTrieNodeCodec.reconstruct(
                ArchiveTrieNodeCodec.encodeFull(old), List.of(diffEntry)))
        .isEqualTo(newNode);
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

    final Bytes diffEntry = ArchiveTrieNodeCodec.encodeDiff(old, newNode);
    assertThat(ArchiveTrieNodeCodec.decode(diffEntry).isFull()).isFalse();
    assertThat(
            ArchiveTrieNodeCodec.reconstruct(
                ArchiveTrieNodeCodec.encodeFull(old), List.of(diffEntry)))
        .isEqualTo(newNode);
  }

  @Test
  void alignedEncoderPureTrailingInsertionRoundTrips() {
    final Bytes old = fill(50, 0x11);
    final Bytes newNode = Bytes.concatenate(old, fill(20, 0x22));
    final Bytes diffEntry = ArchiveTrieNodeCodec.encodeDiff(old, newNode);
    assertThat(
            ArchiveTrieNodeCodec.reconstruct(
                ArchiveTrieNodeCodec.encodeFull(old), List.of(diffEntry)))
        .isEqualTo(newNode);
  }

  @Test
  void alignedEncoderPureTrailingDeletionRoundTrips() {
    final Bytes newNode = fill(50, 0x11);
    final Bytes old = Bytes.concatenate(newNode, fill(20, 0x22));
    final Bytes diffEntry = ArchiveTrieNodeCodec.encodeDiff(old, newNode);
    assertThat(
            ArchiveTrieNodeCodec.reconstruct(
                ArchiveTrieNodeCodec.encodeFull(old), List.of(diffEntry)))
        .isEqualTo(newNode);
  }

  @Test
  void alignedEncoderLeadingInsertionRoundTrips() {
    final Bytes shared = fill(50, 0x11);
    final Bytes old = shared;
    final Bytes newNode = Bytes.concatenate(fill(20, 0x22), shared);
    final Bytes diffEntry = ArchiveTrieNodeCodec.encodeDiff(old, newNode);
    assertThat(
            ArchiveTrieNodeCodec.reconstruct(
                ArchiveTrieNodeCodec.encodeFull(old), List.of(diffEntry)))
        .isEqualTo(newNode);
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

    final Bytes diffEntry = ArchiveTrieNodeCodec.encodeDiff(old, newNode);
    final ArchiveTrieNodeEntry diff = ArchiveTrieNodeCodec.decode(diffEntry);
    assertThat(diff.isFull()).isFalse();
    assertThat(
            ArchiveTrieNodeCodec.reconstruct(
                ArchiveTrieNodeCodec.encodeFull(old), List.of(diffEntry)))
        .isEqualTo(newNode);

    // Only the two changed children (~65 bytes of data) plus op overhead — nowhere near a spanning
    // INSERT of the whole c1..c3 span (~101 bytes) or the full node (~170 bytes).
    assertThat(diffEntry.size()).isLessThan(90);
    // Unchanged c2 (between the two changes) must be COPYed, not re-stored: ≥2 COPY ops.
    assertThat(countOps(diff.patchBody().toArray(), 0)).isGreaterThanOrEqualTo(2);
  }

  // ---------------------------------------------------------------------------
  // reconstruct — round-trip correctness
  // ---------------------------------------------------------------------------

  @Test
  void reconstructWithNoDiffsReturnsFullNodeByteExact() {
    final Bytes n = node(0x01, 0x02, 0x03);
    assertThat(ArchiveTrieNodeCodec.reconstruct(ArchiveTrieNodeCodec.encodeFull(n), List.of()))
        .isEqualTo(n);
  }

  @Test
  void roundTripSingleByteChange() {
    // Use 10-byte nodes so the 7-byte patch beats the node size (avoids FULL fallback).
    final Bytes old = Bytes.concatenate(node(0xAA, 0xBB, 0xCC), fill(7, 0x00));
    final Bytes newNode = Bytes.concatenate(node(0xAA, 0xFF, 0xCC), fill(7, 0x00));
    final Bytes diff = ArchiveTrieNodeCodec.encodeDiff(old, newNode);
    assertThat(ArchiveTrieNodeCodec.decode(diff).isFull()).isFalse(); // must be a genuine DIFF
    assertThat(
            ArchiveTrieNodeCodec.reconstruct(ArchiveTrieNodeCodec.encodeFull(old), List.of(diff)))
        .isEqualTo(newNode);
  }

  @Test
  void roundTripHashSizedBlockChange() {
    // Simulates a 32-byte hash ref changing inside a larger node (dominant trie change pattern).
    final Bytes prefix = fill(3, 0x01);
    final Bytes suffix = fill(5, 0x02);
    final Bytes oldHash = fill(32, 0x00);
    final Bytes newHash = fill(32, 0xAA);
    final Bytes old = Bytes.concatenate(prefix, oldHash, suffix);
    final Bytes newNode = Bytes.concatenate(prefix, newHash, suffix);

    final Bytes diff = ArchiveTrieNodeCodec.encodeDiff(old, newNode);
    assertThat(diff.size()).isLessThan(newNode.size() + 1); // patch beats FULL
    assertThat(
            ArchiveTrieNodeCodec.reconstruct(ArchiveTrieNodeCodec.encodeFull(old), List.of(diff)))
        .isEqualTo(newNode);
  }

  @Test
  void roundTripPrefixOnlyChange() {
    // First byte changes; suffix is long and unchanged.
    final Bytes old = Bytes.concatenate(node(0x01), fill(50, 0xBB));
    final Bytes newNode = Bytes.concatenate(node(0x02), fill(50, 0xBB));
    final Bytes diff = ArchiveTrieNodeCodec.encodeDiff(old, newNode);
    assertThat(
            ArchiveTrieNodeCodec.reconstruct(ArchiveTrieNodeCodec.encodeFull(old), List.of(diff)))
        .isEqualTo(newNode);
  }

  @Test
  void roundTripSuffixOnlyChange() {
    // Last byte changes; prefix is long and unchanged.
    final Bytes old = Bytes.concatenate(fill(50, 0xBB), node(0x01));
    final Bytes newNode = Bytes.concatenate(fill(50, 0xBB), node(0x02));
    final Bytes diff = ArchiveTrieNodeCodec.encodeDiff(old, newNode);
    assertThat(
            ArchiveTrieNodeCodec.reconstruct(ArchiveTrieNodeCodec.encodeFull(old), List.of(diff)))
        .isEqualTo(newNode);
  }

  @Test
  void roundTripSizeIncreaseChange() {
    // new node is longer than old (e.g. an extension node's path grows).
    // On such a tiny node any patch exceeds the 4-byte node, so encodeDiff falls back to FULL.
    final Bytes old = node(0xAA, 0xBB);
    final Bytes newNode = node(0xAA, 0xCC, 0xDD, 0xEE);
    final Bytes diff = ArchiveTrieNodeCodec.encodeDiff(old, newNode);
    final ArchiveTrieNodeEntry entry = ArchiveTrieNodeCodec.decode(diff);
    assertThat(entry.isFull()).isTrue();
    assertThat(entry.fullNode()).isEqualTo(newNode);
  }

  @Test
  void roundTripSizeDecreaseChange() {
    // new node is shorter than old.
    // On such a tiny node any patch exceeds the 2-byte node, so encodeDiff falls back to FULL.
    final Bytes old = node(0xAA, 0xBB, 0xCC, 0xDD);
    final Bytes newNode = node(0xAA, 0xEE);
    final Bytes diff = ArchiveTrieNodeCodec.encodeDiff(old, newNode);
    final ArchiveTrieNodeEntry entry = ArchiveTrieNodeCodec.decode(diff);
    assertThat(entry.isFull()).isTrue();
    assertThat(entry.fullNode()).isEqualTo(newNode);
  }

  @Test
  void reconstructAppliesMultipleDiffsInAscendingOrder() {
    // Use 10-byte nodes so all 1-byte-change patches (7 bytes) are smaller than the node.
    final Bytes v1 = Bytes.concatenate(node(0xAA, 0xBB, 0xCC), fill(7, 0x00));
    final Bytes v2 = Bytes.concatenate(node(0xAA, 0xFF, 0xCC), fill(7, 0x00));
    final Bytes v3 = Bytes.concatenate(node(0xDD, 0xFF, 0xCC), fill(7, 0x00));
    final Bytes diff1 = ArchiveTrieNodeCodec.encodeDiff(v1, v2);
    final Bytes diff2 = ArchiveTrieNodeCodec.encodeDiff(v2, v3);
    assertThat(
            ArchiveTrieNodeCodec.reconstruct(
                ArchiveTrieNodeCodec.encodeFull(v1), List.of(diff1, diff2)))
        .isEqualTo(v3);
  }

  @Test
  void reconstructChainOfHashBlockChanges() {
    // Three successive mutations each changing the same 32-byte field.
    final Bytes prefix = fill(4, 0x01);
    final Bytes suffix = fill(4, 0x02);
    final Bytes v1 = Bytes.concatenate(prefix, fill(32, 0x00), suffix);
    final Bytes v2 = Bytes.concatenate(prefix, fill(32, 0x11), suffix);
    final Bytes v3 = Bytes.concatenate(prefix, fill(32, 0x22), suffix);
    final Bytes diff1 = ArchiveTrieNodeCodec.encodeDiff(v1, v2);
    final Bytes diff2 = ArchiveTrieNodeCodec.encodeDiff(v2, v3);
    assertThat(
            ArchiveTrieNodeCodec.reconstruct(
                ArchiveTrieNodeCodec.encodeFull(v1), List.of(diff1, diff2)))
        .isEqualTo(v3);
  }

  // ---------------------------------------------------------------------------
  // reconstruct — input validation
  // ---------------------------------------------------------------------------

  @Test
  void reconstructRejectsNonFullBaseEntry() {
    // Use a large node so the single-byte diff produces a pure DIFF entry (not a FULL
    // fallback), ensuring the first arg is non-full and reconstruct correctly rejects it.
    final Bytes old = fill(40, 0x01);
    final byte[] arr = old.toArray();
    arr[20] = (byte) 0x02;
    final Bytes diff = ArchiveTrieNodeCodec.encodeDiff(old, Bytes.wrap(arr));
    assertThat(ArchiveTrieNodeCodec.decode(diff).isFull()).isFalse(); // sanity: must be pure DIFF
    assertThatThrownBy(() -> ArchiveTrieNodeCodec.reconstruct(diff, List.of()))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void reconstructRejectsFullEntryInDiffList() {
    final Bytes n = node(0x01, 0x02, 0x03);
    final Bytes full1 = ArchiveTrieNodeCodec.encodeFull(n);
    final Bytes full2 = ArchiveTrieNodeCodec.encodeFull(node(0x04, 0x05, 0x06));
    assertThatThrownBy(() -> ArchiveTrieNodeCodec.reconstruct(full1, List.of(full2)))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void reconstructRejectsDeletionTombstoneInDiffList() {
    final Bytes n = node(0x01, 0x02, 0x03);
    final Bytes full = ArchiveTrieNodeCodec.encodeFull(n);
    final Bytes tombstone = ArchiveTrieNodeCodec.encodeDiff(n, null);
    assertThatThrownBy(() -> ArchiveTrieNodeCodec.reconstruct(full, List.of(tombstone)))
        .isInstanceOf(IllegalArgumentException.class);
  }

  // ---------------------------------------------------------------------------
  // ArchiveTrieNodeEntry.patchBody()
  // ---------------------------------------------------------------------------

  @Test
  void patchBodyThrowsOnFullEntry() {
    final ArchiveTrieNodeEntry e =
        ArchiveTrieNodeCodec.decode(ArchiveTrieNodeCodec.encodeFull(node(0x01)));
    assertThatThrownBy(e::patchBody).isInstanceOf(IllegalStateException.class);
  }

  @Test
  void patchBodyThrowsOnDeletionEntry() {
    final ArchiveTrieNodeEntry e =
        ArchiveTrieNodeCodec.decode(ArchiveTrieNodeCodec.encodeDiff(node(0x01), null));
    assertThatThrownBy(e::patchBody).isInstanceOf(IllegalStateException.class);
  }

  @Test
  void patchBodyReturnsNonNullForDiffEntry() {
    final Bytes old = fill(40, 0x01);
    final byte[] arr = old.toArray();
    arr[20] = (byte) 0xFF;
    final Bytes mutated = Bytes.wrap(arr);
    final ArchiveTrieNodeEntry e =
        ArchiveTrieNodeCodec.decode(ArchiveTrieNodeCodec.encodeDiff(old, mutated));
    assertThat(e.isFull()).isFalse();
    assertThat(e.patchBody()).isNotNull();
  }

  @Test
  void encodeDiffProducesKnownWireBytes() {
    // old: 40 bytes of 0x01; new: same with byte 20 = 0xFF
    // prefix=20, REPLACE(1, 0xFF), suffix=19 implicit
    // COPY(20):    type=0, len=20  → [0x00, 0x14]
    // REPLACE(1):  type=3, len=1   → [0xC0, 0x01], then data [0xFF]
    final Bytes oldNode = fill(40, 0x01);
    final byte[] newArr = oldNode.toArray();
    newArr[20] = (byte) 0xFF;
    final Bytes newNode = Bytes.wrap(newArr);
    final ArchiveTrieNodeEntry diff =
        ArchiveTrieNodeCodec.decode(ArchiveTrieNodeCodec.encodeDiff(oldNode, newNode));
    assertThat(diff.isFull()).isFalse();
    assertThat(diff.patchBody().toArray())
        .isEqualTo(new byte[] {0x00, 0x14, (byte) 0xC0, 0x01, (byte) 0xFF});
  }

  @Test
  void encodeDiffMultipleNonAdjacentChangesProducesMultipleRuns() {
    // 20-byte arrays: bytes 2 and 14 differ; 11-byte gap between them satisfies MATCH_THRESHOLD
    final byte[] oldBytes = new byte[20];
    final byte[] newBytes = new byte[20];
    oldBytes[2] = 0x01;
    newBytes[2] = 0x02;
    oldBytes[14] = 0x03;
    newBytes[14] = 0x04;
    final Bytes oldNode = Bytes.wrap(oldBytes);
    final Bytes newNode = Bytes.wrap(newBytes);

    final Bytes diffEntry = ArchiveTrieNodeCodec.encodeDiff(oldNode, newNode);
    final ArchiveTrieNodeEntry diff = ArchiveTrieNodeCodec.decode(diffEntry);
    assertThat(diff.isFull()).isFalse();

    // Verify round-trip
    assertThat(
            ArchiveTrieNodeCodec.reconstruct(
                ArchiveTrieNodeCodec.encodeFull(oldNode), List.of(diffEntry)))
        .isEqualTo(newNode);

    // Verify two separate REPLACE ops (one per changed region, not one spanning both)
    final byte[] body = diff.patchBody().toArray();
    int replaceCount = 0;
    int pos = 0;
    while (pos < body.length) {
      final int first = body[pos++] & 0xFF;
      final int second = body[pos++] & 0xFF;
      final int type = first >> 6;
      final int length = ((first & 0x3F) << 8) | second;
      if (type == 3) { // OP_REPLACE
        replaceCount++;
        pos += length;
      } else if (type == 2) { // OP_INSERT
        pos += length;
      }
    }
    assertThat(replaceCount).isEqualTo(2);
  }
}
