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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.hyperledger.besu.ethereum.rlp.RLP;

import java.util.List;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;

class ArchiveTrieNodeCodecTest {

  private static Bytes shortNode(final Bytes path, final Bytes value) {
    return RLP.encode(
        out -> {
          out.startList();
          out.writeBytes(path);
          out.writeBytes(value);
          out.endList();
        });
  }

  private static Bytes branchNode(final Bytes[] children, final Bytes value) {
    return RLP.encode(
        out -> {
          out.startList();
          for (final Bytes child : children) {
            if (child.isEmpty()) {
              out.writeNull();
            } else {
              out.writeRaw(child);
            }
          }
          out.writeBytes(value);
          out.endList();
        });
  }

  private static Bytes[] emptyBranchChildren() {
    final Bytes[] children = new Bytes[16];
    java.util.Arrays.fill(children, Bytes.EMPTY);
    return children;
  }

  @Test
  void encodeFullProducesEntryFullBitWithNodeBytesAppended() {
    final Bytes node = shortNode(Bytes.fromHexString("0x0102"), Bytes.fromHexString("0x03"));
    final Bytes entry = ArchiveTrieNodeCodec.encodeFull(node);
    final ArchiveTrieNodeEntry decoded = ArchiveTrieNodeCodec.decode(entry);
    assertThat(decoded.isFull()).isTrue();
    assertThat(decoded.fullNode()).isEqualTo(node);
  }

  @Test
  void encodeDiffWithNullOldNodeIsCreationFull() {
    final Bytes node = shortNode(Bytes.fromHexString("0x01"), Bytes.fromHexString("0x02"));
    final ArchiveTrieNodeEntry decoded =
        ArchiveTrieNodeCodec.decode(ArchiveTrieNodeCodec.encodeDiff(null, node));
    assertThat(decoded.isFull()).isTrue();
    assertThat(decoded.fullNode()).isEqualTo(node);
  }

  @Test
  void encodeDiffWithNullNewNodeIsDeletionTombstoneWithNoBody() {
    final Bytes node = shortNode(Bytes.fromHexString("0x01"), Bytes.fromHexString("0x02"));
    final Bytes entry = ArchiveTrieNodeCodec.encodeDiff(node, null);
    assertThat(entry.size()).isEqualTo(1); // metadata byte only
    final ArchiveTrieNodeEntry decoded = ArchiveTrieNodeCodec.decode(entry);
    assertThat(decoded.isDeletion()).isTrue();
    assertThat(decoded.isFull()).isFalse();
  }

  @Test
  void encodeDiffWithBothNullThrows() {
    assertThatThrownBy(() -> ArchiveTrieNodeCodec.encodeDiff(null, null))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void encodeDiffAcrossNodeTypeChangeIsPlainFullOfNewNode() {
    final Bytes[] children = emptyBranchChildren();
    final Bytes branch = branchNode(children, Bytes.EMPTY);
    final Bytes shortN = shortNode(Bytes.fromHexString("0x01"), Bytes.fromHexString("0x02"));

    final ArchiveTrieNodeEntry branchToShort =
        ArchiveTrieNodeCodec.decode(ArchiveTrieNodeCodec.encodeDiff(branch, shortN));
    assertThat(branchToShort.isFull()).isTrue();
    assertThat(branchToShort.fullNode()).isEqualTo(shortN);

    final ArchiveTrieNodeEntry shortToBranch =
        ArchiveTrieNodeCodec.decode(ArchiveTrieNodeCodec.encodeDiff(shortN, branch));
    assertThat(shortToBranch.isFull()).isTrue();
    assertThat(shortToBranch.fullNode()).isEqualTo(branch);
  }

  @Test
  void branchDiffCapturesSingleChangedChildSlot() {
    final Bytes[] oldChildren = emptyBranchChildren();
    oldChildren[3] = Bytes.fromHexString("0xa0" + "11".repeat(32)); // 33-byte hash ref
    final Bytes[] newChildren = oldChildren.clone();
    newChildren[3] = Bytes.fromHexString("0xa0" + "22".repeat(32));
    final Bytes oldNode = branchNode(oldChildren, Bytes.EMPTY);
    final Bytes newNode = branchNode(newChildren, Bytes.EMPTY);

    final ArchiveTrieNodeEntry diff =
        ArchiveTrieNodeCodec.decode(ArchiveTrieNodeCodec.encodeDiff(oldNode, newNode));
    assertThat(diff.isFull()).isFalse();
    assertThat(diff.isBranchNode()).isTrue();
    assertThat(diff.changedChildRefs().keySet()).containsExactly(3);
    assertThat(diff.changedChildRefs()).containsEntry(3, newChildren[3]);
    assertThat(diff.changedValue()).isEmpty();
  }

  @Test
  void branchDiffCapturesEmptyToHashRefAndHashRefToEmptyTransitions() {
    final Bytes[] oldChildren = emptyBranchChildren();
    final Bytes[] newChildren = emptyBranchChildren();
    newChildren[0] = Bytes.fromHexString("0xa0" + "33".repeat(32)); // empty -> hash ref
    oldChildren[5] = Bytes.fromHexString("0xa0" + "44".repeat(32));
    // newChildren[5] left empty -> hash ref -> empty
    final Bytes oldNode = branchNode(oldChildren, Bytes.EMPTY);
    final Bytes newNode = branchNode(newChildren, Bytes.EMPTY);

    final ArchiveTrieNodeEntry diff =
        ArchiveTrieNodeCodec.decode(ArchiveTrieNodeCodec.encodeDiff(oldNode, newNode));
    assertThat(diff.changedChildRefs().keySet()).containsExactly(0, 5);
    assertThat(diff.changedChildRefs().get(0)).isEqualTo(newChildren[0]);
    assertThat(diff.changedChildRefs().get(5)).isEqualTo(Bytes.fromHexString("0x80")); // RLP null
  }

  @Test
  void branchDiffCapturesTerminalValueChange() {
    final Bytes[] children = emptyBranchChildren();
    final Bytes oldNode = branchNode(children, Bytes.fromHexString("0xaa"));
    final Bytes newNode = branchNode(children, Bytes.fromHexString("0xbb"));

    final ArchiveTrieNodeEntry diff =
        ArchiveTrieNodeCodec.decode(ArchiveTrieNodeCodec.encodeDiff(oldNode, newNode));
    assertThat(diff.changedChildRefs()).isEmpty();
    assertThat(diff.changedValue()).contains(Bytes.fromHexString("0xbb"));
  }

  @Test
  void branchChildRefLargerThan255BytesRoundTripsSinceItIsNoLongerLengthPrefixed() {
    // Child refs are self-delimiting raw RLP now (Optimization #2): unlike the old external
    // 1-byte length prefix, there is no longer a 255-byte cap on a changed child ref's size.
    final Bytes[] oldChildren = emptyBranchChildren();
    final Bytes[] newChildren = emptyBranchChildren();
    newChildren[0] = RLP.encode(out -> out.writeBytes(Bytes.wrap(new byte[300])));
    final Bytes oldNode = branchNode(oldChildren, Bytes.EMPTY);
    final Bytes newNode = branchNode(newChildren, Bytes.EMPTY);

    final ArchiveTrieNodeEntry diff =
        ArchiveTrieNodeCodec.decode(ArchiveTrieNodeCodec.encodeDiff(oldNode, newNode));
    assertThat(diff.changedChildRefs().keySet()).containsExactly(0);
    assertThat(diff.changedChildRefs().get(0)).isEqualTo(newChildren[0]);
  }

  @Test
  void branchChildRefAt255BytesRoundTrips() {
    // Retained as a regression check at what used to be the length-prefix boundary — no longer a
    // meaningful limit, but still a real size worth covering.
    final Bytes[] oldChildren = emptyBranchChildren();
    final Bytes[] newChildren = emptyBranchChildren();
    newChildren[0] = RLP.encode(out -> out.writeBytes(Bytes.wrap(new byte[253])));
    final Bytes oldNode = branchNode(oldChildren, Bytes.EMPTY);
    final Bytes newNode = branchNode(newChildren, Bytes.EMPTY);
    final ArchiveTrieNodeEntry diff =
        ArchiveTrieNodeCodec.decode(ArchiveTrieNodeCodec.encodeDiff(oldNode, newNode));
    assertThat(diff.isFull()).isFalse();
    assertThat(diff.isBranchNode()).isTrue();
    assertThat(diff.changedChildRefs().keySet()).containsExactly(0);
    assertThat(diff.changedChildRefs().get(0).size()).isEqualTo(255);
  }

  @Test
  void shortNodePathAt65535BytesIsAllowedAnd65536Throws() {
    // The path field is a decoded byte payload (readBytes(), not self-delimiting RLP), so it
    // still carries an explicit 2-byte length prefix and its 65535-byte limit is unchanged.
    final Bytes oldNode = shortNode(Bytes.fromHexString("0x01"), Bytes.EMPTY);
    final Bytes at65535 = shortNode(Bytes.wrap(new byte[65535]), Bytes.EMPTY);
    final ArchiveTrieNodeEntry diff =
        ArchiveTrieNodeCodec.decode(ArchiveTrieNodeCodec.encodeDiff(oldNode, at65535));
    assertThat(diff.changedKey()).isPresent();

    final Bytes at65536 = shortNode(Bytes.wrap(new byte[65536]), Bytes.EMPTY);
    assertThatThrownBy(() -> ArchiveTrieNodeCodec.encodeDiff(oldNode, at65536))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void shortNodeValueLargerThan65535BytesRoundTripsSinceItIsNoLongerLengthPrefixed() {
    // The value field is self-delimiting raw RLP now (Optimization #2): unlike the old external
    // 2-byte length prefix, there is no longer a 65535-byte cap on a changed value's size.
    final Bytes oldNode = shortNode(Bytes.fromHexString("0x01"), Bytes.EMPTY);
    final Bytes largeValue = shortNode(Bytes.fromHexString("0x01"), Bytes.wrap(new byte[70000]));
    final ArchiveTrieNodeEntry diff =
        ArchiveTrieNodeCodec.decode(ArchiveTrieNodeCodec.encodeDiff(oldNode, largeValue));
    assertThat(diff.isFull()).isFalse();
    assertThat(diff.isBranchNode()).isFalse();
    assertThat(diff.isDeletion()).isFalse();
    assertThat(diff.changedShortNodeValue()).isPresent();
  }

  @Test
  void shortNodeDiffCapturesKeyOnlyChange() {
    final Bytes oldNode = shortNode(Bytes.fromHexString("0x0102"), Bytes.fromHexString("0xaa"));
    final Bytes newNode = shortNode(Bytes.fromHexString("0x0103"), Bytes.fromHexString("0xaa"));
    final ArchiveTrieNodeEntry diff =
        ArchiveTrieNodeCodec.decode(ArchiveTrieNodeCodec.encodeDiff(oldNode, newNode));
    assertThat(diff.isFull()).isFalse();
    assertThat(diff.isBranchNode()).isFalse();
    assertThat(diff.isDeletion()).isFalse();
    assertThat(diff.changedKey()).contains(Bytes.fromHexString("0x0103"));
    assertThat(diff.changedShortNodeValue()).isEmpty();
  }

  @Test
  void shortNodeDiffCapturesValueOnlyChange() {
    final Bytes oldNode = shortNode(Bytes.fromHexString("0x0102"), Bytes.fromHexString("0xaa"));
    final Bytes newNode = shortNode(Bytes.fromHexString("0x0102"), Bytes.fromHexString("0xbb"));
    final ArchiveTrieNodeEntry diff =
        ArchiveTrieNodeCodec.decode(ArchiveTrieNodeCodec.encodeDiff(oldNode, newNode));
    assertThat(diff.changedKey()).isEmpty();
    assertThat(
            diff.changedShortNodeValue()
                .map(RLP::input)
                .map(org.hyperledger.besu.ethereum.rlp.RLPInput::readBytes))
        .contains(Bytes.fromHexString("0xbb"));
  }

  @Test
  void shortNodeDiffCapturesBothKeyAndValueChange() {
    final Bytes oldNode = shortNode(Bytes.fromHexString("0x0102"), Bytes.fromHexString("0xaa"));
    final Bytes newNode = shortNode(Bytes.fromHexString("0x0103"), Bytes.fromHexString("0xbb"));
    final ArchiveTrieNodeEntry diff =
        ArchiveTrieNodeCodec.decode(ArchiveTrieNodeCodec.encodeDiff(oldNode, newNode));
    assertThat(diff.changedKey()).contains(Bytes.fromHexString("0x0103"));
    assertThat(diff.changedShortNodeValue()).isPresent();
  }

  @Test
  void shortNodeDiffWithNeitherKeyNorValueChangedEncodesAndDecodesCleanly() {
    // Callers should avoid producing a true no-op diff, but the codec must still handle it.
    final Bytes node = shortNode(Bytes.fromHexString("0x0102"), Bytes.fromHexString("0xaa"));
    final ArchiveTrieNodeEntry diff =
        ArchiveTrieNodeCodec.decode(ArchiveTrieNodeCodec.encodeDiff(node, node));
    assertThat(diff.isFull()).isFalse();
    assertThat(diff.isBranchNode()).isFalse();
    assertThat(diff.isDeletion()).isFalse();
    assertThat(diff.changedKey()).isEmpty();
    assertThat(diff.changedShortNodeValue()).isEmpty();
  }

  @Test
  void reconstructWithNoDiffsReturnsFullNodeByteExact() {
    final Bytes node = shortNode(Bytes.fromHexString("0x0102"), Bytes.fromHexString("0xaa"));
    final Bytes result =
        ArchiveTrieNodeCodec.reconstruct(ArchiveTrieNodeCodec.encodeFull(node), List.of());
    assertThat(result).isEqualTo(node);
  }

  @Test
  void reconstructAppliesSingleBranchDiffOnTopOfFull() {
    final Bytes[] children = emptyBranchChildren();
    final Bytes base = branchNode(children, Bytes.EMPTY);
    final Bytes[] mutated = children.clone();
    mutated[7] = Bytes.fromHexString("0xa0" + "55".repeat(32));
    final Bytes next = branchNode(mutated, Bytes.EMPTY);
    final Bytes diffEntry = ArchiveTrieNodeCodec.encodeDiff(base, next);
    final Bytes reconstructed =
        ArchiveTrieNodeCodec.reconstruct(ArchiveTrieNodeCodec.encodeFull(base), List.of(diffEntry));
    assertThat(reconstructed).isEqualTo(next);
  }

  @Test
  void reconstructAppliesMultipleShortNodeDiffsInAscendingOrder() {
    final Bytes v1 = shortNode(Bytes.fromHexString("0x01"), Bytes.fromHexString("0xaa"));
    final Bytes v2 = shortNode(Bytes.fromHexString("0x01"), Bytes.fromHexString("0xbb"));
    final Bytes v3 = shortNode(Bytes.fromHexString("0x02"), Bytes.fromHexString("0xbb"));
    final Bytes diff1 = ArchiveTrieNodeCodec.encodeDiff(v1, v2);
    final Bytes diff2 = ArchiveTrieNodeCodec.encodeDiff(v2, v3);

    final Bytes reconstructed =
        ArchiveTrieNodeCodec.reconstruct(
            ArchiveTrieNodeCodec.encodeFull(v1), List.of(diff1, diff2));
    assertThat(reconstructed).isEqualTo(v3);
  }

  @Test
  void reconstructRejectsNonFullBaseEntry() {
    final Bytes node = shortNode(Bytes.fromHexString("0x01"), Bytes.fromHexString("0xaa"));
    final Bytes diffEntry = ArchiveTrieNodeCodec.encodeDiff(node, node);
    assertThatThrownBy(() -> ArchiveTrieNodeCodec.reconstruct(diffEntry, List.of()))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void reconstructRejectsFullOrTombstoneEntryInDiffList() {
    final Bytes node = shortNode(Bytes.fromHexString("0x01"), Bytes.fromHexString("0xaa"));
    final Bytes fullEntry = ArchiveTrieNodeCodec.encodeFull(node);
    assertThatThrownBy(
            () ->
                ArchiveTrieNodeCodec.reconstruct(
                    fullEntry, List.of(ArchiveTrieNodeCodec.encodeDiff(node, null))))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void reconstructRejectsNodeTypeMismatchBetweenBaseAndDiff() {
    final Bytes[] children = emptyBranchChildren();
    final Bytes branch = branchNode(children, Bytes.EMPTY);
    final Bytes shortN = shortNode(Bytes.fromHexString("0x01"), Bytes.fromHexString("0xaa"));
    // A short-node diff (no type-change bits set) fed against a branch base.
    final Bytes shortDiff = ArchiveTrieNodeCodec.encodeDiff(shortN, shortN);
    assertThatThrownBy(
            () ->
                ArchiveTrieNodeCodec.reconstruct(
                    ArchiveTrieNodeCodec.encodeFull(branch), List.of(shortDiff)))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
