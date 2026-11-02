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
package org.hyperledger.besu.ethereum.trie.patricia;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.ethereum.trie.MissingNode;
import org.hyperledger.besu.ethereum.trie.Node;
import org.hyperledger.besu.ethereum.trie.NullNode;
import org.hyperledger.besu.ethereum.trie.StoredNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;

class BranchNodeTest {

  private final DefaultNodeFactory<Bytes> nodeFactory =
      new DefaultNodeFactory<>(Function.identity());

  @Test
  void encodesEveryKindOfChildAsItsReference() {
    final List<Node<Bytes>> children = new ArrayList<>();
    for (int i = 0; i < 16; i++) {
      children.add(NullNode.instance());
    }
    // referenced by hash: a stored node, a leaf too large to inline, a branch
    children.set(0, new StoredNode<>(nodeFactory, Bytes.of(0), Bytes32.random()));
    children.set(1, nodeFactory.createLeaf(Bytes.of(1, 2, 16), Bytes.random(40)));
    children.set(2, branchWithLeaves());
    // inlined: a leaf small enough to inline and an extension over it
    children.set(3, nodeFactory.createLeaf(Bytes.of(3, 16), Bytes.of(7)));
    children.set(
        4,
        nodeFactory.createExtension(
            Bytes.of(4), nodeFactory.createLeaf(Bytes.of(16), Bytes.of(1))));
    // a missing node references itself as an empty node
    children.set(5, new MissingNode<>(Bytes32.random(), Bytes.of(5)));

    for (final Optional<Bytes> value :
        List.of(Optional.<Bytes>empty(), Optional.of(Bytes.of(5)), Optional.of(Bytes.random(70)))) {
      final BranchNode<Bytes> branch =
          (BranchNode<Bytes>) nodeFactory.createBranch(new ArrayList<>(children), value);

      assertThat(branch.getEncodedBytes()).isEqualTo(encodedPartByPart(branch));
    }
  }

  private Node<Bytes> branchWithLeaves() {
    final List<Node<Bytes>> children = new ArrayList<>();
    for (int i = 0; i < 16; i++) {
      children.add(
          i % 3 == 0
              ? nodeFactory.createLeaf(Bytes.of(i, 16), Bytes.random(33))
              : NullNode.instance());
    }
    return nodeFactory.createBranch(children, Optional.empty());
  }

  /** The branch's list, written from each child's own reference. */
  private static Bytes encodedPartByPart(final BranchNode<Bytes> branch) {
    final BytesValueRLPOutput out = new BytesValueRLPOutput();
    out.startList();
    for (final Node<Bytes> child : branch.getChildren()) {
      out.writeRaw(child.getEncodedBytesRef());
    }
    if (branch.getValue().isPresent()) {
      out.writeBytes(branch.getValue().get());
    } else {
      out.writeNull();
    }
    out.endList();
    return out.encoded();
  }
}
