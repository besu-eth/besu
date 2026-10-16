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
package org.hyperledger.besu.ethereum.trie;

import org.hyperledger.besu.ethereum.trie.patricia.BranchNode;
import org.hyperledger.besu.ethereum.trie.patricia.ExtensionNode;
import org.hyperledger.besu.ethereum.trie.patricia.GetVisitor;
import org.hyperledger.besu.ethereum.trie.patricia.LeafNode;

import java.util.ArrayList;
import java.util.List;

public class ProofVisitor<V> extends GetVisitor<V> implements PathNodeVisitor<V> {

  private final Node<V> rootNode;
  private final List<Node<V>> proof = new ArrayList<>();

  public ProofVisitor(final Node<V> rootNode) {
    this.rootNode = rootNode;
  }

  @Override
  public Node<V> visit(final ExtensionNode<V> extensionNode, final byte[] path, final int offset) {
    maybeTrackNode(extensionNode);
    return super.visit(extensionNode, path, offset);
  }

  @Override
  public Node<V> visit(final BranchNode<V> branchNode, final byte[] path, final int offset) {
    maybeTrackNode(branchNode);
    return super.visit(branchNode, path, offset);
  }

  @Override
  public Node<V> visit(final LeafNode<V> leafNode, final byte[] path, final int offset) {
    maybeTrackNode(leafNode);
    return super.visit(leafNode, path, offset);
  }

  @Override
  public Node<V> visit(final NullNode<V> nullNode, final byte[] path, final int offset) {
    return super.visit(nullNode, path, offset);
  }

  public List<Node<V>> getProof() {
    return proof;
  }

  private void maybeTrackNode(final Node<V> node) {
    if (node.equals(rootNode) || node.isReferencedByHash()) {
      proof.add(node);
    }
  }
}
