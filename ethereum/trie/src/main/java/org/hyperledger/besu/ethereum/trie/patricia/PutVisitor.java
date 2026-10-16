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
package org.hyperledger.besu.ethereum.trie.patricia;

import static org.hyperledger.besu.ethereum.trie.Nibbles.commonPrefixLength;
import static org.hyperledger.besu.ethereum.trie.Nibbles.slice;

import org.hyperledger.besu.ethereum.trie.CompactEncoding;
import org.hyperledger.besu.ethereum.trie.Node;
import org.hyperledger.besu.ethereum.trie.NodeFactory;
import org.hyperledger.besu.ethereum.trie.NullNode;
import org.hyperledger.besu.ethereum.trie.PathNodeVisitor;

public class PutVisitor<V> implements PathNodeVisitor<V> {
  private final NodeFactory<V> nodeFactory;
  private final V value;

  public PutVisitor(final NodeFactory<V> nodeFactory, final V value) {
    this.nodeFactory = nodeFactory;
    this.value = value;
  }

  @Override
  public Node<V> visit(final ExtensionNode<V> extensionNode, final byte[] path, final int offset) {
    final byte[] extensionPath = extensionNode.path();
    final int commonPathLength = commonPrefixLength(extensionPath, path, offset);
    assert commonPathLength < path.length - offset
        : "Visiting path doesn't end with a non-matching terminator";

    if (commonPathLength == extensionPath.length) {
      final Node<V> newChild =
          extensionNode.getChild().accept(this, path, offset + commonPathLength);
      return extensionNode.replaceChild(newChild);
    }

    // path diverges before the end of the extension - create a new branch

    final byte leafIndex = path[offset + commonPathLength];
    final byte[] leafPath = slice(path, offset + commonPathLength + 1);

    final byte extensionIndex = extensionPath[commonPathLength];
    final Node<V> updatedExtension =
        extensionNode.replacePath(slice(extensionPath, commonPathLength + 1));
    final Node<V> leaf = nodeFactory.createLeaf(leafPath, value);
    final Node<V> branch =
        nodeFactory.createBranch(leafIndex, leaf, extensionIndex, updatedExtension);

    if (commonPathLength > 0) {
      return nodeFactory.createExtension(slice(extensionPath, 0, commonPathLength), branch);
    } else {
      return branch;
    }
  }

  @Override
  public Node<V> visit(final BranchNode<V> branchNode, final byte[] path, final int offset) {
    assert path.length > offset : "Visiting path doesn't end with a non-matching terminator";

    final byte childIndex = path[offset];
    if (childIndex == CompactEncoding.LEAF_TERMINATOR) {
      return branchNode.replaceValue(value);
    }

    final Node<V> updatedChild = branchNode.child(childIndex).accept(this, path, offset + 1);
    return branchNode.replaceChild(childIndex, updatedChild);
  }

  @Override
  public Node<V> visit(final LeafNode<V> leafNode, final byte[] path, final int offset) {
    final byte[] leafPath = leafNode.path();
    final int commonPathLength = commonPrefixLength(leafPath, path, offset);
    final int remainingPathLength = path.length - offset;

    // Check if the current leaf node should be replaced
    if (commonPathLength == leafPath.length && commonPathLength == remainingPathLength) {
      return nodeFactory.createLeaf(leafPath, value);
    }

    assert commonPathLength < leafPath.length && commonPathLength < remainingPathLength
        : "Should not have consumed non-matching terminator";

    // The current leaf path must be split to accommodate the new value.

    final byte newLeafIndex = path[offset + commonPathLength];
    final byte[] newLeafPath = slice(path, offset + commonPathLength + 1);

    final byte updatedLeafIndex = leafPath[commonPathLength];

    final Node<V> updatedLeaf = leafNode.replacePath(slice(leafPath, commonPathLength + 1));
    final Node<V> leaf = nodeFactory.createLeaf(newLeafPath, value);
    final Node<V> branch =
        nodeFactory.createBranch(updatedLeafIndex, updatedLeaf, newLeafIndex, leaf);
    if (commonPathLength > 0) {
      return nodeFactory.createExtension(slice(leafPath, 0, commonPathLength), branch);
    } else {
      return branch;
    }
  }

  @Override
  public Node<V> visit(final NullNode<V> nullNode, final byte[] path, final int offset) {
    return nodeFactory.createLeaf(slice(path, offset), value);
  }
}
