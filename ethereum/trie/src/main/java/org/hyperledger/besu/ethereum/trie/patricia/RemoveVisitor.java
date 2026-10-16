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

import org.hyperledger.besu.ethereum.trie.CompactEncoding;
import org.hyperledger.besu.ethereum.trie.Node;
import org.hyperledger.besu.ethereum.trie.NullNode;
import org.hyperledger.besu.ethereum.trie.PathNodeVisitor;

public class RemoveVisitor<V> implements PathNodeVisitor<V> {
  private final Node<V> NULL_NODE_RESULT = NullNode.instance();

  private final boolean allowFlatten;

  public RemoveVisitor() {
    allowFlatten = true;
  }

  public RemoveVisitor(final boolean allowFlatten) {
    this.allowFlatten = allowFlatten;
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

    // path diverges before the end of the extension, so it cannot match

    return extensionNode;
  }

  @Override
  public Node<V> visit(final BranchNode<V> branchNode, final byte[] path, final int offset) {
    assert path.length > offset : "Visiting path doesn't end with a non-matching terminator";

    final byte childIndex = path[offset];
    if (childIndex == CompactEncoding.LEAF_TERMINATOR) {
      return branchNode.removeValue();
    }

    final Node<V> updatedChild = branchNode.child(childIndex).accept(this, path, offset + 1);
    return branchNode.replaceChild(childIndex, updatedChild, allowFlatten);
  }

  @Override
  public Node<V> visit(final LeafNode<V> leafNode, final byte[] path, final int offset) {
    final byte[] leafPath = leafNode.path();
    final int commonPathLength = commonPrefixLength(leafPath, path, offset);
    return (commonPathLength == leafPath.length) ? NULL_NODE_RESULT : leafNode;
  }

  @Override
  public Node<V> visit(final NullNode<V> nullNode, final byte[] path, final int offset) {
    return NULL_NODE_RESULT;
  }
}
