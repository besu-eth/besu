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

import org.hyperledger.besu.ethereum.trie.CompactEncoding;
import org.hyperledger.besu.ethereum.trie.Node;
import org.hyperledger.besu.ethereum.trie.NodeFactory;
import org.hyperledger.besu.ethereum.trie.NullNode;
import org.hyperledger.besu.ethereum.trie.PathNodeVisitor;

import org.apache.tuweni.bytes.Bytes;

/**
 * Path visitor that inserts or replaces a leaf value.
 *
 * <p>Subclasses may override the protected hooks to specialize behavior while reusing split/insert
 * logic — for example collapsing completed left siblings during ascending-key bulk inserts.
 */
public class PutVisitor<V> implements PathNodeVisitor<V> {
  protected final NodeFactory<V> nodeFactory;
  private final V value;

  public PutVisitor(final NodeFactory<V> nodeFactory, final V value) {
    this.nodeFactory = nodeFactory;
    this.value = value;
  }

  @Override
  public Node<V> visit(final ExtensionNode<V> extensionNode, final Bytes path) {
    final Bytes extensionPath = extensionNode.getPath();
    final int commonPathLength = extensionPath.commonPrefixLength(path);
    assert commonPathLength < path.size()
        : "Visiting path doesn't end with a non-matching terminator";

    if (commonPathLength == extensionPath.size()) {
      final Node<V> newChild = extensionNode.getChild().accept(this, path.slice(commonPathLength));
      return extensionNode.replaceChild(newChild);
    }

    // path diverges before the end of the extension - create a new branch

    final byte leafIndex = path.get(commonPathLength);
    final Bytes leafPath = path.slice(commonPathLength + 1);

    final byte extensionIndex = extensionPath.get(commonPathLength);
    validateSplitDirection(leafIndex, extensionIndex);
    final Node<V> leaf = newNode(leafPath);
    final Node<V> updatedExtension =
        mapAttachedSibling(extensionNode.replacePath(extensionPath.slice(commonPathLength + 1)));
    final Node<V> branch =
        nodeFactory.createBranch(leafIndex, leaf, extensionIndex, updatedExtension);

    if (commonPathLength > 0) {
      return nodeFactory.createExtension(extensionPath.slice(0, commonPathLength), branch);
    } else {
      return branch;
    }
  }

  @Override
  public Node<V> visit(final BranchNode<V> branchNode, final Bytes path) {
    assert path.size() > 0 : "Visiting path doesn't end with a non-matching terminator";

    final byte childIndex = path.get(0);
    if (childIndex == CompactEncoding.LEAF_TERMINATOR) {
      return branchNode.replaceValue(value);
    }

    final BranchNode<V> prepared = beforeDescendChild(branchNode, childIndex);
    final Node<V> updatedChild = prepared.child(childIndex).accept(this, path.slice(1));
    return prepared.replaceChild(childIndex, updatedChild);
  }

  @Override
  public Node<V> visit(final LeafNode<V> leafNode, final Bytes path) {
    final Bytes leafPath = leafNode.getPath();
    final int commonPathLength = leafPath.commonPrefixLength(path);

    // Check if the current leaf node should be replaced
    if (commonPathLength == leafPath.size() && commonPathLength == path.size()) {
      return nodeFactory.createLeaf(leafPath, value);
    }

    assert commonPathLength < leafPath.size() && commonPathLength < path.size()
        : "Should not have consumed non-matching terminator";

    // The current leaf path must be split to accommodate the new value.

    final byte newLeafIndex = path.get(commonPathLength);
    final Bytes newLeafPath = path.slice(commonPathLength + 1);

    final byte updatedLeafIndex = leafPath.get(commonPathLength);

    validateSplitDirection(newLeafIndex, updatedLeafIndex);
    final Node<V> leaf = newNode(newLeafPath);
    final Node<V> updatedLeaf =
        mapAttachedSibling(leafNode.replacePath(leafPath.slice(commonPathLength + 1)));
    final Node<V> branch =
        nodeFactory.createBranch(updatedLeafIndex, updatedLeaf, newLeafIndex, leaf);
    if (commonPathLength > 0) {
      return nodeFactory.createExtension(leafPath.slice(0, commonPathLength), branch);
    } else {
      return branch;
    }
  }

  @Override
  public Node<V> visit(final NullNode<V> nullNode, final Bytes path) {
    return newNode(path);
  }

  /**
   * Node placed for an absent key with {@code path} left below it (the root, an empty child, or the
   * new side of a split).
   *
   * <p>Stock put creates the leaf. Subclasses may place a prebuilt subtree whose first key is being
   * inserted.
   *
   * @param path nibbles of the key below the node's position, terminator included
   * @return node to attach
   */
  protected Node<V> newNode(final Bytes path) {
    return nodeFactory.createLeaf(path, value);
  }

  /**
   * Invoked immediately before descending into a branch's child.
   *
   * <p>Stock put returns {@code branchNode} unchanged. Subclasses may return a branch with reshaped
   * siblings (for example collapsing completed left-nibble subtrees) before the recursive visit.
   *
   * @param branchNode branch whose child will be updated next
   * @param childIndex nibble selector for the child being descended into ({@code 0}-{@code 15})
   * @return branch to descend from (may be the same instance or a replacement)
   */
  protected BranchNode<V> beforeDescendChild(
      final BranchNode<V> branchNode, final byte childIndex) {
    return branchNode;
  }

  /**
   * Maps a sibling node being attached beside a newly inserted leaf after a split.
   *
   * <p>Stock put returns the node unchanged (identity). Subclasses may transform the attached
   * sibling — for example replacing a completed left subtree with a hash stub.
   *
   * @param sibling existing subtree attached as the opposite child of the new leaf
   * @return node to store in that sibling slot
   */
  protected Node<V> mapAttachedSibling(final Node<V> sibling) {
    return sibling;
  }

  /**
   * Validates (or gates) the direction of a split/insert relative to an existing key or subtree.
   *
   * <p>Stock put allows any insert order. Subclasses may reject directions that violate their
   * ordering constraints (for example requiring the new leaf nibble to be strictly greater than the
   * existing sibling nibble).
   *
   * @param newLeafIndex nibble that places the new leaf ({@code 0}-{@code 15})
   * @param siblingIndex nibble of the existing remnant being attached beside it
   */
  protected void validateSplitDirection(final byte newLeafIndex, final byte siblingIndex) {
    // no-op
  }
}
