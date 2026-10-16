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

import static org.hyperledger.besu.ethereum.trie.Nibbles.commonPrefixLength;
import static org.hyperledger.besu.ethereum.trie.Nibbles.slice;

import org.hyperledger.besu.ethereum.trie.CompactEncoding;
import org.hyperledger.besu.ethereum.trie.Node;
import org.hyperledger.besu.ethereum.trie.NodeFactory;
import org.hyperledger.besu.ethereum.trie.NullNode;
import org.hyperledger.besu.ethereum.trie.PathNodeVisitor;

import java.util.Optional;
import java.util.function.Function;

/**
 * A trie visitor that reads the existing value at the target path, applies a deferred function to
 * produce the new value, then updates or removes the leaf in a single descent.
 *
 * <p>Traversal through branch and extension nodes is identical to {@link PutVisitor}. At the
 * terminal position (exact leaf match or null), the visitor calls {@code
 * merger.apply(existingValue)} instead of using a pre-computed value:
 *
 * <ul>
 *   <li>If the merger returns present: the leaf is created or updated.
 *   <li>If the merger returns empty: the key is removed (leaf becomes {@link NullNode}).
 * </ul>
 *
 * <p>When the traversal path diverges from an existing leaf or extension (i.e. inserting a brand
 * new key), the merger is called with {@link Optional#empty()} to compute the value for the new
 * key. If the merger returns empty in that case, the trie is left unchanged.
 */
public class DeferredPutVisitor<V> implements PathNodeVisitor<V> {

  private final NodeFactory<V> nodeFactory;
  private final Function<Optional<V>, Optional<V>> merger;

  public DeferredPutVisitor(
      final NodeFactory<V> nodeFactory, final Function<Optional<V>, Optional<V>> merger) {
    this.nodeFactory = nodeFactory;
    this.merger = merger;
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

    // Path diverges before end of extension: this is a brand-new key insertion.
    final Optional<V> newValue = merger.apply(Optional.empty());
    if (newValue.isEmpty()) {
      return extensionNode;
    }

    final byte leafIndex = path[offset + commonPathLength];
    final byte[] leafPath = slice(path, offset + commonPathLength + 1);
    final byte extensionIndex = extensionPath[commonPathLength];
    final Node<V> updatedExtension =
        extensionNode.replacePath(slice(extensionPath, commonPathLength + 1));
    final Node<V> leaf = nodeFactory.createLeaf(leafPath, newValue.get());
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
      final Optional<V> merged = merger.apply(branchNode.getValue());
      if (merged.isPresent()) {
        return branchNode.replaceValue(merged.get());
      } else {
        return branchNode.removeValue();
      }
    }

    final Node<V> updatedChild = branchNode.child(childIndex).accept(this, path, offset + 1);
    return branchNode.replaceChild(childIndex, updatedChild);
  }

  @Override
  public Node<V> visit(final LeafNode<V> leafNode, final byte[] path, final int offset) {
    final byte[] leafPath = leafNode.path();
    final int commonPathLength = commonPrefixLength(leafPath, path, offset);
    final int remainingPathLength = path.length - offset;

    if (commonPathLength == leafPath.length && commonPathLength == remainingPathLength) {
      // Exact match: apply deferred function to existing leaf value.
      final Optional<V> merged = merger.apply(leafNode.getValue());
      if (merged.isPresent()) {
        return nodeFactory.createLeaf(leafPath, merged.get());
      } else {
        return NullNode.instance();
      }
    }

    assert commonPathLength < leafPath.length && commonPathLength < remainingPathLength
        : "Should not have consumed non-matching terminator";

    // Paths diverge: brand-new key insertion (no prior value).
    final Optional<V> newValue = merger.apply(Optional.empty());
    if (newValue.isEmpty()) {
      return leafNode;
    }

    final byte newLeafIndex = path[offset + commonPathLength];
    final byte[] newLeafPath = slice(path, offset + commonPathLength + 1);
    final byte updatedLeafIndex = leafPath[commonPathLength];
    final Node<V> updatedLeaf = leafNode.replacePath(slice(leafPath, commonPathLength + 1));
    final Node<V> leaf = nodeFactory.createLeaf(newLeafPath, newValue.get());
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
    final Optional<V> merged = merger.apply(Optional.empty());
    if (merged.isPresent()) {
      return nodeFactory.createLeaf(slice(path, offset), merged.get());
    } else {
      return NullNode.instance();
    }
  }
}
