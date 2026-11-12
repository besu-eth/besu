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

import org.hyperledger.besu.ethereum.trie.Node;
import org.hyperledger.besu.ethereum.trie.NodeFactory;
import org.hyperledger.besu.ethereum.trie.NodeUpdater;
import org.hyperledger.besu.ethereum.trie.NullNode;
import org.hyperledger.besu.ethereum.trie.StoredNode;
import org.hyperledger.besu.ethereum.trie.TrieBytes;

import java.util.Optional;
import java.util.function.Function;

import org.apache.tuweni.bytes.Bytes;

/**
 * Ascending-key Patricia {@link PutVisitor} that collapses finished subtrees to {@link StoredNode}
 * hash stubs so live memory stays O(depth).
 *
 * <p>Keys MUST arrive in strictly ascending order. A key finishes the subtree of the previous key
 * where it leaves its path: at the branch where it takes an empty slot, or at the split it makes.
 * Only that subtree is collapsed, once its nodes are handed to the sink at their location.
 *
 * <p>Node-split / put rules come from {@link PutVisitor}; this subclass only overrides the
 * extension hooks ({@link #newNode}, {@link #beforeDescendChild}, {@link #mapAttachedSibling},
 * {@link #validateSplitDirection}). Small embedded nodes ({@code !isReferencedByHash()}) are left
 * as-is — they are already O(1) in encoded size.
 */
public final class AscendingCollapsePutVisitor extends PutVisitor<Bytes> {

  private final Optional<Function<Bytes, Node<Bytes>>> prebuilt;
  private final Bytes finished;
  private final NodeUpdater sink;

  /**
   * Inserts a leaf.
   *
   * @param finished the location of the subtree the key finishes, null for the first key
   * @param sink receives the nodes of that subtree
   */
  public AscendingCollapsePutVisitor(
      final NodeFactory<Bytes> nodeFactory,
      final Bytes value,
      final Bytes finished,
      final NodeUpdater sink) {
    this(nodeFactory, value, Optional.empty(), finished, sink);
  }

  /**
   * Places a prebuilt subtree instead of a leaf: {@code prebuilt} returns it for the path left
   * below its position, and the visitor navigates with the subtree's first key and its value.
   */
  public AscendingCollapsePutVisitor(
      final NodeFactory<Bytes> nodeFactory,
      final Bytes firstValue,
      final Optional<Function<Bytes, Node<Bytes>>> prebuilt,
      final Bytes finished,
      final NodeUpdater sink) {
    super(nodeFactory, firstValue);
    this.prebuilt = prebuilt;
    this.finished = finished;
    this.sink = sink;
  }

  @Override
  protected Node<Bytes> newNode(final Bytes path) {
    return prebuilt.map(subtree -> subtree.apply(path)).orElseGet(() -> super.newNode(path));
  }

  @Override
  protected BranchNode<Bytes> beforeDescendChild(
      final BranchNode<Bytes> branchNode, final byte childIndex) {
    // Ascending inserts: the key takes an empty slot only at the branch where it leaves the path of
    // the previous key, whose subtree, the nearest on the left, is then finished.
    if (finished == null || !(branchNode.child(childIndex) instanceof NullNode)) {
      return branchNode;
    }
    final byte finishedIndex = finished.get(finished.size() - 1);
    final Node<Bytes> child = branchNode.child(finishedIndex);
    final Node<Bytes> collapsed = collapse(nodeFactory, sink, child, finished);
    if (collapsed == child) {
      return branchNode;
    }
    @SuppressWarnings("unchecked")
    final BranchNode<Bytes> rebuilt =
        (BranchNode<Bytes>) branchNode.replaceChild(finishedIndex, collapsed);
    return rebuilt;
  }

  @Override
  protected Node<Bytes> mapAttachedSibling(final Node<Bytes> sibling) {
    // a split finishes the subtree of the previous key, attached beside the new key
    return collapse(nodeFactory, sink, sibling, finished);
  }

  @Override
  protected void validateSplitDirection(final byte newLeafIndex, final byte siblingIndex) {
    if (Byte.toUnsignedInt(newLeafIndex) < Byte.toUnsignedInt(siblingIndex)) {
      throw new IllegalArgumentException("keys must be inserted in strictly ascending order");
    }
  }

  /**
   * Hands a finished Patricia subtree to the sink and replaces it with a {@link StoredNode} hash
   * stub when the node is hash-referenced. Does not require a backing DB — {@link
   * StoredNode#getEncodedBytesRef()} / {@link StoredNode#getHash()} work without loading.
   */
  static Node<Bytes> collapse(
      final NodeFactory<Bytes> nodeFactory,
      final NodeUpdater sink,
      final Node<Bytes> node,
      final Bytes location) {
    if (node instanceof NullNode || node instanceof StoredNode || !node.isReferencedByHash()) {
      return node;
    }
    commit(sink, node, location);
    return new StoredNode<>(nodeFactory, location, node.getHash());
  }

  /**
   * Hands {@code node} and the nodes below it that are not stubs yet to the sink, children first.
   * Below a finished subtree only its right edge is left, so this is O(depth).
   */
  static void commit(final NodeUpdater sink, final Node<Bytes> node, final Bytes location) {
    if (node instanceof BranchNode<Bytes> branch) {
      for (int i = 0; i < branch.maxChild(); i++) {
        final Node<Bytes> child = branch.child((byte) i);
        if (isLeftToCommit(child)) {
          commit(sink, child, TrieBytes.append(location, (byte) i));
        }
      }
    } else if (node instanceof ExtensionNode<Bytes> extension
        && isLeftToCommit(extension.getChild())) {
      commit(sink, extension.getChild(), TrieBytes.concat(location, extension.getPath()));
    }
    sink.store(location, node.getHash(), node.getEncodedBytes());
  }

  private static boolean isLeftToCommit(final Node<Bytes> node) {
    return !(node instanceof NullNode || node instanceof StoredNode) && node.isReferencedByHash();
  }
}
