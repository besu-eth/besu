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

import static org.hyperledger.besu.ethereum.trie.Nibbles.commonPrefixLength;
import static org.hyperledger.besu.ethereum.trie.Nibbles.slice;

import org.hyperledger.besu.ethereum.trie.patricia.BranchNode;
import org.hyperledger.besu.ethereum.trie.patricia.DefaultNodeFactory;
import org.hyperledger.besu.ethereum.trie.patricia.ExtensionNode;
import org.hyperledger.besu.ethereum.trie.patricia.LeafNode;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import org.apache.tuweni.bytes.Bytes;

public class RestoreVisitor<V> implements PathNodeVisitor<V> {

  private final NodeFactory<V> nodeFactory;
  private final V value;
  private final NodeVisitor<V> persistVisitor;

  public RestoreVisitor(
      final Function<V, Bytes> valueSerializer,
      final V value,
      final NodeVisitor<V> persistVisitor) {
    this.nodeFactory = new DefaultNodeFactory<>(valueSerializer);
    this.value = value;
    this.persistVisitor = persistVisitor;
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
    BranchNode<V> workingNode = branchNode;

    final byte childIndex = path[offset];
    if (childIndex == CompactEncoding.LEAF_TERMINATOR) {
      return workingNode.replaceValue(value);
    }

    for (byte i = 0; i < childIndex; i++) {
      workingNode = persistNode(workingNode, i);
    }

    final Node<V> updatedChild = workingNode.child(childIndex).accept(this, path, offset + 1);
    return workingNode.replaceChild(childIndex, updatedChild);
  }

  private BranchNode<V> persistNode(final BranchNode<V> parent, final byte index) {
    final Node<V> child = parent.getChildren().get(index);
    if (!(child instanceof StoredNode)) {
      child.accept(persistVisitor);
      final PersistedNode<V> persistedNode =
          new PersistedNode<>(null, child.hash(), child.encodedRef());
      return (BranchNode<V>) parent.replaceChild(index, persistedNode);
    } else {
      return parent;
    }
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

  static class PersistedNode<V> implements Node<V> {
    private final byte[] path;
    private final byte[] hash;
    private final byte[] refRlp;

    PersistedNode(final byte[] path, final byte[] hash, final byte[] refRlp) {
      this.path = path;
      this.hash = hash;
      this.refRlp = refRlp;
    }

    /**
     * @return True if the node needs to be persisted.
     */
    @Override
    public boolean isDirty() {
      return false;
    }

    /** Marks the node as being modified (needs to be persisted); */
    @Override
    public void markDirty() {
      throw new UnsupportedOperationException(
          "A persisted node cannot ever be dirty since it's loaded from storage");
    }

    @Override
    public boolean isHealNeeded() {
      return false;
    }

    @Override
    public void markHealNeeded() {
      throw new UnsupportedOperationException(
          "A persisted node cannot be healed since it's loaded from storage");
    }

    @Override
    public Node<V> accept(final PathNodeVisitor<V> visitor, final byte[] path, final int offset) {
      // do nothing
      return this;
    }

    @Override
    public void accept(final NodeVisitor<V> visitor) {
      // do nothing
    }

    @Override
    public void accept(final byte[] location, final LocationNodeVisitor<V> visitor) {
      // do nothing
    }

    @Override
    public byte[] path() {
      return path;
    }

    @Override
    public Optional<V> getValue() {
      throw new UnsupportedOperationException(
          "A persisted node cannot have a value, as it's already been restored.");
    }

    @Override
    public List<Node<V>> getChildren() {
      return List.of();
    }

    @Override
    public byte[] encoded() {
      throw new UnsupportedOperationException(
          "A persisted node cannot have rlp, as it's already been restored.");
    }

    @Override
    public int encodedRefSize() {
      return refRlp.length;
    }

    @Override
    public int writeEncodedRef(final byte[] out, final int pos) {
      System.arraycopy(refRlp, 0, out, pos, refRlp.length);
      return pos + refRlp.length;
    }

    @Override
    public byte[] encodedRef() {
      return refRlp;
    }

    @Override
    public boolean isReferencedByHash() {
      // Persisted nodes represent only nodes that are referenced by hash
      return true;
    }

    @Override
    public byte[] hash() {
      return hash;
    }

    @Override
    public Node<V> replacePath(final byte[] path) {
      throw new UnsupportedOperationException(
          "A persisted node cannot be replaced, as it's already been restored.");
    }

    @Override
    public void unload() {
      throw new UnsupportedOperationException(
          "A persisted node cannot be unloaded, as it's already been restored.");
    }

    @Override
    public String print() {
      return "PersistedNode:"
          + "\n\tPath: "
          + Nibbles.toHexString(path())
          + "\n\tHash: "
          + Nibbles.toHexString(hash());
    }
  }
}
