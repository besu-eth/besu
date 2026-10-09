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
import org.hyperledger.besu.ethereum.trie.patricia.LeafNode;

import org.apache.tuweni.bytes.Bytes;

public class CommitVisitor<V> implements LocationNodeVisitor<V> {

  protected final NodeUpdater nodeUpdater;

  public CommitVisitor(final NodeUpdater nodeUpdater) {
    this.nodeUpdater = nodeUpdater;
  }

  @Override
  public void visit(final Bytes location, final ExtensionNode<V> extensionNode) {
    if (!extensionNode.isDirty()) {
      return;
    }

    final Node<V> child = extensionNode.getChild();
    if (child.isDirty()) {
      child.accept(BytesConcatenation.concatenate(location, extensionNode.getPath()), this);
    }

    maybeStoreNode(location, extensionNode);
  }

  @Override
  public void visit(final Bytes location, final BranchNode<V> branchNode) {
    if (!branchNode.isDirty()) {
      return;
    }

    for (int i = 0; i < branchNode.maxChild(); ++i) {
      final Node<V> child = branchNode.child((byte) i);
      if (child.isDirty()) {
        child.accept(BytesConcatenation.append(location, (byte) i), this);
      }
    }

    maybeStoreNode(location, branchNode);
  }

  @Override
  public void visit(final Bytes location, final LeafNode<V> leafNode) {
    if (!leafNode.isDirty()) {
      return;
    }

    maybeStoreNode(location, leafNode);
  }

  @Override
  public void visit(final Bytes location, final NullNode<V> nullNode) {}

  public void maybeStoreNode(final Bytes location, final Node<V> node) {
    final Bytes nodeRLP = node.getEncodedBytes();
    if (nodeRLP.size() >= 32) {
      this.nodeUpdater.store(location, node.getHash(), nodeRLP);
    }
  }
}
