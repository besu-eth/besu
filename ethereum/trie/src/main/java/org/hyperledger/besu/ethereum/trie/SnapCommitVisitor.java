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

import static org.hyperledger.besu.ethereum.trie.RangeManager.createPath;

import org.hyperledger.besu.ethereum.trie.patricia.BranchNode;
import org.hyperledger.besu.ethereum.trie.patricia.ExtensionNode;

import java.util.Arrays;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

public class SnapCommitVisitor<V> extends CommitVisitor<V> implements LocationNodeVisitor<V> {

  private final byte[] startKeyPath;
  private final byte[] endKeyPath;

  public SnapCommitVisitor(
      final NodeUpdater nodeUpdater, final Bytes32 startKeyHash, final Bytes32 endKeyHash) {
    super(nodeUpdater);
    this.startKeyPath = createPath(startKeyHash.toArrayUnsafe());
    this.endKeyPath = createPath(endKeyHash.toArrayUnsafe());
  }

  /**
   * Visits an extension node during a traversal operation.
   *
   * <p>This method is called when visiting an extension node. It checks if the node is marked as
   * "dirty" (indicating changes that have not been persisted). If the node is clean, the method
   * returns immediately. For dirty nodes, it recursively visits any dirty child nodes,
   * concatenating the current location with the extension node's path to form the full path to the
   * child.
   *
   * <p>Additionally, it checks if the child node requires healing (e.g., if it's falls outside the
   * specified range defined by {@code startKeyPath} and {@code endKeyPath}). If healing is needed,
   * the extension node is marked accordingly.
   *
   * <p>Finally, it attempts to persist the extension node if applicable.
   *
   * @param location The current location, as nibbles.
   * @param extensionNode The extension node being visited.
   */
  @Override
  public void visit(final byte[] location, final ExtensionNode<V> extensionNode) {
    if (!extensionNode.isDirty()) {
      return;
    }

    final Node<V> child = extensionNode.getChild();
    final byte[] childLocation = Nibbles.concat(location, extensionNode.path());
    if (child.isDirty()) {
      child.accept(childLocation, this);
    }
    if (child.isHealNeeded() || !isInRange(childLocation, startKeyPath, endKeyPath)) {
      extensionNode.markHealNeeded(); // not save an incomplete node
    }

    maybeStoreNode(Bytes.wrap(location), extensionNode);
  }

  /**
   * Visits a branch node during a traversal operation.
   *
   * <p>This method is invoked when visiting a branch node. It first checks if the branch node is
   * marked as "dirty" (indicating changes that have not been persisted). If the node is clean, the
   * method returns immediately.
   *
   * <p>For dirty branch nodes, it iterates through each child node. For each child, if the child is
   * dirty, it recursively visits the child, passing along the concatenated path (current location
   * plus the child's index) to the child's accept method.
   *
   * <p>Additionally, it checks if the child node requires healing (e.g., if it's falls outside the
   * specified range of interest defined by {@code startKeyPath} and {@code endKeyPath}). If healing
   * is needed, the branch node is marked accordingly.
   *
   * <p>Finally, it attempts to persist the branch node if applicable.
   *
   * @param location The current location, as nibbles.
   * @param branchNode The branch node being visited.
   */
  @Override
  public void visit(final byte[] location, final BranchNode<V> branchNode) {
    if (!branchNode.isDirty()) {
      return;
    }

    for (int i = 0; i < branchNode.maxChild(); ++i) {
      final byte[] childLocation = Nibbles.append(location, i);
      final Node<V> child = branchNode.child((byte) i);
      if (child.isDirty()) {
        child.accept(childLocation, this);
      }
      if (child.isHealNeeded() || !isInRange(childLocation, startKeyPath, endKeyPath)) {
        branchNode.markHealNeeded(); // not save an incomplete node
      }
    }

    maybeStoreNode(Bytes.wrap(location), branchNode);
  }

  private boolean isInRange(
      final byte[] location, final byte[] startKeyPath, final byte[] endKeyPath) {
    final byte[] path = Arrays.copyOf(location, Bytes32.SIZE * 2);
    return Arrays.compare(path, startKeyPath) >= 0 && Arrays.compare(path, endKeyPath) <= 0;
  }
}
