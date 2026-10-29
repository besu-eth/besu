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
import org.apache.tuweni.bytes.MutableBytes;

/**
 * Implements a visitor for persisting changes to nodes during a snap synchronization process,
 * focusing specifically on nodes that are marked as "dirty" but not as "heal needed".
 *
 * <p>This visitor plays a crucial role in the snap synchronization by identifying nodes that have
 * been modified and require persistence ("dirty" nodes). The key functionality of this visitor is
 * its selective persistence approach: it only persists changes to dirty nodes that are not marked
 * as "heal needed". This strategy is designed to prevent future inconsistencies within the tree by
 * ensuring that only nodes that are both modified and currently consistent with the rest of the
 * structure are persisted. Nodes marked as "heal needed" are excluded from immediate persistence to
 * allow for their proper healing in a controlled manner, ensuring the integrity and consistency of
 * the data structure.
 */
public class SnapCommitVisitor<V> extends CommitVisitor<V> implements LocationNodeVisitor<V> {

  private final Bytes startKeyPath;
  private final Bytes endKeyPath;

  public SnapCommitVisitor(
      final NodeUpdater nodeUpdater, final Bytes32 startKeyHash, final Bytes32 endKeyHash) {
    super(nodeUpdater);
    this.startKeyPath = createPath(startKeyHash);
    this.endKeyPath = createPath(endKeyHash);
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
   * @param location The current location represented as {@link Bytes}.
   * @param extensionNode The extension node being visited.
   */
  @Override
  public void visit(final Bytes location, final ExtensionNode<V> extensionNode) {
    if (!extensionNode.isDirty()) {
      return;
    }

    final Node<V> child = extensionNode.getChild();
    if (child.isDirty()) {
      child.accept(TrieBytes.concat(location, extensionNode.getPath()), this);
    }
    if (child.isHealNeeded()
        || !isInRange(
            TrieBytes.concat(location, extensionNode.getPath()), startKeyPath, endKeyPath)) {
      extensionNode.markHealNeeded(); // not save an incomplete node
    }

    maybeStoreNode(location, extensionNode);
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
   * @param location The current location represented as {@link Bytes}.
   * @param branchNode The branch node being visited.
   */
  @Override
  public void visit(final Bytes location, final BranchNode<V> branchNode) {
    if (!branchNode.isDirty()) {
      return;
    }

    for (int i = 0; i < branchNode.maxChild(); ++i) {
      final Node<V> child = branchNode.child((byte) i);
      if (child.isDirty()) {
        child.accept(TrieBytes.append(location, (byte) i), this);
      }
      if (child.isHealNeeded()
          || !isInRange(TrieBytes.append(location, (byte) i), startKeyPath, endKeyPath)) {
        branchNode.markHealNeeded(); // not save an incomplete node
      }
    }

    maybeStoreNode(location, branchNode);
  }

  private boolean isInRange(
      final Bytes location, final Bytes startKeyPath, final Bytes endKeyPath) {
    final MutableBytes path = MutableBytes.create(Bytes32.SIZE * 2);
    path.set(0, location);
    return Arrays.compare(path.toArrayUnsafe(), startKeyPath.toArrayUnsafe()) >= 0
        && Arrays.compare(path.toArrayUnsafe(), endKeyPath.toArrayUnsafe()) <= 0;
  }
}
