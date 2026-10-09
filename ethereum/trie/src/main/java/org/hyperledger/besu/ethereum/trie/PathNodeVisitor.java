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

/** Visits nodes along a path; the remaining path is the nibbles of {@code path} from offset. */
public interface PathNodeVisitor<V> {

  Node<V> visit(ExtensionNode<V> extensionNode, byte[] path, int offset);

  Node<V> visit(BranchNode<V> branchNode, byte[] path, int offset);

  Node<V> visit(LeafNode<V> leafNode, byte[] path, int offset);

  Node<V> visit(NullNode<V> nullNode, byte[] path, int offset);
}
