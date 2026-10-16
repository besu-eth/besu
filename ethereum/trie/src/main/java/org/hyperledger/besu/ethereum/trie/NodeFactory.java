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

import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;

public interface NodeFactory<V> {

  Node<V> createExtension(byte[] path, Node<V> child);

  Node<V> createBranch(byte leftIndex, Node<V> left, byte rightIndex, Node<V> right);

  Node<V> createBranch(List<Node<V>> newChildren, Optional<V> value);

  Node<V> createLeaf(byte[] path, V value);

  /** See {@link #createLeaf(byte[], Object)}. */
  default Node<V> createLeaf(final Bytes path, final V value) {
    return createLeaf(path.toArray(), value);
  }

  default Optional<Node<V>> retrieve(final byte[] location, final byte[] hash) {
    throw new UnsupportedOperationException("retrieve is not implemented");
  }
}
