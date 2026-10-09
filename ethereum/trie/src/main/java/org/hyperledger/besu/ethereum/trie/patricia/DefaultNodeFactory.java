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

import org.hyperledger.besu.ethereum.trie.Node;
import org.hyperledger.besu.ethereum.trie.NodeFactory;
import org.hyperledger.besu.ethereum.trie.NullNode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import org.apache.tuweni.bytes.Bytes;

public class DefaultNodeFactory<V> implements NodeFactory<V> {
  @SuppressWarnings("rawtypes")
  private static final Node NULL_NODE = NullNode.instance();

  public static final int NB_CHILD = 16;

  private final Function<V, Bytes> valueSerializer;

  public DefaultNodeFactory(final Function<V, Bytes> valueSerializer) {
    this.valueSerializer = valueSerializer;
  }

  @Override
  public Node<V> createExtension(final byte[] path, final Node<V> child) {
    return new ExtensionNode<>(path, child, this);
  }

  @SuppressWarnings("unchecked")
  @Override
  public Node<V> createBranch(
      final byte leftIndex, final Node<V> left, final byte rightIndex, final Node<V> right) {
    assert (leftIndex <= NB_CHILD);
    assert (rightIndex <= NB_CHILD);
    assert (leftIndex != rightIndex);

    final ArrayList<Node<V>> children =
        new ArrayList<>(Collections.nCopies(NB_CHILD, (Node<V>) NULL_NODE));
    if (leftIndex == NB_CHILD) {
      children.set(rightIndex, right);
      return createBranch(children, left.getValue());
    } else if (rightIndex == NB_CHILD) {
      children.set(leftIndex, left);
      return createBranch(children, right.getValue());
    } else {
      children.set(leftIndex, left);
      children.set(rightIndex, right);
      return createBranch(children, Optional.empty());
    }
  }

  @Override
  public Node<V> createBranch(final List<Node<V>> children, final Optional<V> value) {
    return new BranchNode<>(children, value, this, valueSerializer);
  }

  @Override
  public Node<V> createLeaf(final byte[] path, final V value) {
    return new LeafNode<>(path, value, this, valueSerializer);
  }
}
