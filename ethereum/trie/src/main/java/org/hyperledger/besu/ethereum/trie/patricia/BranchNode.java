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

import org.hyperledger.besu.ethereum.trie.Keccak256;
import org.hyperledger.besu.ethereum.trie.LocationNodeVisitor;
import org.hyperledger.besu.ethereum.trie.Nibbles;
import org.hyperledger.besu.ethereum.trie.Node;
import org.hyperledger.besu.ethereum.trie.NodeFactory;
import org.hyperledger.besu.ethereum.trie.NodeVisitor;
import org.hyperledger.besu.ethereum.trie.NullNode;
import org.hyperledger.besu.ethereum.trie.PathNodeVisitor;
import org.hyperledger.besu.ethereum.trie.TrieRlp;

import java.lang.ref.SoftReference;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

import org.apache.tuweni.bytes.Bytes;

public class BranchNode<V> implements Node<V> {

  @SuppressWarnings("rawtypes")
  protected static final Node NULL_NODE = NullNode.instance();

  private final byte[] location;
  private final List<Node<V>> children;
  private final Optional<V> value;
  protected final NodeFactory<V> nodeFactory;
  private final Function<V, Bytes> valueSerializer;
  protected WeakReference<byte[]> encodedBytes;
  private SoftReference<byte[]> hash;
  private boolean dirty = false;
  private boolean needHeal = false;

  public BranchNode(
      final byte[] location,
      final List<Node<V>> children,
      final Optional<V> value,
      final NodeFactory<V> nodeFactory,
      final Function<V, Bytes> valueSerializer) {
    assert (children.size() == maxChild());
    this.location = location;
    this.children = children;
    this.value = value;
    this.nodeFactory = nodeFactory;
    this.valueSerializer = valueSerializer;
  }

  public BranchNode(
      final List<Node<V>> children,
      final Optional<V> value,
      final NodeFactory<V> nodeFactory,
      final Function<V, Bytes> valueSerializer) {
    this(null, children, value, nodeFactory, valueSerializer);
  }

  @Override
  public Node<V> accept(final PathNodeVisitor<V> visitor, final byte[] path, final int offset) {
    return visitor.visit(this, path, offset);
  }

  @Override
  public void accept(final NodeVisitor<V> visitor) {
    visitor.visit(this);
  }

  @Override
  public void accept(final byte[] location, final LocationNodeVisitor<V> visitor) {
    visitor.visit(location, this);
  }

  @Override
  public byte[] location() {
    return location;
  }

  @Override
  public byte[] path() {
    return Nibbles.EMPTY;
  }

  @Override
  public Optional<V> getValue() {
    return value;
  }

  @Override
  public List<Node<V>> getChildren() {
    return Collections.unmodifiableList(children);
  }

  public Node<V> child(final byte index) {
    return children.get(index);
  }

  @Override
  public byte[] encoded() {
    if (encodedBytes != null) {
      final byte[] encoded = encodedBytes.get();
      if (encoded != null) {
        return encoded;
      }
    }
    final int maxChild = maxChild();
    final byte[] serializedValue =
        value.isPresent() ? valueSerializer.apply(value.get()).toArrayUnsafe() : null;
    int payloadSize = serializedValue == null ? 1 : TrieRlp.bytesSize(serializedValue);
    for (int i = 0; i < maxChild; ++i) {
      payloadSize += children.get(i).encodedRefSize();
    }
    final byte[] encoded = new byte[TrieRlp.listSize(payloadSize)];
    int pos = TrieRlp.writeListHeader(encoded, 0, payloadSize);
    for (int i = 0; i < maxChild; ++i) {
      pos = children.get(i).writeEncodedRef(encoded, pos);
    }
    if (serializedValue == null) {
      encoded[pos] = TrieRlp.NULL;
    } else {
      TrieRlp.writeBytes(encoded, pos, serializedValue);
    }
    encodedBytes = new WeakReference<>(encoded);
    return encoded;
  }

  @Override
  public byte[] hash() {
    if (hash != null) {
      final byte[] hashed = hash.get();
      if (hashed != null) {
        return hashed;
      }
    }
    final byte[] hashed = Keccak256.hash(encoded());
    hash = new SoftReference<>(hashed);
    return hashed;
  }

  @Override
  public Node<V> replacePath(final byte[] newPath) {
    return nodeFactory.createExtension(newPath, this);
  }

  public Node<V> replaceChild(final byte index, final Node<V> updatedChild) {
    return replaceChild(index, updatedChild, true);
  }

  public Node<V> replaceChild(
      final byte index, final Node<V> updatedChild, final boolean allowFlatten) {
    final ArrayList<Node<V>> newChildren = new ArrayList<>(children);
    newChildren.set(index, updatedChild);

    if (updatedChild == NULL_NODE) {
      if (value.isPresent() && !hasChildren()) {
        return nodeFactory.createLeaf(new byte[] {index}, value.get());
      } else if (value.isEmpty() && allowFlatten) {
        final Optional<Node<V>> flattened = maybeFlatten(newChildren);
        if (flattened.isPresent()) {
          return flattened.get();
        }
      }
    }

    return nodeFactory.createBranch(newChildren, value);
  }

  @SuppressWarnings("unchecked")
  public Node<V> replaceAllChildren(
      final List<Node<V>> updatedChildren, final boolean allowFlatten) {
    final ArrayList<Node<V>> newChildren = new ArrayList<>(updatedChildren);
    if (value.isPresent() && !hasChildren()) {
      return nodeFactory.createLeaf(path(), value.get());
    } else if (value.isEmpty() && allowFlatten) {
      final Optional<Node<V>> flattened = maybeFlatten(newChildren);
      if (flattened.isPresent()) {
        return flattened.get();
      } else {
        final BranchNode<V> branch = (BranchNode<V>) nodeFactory.createBranch(newChildren, value);
        if (!branch.hasChildren()) {
          return NULL_NODE;
        } else {
          return branch;
        }
      }
    }
    return nodeFactory.createBranch(newChildren, value);
  }

  public Node<V> replaceValue(final V value) {
    return nodeFactory.createBranch(children, Optional.of(value));
  }

  public Node<V> removeValue() {
    return maybeFlatten(children).orElse(nodeFactory.createBranch(children, Optional.empty()));
  }

  protected boolean hasChildren() {
    for (final Node<V> child : children) {
      if (child != NULL_NODE) {
        return true;
      }
    }
    return false;
  }

  protected Optional<Node<V>> maybeFlatten(final List<Node<V>> children) {
    final int onlyChildIndex = findOnlyChild(children);
    if (onlyChildIndex >= 0) {
      // replace the path of the only child and return it
      final Node<V> onlyChild = children.get(onlyChildIndex);
      return Optional.of(onlyChild.replacePath(Nibbles.prepend(onlyChildIndex, onlyChild.path())));
    }
    return Optional.empty();
  }

  private int findOnlyChild(final List<Node<V>> children) {
    int onlyChildIndex = -1;
    assert (children.size() == maxChild());
    for (int i = 0; i < maxChild(); ++i) {
      if (children.get(i) != NULL_NODE) {
        if (onlyChildIndex >= 0) {
          return -1;
        }
        onlyChildIndex = i;
      }
    }
    return onlyChildIndex;
  }

  @Override
  public String print() {
    final StringBuilder builder = new StringBuilder();
    builder.append("Branch:");
    builder.append("\n\tRef: ").append(Nibbles.toHexString(encodedRef()));
    for (int i = 0; i < maxChild(); i++) {
      final Node<V> child = child((byte) i);
      if (!Objects.equals(child, NullNode.instance())) {
        final String branchLabel = "[" + Integer.toHexString(i) + "] ";
        final String childRep = child.print().replaceAll("\n\t", "\n\t\t");
        builder.append("\n\t").append(branchLabel).append(childRep);
      }
    }
    builder.append("\n\tValue: ").append(getValue().map(Object::toString).orElse("empty"));
    return builder.toString();
  }

  @Override
  public boolean isDirty() {
    return dirty;
  }

  @Override
  public void markDirty() {
    dirty = true;
  }

  @Override
  public boolean isHealNeeded() {
    return needHeal;
  }

  @Override
  public void markHealNeeded() {
    this.needHeal = true;
  }

  public int maxChild() {
    return 16;
  }
}
