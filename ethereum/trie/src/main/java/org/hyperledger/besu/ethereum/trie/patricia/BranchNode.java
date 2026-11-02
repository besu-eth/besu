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

import static org.hyperledger.besu.crypto.Hash.keccak256;

import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.trie.LocationNodeVisitor;
import org.hyperledger.besu.ethereum.trie.Node;
import org.hyperledger.besu.ethereum.trie.NodeFactory;
import org.hyperledger.besu.ethereum.trie.NodeVisitor;
import org.hyperledger.besu.ethereum.trie.NullNode;
import org.hyperledger.besu.ethereum.trie.PathNodeVisitor;
import org.hyperledger.besu.ethereum.trie.StoredNode;

import java.lang.ref.SoftReference;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.bytes.MutableBytes;

public class BranchNode<V> implements Node<V> {

  @SuppressWarnings("rawtypes")
  protected static final Node NULL_NODE = NullNode.instance();

  // A hash reference is the 32 bytes of the hash as an RLP string: its prefix, then the hash.
  private static final byte HASH_REFERENCE_PREFIX = (byte) (0x80 + Bytes32.SIZE);
  private static final int HASH_REFERENCE_SIZE = 1 + Bytes32.SIZE;

  private final Optional<Bytes> location;
  private final List<Node<V>> children;
  private final Optional<V> value;
  protected final NodeFactory<V> nodeFactory;
  private final Function<V, Bytes> valueSerializer;
  protected WeakReference<Bytes> encodedBytes;
  private SoftReference<Bytes32> hash;
  private boolean dirty = false;
  private boolean needHeal = false;

  public BranchNode(
      final Bytes location,
      final List<Node<V>> children,
      final Optional<V> value,
      final NodeFactory<V> nodeFactory,
      final Function<V, Bytes> valueSerializer) {
    assert (children.size() == maxChild());
    this.location = Optional.ofNullable(location);
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
    assert (children.size() == maxChild());
    this.location = Optional.empty();
    this.children = children;
    this.value = value;
    this.nodeFactory = nodeFactory;
    this.valueSerializer = valueSerializer;
  }

  @Override
  public Node<V> accept(final PathNodeVisitor<V> visitor, final Bytes path) {
    return visitor.visit(this, path);
  }

  @Override
  public void accept(final NodeVisitor<V> visitor) {
    visitor.visit(this);
  }

  @Override
  public void accept(final Bytes location, final LocationNodeVisitor<V> visitor) {
    visitor.visit(location, this);
  }

  @Override
  public Optional<Bytes> getLocation() {
    return location;
  }

  @Override
  public Bytes getPath() {
    return Bytes.EMPTY;
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
  public Bytes getEncodedBytes() {
    if (encodedBytes != null) {
      final Bytes encoded = encodedBytes.get();
      if (encoded != null) {
        return encoded;
      }
    }
    final Bytes encoded = encode();
    encodedBytes = new WeakReference<>(encoded);
    return encoded;
  }

  /**
   * The list of the children's references and the value, written into one array. A child referenced
   * by hash, as most are, is written from its hash rather than from an encoding of the hash made
   * for it, and the list is not assembled from parts.
   */
  private Bytes encode() {
    final Bytes[] inlineChildren = new Bytes[maxChild()];
    int payloadSize = 0;
    for (int i = 0; i < maxChild(); ++i) {
      final Node<V> child = children.get(i);
      if (isReferencedByItsHash(child)) {
        payloadSize += HASH_REFERENCE_SIZE;
      } else {
        inlineChildren[i] = child.getEncodedBytesRef();
        payloadSize += inlineChildren[i].size();
      }
    }
    final Bytes encodedValue =
        value.isPresent() ? RLP.encodeOne(valueSerializer.apply(value.get())) : RLP.NULL;
    payloadSize += encodedValue.size();

    final Bytes header = RLP.listHeader(payloadSize);
    final MutableBytes encoded = MutableBytes.create(header.size() + payloadSize);
    header.copyTo(encoded, 0);
    int offset = header.size();
    for (int i = 0; i < maxChild(); ++i) {
      if (inlineChildren[i] == null) {
        encoded.set(offset, HASH_REFERENCE_PREFIX);
        children.get(i).getHash().copyTo(encoded, offset + 1);
        offset += HASH_REFERENCE_SIZE;
      } else {
        inlineChildren[i].copyTo(encoded, offset);
        offset += inlineChildren[i].size();
      }
    }
    encodedValue.copyTo(encoded, offset);
    return encoded;
  }

  /**
   * Whether the child's reference is the encoding of its hash, for the kinds of node whose
   * reference is known to be: any other kind gives its own reference.
   */
  private static boolean isReferencedByItsHash(final Node<?> child) {
    return child instanceof StoredNode
        || ((child instanceof BranchNode
                || child instanceof ExtensionNode
                || child instanceof LeafNode)
            && child.isReferencedByHash());
  }

  @Override
  public Bytes getEncodedBytesRef() {
    if (isReferencedByHash()) {
      return RLP.encodeOne(getHash());
    } else {
      return getEncodedBytes();
    }
  }

  @Override
  public Bytes32 getHash() {
    if (hash != null) {
      final Bytes32 hashed = hash.get();
      if (hashed != null) {
        return hashed;
      }
    }
    final Bytes32 hashed = keccak256(getEncodedBytes());
    hash = new SoftReference<>(hashed);
    return hashed;
  }

  @Override
  public Node<V> replacePath(final Bytes newPath) {
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
        return nodeFactory.createLeaf(Bytes.of(index), value.get());
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
      return nodeFactory.createLeaf(getPath(), value.get());
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
      final Bytes onlyChildPath = onlyChild.getPath();
      final MutableBytes completePath = MutableBytes.create(1 + onlyChildPath.size());
      completePath.set(0, (byte) onlyChildIndex);
      onlyChildPath.copyTo(completePath, 1);
      return Optional.of(onlyChild.replacePath(completePath));
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
    builder.append("\n\tRef: ").append(getEncodedBytesRef());
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
