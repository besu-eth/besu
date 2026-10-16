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

import org.hyperledger.besu.ethereum.trie.CompactEncoding;
import org.hyperledger.besu.ethereum.trie.Keccak256;
import org.hyperledger.besu.ethereum.trie.LocationNodeVisitor;
import org.hyperledger.besu.ethereum.trie.Nibbles;
import org.hyperledger.besu.ethereum.trie.Node;
import org.hyperledger.besu.ethereum.trie.NodeFactory;
import org.hyperledger.besu.ethereum.trie.NodeVisitor;
import org.hyperledger.besu.ethereum.trie.PathNodeVisitor;
import org.hyperledger.besu.ethereum.trie.TrieRlp;

import java.lang.ref.SoftReference;
import java.lang.ref.WeakReference;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class ExtensionNode<V> implements Node<V> {

  private final byte[] location;
  private final byte[] path;
  private final Node<V> child;
  private final NodeFactory<V> nodeFactory;
  private WeakReference<byte[]> rlp;
  private SoftReference<byte[]> hash;
  private boolean dirty = false;
  private boolean needHeal = false;

  public ExtensionNode(
      final byte[] location,
      final byte[] path,
      final Node<V> child,
      final NodeFactory<V> nodeFactory) {
    assert (path.length > 0);
    assert (path[path.length - 1] != CompactEncoding.LEAF_TERMINATOR)
        : "Extension path ends in a leaf terminator";
    this.location = location;
    this.path = path;
    this.child = child;
    this.nodeFactory = nodeFactory;
  }

  public ExtensionNode(final byte[] path, final Node<V> child, final NodeFactory<V> nodeFactory) {
    this(null, path, child, nodeFactory);
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
    return path;
  }

  @Override
  public Optional<V> getValue() {
    return Optional.empty();
  }

  @Override
  public List<Node<V>> getChildren() {
    return Collections.singletonList(child);
  }

  public Node<V> getChild() {
    return child;
  }

  @Override
  public byte[] encoded() {
    if (rlp != null) {
      final byte[] encoded = rlp.get();
      if (encoded != null) {
        return encoded;
      }
    }
    final byte[] encodedPath = CompactEncoding.encode(path);
    final int payloadSize = TrieRlp.bytesSize(encodedPath) + child.encodedRefSize();
    final byte[] encoded = new byte[TrieRlp.listSize(payloadSize)];
    int pos = TrieRlp.writeListHeader(encoded, 0, payloadSize);
    pos = TrieRlp.writeBytes(encoded, pos, encodedPath);
    child.writeEncodedRef(encoded, pos);
    rlp = new WeakReference<>(encoded);
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

  public Node<V> replaceChild(final Node<V> updatedChild) {
    // collapse this extension - if the child is a branch, it will create a new extension
    return updatedChild.replacePath(Nibbles.concat(path, updatedChild.path()));
  }

  @Override
  public Node<V> replacePath(final byte[] path) {
    if (path.length == 0) {
      return child;
    }
    return nodeFactory.createExtension(path, child);
  }

  @Override
  public String print() {
    final StringBuilder builder = new StringBuilder();
    final String childRep = getChild().print().replaceAll("\n\t", "\n\t\t");
    builder
        .append("Extension:")
        .append("\n\tRef: ")
        .append(Nibbles.toHexString(encodedRef()))
        .append("\n\tPath: ")
        .append(Nibbles.toHexString(CompactEncoding.encode(path)))
        .append("\n\t")
        .append(childRep);
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
}
