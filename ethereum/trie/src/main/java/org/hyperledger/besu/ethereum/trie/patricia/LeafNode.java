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
import java.util.function.Function;

import org.apache.tuweni.bytes.Bytes;

public class LeafNode<V> implements Node<V> {
  private final byte[] location;
  private final byte[] path;
  protected final V value;
  private final NodeFactory<V> nodeFactory;
  protected final Function<V, Bytes> valueSerializer;
  protected WeakReference<byte[]> encodedBytes;
  private SoftReference<byte[]> hash;
  private boolean dirty = false;

  public LeafNode(
      final byte[] location,
      final byte[] path,
      final V value,
      final NodeFactory<V> nodeFactory,
      final Function<V, Bytes> valueSerializer) {
    this.location = location;
    this.path = path;
    this.value = value;
    this.nodeFactory = nodeFactory;
    this.valueSerializer = valueSerializer;
  }

  public LeafNode(
      final byte[] path,
      final V value,
      final NodeFactory<V> nodeFactory,
      final Function<V, Bytes> valueSerializer) {
    this(null, path, value, nodeFactory, valueSerializer);
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
    return Optional.of(value);
  }

  @Override
  public List<Node<V>> getChildren() {
    return Collections.emptyList();
  }

  @Override
  public byte[] encoded() {
    if (encodedBytes != null) {
      final byte[] encoded = encodedBytes.get();
      if (encoded != null) {
        return encoded;
      }
    }

    final byte[] encodedPath = CompactEncoding.encode(path);
    final byte[] serializedValue = valueSerializer.apply(value).toArrayUnsafe();
    final int payloadSize = TrieRlp.bytesSize(encodedPath) + TrieRlp.bytesSize(serializedValue);
    final byte[] encoded = new byte[TrieRlp.listSize(payloadSize)];
    int pos = TrieRlp.writeListHeader(encoded, 0, payloadSize);
    pos = TrieRlp.writeBytes(encoded, pos, encodedPath);
    TrieRlp.writeBytes(encoded, pos, serializedValue);
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
  public Node<V> replacePath(final byte[] path) {
    return nodeFactory.createLeaf(path, value);
  }

  @Override
  public String print() {
    return "Leaf:"
        + "\n\tRef: "
        + Nibbles.toHexString(encodedRef())
        + "\n\tPath: "
        + Nibbles.toHexString(CompactEncoding.encode(path))
        + "\n\tValue: "
        + getValue().map(Object::toString).orElse("empty");
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
    return false;
  }

  @Override
  public void markHealNeeded() {
    // nothing to do a leaf don't have child
  }
}
