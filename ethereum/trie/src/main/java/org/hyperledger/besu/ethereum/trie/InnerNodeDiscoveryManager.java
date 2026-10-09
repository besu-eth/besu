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
import static org.hyperledger.besu.ethereum.trie.RangeManager.isInRange;

import org.hyperledger.besu.ethereum.trie.patricia.BranchNode;
import org.hyperledger.besu.ethereum.trie.patricia.ExtensionNode;
import org.hyperledger.besu.ethereum.trie.patricia.LeafNode;
import org.hyperledger.besu.ethereum.trie.patricia.StoredNodeFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.immutables.value.Value;

public class InnerNodeDiscoveryManager<V> extends StoredNodeFactory<V> {

  private final List<InnerNode> innerNodes = new ArrayList<>();

  private final byte[] startKeyPath, endKeyPath;

  private final boolean allowMissingElementInRange;

  public InnerNodeDiscoveryManager(
      final NodeLoader nodeLoader,
      final Function<V, Bytes> valueSerializer,
      final Function<Bytes, V> valueDeserializer,
      final Bytes32 startKeyHash,
      final Bytes32 endKeyHash,
      final boolean allowMissingElementInRange) {
    super(nodeLoader, valueSerializer, valueDeserializer);
    this.startKeyPath = createPath(startKeyHash.toArrayUnsafe());
    this.endKeyPath = createPath(endKeyHash.toArrayUnsafe());
    this.allowMissingElementInRange = allowMissingElementInRange;
  }

  @Override
  protected Node<V> decodeExtension(
      final byte[] location,
      final byte[] path,
      final TrieRlpReader valueRlp,
      final Supplier<String> errMessage) {
    final ExtensionNode<V> vNode =
        (ExtensionNode<V>) super.decodeExtension(location, path, valueRlp, errMessage);
    if (isInRange(Nibbles.append(location, 0), startKeyPath, endKeyPath)) {
      innerNodes.add(
          ImmutableInnerNode.builder()
              .location(Bytes.wrap(location))
              .path(Bytes.of(0, CompactEncoding.LEAF_TERMINATOR))
              .build());
    }
    return vNode;
  }

  @Override
  protected BranchNode<V> decodeBranch(
      final byte[] location, final TrieRlpReader nodeRLPs, final Supplier<String> errMessage) {
    final BranchNode<V> vBranchNode = super.decodeBranch(location, nodeRLPs, errMessage);
    final List<Node<V>> children = vBranchNode.getChildren();
    for (int i = 0; i < children.size(); i++) {
      if (isInRange(Nibbles.append(location, i), startKeyPath, endKeyPath)) {
        innerNodes.add(
            ImmutableInnerNode.builder()
                .location(Bytes.wrap(location))
                .path(Bytes.of(i, CompactEncoding.LEAF_TERMINATOR))
                .build());
      }
    }
    return vBranchNode;
  }

  @Override
  protected LeafNode<V> decodeLeaf(
      final byte[] location,
      final byte[] path,
      final TrieRlpReader valueRlp,
      final Supplier<String> errMessage) {
    final LeafNode<V> vLeafNode = super.decodeLeaf(location, path, valueRlp, errMessage);
    final byte[] concatenatePath = Nibbles.concat(location, path);
    if (isInRange(
        Nibbles.slice(concatenatePath, 0, concatenatePath.length - 1), startKeyPath, endKeyPath)) {
      innerNodes.add(
          ImmutableInnerNode.builder()
              .location(Bytes.wrap(location))
              .path(Bytes.wrap(path))
              .build());
    }
    return vLeafNode;
  }

  @Override
  public Optional<Node<V>> retrieve(final byte[] location, final byte[] hash)
      throws MerkleTrieException {

    return super.retrieve(location, hash)
        .map(
            vNode -> {
              vNode.markDirty();
              return vNode;
            })
        .or(
            () -> {
              if (!allowMissingElementInRange && isInRange(location, startKeyPath, endKeyPath)) {
                return Optional.empty();
              }
              return Optional.of(new MissingNode<>(hash, location));
            });
  }

  public List<InnerNode> getInnerNodes() {
    return List.copyOf(innerNodes);
  }

  public static Bytes32 decodePath(final Bytes bytes) {
    return Bytes32.wrap(decodePath(bytes.toArrayUnsafe()));
  }

  /** Packs a location of at most 64 nibbles, right padded with zeros, into 32 bytes. */
  public static byte[] decodePath(final byte[] location) {
    if (location.length > Bytes32.SIZE * 2) {
      throw new IllegalArgumentException(
          "Path of " + location.length + " nibbles is too long for a 32 bytes key");
    }
    final byte[] decoded = new byte[Bytes32.SIZE];
    for (int pathPos = 0, decodedPos = 0;
        pathPos < location.length;
        pathPos += 2, decodedPos += 1) {
      final byte high = location[pathPos];
      final byte low = pathPos + 1 < location.length ? location[pathPos + 1] : 0;
      if ((high & 0xf0) != 0 || (low & 0xf0) != 0) {
        throw new IllegalArgumentException("Invalid path: contains elements larger than a nibble");
      }
      decoded[decodedPos] = (byte) (high << 4 | (low & 0xff));
    }
    return decoded;
  }

  @Value.Immutable
  public interface InnerNode {
    Bytes location();

    Bytes path();
  }
}
