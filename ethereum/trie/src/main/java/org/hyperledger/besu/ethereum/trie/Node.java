/*
 * Copyright ConsenSys AG.
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
import org.apache.tuweni.bytes.Bytes32;

/**
 * A trie node. Paths, locations, encodings and hashes are byte arrays that must never be modified.
 */
public interface Node<V> {

  /**
   * Visits this node with the path nibbles of {@code path} starting at {@code offset}.
   *
   * @param visitor the visitor
   * @param path the full path being visited
   * @param offset the position in {@code path} of the nibble reaching this node
   * @return the node returned by the visitor
   */
  Node<V> accept(PathNodeVisitor<V> visitor, byte[] path, int offset);

  void accept(NodeVisitor<V> visitor);

  void accept(byte[] location, LocationNodeVisitor<V> visitor);

  /** The nibble path of this node. */
  byte[] path();

  /** The location of this node in the trie, or null when unknown. */
  default byte[] location() {
    return null;
  }

  Optional<V> getValue();

  List<Node<V>> getChildren();

  /** The RLP encoding of this node. */
  byte[] encoded();

  /** The keccak hash of the RLP encoding of this node. */
  byte[] hash();

  /**
   * Whether a reference to this node should be represented as a hash of the rlp, or the node rlp
   * itself should be inlined (the rlp stored directly in the parent node). If true, the node is
   * referenced by hash. If false, the node is referenced by its rlp-encoded value.
   *
   * @return true if this node should be referenced by hash
   */
  default boolean isReferencedByHash() {
    return encoded().length >= 32;
  }

  /** Size of the reference to this node in the encoding of its parent. */
  default int encodedRefSize() {
    return isReferencedByHash() ? TrieRlp.HASH_REF_SIZE : encoded().length;
  }

  /**
   * Writes the reference to this node in the encoding of its parent.
   *
   * @param out the encoding of the parent
   * @param pos where to write the reference
   * @return the position following the reference
   */
  default int writeEncodedRef(final byte[] out, final int pos) {
    if (isReferencedByHash()) {
      return TrieRlp.writeHash(out, pos, hash());
    }
    final byte[] encoded = encoded();
    System.arraycopy(encoded, 0, out, pos, encoded.length);
    return pos + encoded.length;
  }

  /** The reference to this node in the encoding of its parent. */
  default byte[] encodedRef() {
    final byte[] ref = new byte[encodedRefSize()];
    writeEncodedRef(ref, 0);
    return ref;
  }

  Node<V> replacePath(byte[] path);

  /** Marks the node as needing to be persisted */
  void markDirty();

  /**
   * Is this node not persisted and needs to be?
   *
   * @return True if the node needs to be persisted.
   */
  boolean isDirty();

  String print();

  /** Unloads the node if it is, for example, a StoredNode. */
  default void unload() {}

  /**
   * Return if a node needs heal. If one of its children missing in the storage
   *
   * @return true if the node need heal
   */
  boolean isHealNeeded();

  /**
   * Marking a node as need heal means that one of its children is not yet present in the storage
   */
  void markHealNeeded();

  /** See {@link #path()}. */
  default Bytes getPath() {
    return Bytes.wrap(path());
  }

  /** See {@link #location()}. */
  default Optional<Bytes> getLocation() {
    final byte[] location = location();
    return location == null ? Optional.empty() : Optional.of(Bytes.wrap(location));
  }

  /** See {@link #encoded()}. */
  default Bytes getEncodedBytes() {
    return Bytes.wrap(encoded());
  }

  /** See {@link #encodedRef()}. */
  default Bytes getEncodedBytesRef() {
    return Bytes.wrap(encodedRef());
  }

  /** See {@link #hash()}. */
  default Bytes32 getHash() {
    return Bytes32.wrap(hash());
  }
}
