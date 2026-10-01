/*
 * Copyright contributors to Besu.
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
package org.hyperledger.besu.ethereum.trie.immutabletree;

import static org.hyperledger.besu.crypto.Hash.keccak256;

import org.hyperledger.besu.ethereum.trie.Node;
import org.hyperledger.besu.ethereum.trie.NullNode;
import org.hyperledger.besu.ethereum.trie.StoredNode;
import org.hyperledger.besu.ethereum.trie.patricia.BranchNode;
import org.hyperledger.besu.ethereum.trie.patricia.ExtensionNode;
import org.hyperledger.besu.ethereum.trie.patricia.LeafNode;
import org.hyperledger.besu.ethereum.trie.patricia.StoredNodeFactory;

import java.util.Optional;
import java.util.function.Function;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.bytes.MutableBytes;

/**
 * Decodes stored nodes into tree nodes.
 *
 * <p>Decoding is delegated to Besu's {@link StoredNodeFactory}, so the RLP rules (list sizes,
 * compact paths, leaf versus extension, inline versus hashed children, the branch value) are the
 * ones the rest of Besu uses. The decoded node is then converted: hashed children stay stored (a
 * {@link StoredTreeNode}, or a compact slot of a branch), so loading a node never loads its
 * children.
 */
final class TreeNodeCodec {

  // decode() never retrieves children, so the loader is never called
  private static final StoredNodeFactory<Bytes> DECODER =
      new StoredNodeFactory<>(
          (location, hash) -> Optional.empty(), Function.identity(), Function.identity());

  private TreeNodeCodec() {}

  /**
   * Decodes a node read from storage.
   *
   * @param location the node's location, the prefix of its children's locations
   * @param hash the hash the node was loaded by
   * @param rlp the stored encoding
   * @param stamp the access stamp of the loading session
   * @return the decoded node, with its hashed children as placeholders
   */
  static TreeNode decode(
      final Bytes location, final Bytes32 hash, final Bytes rlp, final int stamp) {
    assert hash.equals(keccak256(rlp)) : "Node hash " + keccak256(rlp) + " != expected " + hash;
    final TreeNode node = convert(DECODER.decode(null, rlp), location, Bytes.EMPTY, stamp);
    if (node instanceof MaterializedTreeNode materialized) {
      materialized.setLoadedEncoding(rlp, hash);
    }
    return node;
  }

  private static TreeNode convert(
      final Node<Bytes> node, final Bytes prefix, final Bytes suffix, final int stamp) {
    return switch (node) {
      case NullNode<Bytes> ignored -> EmptyTreeNode.INSTANCE;
      case LeafNode<Bytes> leaf ->
          new LeafTreeNode(
              MaterializedTreeNode.LOADED,
              stamp,
              leaf.getPath(),
              leaf.getValue().orElseThrow().copy());
      case ExtensionNode<Bytes> extension -> {
        final Bytes location = TreePaths.concat(prefix, suffix);
        yield new ExtensionTreeNode(
            MaterializedTreeNode.LOADED,
            stamp,
            extension.getPath(),
            child(extension.getChild(), location, extension.getPath(), stamp));
      }
      case BranchNode<Bytes> branch -> {
        final Bytes location = TreePaths.concat(prefix, suffix);
        final TreeNode[] slots = new TreeNode[TreePaths.RADIX];
        byte[] storedHashes = null;
        for (int i = 0; i < TreePaths.RADIX; i++) {
          final Node<Bytes> child = branch.child((byte) i);
          if (child instanceof StoredNode<Bytes> stored) {
            // kept compactly: a null slot and the hash in one array for all children
            if (storedHashes == null) {
              storedHashes = new byte[TreePaths.RADIX * Bytes32.SIZE];
            }
            stored.getHash().copyTo(MutableBytes.wrap(storedHashes), i * Bytes32.SIZE);
          } else {
            slots[i] = convert(child, location, TreePaths.nibble(i), stamp);
          }
        }
        yield new BranchTreeNode(
            MaterializedTreeNode.LOADED,
            stamp,
            new BranchTreeNode.Children(
                slots, storedHashes, storedHashes == null ? null : location),
            branch.getValue().map(Bytes::copy).orElse(null));
      }
      default ->
          throw new IllegalStateException("Unexpected node type " + node.getClass().getName());
    };
  }

  private static TreeNode child(
      final Node<Bytes> child, final Bytes location, final Bytes step, final int stamp) {
    if (child instanceof StoredNode<Bytes> stored) {
      // Copied so the placeholder does not keep the parent's whole encoding alive
      return new StoredTreeNode(location, step, Bytes32.wrap(stored.getHash().toArray()));
    }
    return convert(child, location, step, stamp);
  }
}
