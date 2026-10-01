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

import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/**
 * A node of an immutable Merkle Patricia tree.
 *
 * <p>The logical content of a node never changes. Updates build new nodes and share every untouched
 * subtree with the tree they started from, so any number of roots can be alive at once. The only
 * mutable state is memoization (hash, inline encoding, access stamps) and the child slots of
 * branches and extensions, which may swap a {@link StoredTreeNode} for the node it stands for (on
 * load) and back (on prune). Both forms describe the same content, so readers racing with such a
 * swap see a valid tree either way.
 *
 * <p>Encoding follows Besu's Patricia trie exactly: compact-encoded paths, the leaf terminator
 * nibble, the branch value in the 17th list item, and children embedded inline when their encoding
 * is shorter than 32 bytes, referenced by hash otherwise.
 */
public abstract sealed class TreeNode permits EmptyTreeNode, StoredTreeNode, MaterializedTreeNode {

  TreeNode() {}

  /**
   * Returns the keccak256 hash of this node's RLP encoding.
   *
   * @return the node hash
   */
  public abstract Bytes32 hash();

  /**
   * Whether a parent references this node by its hash rather than embedding its encoding.
   *
   * @return true if the encoding is at least 32 bytes long
   */
  public abstract boolean isReferencedByHash();

  /**
   * Writes what a parent embeds for this node: the encoding itself when it is shorter than 32
   * bytes, the RLP of the hash otherwise.
   *
   * @param out the parent's RLP output
   */
  abstract void writeRef(BytesValueRLPOutput out);

  /**
   * Returns what a parent embeds for this node.
   *
   * @return the inline encoding or the RLP-encoded hash
   */
  public Bytes encodedRef() {
    final BytesValueRLPOutput out = new BytesValueRLPOutput();
    writeRef(out);
    return out.encoded();
  }
}
