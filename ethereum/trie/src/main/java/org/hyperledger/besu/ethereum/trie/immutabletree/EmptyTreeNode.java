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
import org.hyperledger.besu.ethereum.trie.MerkleTrie;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/** The empty tree, and an empty child slot of a branch. Encoded as the RLP empty string. */
public final class EmptyTreeNode extends TreeNode {

  /** The only instance; slots are compared against it by reference. */
  public static final EmptyTreeNode INSTANCE = new EmptyTreeNode();

  private EmptyTreeNode() {}

  @Override
  public Bytes32 hash() {
    return MerkleTrie.EMPTY_TRIE_NODE_HASH;
  }

  @Override
  public boolean isReferencedByHash() {
    return false;
  }

  @Override
  void writeRef(final BytesValueRLPOutput out) {
    out.writeNull();
  }

  /**
   * Returns the RLP encoding of the empty tree.
   *
   * @return the RLP empty string
   */
  public Bytes encoded() {
    return MerkleTrie.EMPTY_TRIE_NODE;
  }

  @Override
  public String toString() {
    return "Empty";
  }
}
