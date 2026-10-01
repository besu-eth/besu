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
 * A child that is not loaded: only its location and hash are known. It stands for a node that is
 * referenced by hash, so it is never inline.
 *
 * <p>A placeholder holds no reference to where it can be loaded from. Whoever traverses the tree
 * brings a {@link TreeSession} whose loader is consistent with the root it started from, and every
 * node reachable from a root can be loaded from any storage consistent with that root.
 *
 * <p>The location is kept as the parent's location plus the step to this child, so the siblings of
 * a decoded branch share one prefix instead of each holding a copy.
 */
public final class StoredTreeNode extends TreeNode {

  private final Bytes locationPrefix;
  private final Bytes locationSuffix;
  private final Bytes32 hash;

  /**
   * Creates a placeholder for the node at {@code location} with the given hash.
   *
   * @param location the node's location (its nibble path from the root)
   * @param hash the node's hash
   */
  public StoredTreeNode(final Bytes location, final Bytes32 hash) {
    this(location, Bytes.EMPTY, hash);
  }

  StoredTreeNode(final Bytes locationPrefix, final Bytes locationSuffix, final Bytes32 hash) {
    this.locationPrefix = locationPrefix;
    this.locationSuffix = locationSuffix;
    this.hash = hash;
  }

  /**
   * Returns the location of the node this placeholder stands for.
   *
   * @return the nibble path from the root
   */
  public Bytes location() {
    return TreePaths.concat(locationPrefix, locationSuffix);
  }

  @Override
  public Bytes32 hash() {
    return hash;
  }

  @Override
  public boolean isReferencedByHash() {
    // Only nodes of 32 bytes or more are stored on their own, like Besu's StoredNode
    return true;
  }

  @Override
  void writeRef(final BytesValueRLPOutput out) {
    out.writeBytes(hash);
  }

  @Override
  public String toString() {
    return "Stored[" + location() + ", " + hash + "]";
  }
}
