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

import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/**
 * A node whose content is in memory: a leaf, an extension or a branch.
 *
 * <p>Each node records the session that created it. A session commits exactly the nodes it created,
 * which is Besu's dirty-only commit without a mutable dirty flag: a node that another session
 * created, or that was decoded from storage, belongs to a root that is already consistent with the
 * storage being written to.
 */
public abstract sealed class MaterializedTreeNode extends TreeNode
    permits LeafTreeNode, ExtensionTreeNode, BranchTreeNode {

  /** Creator id of nodes decoded from storage. Session ids are never 0. */
  static final int LOADED = 0;

  private final int createdBy;

  /**
   * Block stamp of the last traversal through this node. Racy by design: it only steers pruning.
   */
  int lastAccess;

  /** Marks the node as visited by the current prune walk, so shared subtrees are walked once. */
  int visitEpoch;

  // At most one of the two is ever set, which is what tells an inline node from a hashed one.
  private volatile Bytes32 hash;
  private volatile Bytes inlineEncoding;

  MaterializedTreeNode(final int createdBy, final int stamp) {
    this.createdBy = createdBy;
    this.lastAccess = stamp;
  }

  /**
   * Builds the RLP encoding from the node's content and its children's references.
   *
   * @return the RLP encoding
   */
  abstract Bytes computeEncoding();

  /**
   * Records the encoding and hash of a node decoded from storage, so it is never hashed again.
   *
   * @param rlp the encoding read from storage
   * @param knownHash the hash it was loaded by
   */
  final void setLoadedEncoding(final Bytes rlp, final Bytes32 knownHash) {
    if (rlp.size() < 32) {
      inlineEncoding = rlp;
    } else {
      hash = knownHash;
    }
  }

  /**
   * Computes the hash, or the inline encoding for nodes shorter than 32 bytes, if not yet known.
   */
  final void ensureRef() {
    if (inlineEncoding != null || hash != null) {
      return;
    }
    final Bytes encoding = computeEncoding();
    if (encoding.size() < 32) {
      inlineEncoding = encoding;
    } else {
      hash = keccak256(encoding);
    }
  }

  @Override
  public final Bytes32 hash() {
    final Bytes32 known = hash;
    if (known != null) {
      return known;
    }
    ensureRef();
    final Bytes32 computed = hash;
    // Inline nodes only need a hash as a root, so it is not memoized for them
    return computed != null ? computed : keccak256(inlineEncoding);
  }

  @Override
  public final boolean isReferencedByHash() {
    ensureRef();
    return inlineEncoding == null;
  }

  @Override
  final void writeRef(final BytesValueRLPOutput out) {
    ensureRef();
    final Bytes inline = inlineEncoding;
    if (inline != null) {
      out.writeRaw(inline);
    } else {
      out.writeBytes(hash);
    }
  }

  /**
   * Returns the RLP encoding of this node. Only the encoding of inline nodes is kept; others are
   * rebuilt from their children's references, which needs no hashing.
   *
   * @return the RLP encoding
   */
  public final Bytes encoded() {
    final Bytes inline = inlineEncoding;
    return inline != null ? inline : computeEncoding();
  }

  /**
   * Whether the given session created this node, i.e. whether it is dirty for that session.
   *
   * @param sessionId the session id
   * @return true if the node was created by the session
   */
  final boolean isCreatedBy(final int sessionId) {
    return createdBy == sessionId;
  }

  final boolean isLoaded() {
    return createdBy == LOADED;
  }

  final void touch(final int stamp) {
    if (lastAccess < stamp) {
      lastAccess = stamp;
    }
  }
}
