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

import org.hyperledger.besu.ethereum.trie.MerkleTrie;
import org.hyperledger.besu.ethereum.trie.MerkleTrieException;
import org.hyperledger.besu.ethereum.trie.NodeLoader;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/**
 * The context an operation on a tree runs in: where missing nodes are loaded from, which nodes are
 * dirty, and the block stamp that marks visited nodes as recently used.
 *
 * <p>The loader must be consistent with the root the session works on, which is the same contract
 * as a {@code StoredMerklePatriciaTrie}. Nodes never capture it: a placeholder is resolved with the
 * loader of whichever session reaches it.
 *
 * <p>A node is dirty for a session if and only if the session created it. Committing a session
 * therefore writes exactly the nodes the session's updates produced.
 */
public final class TreeSession {

  private static final AtomicInteger IDS = new AtomicInteger();

  private final int id;
  private final NodeLoader loader;
  private final int stamp;
  private final LongAdder loads;

  private TreeSession(final NodeLoader loader, final int stamp, final LongAdder loads) {
    this.id = nextId();
    this.loader = loader;
    this.stamp = stamp;
    this.loads = loads;
  }

  /**
   * Creates a session.
   *
   * @param loader loads stored nodes by location and hash, consistent with the session's root
   * @param stamp the block stamp recorded on visited nodes
   * @return a session with a fresh id
   */
  public static TreeSession create(final NodeLoader loader, final int stamp) {
    return new TreeSession(loader, stamp, new LongAdder());
  }

  private static int nextId() {
    int id;
    do {
      id = IDS.incrementAndGet();
    } while (id == MaterializedTreeNode.LOADED);
    return id;
  }

  /**
   * Returns a session with the same loader and a fresh id, so the nodes created so far are no
   * longer dirty. Used once they have been committed.
   *
   * @return the renewed session
   */
  public TreeSession renew() {
    return new TreeSession(loader, stamp, loads);
  }

  int id() {
    return id;
  }

  int stamp() {
    return stamp;
  }

  /**
   * Returns how many nodes this session and its renewals loaded from the loader.
   *
   * @return the number of loads
   */
  public long loadCount() {
    return loads.sum();
  }

  /**
   * Loads the root node of a tree. The root's children stay stored until they are touched.
   *
   * @param rootHash the root hash
   * @return the root node
   */
  public TreeNode loadRoot(final Bytes32 rootHash) {
    if (rootHash.equals(MerkleTrie.EMPTY_TRIE_NODE_HASH)) {
      return EmptyTreeNode.INSTANCE;
    }
    return load(Bytes.EMPTY, rootHash);
  }

  TreeNode load(final StoredTreeNode stored) {
    return load(stored.location(), stored.hash());
  }

  private TreeNode load(final Bytes location, final Bytes32 hash) {
    final Bytes rlp =
        loader
            .getNode(location, hash)
            .orElseThrow(
                () ->
                    new MerkleTrieException(
                        "Unable to load trie node value for hash " + hash + " location " + location,
                        hash,
                        location));
    loads.increment();
    return TreeNodeCodec.decode(location, hash, rlp, stamp);
  }
}
