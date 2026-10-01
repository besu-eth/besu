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

import org.hyperledger.besu.ethereum.trie.MerkleTrie;
import org.hyperledger.besu.ethereum.trie.Node;
import org.hyperledger.besu.ethereum.trie.NodeLoader;
import org.hyperledger.besu.ethereum.trie.NodeUpdater;
import org.hyperledger.besu.ethereum.trie.patricia.StoredMerklePatriciaTrie;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/**
 * Trie node storage keyed by location, holding only the latest node per location and validating
 * hashes on read, like Bonsai's trie branch storage. A reader asking for a node that has since been
 * overwritten at its location gets nothing, so tests catch any read inconsistent with the root.
 */
final class PathBasedNodeStore implements NodeLoader, NodeUpdater {

  private final Map<Bytes, Bytes> nodes = new HashMap<>();
  private final AtomicInteger reads = new AtomicInteger();

  PathBasedNodeStore copy() {
    final PathBasedNodeStore copy = new PathBasedNodeStore();
    copy.nodes.putAll(nodes);
    return copy;
  }

  @Override
  public synchronized Optional<Bytes> getNode(final Bytes location, final Bytes32 hash) {
    reads.incrementAndGet();
    if (hash.equals(MerkleTrie.EMPTY_TRIE_NODE_HASH)) {
      return Optional.of(MerkleTrie.EMPTY_TRIE_NODE);
    }
    return Optional.ofNullable(nodes.get(location)).filter(rlp -> keccak256(rlp).equals(hash));
  }

  @Override
  public synchronized void store(final Bytes location, final Bytes32 hash, final Bytes value) {
    // Bonsai skips the empty root
    if (!hash.equals(MerkleTrie.EMPTY_TRIE_NODE_HASH)) {
      nodes.put(location.copy(), value);
    }
  }

  int reads() {
    return reads.get();
  }

  void resetReads() {
    reads.set(0);
  }

  StoredMerklePatriciaTrie<Bytes, Bytes> besuTrie(final Bytes32 root) {
    return new StoredMerklePatriciaTrie<>(this, root, Function.identity(), Function.identity());
  }

  /** Location to encoding of every node reachable from {@code root}, read through Besu's trie. */
  Map<Bytes, Bytes> reachableNodes(final Bytes32 root) {
    final Map<Bytes, Bytes> reachable = new HashMap<>();
    if (root.equals(MerkleTrie.EMPTY_TRIE_NODE_HASH)) {
      return reachable;
    }
    besuTrie(root)
        .visitAll(
            (Node<Bytes> node) -> {
              if (node.isReferencedByHash() || node.getLocation().orElseThrow().isEmpty()) {
                reachable.put(node.getLocation().orElseThrow(), node.getEncodedBytes());
              }
            });
    return reachable;
  }
}
