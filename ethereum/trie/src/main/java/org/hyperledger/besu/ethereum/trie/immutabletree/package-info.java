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

/**
 * Immutable, persistent Merkle Patricia trees and a cache of them shared across blocks.
 *
 * <ul>
 *   <li>{@link org.hyperledger.besu.ethereum.trie.immutabletree.TreeNode} and its subclasses are
 *       the nodes. Updates never modify a node: they build new nodes on the updated paths and share
 *       everything else, so every root stays valid.
 *   <li>{@link org.hyperledger.besu.ethereum.trie.immutabletree.StoredTreeNode} stands for a node
 *       that is not loaded (location and hash). Opening a root loads its root node only; the rest
 *       is loaded as traversals touch it.
 *   <li>{@link org.hyperledger.besu.ethereum.trie.immutabletree.TreeSession} carries the loader and
 *       identifies the nodes an update creates, which are the ones its commit writes.
 *   <li>{@link org.hyperledger.besu.ethereum.trie.immutabletree.ImmutableTreeCache} registers roots
 *       by hash and kind, binds the head and new payload roots, and prunes.
 *   <li>{@link org.hyperledger.besu.ethereum.trie.immutabletree.ImmutableTreeMerkleTrie} exposes a
 *       tree as a {@code MerkleTrie} for state root computations.
 * </ul>
 *
 * <p>Roots, encodings and stored nodes are the same as those of Besu's Patricia tries; see {@code
 * docs/trie/immutable-tree-cache.md}.
 */
package org.hyperledger.besu.ethereum.trie.immutabletree;
