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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.trienode;

import org.hyperledger.besu.datatypes.Hash;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/**
 * Key layout of Bonsai trie nodes.
 *
 * <p>A trie node is keyed by its location (nibble path) in the world state trie, or by the 32-byte
 * hash of the owning account followed by its location for a node of an account storage trie. World
 * state trie locations can never reach {@value #TRIE_PREFIX_SIZE} nibbles (it would take a 124-bit
 * hash prefix collision), so a key shorter than {@value #TRIE_PREFIX_SIZE} bytes is always a world
 * state trie location and a longer one always carries a trie prefix.
 */
public final class TrieNodeKey {

  public static final int TRIE_PREFIX_SIZE = Bytes32.SIZE;

  private TrieNodeKey() {}

  /**
   * Key of the node at {@code location} in the storage trie of {@code accountHash}.
   *
   * @param accountHash the hash of the account owning the storage trie
   * @param location the location of the node in that storage trie
   * @return the trie node key
   */
  public static Bytes of(final Hash accountHash, final Bytes location) {
    return Bytes.concatenate(accountHash.getBytes(), location);
  }

  /**
   * Returns whether the key carries a trie prefix, i.e. whether the node belongs to a storage trie.
   *
   * @param key a trie node key
   * @return true if the key carries a trie prefix
   */
  public static boolean hasTriePrefix(final Bytes key) {
    return key.size() >= TRIE_PREFIX_SIZE;
  }

  /**
   * Returns the trie prefix of a key that carries one.
   *
   * @param key a trie node key carrying a trie prefix
   * @return the trie prefix of the key
   */
  public static Bytes triePrefix(final Bytes key) {
    return key.slice(0, TRIE_PREFIX_SIZE);
  }

  /**
   * Returns the location of the node within its trie, i.e. the key without its trie prefix.
   *
   * @param key a trie node key
   * @return the location of the node within its trie
   */
  public static Bytes location(final Bytes key) {
    return hasTriePrefix(key) ? key.slice(TRIE_PREFIX_SIZE) : key;
  }
}
