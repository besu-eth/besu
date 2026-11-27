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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.trielog;

import org.hyperledger.besu.datatypes.Hash;

/**
 * A trie log needed to roll a Bonsai world state to a target block is absent. Unlike a trie fault,
 * this means the state cannot be reconstructed, not that stored state is corrupt: the logs were
 * pruned or were never stored (for example on a node upgraded or snap-synced without the historical
 * trie logs). Callers treat it as the state being unavailable.
 */
public class MissingTrieLogException extends RuntimeException {

  private final Hash blockHash;

  /**
   * Constructs a new MissingTrieLogException.
   *
   * @param blockHash the block hash whose trie log is missing
   */
  public MissingTrieLogException(final Hash blockHash) {
    super("Missing trie log for block hash " + blockHash);
    this.blockHash = blockHash;
  }

  /**
   * The block hash whose trie log is missing.
   *
   * @return the block hash
   */
  public Hash getBlockHash() {
    return blockHash;
  }
}
