/*
 * Copyright contributors to Hyperledger Besu.
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
package org.hyperledger.besu.plugin.services.worldstate;

import org.hyperledger.besu.evm.worldstate.MutableWorldView;
import org.hyperledger.besu.evm.worldstate.WorldState;
import org.hyperledger.besu.plugin.data.BlockHeader;

/**
 * Represents a mutable view of the Ethereum world state, allowing queries and modifications to
 * account balances, nonces, code, and storage.
 *
 * <p>Implementations manage the state root hash and persist changes to underlying storage through
 * {@link StateRootCommitter}.
 *
 * <p>Extends both {@link org.hyperledger.besu.evm.worldstate.WorldState} and {@link
 * org.hyperledger.besu.evm.worldstate.MutableWorldView}.
 */
public interface MutableWorldState extends WorldState, MutableWorldView {

  /**
   * Persist accumulated changes to underlying storage.
   *
   * @param blockHeader If persisting for an imported block, the block hash of the world state this
   *     represents. If this does not represent a forward transition from one block to the next
   *     `null` should be passed in.
   * @param committer An implementation of {@link StateRootCommitter} responsible for recomputing
   *     the state root and committing the state changes to storage.
   */
  void persist(BlockHeader blockHeader, StateRootCommitter committer);

  /**
   * Persist accumulated changes to underlying storage.
   *
   * @param blockHeader If persisting for an imported block, the block hash of the world state this
   *     represents. If this does not represent a forward transition from one block to the next
   *     `null` should be passed in.
   */
  default void persist(final BlockHeader blockHeader) {
    persist(blockHeader, StateRootCommitter.SYNCHRONOUS);
  }

  /**
   * Freezes the storage by preventing any further changes to it
   *
   * @return this instance
   */
  default MutableWorldState freezeStorage() {
    // no op
    throw new UnsupportedOperationException("cannot freeze");
  }

  /**
   * Disables the trie structure used for storage indexing.
   *
   * @return this instance
   */
  default MutableWorldState disableTrie() {
    // no op
    throw new UnsupportedOperationException("cannot disable trie");
  }
}
