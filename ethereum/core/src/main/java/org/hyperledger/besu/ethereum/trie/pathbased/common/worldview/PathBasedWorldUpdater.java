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
package org.hyperledger.besu.ethereum.trie.pathbased.common.worldview;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.plugin.services.trielogs.TrieLogAccumulator;

import java.util.Set;

/**
 * Unified interface for path-based world state updaters.
 *
 * <p>Implementations include the traditional {@code PathBasedWorldStateUpdateAccumulator} (used for
 * standard sequential block processing) and the BAL-backed {@code BalWorldUpdater} (used for
 * parallel BAL-based execution). Code that drives EVM execution, root computation, trie-log
 * generation, and state integration should depend on this interface rather than on any specific
 * implementation so that both strategies are interchangeable.
 */
public interface PathBasedWorldUpdater extends WorldUpdater, TrieLogAccumulator, PathBasedWorldView {

  /**
   * Returns the set of account addresses whose storage must be fully cleared because those accounts
   * were self-destructed during execution.
   *
   * @return set of addresses whose storage should be cleared
   */
  Set<Address> getStorageToClear();

  /**
   * Resets this updater to a clean state, discarding all tracked mutations. Called after
   * {@code persist()} completes (successfully or not) to prepare the updater for the next block.
   *
   * <p>The default implementation is a no-op for read-only/BAL-backed updaters that do not
   * maintain mutable state.
   */
  default void reset() {}

  /**
   * Returns {@code true} if any state change has been accumulated since the last call to {@link
   * #resetAccumulatorStateChanged()}.
   *
   * <p>Used by {@code PathBasedWorldState.rootHash()} to decide whether the cached root hash is
   * still valid. The default returns {@code false}, which is correct for read-only updaters that
   * never drive root computation.
   */
  default boolean isAccumulatorStateChanged() {
    return false;
  }

  /**
   * Resets the "state changed" flag so that subsequent calls to {@link #isAccumulatorStateChanged()}
   * return {@code false} until the next mutation is recorded.
   *
   * <p>The default is a no-op for read-only/BAL-backed updaters.
   */
  default void resetAccumulatorStateChanged() {}

  /**
   * Returns a deep copy of this updater. The copy is independent: mutations to the copy do not
   * affect the original and vice versa.
   *
   * <p>Used by {@code PathBasedWorldState.rootHash()} to compute a "dry-run" root without
   * disturbing the live updater state.
   *
   * @return a deep copy of this updater
   */
  PathBasedWorldUpdater copy();
}
