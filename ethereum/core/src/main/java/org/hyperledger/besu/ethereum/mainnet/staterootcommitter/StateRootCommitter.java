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
package org.hyperledger.besu.ethereum.mainnet.staterootcommitter;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.MutableWorldState;
import org.hyperledger.besu.plugin.data.BlockHeader;
import org.hyperledger.besu.plugin.services.metrics.OperationTimer;
import org.hyperledger.besu.plugin.services.storage.WorldStateKeyValueStorage;

import java.util.function.Supplier;

/**
 * Strategy for computing the state root hash during block persistence.
 *
 * <p>The caller provides a {@code stateRootSupplier} that encapsulates the standard (synchronous)
 * trie computation. Implementations may:
 *
 * <ul>
 *   <li>Simply invoke the supplier (sync mode)
 *   <li>Ignore it and return a pre-computed root (BAL trusted mode)
 *   <li>Invoke it, then cross-check against an independently computed root (BAL verification mode)
 * </ul>
 */
public interface StateRootCommitter {

  /** Computes the state root synchronously via the standard trie path. */
  StateRootCommitter SYNCHRONOUS =
      (supplier, worldState, stateUpdater, blockHeader) -> supplier.get();

  /**
   * Compute (or retrieve) the state root and apply any additional side-effects.
   *
   * @param stateRootSupplier lazily computes the state root via the standard trie path
   * @param worldState the world state being persisted (used by BAL to import state changes)
   * @param stateUpdater the storage updater (used by BAL to merge trie nodes)
   * @param blockHeader the block being persisted
   * @return the authoritative state root hash
   */
  Hash computeRoot(
      Supplier<Hash> stateRootSupplier,
      MutableWorldState worldState,
      WorldStateKeyValueStorage.Updater stateUpdater,
      BlockHeader blockHeader);

  default void cancel() {}

  default StateRootCommitter timed(final OperationTimer timer) {
    final StateRootCommitter delegate = this;
    return new StateRootCommitter() {
      @Override
      public Hash computeRoot(
          final Supplier<Hash> stateRootSupplier,
          final MutableWorldState worldState,
          final WorldStateKeyValueStorage.Updater stateUpdater,
          final BlockHeader blockHeader) {
        try (var ignored = timer.startTimer()) {
          return delegate.computeRoot(stateRootSupplier, worldState, stateUpdater, blockHeader);
        }
      }

      @Override
      public void cancel() {
        delegate.cancel();
      }
    };
  }
}
