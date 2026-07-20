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

import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.plugin.data.BlockHeader;
import org.hyperledger.besu.plugin.services.metrics.OperationTimer;

/**
 * Computes the state root when a block's world state is persisted.
 *
 * <p>Implementations may compute synchronously from accumulated updates or delegate to background
 * computation (for example BAL).
 */
public interface StateRootCommitter {

  /**
   * Compute the state root and any deferred storage writes for the current world state.
   *
   * @param worldState the world state being persisted
   * @param blockHeader the block being persisted
   * @param worldUpdater the world state updater with accumulated changes
   * @return the computation result
   */
  StateRootComputation compute(
      MutableWorldState worldState, BlockHeader blockHeader, WorldUpdater worldUpdater);

  /** Cancel any background computation started by this committer (no-op by default). */
  default void cancel() {}

  /**
   * Wraps this committer with timing instrumentation.
   *
   * @param timer the timer used to measure execution time
   * @return a new committer that delegates to this instance while recording timing metrics
   */
  default StateRootCommitter timed(final OperationTimer timer) {
    final StateRootCommitter delegate = this;
    return (worldState, blockHeader, accumulator) -> {
      try (var ignored = timer.startTimer()) {
        return delegate.compute(worldState, blockHeader, accumulator);
      }
    };
  }
}
