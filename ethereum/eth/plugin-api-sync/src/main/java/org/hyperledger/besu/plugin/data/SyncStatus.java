/*
 * Copyright ConsenSys AG.
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
package org.hyperledger.besu.plugin.data;

import org.hyperledger.besu.plugin.Unstable;

import java.util.Optional;

/** The interface Sync status. */
public interface SyncStatus {

  /**
   * Get the height of the block at which this synchronization attempt began.
   *
   * @return height of the block at which this synchronization attempt began.
   */
  long getStartingBlock();

  /**
   * Get the height of the last block the synchronizer received
   *
   * @return the height of the last block the synchronizer received
   */
  long getCurrentBlock();

  /**
   * Get the height of the highest known block.
   *
   * @return the height of the highest known block.
   */
  long getHighestBlock();

  /**
   * PulledStates is the number of state entries fetched so far, or empty if this is not known or
   * not relevant.
   *
   * @return count of pulled states
   */
  @Unstable
  default Optional<Long> getPulledStates() {
    return Optional.empty();
  }

  /**
   * KnownStates is the number of states the node knows of so far, or empty if this is not known or
   * not relevant.
   *
   * @return count of known states
   */
  @Unstable
  default Optional<Long> getKnownStates() {
    return Optional.empty();
  }
}
